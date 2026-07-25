"""
OCR routing layer for PDF pages that failed text extraction.

Public API
----------
    route_pdf_pages_through_ocr(
        file_path: Path,
        raw_document: RawDocument,
        pages: list[ParsedPage],
    ) -> list[ParsedPage]

Design
------
This module is a thin orchestration layer, not a parser.
It delegates OCR-backed extraction to parse_with_docling(), which internally
uses Docling's embedded RapidOCR engine for pages that carry no text layer.

Two-pass PDF pipeline:
    Pass 1 — pypdf (fast, born-digital text extraction)
    Pass 2 — this module, called only when Pass 1 produced empty pages

Trigger rule:
    A page is an OCR candidate if extraction_status == "empty".
    "weak" pages are deliberately excluded from this step. Weak pages
    have some extractable text; routing them through OCR introduces noise
    risk without a clear quality benefit. This can be reconsidered once
    we have sample scanned documents that trigger "weak" consistently.

Field update rules:
    When OCR recovery succeeds for a page (Docling produces non-empty text):
        updated:      raw_text, normalized_text, word_count, char_count,
                      parse_method, extraction_status, ocr_engine,
                      section_title (if Docling detected a heading),
                      layout_blocks (if Docling produced them)
        never changed: page_id, doc_id, page_number
        left as-is:   ocr_confidence
            Reason: Docling's public API does not expose a per-page mean
            confidence score from RapidOCR. Setting a synthetic value would
            be dishonest and mislead downstream quality checks. Remains None.

parse_method vs ocr_engine:
    parse_method="rapidocr" records WHICH extraction path produced the text
        (Docling + RapidOCR). This is the parser-path label, not an engine name.
    ocr_engine="rapidocr"   records the underlying OCR engine that was used.
    Both are "rapidocr" here because Docling's OCR path uses RapidOCR.
    When PaddleOCR is wired in a later step, parse_method="paddleocr" and
    ocr_engine="paddleocr" will distinguish it cleanly.
"""
from __future__ import annotations

from pathlib import Path
from typing import List

from src.core.config import config
from src.schema.models import ParsedPage, RawDocument
from src.utils.logging_utils import get_logger
from src.utils.text_utils import classify_extraction_status, clean_text, normalize_text

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Backend dispatch helpers                                                     #
# --------------------------------------------------------------------------- #
# Both helpers defer their heavy imports to call time.  ocr_router is         #
# therefore safe to import without triggering torch, Docling, or Azure SDK    #
# loading.  Monkeypatch these module-level names in tests instead of          #
# injecting into sys.modules.                                                 #
# --------------------------------------------------------------------------- #


def _run_local_ocr(file_path: Path):
    """Run Docling/RapidOCR on the full PDF (deferred import)."""
    from src.parsing.docling_parser import parse_with_docling  # noqa: PLC0415

    return parse_with_docling(file_path)


def _run_azure_di_ocr(
    endpoint: str,
    file_path: Path,
    empty_pages: List[ParsedPage],
) -> List[ParsedPage]:
    """Run Azure AI Document Intelligence on empty pages (deferred import)."""
    from src.ocr.azure_di_ocr import AzureDiOcrAdapter  # noqa: PLC0415

    adapter = AzureDiOcrAdapter(endpoint=endpoint)
    return adapter.recover_pages(file_path, empty_pages)


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #


def route_pdf_pages_through_ocr(
    file_path: Path,
    raw_document: RawDocument,
    pages: List[ParsedPage],
) -> List[ParsedPage]:
    """
    Attempt OCR recovery for PDF pages where pypdf extracted no text.

    Inspects the page list produced by Pass 1 (pypdf) and, for each page
    where extraction_status == "empty", routes to the configured OCR backend:

        ocr_backend="local"    — Docling/RapidOCR (default)
        ocr_backend="azure_di" — Azure AI Document Intelligence prebuilt-read

    Pages that are not candidates are returned unchanged.

    Args:
        file_path:    Path to the original PDF file.
        raw_document: RawDocument produced by the pypdf pass (provenance only).
        pages:        ParsedPage list from the pypdf pass (Pass 1 output).

    Returns:
        Updated list of ParsedPage records.  Same length, same order as input.
        Identity fields (page_id, doc_id, page_number) are never modified.
        Non-candidate pages are returned as-is (same object, no copy).

    Raises:
        Nothing.  All backend failures are caught and logged.
        A page that fails OCR recovery retains its original "empty" record.
    """
    # Identify candidate pages (0-indexed positions in the input list).
    empty_indices = [
        i for i, p in enumerate(pages) if p.extraction_status == "empty"
    ]

    if not empty_indices:
        logger.debug(
            "ocr_router: no empty pages — skipping OCR pass",
            file=file_path.name,
            total_pages=len(pages),
        )
        return pages

    # ------------------------------------------------------------------ #
    # Azure DI OCR backend                                                #
    # ------------------------------------------------------------------ #

    if config.ocr_backend == "azure_di":
        logger.info(
            "ocr_router: routing empty pages through Azure DI",
            file=file_path.name,
            empty_page_count=len(empty_indices),
            total_pages=len(pages),
        )
        empty_page_list = [pages[i] for i in empty_indices]
        try:
            recovered_pages = _run_azure_di_ocr(
                config.azure_di_endpoint, file_path, empty_page_list
            )
        except Exception as exc:
            logger.warning(
                "ocr_router: Azure DI recovery failed — OCR recovery skipped",
                file=file_path.name,
                error=str(exc),
            )
            return pages

        result = list(pages)
        recovered = 0
        for idx, updated in zip(empty_indices, recovered_pages):
            result[idx] = updated
            if updated.extraction_status != "empty":
                recovered += 1

        logger.info(
            "ocr_router: Azure DI OCR pass complete",
            file=file_path.name,
            routed=len(empty_indices),
            recovered=recovered,
            still_empty=len(empty_indices) - recovered,
        )
        return result

    # ------------------------------------------------------------------ #
    # Local (Docling / RapidOCR) backend — default                       #
    # ------------------------------------------------------------------ #

    logger.info(
        "ocr_router: routing empty pages through Docling/RapidOCR",
        file=file_path.name,
        empty_page_count=len(empty_indices),
        total_pages=len(pages),
    )

    # Run Docling on the full PDF once.  The DocumentConverter singleton means
    # weights are already warm if the parser was called earlier in this process.
    try:
        _, docling_pages = _run_local_ocr(file_path)
    except Exception as exc:
        logger.warning(
            "ocr_router: Docling conversion failed — OCR recovery skipped",
            file=file_path.name,
            error=str(exc),
        )
        return pages

    # Build a lookup keyed by page_number (1-indexed, matching pypdf convention).
    docling_by_page: dict[int, ParsedPage] = {
        p.page_number: p for p in docling_pages
    }

    # Work on a shallow copy of the list so we never mutate the caller's list.
    result = list(pages)
    recovered = 0

    for idx in empty_indices:
        original = pages[idx]
        page_no = original.page_number
        docling_page = docling_by_page.get(page_no)

        if docling_page is None:
            logger.debug(
                "ocr_router: Docling produced no page for this number — keeping empty",
                file=file_path.name,
                page_number=page_no,
            )
            continue

        # Re-classify using the Docling-extracted text to determine if recovery
        # actually produced usable content.
        norm = normalize_text(docling_page.raw_text)
        recovered_status = classify_extraction_status(norm)

        # Build the updated page, preserving identity fields exactly.
        updated = ParsedPage(
            # ── identity — never changed ─────────────────────────────────────
            page_id=original.page_id,
            doc_id=original.doc_id,
            page_number=original.page_number,
            # ── recovered text content ───────────────────────────────────────
            raw_text=clean_text(docling_page.raw_text),
            normalized_text=norm,
            word_count=len(norm.split()) if norm.strip() else 0,
            char_count=len(norm),
            # ── OCR metadata ─────────────────────────────────────────────────
            # parse_method records the extraction path; see module docstring.
            parse_method="rapidocr",
            extraction_status=recovered_status,
            ocr_engine="rapidocr",
            # ocr_confidence intentionally left None — RapidOCR confidence is
            # not exposed through Docling's public API. See module docstring.
            ocr_confidence=None,
            # ── layout enrichment (if Docling produced it) ───────────────────
            section_title=docling_page.section_title,
            layout_blocks=docling_page.layout_blocks,
        )

        result[idx] = updated

        if recovered_status != "empty":
            recovered += 1
            logger.debug(
                "ocr_router: page recovered",
                file=file_path.name,
                page_number=page_no,
                status=recovered_status,
                word_count=updated.word_count,
            )
        else:
            logger.debug(
                "ocr_router: Docling also produced empty text for page",
                file=file_path.name,
                page_number=page_no,
            )

    logger.info(
        "ocr_router: OCR pass complete",
        file=file_path.name,
        routed=len(empty_indices),
        recovered=recovered,
        still_empty=len(empty_indices) - recovered,
    )

    return result
