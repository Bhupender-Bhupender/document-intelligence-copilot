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
def _is_ocr_candidate(
    page: ParsedPage,
    *,
    recover_weak: bool = False,
) -> bool:
    """Return True when a page should be sent through OCR."""

    if page.extraction_status == "empty":
        return True

    if (
        recover_weak
        and page.extraction_status == "weak"
    ):
        return True

    return False


def _is_better_recovery(
    original: ParsedPage,
    recovered: ParsedPage,
) -> bool:
    """Accept OCR only when it improves extracted text volume."""

    original_words = int(
        getattr(original, "word_count", 0) or 0
    )

    recovered_words = int(
        getattr(recovered, "word_count", 0) or 0
    )

    return recovered_words > original_words

def route_pdf_pages_through_ocr(
    file_path: Path,
    raw_document: RawDocument,
    pages: List[ParsedPage],
    *,
    recover_weak: bool = False,
) -> List[ParsedPage]:
    """
    Attempt selective OCR recovery for PDF pages.

    Default behaviour is backward compatible:
    only pages classified as ``empty`` are OCR candidates.

    When ``recover_weak=True``, pages classified as ``weak`` are also
    candidates. An OCR result replaces the native extraction only when
    its extracted word count is greater than the original page.
    """

    candidate_indices = [
        i
        for i, page in enumerate(pages)
        if _is_ocr_candidate(
            page,
            recover_weak=recover_weak,
        )
    ]

    if not candidate_indices:
        logger.debug(
            "ocr_router: no OCR candidates",
            file=file_path.name,
            total_pages=len(pages),
            recover_weak=recover_weak,
        )
        return pages

    # --------------------------------------------------------------
    # Azure Document Intelligence
    # --------------------------------------------------------------

    if config.ocr_backend == "azure_di":

        logger.info(
            "ocr_router: routing candidate pages through Azure DI",
            file=file_path.name,
            candidate_page_count=len(
                candidate_indices
            ),
            total_pages=len(pages),
            recover_weak=recover_weak,
        )

        candidate_pages = [
            pages[i]
            for i in candidate_indices
        ]

        try:
            recovered_pages = _run_azure_di_ocr(
                config.azure_di_endpoint,
                file_path,
                candidate_pages,
            )

        except Exception as exc:
            logger.warning(
                "ocr_router: Azure DI recovery failed",
                file=file_path.name,
                error=str(exc),
            )
            return pages

        result = list(pages)

        accepted = 0
        resolved = 0

        for idx, recovered_page in zip(
            candidate_indices,
            recovered_pages,
        ):
            original = pages[idx]

            if not _is_better_recovery(
                original,
                recovered_page,
            ):
                continue

            result[idx] = recovered_page
            accepted += 1

            if recovered_page.extraction_status not in {
                "empty",
                "weak",
            }:
                resolved += 1

        logger.info(
            "ocr_router: Azure DI OCR complete",
            file=file_path.name,
            routed=len(candidate_indices),
            accepted=accepted,
            resolved=resolved,
        )

        return result

    # --------------------------------------------------------------
    # Local Docling / RapidOCR
    # --------------------------------------------------------------

    logger.info(
        "ocr_router: routing candidate pages through Docling/RapidOCR",
        file=file_path.name,
        candidate_page_count=len(
            candidate_indices
        ),
        total_pages=len(pages),
        recover_weak=recover_weak,
    )

    # Docling processes the PDF once. We only use its result for the
    # pages identified as OCR candidates.
    try:
        _, docling_pages = _run_local_ocr(
            file_path
        )

    except Exception as exc:
        logger.warning(
            "ocr_router: Docling conversion failed",
            file=file_path.name,
            error=str(exc),
        )
        return pages

    docling_by_page: dict[
        int,
        ParsedPage,
    ] = {
        page.page_number: page
        for page in docling_pages
    }

    result = list(pages)

    accepted = 0
    resolved = 0

    for idx in candidate_indices:

        original = pages[idx]

        docling_page = docling_by_page.get(
            original.page_number
        )

        if docling_page is None:
            logger.debug(
                "ocr_router: no Docling result for candidate page",
                file=file_path.name,
                page_number=original.page_number,
            )
            continue

        norm = normalize_text(
            docling_page.raw_text
        )

        recovered_status = (
            classify_extraction_status(
                norm
            )
        )

        recovered_page = ParsedPage(
            # Preserve identity
            page_id=original.page_id,
            doc_id=original.doc_id,
            page_number=original.page_number,

            # Recovered content
            raw_text=clean_text(
                docling_page.raw_text
            ),
            normalized_text=norm,
            word_count=(
                len(norm.split())
                if norm.strip()
                else 0
            ),
            char_count=len(norm),

            # OCR provenance
            parse_method="rapidocr",
            extraction_status=recovered_status,
            ocr_engine="rapidocr",
            ocr_confidence=None,

            # Layout enrichment
            section_title=(
                docling_page.section_title
            ),
            layout_blocks=(
                docling_page.layout_blocks
            ),
        )

        # Never replace useful native text with a worse OCR result.
        if not _is_better_recovery(
            original,
            recovered_page,
        ):
            logger.debug(
                "ocr_router: OCR result not better — preserving native page",
                file=file_path.name,
                page_number=original.page_number,
                native_words=original.word_count,
                ocr_words=recovered_page.word_count,
            )
            continue

        result[idx] = recovered_page
        accepted += 1

        if recovered_status not in {
            "empty",
            "weak",
        }:
            resolved += 1

        logger.debug(
            "ocr_router: OCR recovery accepted",
            file=file_path.name,
            page_number=original.page_number,
            original_words=original.word_count,
            recovered_words=(
                recovered_page.word_count
            ),
            recovered_status=(
                recovered_status
            ),
        )

    logger.info(
        "ocr_router: OCR pass complete",
        file=file_path.name,
        routed=len(candidate_indices),
        accepted=accepted,
        resolved=resolved,
        remaining=(
            len(candidate_indices)
            - resolved
        ),
    )

    return result
   
