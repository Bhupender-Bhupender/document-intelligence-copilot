"""
Docling-backed parser for layout-aware document extraction.

Public API
----------
    parse_with_docling(file_path: Path) -> tuple[RawDocument, list[ParsedPage]]

IMPORTANT — Windows DLL ordering
---------------------------------
    torch must be imported before any docling import in this process.
    This module does it unconditionally at import time. Do not re-order imports.

Supported formats
-----------------
    .pdf  .docx  .doc  .pptx  .xlsx  .html  .htm  .md

    The ingestion router exposes a subset of these. The parser accepts all of
    them so the gap can be closed incrementally without touching this module.

Page model
----------
    Paged documents (PDF):
        One ParsedPage per Docling page. Page text is exported with
        doc.export_to_text(page_no=N) so each page is scoped independently.

    Flat documents (markdown, docx without hard page breaks):
        doc.pages is empty. The full document is returned as a single
        ParsedPage with page_number=1.

Layout blocks
-------------
    Built from doc.iterate_items() which yields (DocItem, level) pairs.
    Each item's label is mapped to ParsedBlock.block_type.
    BoundingBox coordinates use the source coordinate system (BOTTOMLEFT for
    PDFs); stored as [x0, y0, x1, y1] floats, or None when unavailable.

Extraction status
-----------------
    Delegated to classify_extraction_status() from src.utils.text_utils:
        "ok"    — word_count >= 20
        "weak"  — 1 <= word_count < 20
        "empty" — word_count == 0
    Consistent with existing pypdf reader thresholds.
"""
from __future__ import annotations

# ── Windows DLL ordering fix ──────────────────────────────────────────────
# torch must be fully initialized before transformers loads c10.dll.
# Keep this as the first non-stdlib / non-future import. Do not move it.
import torch  # noqa: F401
# ─────────────────────────────────────────────────────────────────────────

import hashlib
from pathlib import Path
from typing import List, Optional, Tuple

from docling.document_converter import DocumentConverter
from docling_core.types.doc.labels import DocItemLabel

from src.schema.models import ParsedBlock, ParsedPage, RawDocument
from src.utils.logging_utils import get_logger
from src.utils.text_utils import classify_extraction_status, clean_text, normalize_text

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Formats this parser handles.
# The router exposes a subset; the parser accepts all of them.
# ---------------------------------------------------------------------------
_SUPPORTED_FORMATS: frozenset[str] = frozenset(
    {".pdf", ".docx", ".doc", ".pptx", ".xlsx", ".html", ".htm", ".md"}
)

# ---------------------------------------------------------------------------
# DocItemLabel → ParsedBlock.block_type
# ---------------------------------------------------------------------------
_HEADING_LABELS: frozenset[DocItemLabel] = frozenset(
    {DocItemLabel.TITLE, DocItemLabel.SECTION_HEADER}
)

_LABEL_MAP: dict[DocItemLabel, str] = {
    DocItemLabel.TITLE: "heading",
    DocItemLabel.SECTION_HEADER: "heading",
    DocItemLabel.TEXT: "paragraph",
    DocItemLabel.PARAGRAPH: "paragraph",
    DocItemLabel.LIST_ITEM: "list",
    DocItemLabel.TABLE: "table",
    DocItemLabel.CAPTION: "caption",
}

# ---------------------------------------------------------------------------
# Lazy converter singleton — avoids reloading 770 layout weights per call.
# ---------------------------------------------------------------------------
_converter: Optional[DocumentConverter] = None


def _get_converter() -> DocumentConverter:
    """Return the shared DocumentConverter, initializing on first call."""
    global _converter
    if _converter is None:
        logger.info("docling_parser: initializing DocumentConverter (first call)")
        _converter = DocumentConverter()
    return _converter


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _bbox_to_list(bbox) -> Optional[List[float]]:
    """
    Convert a Docling BoundingBox to [x0, y0, x1, y1] or None.

    Docling uses BOTTOMLEFT origin for PDFs. l=left, b=bottom, r=right, t=top.
    We store as [l, b, r, t] which maps to [x0, y0, x1, y1] in that system.
    """
    if bbox is None:
        return None
    try:
        return [float(bbox.l), float(bbox.b), float(bbox.r), float(bbox.t)]
    except Exception:
        return None


def _items_for_page(doc, page_no: Optional[int]):
    """
    Yield (item, level) from doc.iterate_items() scoped to a single page.

    page_no=None means all items (used for flat/single-page documents where
    elements carry no page provenance).
    """
    for item, level in doc.iterate_items():
        prov = getattr(item, "prov", [])
        if page_no is None:
            yield item, level
        elif prov and prov[0].page_no == page_no:
            yield item, level


def _build_blocks(
    doc,
    doc_id: str,
    page_no: Optional[int],
    page_number: int,
) -> Tuple[List[ParsedBlock], Optional[str]]:
    """
    Build ParsedBlock list and determine section_title for a page.

    Returns (blocks, page_section_title) where page_section_title is the
    text of the first heading-labelled item encountered on this page.

    ParsedBlock.section_title is set to the most recent heading seen before
    (or at) each block — the running section context.
    """
    blocks: List[ParsedBlock] = []
    page_section_title: Optional[str] = None  # first heading on the page
    current_section: Optional[str] = None  # running heading context

    for item, _level in _items_for_page(doc, page_no):
        label: Optional[DocItemLabel] = getattr(item, "label", None)
        if label is None:
            continue

        text_raw: str = getattr(item, "text", None) or ""
        prov = getattr(item, "prov", [])
        bbox = _bbox_to_list(prov[0].bbox if prov else None)
        block_type: str = _LABEL_MAP.get(label, "unknown")

        # Track running section heading.
        if label in _HEADING_LABELS and text_raw.strip():
            current_section = text_raw.strip()
            if page_section_title is None:
                page_section_title = current_section

        blocks.append(
            ParsedBlock(
                doc_id=doc_id,
                page_number=page_number,
                block_type=block_type,
                text=clean_text(text_raw),
                reading_order=len(blocks),
                bounding_box=bbox,
                section_title=current_section,
            )
        )

    return blocks, page_section_title


def _make_parsed_page(
    doc,
    doc_id: str,
    page_number: int,
    page_no_key: Optional[int],
    is_flat: bool,
) -> ParsedPage:
    """
    Build a single ParsedPage from a DoclingDocument for the given page.

    Args:
        doc:         The DoclingDocument from a convert() result.
        doc_id:      doc_id from the parent RawDocument.
        page_number: 1-indexed sequential page number in our output.
        page_no_key: Docling's integer page key (used for paged docs).
                     None for flat documents (markdown, docx).
        is_flat:     True when doc.pages is empty (flat/single-page doc).
    """
    if is_flat:
        raw = doc.export_to_text()
    else:
        raw = doc.export_to_text(page_no=page_no_key)

    raw_text = clean_text(raw)
    norm_text = normalize_text(raw)
    status = classify_extraction_status(norm_text)
    word_count = len(norm_text.split()) if norm_text.strip() else 0
    char_count = len(norm_text)

    scope_page_no = None if is_flat else page_no_key
    blocks, section_title = _build_blocks(doc, doc_id, scope_page_no, page_number)

    return ParsedPage(
        doc_id=doc_id,
        page_number=page_number,
        raw_text=raw_text,
        normalized_text=norm_text,
        word_count=word_count,
        char_count=char_count,
        parse_method="docling",
        extraction_status=status,
        section_title=section_title,
        layout_blocks=blocks,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_with_docling(file_path: Path) -> Tuple[RawDocument, List[ParsedPage]]:
    """
    Parse a document with Docling and return canonical pipeline models.

    Supported formats: .pdf, .docx, .doc, .pptx, .xlsx, .html, .htm, .md

    Per-page failure handling: if Docling raises during individual page
    extraction, that page is recorded with extraction_status="empty" and
    processing continues. The full conversion failure case (converter crash)
    also produces an "empty" page rather than propagating the exception,
    so callers can always inspect extraction_status to detect problems.

    Args:
        file_path: Path to the source document.

    Returns:
        Tuple of (RawDocument, list[ParsedPage]) where:
            - RawDocument.total_pages is set to the count of returned pages.
            - ParsedPage.parse_method is always "docling".
            - ParsedPage.extraction_status reflects text quality.

    Raises:
        FileNotFoundError: If file_path does not exist.
        ValueError: If the file format is not supported by this parser.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix not in _SUPPORTED_FORMATS:
        raise ValueError(
            f"docling_parser does not support format {suffix!r}. "
            f"Supported formats: {sorted(_SUPPORTED_FORMATS)}"
        )

    file_bytes = file_path.read_bytes()
    byte_size = len(file_bytes)
    checksum = hashlib.sha256(file_bytes).hexdigest()

    doc_record = RawDocument(
        source_path=str(file_path.resolve()),
        file_name=file_path.name,
        file_type=suffix.lstrip("."),
        byte_size=byte_size,
        checksum=checksum,
    )

    logger.info(
        "docling_parser: converting",
        file=file_path.name,
        suffix=suffix,
        byte_size=byte_size,
    )

    try:
        result = _get_converter().convert(str(file_path))
        docling_doc = result.document
    except Exception as exc:
        logger.error(
            "docling_parser: conversion failed",
            file=file_path.name,
            error=str(exc),
        )
        # Conversion failed entirely: return a single empty page so callers
        # can detect the failure via extraction_status without crashing.
        empty_page = ParsedPage(
            doc_id=doc_record.doc_id,
            page_number=1,
            raw_text="",
            normalized_text="",
            word_count=0,
            char_count=0,
            parse_method="docling",
            extraction_status="empty",
        )
        doc_record.total_pages = 1
        return doc_record, [empty_page]

    # Determine paging strategy: PDFs have entries in doc.pages; flat formats do not.
    page_keys: List[int] = sorted(docling_doc.pages.keys()) if docling_doc.pages else []
    is_flat: bool = len(page_keys) == 0

    pages: List[ParsedPage] = []

    if is_flat:
        try:
            page = _make_parsed_page(docling_doc, doc_record.doc_id, 1, None, is_flat=True)
        except Exception as exc:
            logger.warning(
                "docling_parser: page extraction failed (flat doc)",
                file=file_path.name,
                error=str(exc),
            )
            page = ParsedPage(
                doc_id=doc_record.doc_id,
                page_number=1,
                raw_text="",
                normalized_text="",
                word_count=0,
                char_count=0,
                parse_method="docling",
                extraction_status="empty",
            )
        pages.append(page)
    else:
        for page_number, page_no_key in enumerate(page_keys, start=1):
            try:
                page = _make_parsed_page(
                    docling_doc,
                    doc_record.doc_id,
                    page_number,
                    page_no_key,
                    is_flat=False,
                )
            except Exception as exc:
                logger.warning(
                    "docling_parser: page extraction failed",
                    file=file_path.name,
                    page=page_number,
                    error=str(exc),
                )
                page = ParsedPage(
                    doc_id=doc_record.doc_id,
                    page_number=page_number,
                    raw_text="",
                    normalized_text="",
                    word_count=0,
                    char_count=0,
                    parse_method="docling",
                    extraction_status="empty",
                )
            pages.append(page)

    doc_record.total_pages = len(pages)

    logger.info(
        "docling_parser: done",
        file=file_path.name,
        total_pages=len(pages),
        statuses=[p.extraction_status for p in pages],
    )

    return doc_record, pages
