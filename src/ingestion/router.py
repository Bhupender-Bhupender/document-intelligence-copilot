"""
Ingestion router.

Accepts a file path, detects its format, and dispatches to the appropriate
reader or parser. Returns a (RawDocument, List[ParsedPage]) pair — the
standard ingestion contract for all downstream pipeline stages.

Routing table:
    .txt, .md  →  text_reader (fast, no model warm-up)
    .pdf       →  pdf_reader (pypdf, Pass 1) → ocr_router (Pass 2, empty pages only)
    .docx      →  docling_parser (Docling layout-aware extraction)

PDF two-pass design:
    Pass 1: pypdf extracts text from born-digital PDFs (fast).
    Pass 2: ocr_router is invoked only when Pass 1 left pages with
            extraction_status == "empty". Those pages are re-extracted
            via Docling/RapidOCR. Non-empty pages are not re-processed.

Deferred formats (not yet validated or routed):
    .doc, .pptx, .xlsx, .html, .htm → UnsupportedFormatError
    These will be enabled incrementally as each format is tested.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

from src.ingestion.readers.pdf_reader import read_pdf_file
from src.ingestion.readers.text_reader import read_text_file
from src.ocr.ocr_router import route_pdf_pages_through_ocr
from src.parsing.docling_parser import parse_with_docling
from src.schema.models import ParsedPage, RawDocument
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# Formats handled by the current readers.
_TEXT_FORMATS = {".txt", ".md"}
_PDF_FORMATS = {".pdf"}

# Formats routed through the Docling parser.
# Expanded incrementally as each format is validated and tested.
_DOCLING_FORMATS = {".docx"}

# Formats known to the architecture but not yet exposed through any reader.
_DEFERRED_FORMATS = {".doc", ".pptx", ".xlsx", ".html", ".htm"}


class UnsupportedFormatError(ValueError):
    """
    Raised when the router receives a file format that has no registered reader.

    Callers can catch this specifically to distinguish "format not supported"
    from "file not found" or other IO errors.
    """


def route_file(file_path: Path) -> Tuple[RawDocument, List[ParsedPage]]:
    """
    Route a file to the appropriate reader based on its suffix.

    Args:
        file_path: Path to the source document.

    Returns:
        Tuple of (RawDocument, List[ParsedPage]).

    Raises:
        FileNotFoundError: If the file does not exist.
        UnsupportedFormatError: If the format has no registered reader.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    suffix = file_path.suffix.lower()

    logger.debug(
        "router: routing file",
        file=file_path.name,
        suffix=suffix,
    )

    if suffix in _TEXT_FORMATS:
        return read_text_file(file_path)

    if suffix in _PDF_FORMATS:
        raw_doc, pages = read_pdf_file(file_path)
        # Pass 2: OCR recovery for pages pypdf could not extract text from.
        # route_pdf_pages_through_ocr is a no-op when all pages are non-empty.
        pages = route_pdf_pages_through_ocr(file_path, raw_doc, pages)
        return raw_doc, pages

    if suffix in _DOCLING_FORMATS:
        return parse_with_docling(file_path)

    if suffix in _DEFERRED_FORMATS:
        raise UnsupportedFormatError(
            f"Format {suffix!r} is not supported by the current ingestion readers. "
            f"Support for {suffix!r} will be added via the Docling parsing lane. "
            f"File: {file_path.name}"
        )

    raise UnsupportedFormatError(
        f"Unrecognised format {suffix!r}. File: {file_path.name}. "
        f"Add a reader for this format or convert the file to a supported type."
    )
