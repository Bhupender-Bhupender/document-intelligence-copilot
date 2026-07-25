"""
Text-based PDF reader using pypdf.

Produces one RawDocument (provenance) and one ParsedPage per PDF page.
This reader handles born-digital PDFs with extractable text layers.

Scanned or complex PDFs will return ParsedPages with extraction_status
"weak" or "empty". These are the clean trigger for Phase 2 OCR routing:
a Phase 2 component simply checks extraction_status and dispatches to
PaddleOCR without needing to re-examine the source file.

Error handling philosophy:
    - A page that fails text extraction returns an "empty" ParsedPage.
    - A PDF that cannot be opened at all returns (RawDocument, []).
    - Neither case raises an exception; failures are logged as warnings.
    This ensures the pipeline can continue processing other documents.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import List, Tuple

from pypdf import PdfReader
from pypdf.errors import PdfReadError

from src.core.config import config
from src.schema.models import ParsedPage, RawDocument
from src.utils.logging_utils import get_logger
from src.utils.text_utils import classify_extraction_status, clean_text, normalize_text

logger = get_logger(__name__)


def read_pdf_file(file_path: Path) -> Tuple[RawDocument, List[ParsedPage]]:
    """
    Read a PDF file and return a RawDocument plus one ParsedPage per page.

    The RawDocument is always created successfully (it is pure provenance).
    ParsedPages are created for each page, with extraction_status set to
    reflect the quality of text extracted by pypdf. If a page fails
    extraction entirely, a ParsedPage with extraction_status="empty" is
    returned for that page rather than raising an exception.

    Args:
        file_path: Path to the PDF file.

    Returns:
        Tuple of (RawDocument, List[ParsedPage]).
        The list is empty only if the file cannot be opened at all.

    Raises:
        FileNotFoundError: If file_path does not exist.
        ValueError: If the file suffix is not .pdf.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_path.suffix.lower() != ".pdf":
        raise ValueError(
            f"pdf_reader only handles .pdf files. Got: {file_path.suffix!r}"
        )

    file_bytes = file_path.read_bytes()
    byte_size = len(file_bytes)
    checksum = hashlib.sha256(file_bytes).hexdigest()

    # Build the RawDocument upfront so every ParsedPage has a valid doc_id
    # even if the PDF is unreadable.
    doc = RawDocument(
        source_path=str(file_path.resolve()),
        file_name=file_path.name,
        file_type="pdf",
        byte_size=byte_size,
        checksum=checksum,
    )

    # Attempt to open the PDF. Return empty page list on failure.
    try:
        reader = PdfReader(str(file_path))
    except PdfReadError as exc:
        logger.warning(
            "pdf_reader: cannot open PDF (PdfReadError)",
            file=file_path.name,
            error=str(exc),
        )
        return doc, []
    except Exception as exc:
        logger.error(
            "pdf_reader: unexpected error opening PDF",
            file=file_path.name,
            error=str(exc),
        )
        return doc, []

    total_pages = len(reader.pages)
    doc.total_pages = total_pages

    pages: List[ParsedPage] = []

    for page_index, pdf_page in enumerate(reader.pages, start=1):
        try:
            raw_text = pdf_page.extract_text() or ""
        except Exception as exc:
            logger.warning(
                "pdf_reader: failed to extract text from page",
                file=file_path.name,
                page=page_index,
                error=str(exc),
            )
            raw_text = ""

        cleaned = clean_text(raw_text)
        normalized = normalize_text(cleaned)
        word_count = len(normalized.split()) if normalized.strip() else 0
        char_count = len(normalized)
        extraction_status = classify_extraction_status(
            normalized,
            empty_threshold=config.extraction_empty_threshold,
            weak_threshold=config.extraction_weak_threshold,
        )

        page = ParsedPage(
            doc_id=doc.doc_id,
            page_number=page_index,
            raw_text=raw_text,
            normalized_text=normalized,
            word_count=word_count,
            char_count=char_count,
            parse_method="pypdf",
            extraction_status=extraction_status,
        )
        pages.append(page)

    ok_count = sum(1 for p in pages if p.extraction_status == "ok")
    weak_count = sum(1 for p in pages if p.extraction_status == "weak")
    empty_count = sum(1 for p in pages if p.extraction_status == "empty")

    logger.info(
        "pdf_reader: read PDF",
        file=file_path.name,
        total_pages=total_pages,
        ok_pages=ok_count,
        weak_pages=weak_count,
        empty_pages=empty_count,
    )

    return doc, pages
