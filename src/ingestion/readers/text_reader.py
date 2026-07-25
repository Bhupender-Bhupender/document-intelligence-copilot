"""
Text and Markdown file reader.

Handles .txt and .md formats. Produces one RawDocument (provenance)
and one ParsedPage (content) per file. No external dependencies
beyond the standard library and project utilities.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import List, Tuple

from src.core.config import config
from src.schema.models import ParsedPage, RawDocument
from src.utils.logging_utils import get_logger
from src.utils.text_utils import classify_extraction_status, clean_text, normalize_text

logger = get_logger(__name__)

_SUPPORTED_SUFFIXES = {".txt", ".md"}


def read_text_file(file_path: Path) -> Tuple[RawDocument, List[ParsedPage]]:
    """
    Read a .txt or .md file and return a RawDocument plus one ParsedPage.

    The single ParsedPage always has page_number=1 and parse_method="text_read".
    extraction_status is derived from the word count of the normalised text
    using the configured thresholds.

    Args:
        file_path: Path to the source file.

    Returns:
        Tuple of (RawDocument, [ParsedPage]). The list always has exactly
        one element for text/md files.

    Raises:
        FileNotFoundError: If file_path does not exist.
        ValueError: If the file suffix is not .txt or .md.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix not in _SUPPORTED_SUFFIXES:
        raise ValueError(
            f"text_reader only handles {_SUPPORTED_SUFFIXES}. Got: {suffix!r}"
        )

    file_bytes = file_path.read_bytes()
    byte_size = len(file_bytes)
    checksum = hashlib.sha256(file_bytes).hexdigest()
    file_type = suffix.lstrip(".")

    raw_text = file_bytes.decode("utf-8", errors="ignore")
    cleaned = clean_text(raw_text)
    normalized = normalize_text(cleaned)
    word_count = len(normalized.split()) if normalized.strip() else 0
    char_count = len(normalized)
    extraction_status = classify_extraction_status(
        normalized,
        empty_threshold=config.extraction_empty_threshold,
        weak_threshold=config.extraction_weak_threshold,
    )

    doc = RawDocument(
        source_path=str(file_path.resolve()),
        file_name=file_path.name,
        file_type=file_type,
        byte_size=byte_size,
        checksum=checksum,
        total_pages=1,
    )

    page = ParsedPage(
        doc_id=doc.doc_id,
        page_number=1,
        raw_text=raw_text,
        normalized_text=normalized,
        word_count=word_count,
        char_count=char_count,
        parse_method="text_read",
        extraction_status=extraction_status,
    )

    logger.info(
        "text_reader: read file",
        file=file_path.name,
        file_type=file_type,
        word_count=word_count,
        extraction_status=extraction_status,
    )

    return doc, [page]
