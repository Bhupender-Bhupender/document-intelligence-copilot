"""
Flat sliding-window word chunker.

Produces DocumentChunk records from ParsedPage records.
This is the Phase 1 baseline chunker — flat, word-count-based,
with configurable size and overlap.

Design decisions:
    - Operates on ParsedPage.normalized_text, not raw_text.
    - Pages with extraction_status="empty" are skipped (return []).
    - Pages with extraction_status="weak" are chunked as-is; they may
      produce a single small chunk which is better than discarding text.
    - chunk_level is always "flat" in this implementation.
    - parent_chunk_id is always None in this implementation.
    - Hierarchical chunking (parent/child levels) is introduced in Phase 3.

Public API:
    chunk_page()   — chunks a single ParsedPage
    chunk_pages()  — chunks all pages of a document, enriching with file metadata
"""
from __future__ import annotations

from typing import List, Optional

from src.core.config import config
from src.schema.models import DocumentChunk, ParsedPage
from src.utils.logging_utils import get_logger
from src.utils.text_utils import normalize_text

logger = get_logger(__name__)


def chunk_page(
    page: ParsedPage,
    chunk_size_words: Optional[int] = None,
    chunk_overlap_words: Optional[int] = None,
    file_name: str = "",
    file_type: str = "",
) -> List[DocumentChunk]:
    """
    Split a single ParsedPage into flat DocumentChunk records.

    Uses a sliding window over the word list. The step size is
    (chunk_size - overlap), so adjacent chunks share `overlap` words.

    Args:
        page: The ParsedPage to chunk.
        chunk_size_words: Override for configured chunk size. Defaults to
            config.chunk_size_words.
        chunk_overlap_words: Override for configured overlap. Defaults to
            config.chunk_overlap_words.
        file_name: Source file name, propagated from the corresponding
            RawDocument. Defaults to empty string for callers that do not
            have file metadata at hand (e.g. unit tests); prefer passing
            this explicitly rather than relying on post-hoc mutation.
        file_type: Source file type (e.g. "txt", "pdf"). Same convention
            as file_name.

    Returns:
        List of DocumentChunk records. Empty if the page has no usable text
        or extraction_status is "empty".

    Raises:
        ValueError: If chunk_size_words <= chunk_overlap_words.
    """
    size = chunk_size_words if chunk_size_words is not None else config.chunk_size_words
    overlap = (
        chunk_overlap_words
        if chunk_overlap_words is not None
        else config.chunk_overlap_words
    )

    if size <= overlap:
        raise ValueError(
            f"chunk_size_words ({size}) must be greater than "
            f"chunk_overlap_words ({overlap})."
        )

    # Empty pages produce no chunks — they are the OCR trigger for Phase 2.
    if page.extraction_status == "empty":
        logger.debug(
            "word_chunker: skipping empty page",
            doc_id=page.doc_id,
            page_number=page.page_number,
        )
        return []

    text = normalize_text(page.normalized_text)
    words = text.split() if text.strip() else []

    if not words:
        return []

    chunks: List[DocumentChunk] = []
    step = size - overlap
    chunk_index = 0

    for start in range(0, len(words), step):
        end = start + size
        chunk_words = words[start:end]

        if not chunk_words:
            break

        chunk = DocumentChunk(
            doc_id=page.doc_id,
            page_id=page.page_id,
            page_number=page.page_number,
            file_name=file_name,
            file_type=file_type,
            section_title=page.section_title,
            text=" ".join(chunk_words),
            word_count=len(chunk_words),
            chunk_index=chunk_index,
            chunk_level="flat",
        )
        chunks.append(chunk)
        chunk_index += 1

        if end >= len(words):
            break

    logger.debug(
        "word_chunker: chunked page",
        doc_id=page.doc_id,
        page_number=page.page_number,
        extraction_status=page.extraction_status,
        chunk_count=len(chunks),
    )

    return chunks


def chunk_pages(
    pages: List[ParsedPage],
    file_name: str,
    file_type: str,
    chunk_size_words: Optional[int] = None,
    chunk_overlap_words: Optional[int] = None,
) -> List[DocumentChunk]:
    """
    Chunk all pages from a document and enrich each chunk with file metadata.

    This is the primary chunking entry point. It accepts the file_name and
    file_type from the corresponding RawDocument so the chunker remains
    decoupled from the ingestion layer.

    Args:
        pages: List of ParsedPage records for the document.
        file_name: From RawDocument.file_name — propagated to each chunk.
        file_type: From RawDocument.file_type — propagated to each chunk.
        chunk_size_words: Optional override for chunk size.
        chunk_overlap_words: Optional override for overlap.

    Returns:
        Flat list of all DocumentChunk records across all pages.
        Empty pages (extraction_status="empty") contribute zero chunks.
    """
    all_chunks: List[DocumentChunk] = []

    for page in pages:
        page_chunks = chunk_page(
            page,
            chunk_size_words,
            chunk_overlap_words,
            file_name=file_name,
            file_type=file_type,
        )
        all_chunks.extend(page_chunks)

    logger.info(
        "word_chunker: chunked document",
        file_name=file_name,
        pages_processed=len(pages),
        total_chunks=len(all_chunks),
    )

    return all_chunks
