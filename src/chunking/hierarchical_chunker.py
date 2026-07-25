"""
Hierarchical chunker: produces parent and child chunks from parsed pages.

Public API
----------
    build_hierarchical_chunks(
        raw_document: RawDocument,
        pages: list[ParsedPage],
    ) -> tuple[list[DocumentChunk], list[DocumentChunk]]

    Returns (parent_chunks, child_chunks).

Design
------
This chunker is the Phase 3 hierarchical layer. It lives alongside the flat
word_chunker and does not replace it. The two chunkers serve different purposes:

    word_chunker.py            — flat baseline; Phase 1; retrieval-only use case
    hierarchical_chunker.py   — parent/child; Phase 3; hierarchical RAG use case

Architecture fit:
    Parent chunks  → synthesis store (Phase 4+): broad context for LLM answer generation
    Child chunks   → retrieval index (Phase 4+): fine-grained units for embedding and BM25

Parent/child relationship:
    Every child chunk links to exactly one parent via parent_chunk_id.
    A parent chunk has parent_chunk_id=None.
    Child chunks are produced by sliding a smaller window over the parent's text.

Chunking strategy (two paths, explicit):

    Path 1 — Structured (layout blocks with headings present):
        Pages with Docling layout_blocks that include at least one heading are
        split by section. Consecutive non-heading blocks under a heading form a
        "segment". Segments are merged greedily into parent chunks up to the
        parent_chunk_size_words target. Each parent is then sub-divided into
        child chunks using a sliding window.

    Path 2 — Unstructured fallback (no layout blocks, or no headings):
        The page's normalized_text is split by word-window into parent chunks.
        Each parent is then sub-divided into child chunks.

    The two paths produce identical output types. Callers see no difference.

Empty / weak page rules:
    "empty" pages (extraction_status == "empty") — skipped; contribute 0 chunks.
    "weak"  pages (extraction_status == "weak")  — included. Even if a page has
        very little text, producing one parent + one child is more useful than
        discarding the content. Consistent with the flat chunker.

Determinism:
    chunk_id values are derived deterministically from:
        hashlib.sha256(f"{doc_id}|{level}|{seq_index}".encode()).hexdigest()[:32]
    where seq_index is a global counter within a single build_hierarchical_chunks
    call (incremented for every chunk produced, parents and children together).
    This guarantees:
        - uniqueness within a document
        - reproducibility: the same input always produces the same chunk_ids
        - no dependency on wall-clock time or random UUIDs

    Note: chunk_ids are document-scoped. If the same file is ingested twice it
    will produce the same chunk_ids, which can be used for idempotent upserts
    in the index (Phase 4).
"""
from __future__ import annotations

import hashlib
from typing import List, Optional, Tuple

from src.core.config import config
from src.schema.models import DocumentChunk, ParsedPage, RawDocument
from src.utils.logging_utils import get_logger
from src.utils.text_utils import normalize_text

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Internal types
# ---------------------------------------------------------------------------


class _Segment:
    """A contiguous block of text with an optional section heading."""

    __slots__ = ("text", "section_title")

    def __init__(self, text: str, section_title: Optional[str]) -> None:
        self.text = text
        self.section_title = section_title


# ---------------------------------------------------------------------------
# Deterministic ID generation
# ---------------------------------------------------------------------------


def _make_chunk_id(doc_id: str, level: str, seq: int) -> str:
    """
    Derive a deterministic chunk_id from document context.

    The ID is a 32-character hex string (SHA-256 truncated).
    seq is a monotonically increasing counter within one build call,
    shared across parents and children to guarantee global uniqueness.
    """
    key = f"{doc_id}|{level}|{seq}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Sliding-window word splitter
# ---------------------------------------------------------------------------


def _word_windows(
    text: str,
    size: int,
    overlap: int,
) -> List[str]:
    """
    Split text into overlapping word windows.

    Returns a list of strings. Each string contains at most `size` words.
    Adjacent windows share `overlap` words. The last window may be shorter.
    Returns an empty list if text is empty or contains no words.
    """
    words = text.split() if text.strip() else []
    if not words:
        return []
    if size <= overlap:
        raise ValueError(
            f"window size ({size}) must be greater than overlap ({overlap})"
        )

    step = size - overlap
    result: List[str] = []
    for start in range(0, len(words), step):
        chunk_words = words[start : start + size]
        if chunk_words:
            result.append(" ".join(chunk_words))
        if start + size >= len(words):
            break
    return result


# ---------------------------------------------------------------------------
# Segment extraction from layout blocks
# ---------------------------------------------------------------------------


def _segments_from_blocks(page: ParsedPage) -> Optional[List[_Segment]]:
    """
    Extract text segments from Docling layout blocks, grouped by heading.

    Returns None if:
        - no layout_blocks on the page
        - no heading-labelled blocks exist (unstructured path should be used)

    Each returned _Segment covers all content blocks under one heading.
    Text before the first heading is grouped under section_title=None.
    """
    if not page.layout_blocks:
        return None

    has_heading = any(b.block_type == "heading" for b in page.layout_blocks)
    if not has_heading:
        return None

    segments: List[_Segment] = []
    current_heading: Optional[str] = None
    current_texts: List[str] = []

    for block in page.layout_blocks:
        if block.block_type == "heading":
            # Flush the previous segment if it has content.
            accumulated = " ".join(current_texts).strip()
            if accumulated:
                segments.append(_Segment(accumulated, current_heading))
            current_heading = block.text.strip() or None
            current_texts = []
        else:
            text = block.text.strip()
            if text:
                current_texts.append(text)

    # Flush the final segment.
    accumulated = " ".join(current_texts).strip()
    if accumulated:
        segments.append(_Segment(accumulated, current_heading))

    return segments if segments else None


# ---------------------------------------------------------------------------
# Core chunk builders
# ---------------------------------------------------------------------------


def _build_parent_chunks(
    page: ParsedPage,
    raw_document: RawDocument,
    parent_size: int,
    seq_counter: List[int],  # mutable single-element list as a counter reference
) -> List[DocumentChunk]:
    """
    Build parent chunks for one page.

    Tries the structured (block-based) path first; falls back to
    word-window over normalized_text when no layout structure is available.
    """
    segments = _segments_from_blocks(page)

    if segments:
        return _parents_from_segments(page, raw_document, segments, parent_size, seq_counter)
    else:
        return _parents_from_text(page, raw_document, parent_size, seq_counter)


def _parents_from_segments(
    page: ParsedPage,
    raw_document: RawDocument,
    segments: List[_Segment],
    parent_size: int,
    seq_counter: List[int],
) -> List[DocumentChunk]:
    """
    Produce parent chunks by merging layout segments greedily.

    Segments are merged until adding the next segment would exceed
    parent_size words. When a segment alone exceeds parent_size it
    becomes its own parent (no further splitting at this layer).
    """
    parents: List[DocumentChunk] = []
    merged_text: List[str] = []
    merged_section: Optional[str] = None
    merged_words = 0
    chunk_index = 0

    def _flush(text: str, section: Optional[str]) -> None:
        nonlocal chunk_index
        seq = seq_counter[0]
        seq_counter[0] += 1
        chunk = DocumentChunk(
            chunk_id=_make_chunk_id(raw_document.doc_id, "parent", seq),
            doc_id=page.doc_id,
            page_id=page.page_id,
            page_number=page.page_number,
            file_name=raw_document.file_name,
            file_type=raw_document.file_type,
            section_title=section,
            text=text,
            word_count=len(text.split()),
            chunk_index=chunk_index,
            chunk_level="parent",
            parent_chunk_id=None,
        )
        parents.append(chunk)
        chunk_index += 1

    for seg in segments:
        seg_words = len(seg.text.split())
        if merged_words + seg_words > parent_size and merged_text:
            # Flush current accumulation before starting a new parent.
            _flush(" ".join(merged_text), merged_section)
            merged_text = [seg.text]
            merged_section = seg.section_title
            merged_words = seg_words
        else:
            if not merged_text:
                merged_section = seg.section_title
            merged_text.append(seg.text)
            merged_words += seg_words

    if merged_text:
        _flush(" ".join(merged_text), merged_section)

    return parents


def _parents_from_text(
    page: ParsedPage,
    raw_document: RawDocument,
    parent_size: int,
    seq_counter: List[int],
) -> List[DocumentChunk]:
    """
    Produce parent chunks from page.normalized_text using a word window.

    Used for pages with no layout blocks or no headings in their blocks.
    """
    text = normalize_text(page.normalized_text)
    windows = _word_windows(text, parent_size, overlap=0)
    parents: List[DocumentChunk] = []

    for chunk_index, window_text in enumerate(windows):
        seq = seq_counter[0]
        seq_counter[0] += 1
        chunk = DocumentChunk(
            chunk_id=_make_chunk_id(raw_document.doc_id, "parent", seq),
            doc_id=page.doc_id,
            page_id=page.page_id,
            page_number=page.page_number,
            file_name=raw_document.file_name,
            file_type=raw_document.file_type,
            section_title=page.section_title,
            text=window_text,
            word_count=len(window_text.split()),
            chunk_index=chunk_index,
            chunk_level="parent",
            parent_chunk_id=None,
        )
        parents.append(chunk)

    return parents


def _build_child_chunks(
    parent: DocumentChunk,
    raw_document: RawDocument,
    child_size: int,
    child_overlap: int,
    seq_counter: List[int],
) -> List[DocumentChunk]:
    """
    Produce child chunks by sliding a smaller window over a parent's text.

    Each child inherits: doc_id, page_id, page_number, file_name, file_type,
    section_title from the parent. parent_chunk_id is set to parent.chunk_id.
    """
    windows = _word_windows(parent.text, child_size, child_overlap)
    children: List[DocumentChunk] = []

    for chunk_index, window_text in enumerate(windows):
        seq = seq_counter[0]
        seq_counter[0] += 1
        child = DocumentChunk(
            chunk_id=_make_chunk_id(raw_document.doc_id, "child", seq),
            doc_id=parent.doc_id,
            page_id=parent.page_id,
            page_number=parent.page_number,
            file_name=parent.file_name,
            file_type=parent.file_type,
            section_title=parent.section_title,
            text=window_text,
            word_count=len(window_text.split()),
            chunk_index=chunk_index,
            chunk_level="child",
            parent_chunk_id=parent.chunk_id,
        )
        children.append(child)

    # Edge case: parent text is shorter than child_size (e.g. weak pages).
    # _word_windows handles this by returning a single window with all words.
    # So one child is always produced unless the parent text is empty.

    return children


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_hierarchical_chunks(
    raw_document: RawDocument,
    pages: List[ParsedPage],
    parent_chunk_size_words: Optional[int] = None,
    child_chunk_size_words: Optional[int] = None,
    child_chunk_overlap_words: Optional[int] = None,
) -> Tuple[List[DocumentChunk], List[DocumentChunk]]:
    """
    Build parent and child chunks from all parsed pages of a document.

    Args:
        raw_document:             Document metadata (file_name, file_type, doc_id).
        pages:                    ParsedPage list from ingestion/parsing.
        parent_chunk_size_words:  Override for config.parent_chunk_size_words.
        child_chunk_size_words:   Override for config.child_chunk_size_words.
        child_chunk_overlap_words: Override for config.child_chunk_overlap_words.

    Returns:
        (parent_chunks, child_chunks) — two flat lists.
        Parents have chunk_level="parent", parent_chunk_id=None.
        Children have chunk_level="child", parent_chunk_id set to their parent.
        Empty pages (extraction_status="empty") contribute zero chunks.
        Weak pages contribute at least 1 parent + 1 child.

    Guarantees:
        - Every child's parent_chunk_id matches a chunk_id in parent_chunks.
        - chunk_ids are deterministic: same input → same chunk_ids.
        - chunk_ids are unique within this document.
        - The lists are ordered: by page_number, then by chunk_index.
    """
    p_size = parent_chunk_size_words or config.parent_chunk_size_words
    c_size = child_chunk_size_words or config.child_chunk_size_words
    c_overlap = child_chunk_overlap_words or config.child_chunk_overlap_words

    all_parents: List[DocumentChunk] = []
    all_children: List[DocumentChunk] = []

    # Shared monotonic counter — ensures deterministic, globally unique IDs
    # across all parents and children within this call.
    seq_counter: List[int] = [0]

    for page in pages:
        if page.extraction_status == "empty":
            logger.debug(
                "hierarchical_chunker: skipping empty page",
                doc_id=page.doc_id,
                page_number=page.page_number,
            )
            continue

        parents = _build_parent_chunks(page, raw_document, p_size, seq_counter)

        if not parents:
            logger.debug(
                "hierarchical_chunker: no parent chunks produced for page",
                doc_id=page.doc_id,
                page_number=page.page_number,
                extraction_status=page.extraction_status,
            )
            continue

        children: List[DocumentChunk] = []
        for parent in parents:
            page_children = _build_child_chunks(
                parent, raw_document, c_size, c_overlap, seq_counter
            )
            children.extend(page_children)

        all_parents.extend(parents)
        all_children.extend(children)

        logger.debug(
            "hierarchical_chunker: chunked page",
            doc_id=page.doc_id,
            page_number=page.page_number,
            parents=len(parents),
            children=len(children),
        )

    logger.info(
        "hierarchical_chunker: complete",
        doc_id=raw_document.doc_id,
        file=raw_document.file_name,
        pages_processed=sum(
            1 for p in pages if p.extraction_status != "empty"
        ),
        total_parents=len(all_parents),
        total_children=len(all_children),
    )

    return all_parents, all_children
