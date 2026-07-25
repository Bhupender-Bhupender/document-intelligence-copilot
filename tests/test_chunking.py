"""
Smoke tests for the word-level sliding-window chunker.

Tests cover the primary API (chunk_pages), the inner helper (chunk_page),
edge cases (empty and weak pages), overlap correctness, and field propagation.
"""
from __future__ import annotations

import pytest

from src.chunking.word_chunker import chunk_page, chunk_pages
from src.schema.models import DocumentChunk, ParsedPage


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _make_page(text: str, status: str = "ok", doc_id: str = "d1") -> ParsedPage:
    """Build a minimal ParsedPage with controlled text and extraction_status."""
    words = text.split()
    return ParsedPage(
        doc_id=doc_id,
        page_number=1,
        raw_text=text,
        normalized_text=text,
        word_count=len(words),
        char_count=len(text),
        extraction_status=status,
    )


def _words(text: str):
    return text.split()


# --------------------------------------------------------------------------- #
# Flat chunks                                                                  #
# --------------------------------------------------------------------------- #


class TestChunkPage:
    def test_flat_chunks_have_correct_linkage(self):
        """chunk_id, doc_id, page_id, and chunk_level must be correctly set."""
        text = " ".join(f"word{i}" for i in range(200))
        page = _make_page(text)
        chunks = chunk_page(page, chunk_size_words=80, chunk_overlap_words=20)

        assert len(chunks) > 1
        for i, chunk in enumerate(chunks):
            assert isinstance(chunk, DocumentChunk)
            assert chunk.doc_id == page.doc_id
            assert chunk.page_id == page.page_id
            assert chunk.page_number == page.page_number
            assert chunk.chunk_level == "flat"
            assert chunk.parent_chunk_id is None
            assert chunk.chunk_index == i

    def test_chunk_ids_are_unique(self):
        text = " ".join(f"word{i}" for i in range(200))
        page = _make_page(text)
        chunks = chunk_page(page, chunk_size_words=80, chunk_overlap_words=20)
        ids = [c.chunk_id for c in chunks]
        assert len(ids) == len(set(ids))

    def test_chunk_word_counts_match_text(self):
        """Each chunk's word_count must equal the number of words in its text."""
        text = " ".join(f"word{i}" for i in range(150))
        page = _make_page(text)
        chunks = chunk_page(page, chunk_size_words=50, chunk_overlap_words=10)
        for chunk in chunks:
            assert chunk.word_count == len(chunk.text.split())

    def test_short_text_produces_single_chunk(self):
        """Text shorter than chunk_size must produce exactly one chunk."""
        text = "only five words here"
        page = _make_page(text)
        chunks = chunk_page(page, chunk_size_words=80, chunk_overlap_words=20)
        assert len(chunks) == 1
        assert chunks[0].text.strip() == text


# --------------------------------------------------------------------------- #
# Edge cases: empty and weak pages                                             #
# --------------------------------------------------------------------------- #


class TestEdgeCases:
    def test_empty_page_returns_empty_list(self):
        """extraction_status=empty is the Phase 2 OCR trigger; must return []."""
        page = _make_page("", status="empty")
        chunks = chunk_page(page, chunk_size_words=80, chunk_overlap_words=20)
        assert chunks == []

    def test_weak_page_still_produces_chunks(self):
        """Weak pages have some content; they should be chunked normally."""
        text = "short but parseable text content"
        page = _make_page(text, status="weak")
        chunks = chunk_page(page, chunk_size_words=80, chunk_overlap_words=20)
        assert len(chunks) >= 1
        assert chunks[0].text.strip() != ""


# --------------------------------------------------------------------------- #
# Overlap correctness                                                          #
# --------------------------------------------------------------------------- #


class TestOverlap:
    def test_overlap_words_appear_in_consecutive_chunks(self):
        """
        The last <overlap> words of chunk N must appear at the start of chunk N+1.
        """
        text = " ".join(f"w{i}" for i in range(300))
        page = _make_page(text)
        overlap = 20
        chunks = chunk_page(page, chunk_size_words=80, chunk_overlap_words=overlap)
        assert len(chunks) >= 2, "Need at least 2 chunks to test overlap"

        for i in range(len(chunks) - 1):
            tail_words = _words(chunks[i].text)[-overlap:]
            head_words = _words(chunks[i + 1].text)[:overlap]
            assert tail_words == head_words, (
                f"Overlap mismatch between chunk {i} and {i + 1}"
            )

    def test_invalid_overlap_raises_value_error(self):
        """chunk_size must be strictly greater than chunk_overlap."""
        page = _make_page("some text here with words")
        with pytest.raises(ValueError, match="overlap"):
            chunk_page(page, chunk_size_words=20, chunk_overlap_words=20)


# --------------------------------------------------------------------------- #
# chunk_pages API — field propagation                                          #
# --------------------------------------------------------------------------- #


class TestChunkPages:
    def test_file_name_and_type_propagated(self):
        """chunk_pages must stamp file_name and file_type onto every chunk."""
        pages = [_make_page(" ".join(f"w{i}" for i in range(100)))]
        chunks = chunk_pages(
            pages,
            file_name="company_policy.txt",
            file_type="txt",
        )
        assert len(chunks) > 0
        for chunk in chunks:
            assert chunk.file_name == "company_policy.txt"
            assert chunk.file_type == "txt"

    def test_multiple_pages_aggregated(self):
        """chunk_pages must aggregate chunks from all pages."""
        pages = [
            _make_page(" ".join(f"w{i}" for i in range(90)), doc_id="d1"),
            _make_page(" ".join(f"w{i}" for i in range(90)), doc_id="d1"),
        ]
        chunks = chunk_pages(pages, file_name="test.txt", file_type="txt")
        assert len(chunks) > 0
        # chunk_index resets per page; both pages contribute chunks
        page_ids_seen = {c.page_id for c in chunks}
        assert len(page_ids_seen) == 2

    def test_empty_pages_contribute_no_chunks(self):
        """Empty pages must not contribute any chunks to the output list."""
        pages = [
            _make_page("", status="empty", doc_id="d1"),
            _make_page(" ".join(f"w{i}" for i in range(90)), doc_id="d1"),
        ]
        chunks = chunk_pages(pages, file_name="test.txt", file_type="txt")
        for chunk in chunks:
            assert chunk.page_id != pages[0].page_id
