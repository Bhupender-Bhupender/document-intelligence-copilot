"""
tests/test_citation_builder.py
──────────────────────────────
Unit tests for src/citations/citation_builder.py and the citations wired into
run_pipeline (Stage 5).

Classes
-------
TestCitationBuilderContract   – return type and length invariants
TestCitationMetadataMapping   – every chunk field lands on the right record field
TestQuoteSemantics            – quote_text / quote_start_char / quote_end_char
TestCitationDefaults          – is_verbatim and validation_status defaults
TestCitationIdDeterminism     – citation_id is deterministic and chunk-unique
TestPipelineSources           – run_pipeline.sources is populated by real build_citations
"""

from __future__ import annotations

from typing import List, Optional

import pytest

from src.citations.citation_builder import build_citations
from src.generation.answer_pipeline import run_pipeline
from src.schema.models import CitationRecord, DocumentChunk, RetrievedChunk

# =========================================================================== #
# Helpers                                                                      #
# =========================================================================== #


def _make_chunk(
    chunk_id: str = "c1",
    doc_id: str = "doc1",
    text: str = "Sample chunk text for testing.",
    page_number: int = 1,
    file_name: str = "report.txt",
    section_title: Optional[str] = "Introduction",
) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id="page1",
        file_name=file_name,
        page_number=page_number,
        text=text,
        word_count=len(text.split()),
        retrieval_method="hybrid",
        parent_chunk_id=None,
        fusion_score=0.8,
        rerank_score=0.9,
        section_title=section_title,
    )


class _FakeRetriever:
    def __init__(self, chunks: Optional[List[RetrievedChunk]] = None) -> None:
        self._chunks = chunks or []

    def __call__(self, query: str) -> List[RetrievedChunk]:
        return list(self._chunks)


class _FakeReranker:
    def __call__(self, query: str, chunks: List[RetrievedChunk]) -> List[RetrievedChunk]:
        return list(chunks)


class _FakeParentLookup:
    def __call__(self, chunks: List[RetrievedChunk]) -> List[Optional[DocumentChunk]]:
        return [None] * len(chunks)


class _FakeGenerator:
    def __call__(self, messages: list) -> str:
        return "Fake answer."


def _run_pipeline_fake(chunks: List[RetrievedChunk]) -> List[CitationRecord]:
    """Run pipeline with injected fakes (no real I/O) and return sources."""
    response = run_pipeline(
        query="Test query?",
        _retriever=_FakeRetriever(chunks=chunks),
        _reranker=_FakeReranker(),
        _parent_lookup=_FakeParentLookup(),
        _generator=_FakeGenerator(),
    )
    return response.sources


# =========================================================================== #
# TestCitationBuilderContract                                                  #
# =========================================================================== #


class TestCitationBuilderContract:
    def test_returns_list(self) -> None:
        result = build_citations([_make_chunk()])
        assert isinstance(result, list)

    def test_empty_input_returns_empty_list(self) -> None:
        assert build_citations([]) == []

    def test_single_chunk_produces_single_citation(self) -> None:
        result = build_citations([_make_chunk()])
        assert len(result) == 1

    def test_n_chunks_produce_n_citations(self) -> None:
        chunks = [_make_chunk(f"c{i}") for i in range(5)]
        result = build_citations(chunks)
        assert len(result) == 5


# =========================================================================== #
# TestCitationMetadataMapping                                                  #
# =========================================================================== #


class TestCitationMetadataMapping:
    def test_doc_id_mapped(self) -> None:
        chunk = _make_chunk(doc_id="special-doc")
        record = build_citations([chunk])[0]
        assert record.doc_id == "special-doc"

    def test_file_name_mapped(self) -> None:
        chunk = _make_chunk(file_name="annual_report.pdf")
        record = build_citations([chunk])[0]
        assert record.file_name == "annual_report.pdf"

    def test_page_number_mapped(self) -> None:
        chunk = _make_chunk(page_number=42)
        record = build_citations([chunk])[0]
        assert record.page_number == 42

    def test_section_title_mapped_when_present(self) -> None:
        chunk = _make_chunk(section_title="Executive Summary")
        record = build_citations([chunk])[0]
        assert record.section_title == "Executive Summary"

    def test_section_title_none_when_absent(self) -> None:
        chunk = _make_chunk(section_title=None)
        record = build_citations([chunk])[0]
        assert record.section_title is None

    def test_source_chunk_id_mapped(self) -> None:
        chunk = _make_chunk(chunk_id="chunk-xyz")
        record = build_citations([chunk])[0]
        assert record.source_chunk_id == "chunk-xyz"


# =========================================================================== #
# TestQuoteSemantics                                                           #
# =========================================================================== #


class TestQuoteSemantics:
    def test_quote_text_equals_chunk_text(self) -> None:
        chunk = _make_chunk(text="The revenue grew by 12% in Q3.")
        record = build_citations([chunk])[0]
        assert record.quote_text == chunk.text

    def test_quote_start_char_is_zero(self) -> None:
        chunk = _make_chunk(text="Any text here.")
        record = build_citations([chunk])[0]
        assert record.quote_start_char == 0

    def test_quote_end_char_equals_text_length(self) -> None:
        text = "Specific passage for end-char test."
        chunk = _make_chunk(text=text)
        record = build_citations([chunk])[0]
        assert record.quote_end_char == len(text)


# =========================================================================== #
# TestCitationDefaults                                                         #
# =========================================================================== #


class TestCitationDefaults:
    def test_is_verbatim_is_true(self) -> None:
        record = build_citations([_make_chunk()])[0]
        assert record.is_verbatim is True

    def test_validation_status_is_unverified(self) -> None:
        record = build_citations([_make_chunk()])[0]
        assert record.validation_status == "unverified"


# =========================================================================== #
# TestCitationIdDeterminism                                                    #
# =========================================================================== #


class TestCitationIdDeterminism:
    def test_citation_id_is_str(self) -> None:
        record = build_citations([_make_chunk()])[0]
        assert isinstance(record.citation_id, str)

    def test_same_chunk_same_citation_id(self) -> None:
        chunk = _make_chunk(chunk_id="stable-id", doc_id="doc99", page_number=7)
        id1 = build_citations([chunk])[0].citation_id
        id2 = build_citations([chunk])[0].citation_id
        assert id1 == id2

    def test_different_chunks_different_citation_ids(self) -> None:
        chunk_a = _make_chunk(chunk_id="c-alpha", doc_id="doc1", page_number=1)
        chunk_b = _make_chunk(chunk_id="c-beta", doc_id="doc2", page_number=2)
        id_a = build_citations([chunk_a])[0].citation_id
        id_b = build_citations([chunk_b])[0].citation_id
        assert id_a != id_b


# =========================================================================== #
# TestPipelineSources                                                          #
# =========================================================================== #


class TestPipelineSources:
    """Verify that run_pipeline populates sources via the real build_citations."""

    def test_pipeline_sources_non_empty(self) -> None:
        chunks = [_make_chunk("c1"), _make_chunk("c2")]
        sources = _run_pipeline_fake(chunks)
        assert len(sources) > 0

    def test_pipeline_sources_count_matches_reranked(self) -> None:
        chunks = [_make_chunk(f"c{i}") for i in range(3)]
        sources = _run_pipeline_fake(chunks)
        # _FakeReranker returns all chunks unchanged, so sources == len(chunks)
        assert len(sources) == len(chunks)

    def test_pipeline_sources_are_citation_records(self) -> None:
        chunks = [_make_chunk("c1")]
        sources = _run_pipeline_fake(chunks)
        assert all(isinstance(s, CitationRecord) for s in sources)
