"""
Smoke tests for the canonical schema models.

Validates that all eight pipeline models can be instantiated with
minimal valid data, that defaults are correct, and that Literal
constraints reject invalid values.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.schema.models import (
    AnswerResponse,
    CitationRecord,
    DocumentChunk,
    ParsedBlock,
    ParsedPage,
    RawDocument,
    RetrievedChunk,
    RunManifest,
)


class TestRawDocument:
    def test_create_minimal(self):
        doc = RawDocument(
            source_path="/tmp/test.txt",
            file_name="test.txt",
            file_type="txt",
            byte_size=100,
            checksum="abc123",
        )
        assert doc.doc_id is not None
        assert len(doc.doc_id) == 32  # uuid4().hex
        assert doc.total_pages is None
        assert doc.file_type == "txt"

    def test_doc_ids_are_unique(self):
        doc1 = RawDocument(
            source_path="/tmp/a.txt", file_name="a.txt",
            file_type="txt", byte_size=1, checksum="x",
        )
        doc2 = RawDocument(
            source_path="/tmp/b.txt", file_name="b.txt",
            file_type="txt", byte_size=1, checksum="y",
        )
        assert doc1.doc_id != doc2.doc_id

    def test_total_pages_can_be_set(self):
        doc = RawDocument(
            source_path="/tmp/a.pdf", file_name="a.pdf",
            file_type="pdf", byte_size=500, checksum="z",
            total_pages=3,
        )
        assert doc.total_pages == 3


class TestParsedPage:
    def _make(self, **kwargs) -> ParsedPage:
        defaults = dict(
            doc_id="doc123",
            page_number=1,
            raw_text="",
            normalized_text="",
            word_count=0,
            char_count=0,
        )
        defaults.update(kwargs)
        return ParsedPage(**defaults)

    def test_create_ok_page(self):
        page = self._make(
            raw_text="Hello world this is a test page.",
            normalized_text="Hello world this is a test page.",
            word_count=7,
            char_count=32,
            parse_method="text_read",
            extraction_status="ok",
        )
        assert page.page_id is not None
        assert page.extraction_status == "ok"
        assert page.layout_blocks == []
        assert page.ocr_confidence is None
        assert page.ocr_engine is None

    def test_extraction_status_all_valid_literals(self):
        for status in ("ok", "weak", "empty"):
            page = self._make(extraction_status=status)
            assert page.extraction_status == status

    def test_extraction_status_invalid_raises(self):
        with pytest.raises(ValidationError):
            self._make(extraction_status="corrupted")

    def test_parse_method_invalid_raises(self):
        with pytest.raises(ValidationError):
            self._make(parse_method="unknown_engine")

    def test_page_ids_are_unique(self):
        p1 = self._make(doc_id="d1")
        p2 = self._make(doc_id="d1")
        assert p1.page_id != p2.page_id


class TestParsedBlock:
    def test_create_minimal(self):
        block = ParsedBlock(
            doc_id="d1",
            page_number=1,
            text="A heading block",
            reading_order=0,
            block_type="heading",
        )
        assert block.block_id is not None
        assert block.bounding_box is None
        assert block.section_title is None

    def test_default_block_type(self):
        block = ParsedBlock(
            doc_id="d1", page_number=1, text="text", reading_order=0,
        )
        assert block.block_type == "unknown"


class TestDocumentChunk:
    def _make(self, **kwargs) -> DocumentChunk:
        defaults = dict(
            doc_id="doc1",
            page_id="page1",
            page_number=1,
            file_name="test.txt",
            file_type="txt",
            text="This is a test chunk of text.",
            word_count=7,
            chunk_index=0,
        )
        defaults.update(kwargs)
        return DocumentChunk(**defaults)

    def test_defaults(self):
        chunk = self._make()
        assert chunk.chunk_level == "flat"
        assert chunk.parent_chunk_id is None
        assert chunk.is_indexed is False
        assert chunk.embedding_model is None
        assert chunk.section_title is None

    def test_chunk_id_unique(self):
        c1 = self._make()
        c2 = self._make()
        assert c1.chunk_id != c2.chunk_id

    def test_invalid_chunk_level_raises(self):
        with pytest.raises(ValidationError):
            self._make(chunk_level="sibling")


class TestRetrievedChunk:
    def test_create_minimal(self):
        rc = RetrievedChunk(
            chunk_id="c1",
            doc_id="d1",
            page_id="p1",
            file_name="f.txt",
            page_number=1,
            text="retrieved text here",
            word_count=3,
        )
        assert rc.vector_score is None
        assert rc.bm25_score is None
        assert rc.fusion_score is None
        assert rc.rerank_score is None
        assert rc.retrieval_method == "vector"


class TestCitationRecord:
    def test_create_minimal(self):
        cr = CitationRecord(
            doc_id="d1",
            file_name="policy.txt",
            page_number=1,
            quote_text="all employees must comply",
        )
        assert cr.citation_id is not None
        assert cr.validation_status == "unverified"
        assert cr.is_verbatim is False
        assert cr.source_chunk_id is None

    def test_invalid_validation_status_raises(self):
        with pytest.raises(ValidationError):
            CitationRecord(
                doc_id="d", file_name="f.txt", page_number=1,
                quote_text="text", validation_status="pending",
            )


class TestAnswerResponse:
    def test_create_minimal(self):
        ar = AnswerResponse(
            query="What is the leave policy?",
            answer_text="The leave policy states...",
            model_used="qwen3:8b",
        )
        assert ar.run_id is not None
        assert ar.sources == []
        assert ar.supporting_chunks == []
        assert ar.validation_flags == []
        assert ar.latency_ms is None


class TestRunManifest:
    def test_create_minimal(self):
        rm = RunManifest(run_type="ingest")
        assert rm.run_id is not None
        assert rm.status == "running"
        assert rm.completed_at is None
        assert rm.errors == []
        assert rm.doc_ids_processed == []

    def test_invalid_run_type_raises(self):
        with pytest.raises(ValidationError):
            RunManifest(run_type="migrate")

    def test_all_valid_run_types(self):
        for rt in ("ingest", "chunk", "index", "retrieve", "answer", "full"):
            rm = RunManifest(run_type=rt)
            assert rm.run_type == rt
