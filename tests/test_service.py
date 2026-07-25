"""
Tests for app/service.py — the service layer.

All tests use recording fakes that capture the exact kwargs forwarded
through both index_document and answer_query. No real pipeline I/O.

Test classes
------------
    TestServiceError      — ServiceError is Exception; message; __cause__ chaining
    TestIndexDocument     — forwarding, return type, validation, error wrapping
    TestAnswerQuery       — forwarding, return type, validation, error wrapping
    TestOutputTypes       — public return types are project-native models
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from app.service import ServiceError, answer_query, index_document
from src.indexing.index_builder import IndexManifest
from src.schema.models import AnswerResponse


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


def _make_manifest(index_dir: str = "/tmp/idx") -> IndexManifest:
    """Build a minimal IndexManifest for use in fakes."""
    return IndexManifest(
        index_dir=index_dir,
        embedding_model="MockEmbedding",
        parent_count=2,
        child_count=4,
        doc_ids=["doc-1"],
    )


def _make_response(query: str = "test query") -> AnswerResponse:
    """Build a minimal AnswerResponse for use in fakes."""
    return AnswerResponse(
        query=query,
        answer_text="Test answer.",
        model_used="test-model",
    )


# ---------------------------------------------------------------------------
# Recording fakes
# ---------------------------------------------------------------------------


class _RecordingIndexPipeline:
    """Records the kwargs forwarded by index_document."""

    def __init__(self, result: Optional[IndexManifest] = None, raise_exc: Optional[Exception] = None):
        self.call_kwargs: dict = {}
        self._result = result or _make_manifest()
        self._raise_exc = raise_exc

    def __call__(self, file_path: Path, *, index_dir: Any, embed_model: Any) -> IndexManifest:
        self.call_kwargs = {"file_path": file_path, "index_dir": index_dir, "embed_model": embed_model}
        if self._raise_exc is not None:
            raise self._raise_exc
        return self._result


class _RecordingAnswerPipeline:
    """Records the kwargs forwarded by answer_query."""

    def __init__(self, result: Optional[AnswerResponse] = None, raise_exc: Optional[Exception] = None):
        self.call_kwargs: dict = {}
        self._result: Optional[AnswerResponse] = None
        self._raise_exc = raise_exc

        if result is not None:
            self._result = result

    def __call__(
        self,
        query: str,
        *,
        index_dir: Any,
        retrieval_top_k: int,
        rerank_top_k: int,
        model: Any,
    ) -> AnswerResponse:
        self.call_kwargs = {
            "query": query,
            "index_dir": index_dir,
            "retrieval_top_k": retrieval_top_k,
            "rerank_top_k": rerank_top_k,
            "model": model,
        }
        if self._raise_exc is not None:
            raise self._raise_exc
        return self._result if self._result is not None else _make_response(query)


# ---------------------------------------------------------------------------
# TestServiceError
# ---------------------------------------------------------------------------


class TestServiceError:
    def test_is_exception_subclass(self):
        assert issubclass(ServiceError, Exception)

    def test_message_accessible(self):
        err = ServiceError("something went wrong")
        assert str(err) == "something went wrong"

    def test_cause_chaining(self):
        original = ValueError("root cause")
        try:
            raise ServiceError("wrapped") from original
        except ServiceError as e:
            assert e.__cause__ is original


# ---------------------------------------------------------------------------
# TestIndexDocument
# ---------------------------------------------------------------------------


class TestIndexDocument:
    def test_returns_index_manifest(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        fake = _RecordingIndexPipeline()
        result = index_document(doc, _indexing_pipeline=fake)
        assert isinstance(result, IndexManifest)

    def test_file_path_forwarded(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        fake = _RecordingIndexPipeline()
        index_document(doc, _indexing_pipeline=fake)
        assert fake.call_kwargs["file_path"] == doc

    def test_index_dir_forwarded(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        custom_dir = tmp_path / "my_index"
        fake = _RecordingIndexPipeline()
        index_document(doc, index_dir=custom_dir, _indexing_pipeline=fake)
        assert fake.call_kwargs["index_dir"] == custom_dir

    def test_index_dir_none_by_default(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        fake = _RecordingIndexPipeline()
        index_document(doc, _indexing_pipeline=fake)
        assert fake.call_kwargs["index_dir"] is None

    def test_embed_model_forwarded(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        sentinel = object()
        fake = _RecordingIndexPipeline()
        index_document(doc, embed_model=sentinel, _indexing_pipeline=fake)
        assert fake.call_kwargs["embed_model"] is sentinel

    def test_embed_model_none_by_default(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        fake = _RecordingIndexPipeline()
        index_document(doc, _indexing_pipeline=fake)
        assert fake.call_kwargs["embed_model"] is None

    def test_missing_file_raises_service_error(self, tmp_path):
        missing = tmp_path / "nonexistent.txt"
        fake = _RecordingIndexPipeline()
        with pytest.raises(ServiceError, match="File not found"):
            index_document(missing, _indexing_pipeline=fake)

    def test_missing_file_does_not_call_pipeline(self, tmp_path):
        missing = tmp_path / "nonexistent.txt"
        fake = _RecordingIndexPipeline()
        with pytest.raises(ServiceError):
            index_document(missing, _indexing_pipeline=fake)
        assert fake.call_kwargs == {}

    def test_pipeline_exception_wrapped_as_service_error(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        fake = _RecordingIndexPipeline(raise_exc=RuntimeError("pipeline crash"))
        with pytest.raises(ServiceError, match="Indexing failed"):
            index_document(doc, _indexing_pipeline=fake)

    def test_pipeline_exception_chained(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        original = RuntimeError("pipeline crash")
        fake = _RecordingIndexPipeline(raise_exc=original)
        with pytest.raises(ServiceError) as exc_info:
            index_document(doc, _indexing_pipeline=fake)
        assert exc_info.value.__cause__ is original

    def test_string_file_path_accepted(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("content")
        fake = _RecordingIndexPipeline()
        result = index_document(str(doc), _indexing_pipeline=fake)
        assert isinstance(result, IndexManifest)
        assert fake.call_kwargs["file_path"] == doc


# ---------------------------------------------------------------------------
# TestAnswerQuery
# ---------------------------------------------------------------------------


class TestAnswerQuery:
    def test_returns_answer_response(self):
        fake = _RecordingAnswerPipeline()
        result = answer_query("What is AI?", _answer_pipeline=fake)
        assert isinstance(result, AnswerResponse)

    def test_query_forwarded(self):
        fake = _RecordingAnswerPipeline()
        answer_query("What is AI?", _answer_pipeline=fake)
        assert fake.call_kwargs["query"] == "What is AI?"

    def test_index_dir_forwarded(self, tmp_path):
        custom_dir = tmp_path / "my_index"
        fake = _RecordingAnswerPipeline()
        answer_query("query", index_dir=custom_dir, _answer_pipeline=fake)
        assert fake.call_kwargs["index_dir"] == custom_dir

    def test_index_dir_none_by_default(self):
        fake = _RecordingAnswerPipeline()
        answer_query("query", _answer_pipeline=fake)
        assert fake.call_kwargs["index_dir"] is None

    def test_retrieval_top_k_forwarded(self):
        fake = _RecordingAnswerPipeline()
        answer_query("query", retrieval_top_k=20, _answer_pipeline=fake)
        assert fake.call_kwargs["retrieval_top_k"] == 20

    def test_retrieval_top_k_default_is_10(self):
        fake = _RecordingAnswerPipeline()
        answer_query("query", _answer_pipeline=fake)
        assert fake.call_kwargs["retrieval_top_k"] == 10

    def test_rerank_top_k_forwarded(self):
        fake = _RecordingAnswerPipeline()
        answer_query("query", rerank_top_k=3, _answer_pipeline=fake)
        assert fake.call_kwargs["rerank_top_k"] == 3

    def test_rerank_top_k_default_is_5(self):
        fake = _RecordingAnswerPipeline()
        answer_query("query", _answer_pipeline=fake)
        assert fake.call_kwargs["rerank_top_k"] == 5

    def test_model_forwarded(self):
        fake = _RecordingAnswerPipeline()
        answer_query("query", model="qwen3:8b", _answer_pipeline=fake)
        assert fake.call_kwargs["model"] == "qwen3:8b"

    def test_model_none_by_default(self):
        fake = _RecordingAnswerPipeline()
        answer_query("query", _answer_pipeline=fake)
        assert fake.call_kwargs["model"] is None

    def test_empty_string_raises_service_error(self):
        fake = _RecordingAnswerPipeline()
        with pytest.raises(ServiceError, match="must not be empty"):
            answer_query("", _answer_pipeline=fake)

    def test_whitespace_only_raises_service_error(self):
        fake = _RecordingAnswerPipeline()
        with pytest.raises(ServiceError, match="must not be empty"):
            answer_query("   ", _answer_pipeline=fake)

    def test_empty_query_does_not_call_pipeline(self):
        fake = _RecordingAnswerPipeline()
        with pytest.raises(ServiceError):
            answer_query("", _answer_pipeline=fake)
        assert fake.call_kwargs == {}

    def test_pipeline_exception_wrapped_as_service_error(self):
        fake = _RecordingAnswerPipeline(raise_exc=RuntimeError("pipeline crash"))
        with pytest.raises(ServiceError, match="Query failed"):
            answer_query("question", _answer_pipeline=fake)

    def test_pipeline_exception_chained(self):
        original = RuntimeError("pipeline crash")
        fake = _RecordingAnswerPipeline(raise_exc=original)
        with pytest.raises(ServiceError) as exc_info:
            answer_query("question", _answer_pipeline=fake)
        assert exc_info.value.__cause__ is original


# ---------------------------------------------------------------------------
# TestOutputTypes
# ---------------------------------------------------------------------------


class TestOutputTypes:
    def test_index_document_returns_index_manifest(self, tmp_path):
        doc = tmp_path / "doc.txt"
        doc.write_text("hello world")
        fake = _RecordingIndexPipeline()
        result = index_document(doc, _indexing_pipeline=fake)
        assert type(result).__name__ == "IndexManifest"

    def test_answer_query_returns_answer_response(self):
        fake = _RecordingAnswerPipeline()
        result = answer_query("some question", _answer_pipeline=fake)
        assert type(result).__name__ == "AnswerResponse"

    def test_service_error_is_exception(self):
        assert issubclass(ServiceError, Exception)
