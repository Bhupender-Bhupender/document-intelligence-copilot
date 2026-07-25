"""
Tests for app/ui.py — the Gradio Blocks UI module.

All format helpers and event handlers are tested without running the real
service pipeline.  Event-handler tests use monkeypatching on the names bound
in the ``app.ui`` namespace (``app.ui.index_document`` and
``app.ui.answer_query``).  Format helpers are pure functions and need no
mocking.

Test classes
------------
    TestFormatIndexResult  — pure formatting of IndexManifest fields
    TestFormatCitations    — pure formatting, empty / single / multi citations
    TestFormatFlags        — pure formatting, empty and non-empty flag lists
    TestHandleIndex        — handler: None path, forwarding, ServiceError, exc
    TestHandleAnswer       — handler: outputs, kwarg forwarding, error states
    TestBuildUi            — smoke test; skipped when Gradio is not installed
"""
from __future__ import annotations

from typing import List, Optional

import pytest

from app.service import ServiceError
from app.ui import (
    _format_citations,
    _format_flags,
    _format_index_result,
    _handle_answer,
    _handle_index,
)
from src.indexing.index_builder import IndexManifest
from src.schema.models import AnswerResponse, CitationRecord


# ---------------------------------------------------------------------------
# Shared factories
# ---------------------------------------------------------------------------


def _make_manifest(
    *,
    run_id: str = "testrun-abc123",
    parent_count: int = 3,
    child_count: int = 9,
    doc_ids: Optional[List[str]] = None,
    embedding_model: str = "TestEmbedding",
) -> IndexManifest:
    return IndexManifest(
        run_id=run_id,
        index_dir="/tmp/test_index",
        embedding_model=embedding_model,
        parent_count=parent_count,
        child_count=child_count,
        doc_ids=doc_ids if doc_ids is not None else ["doc-a"],
    )


def _make_citation(
    *,
    file_name: str = "report.pdf",
    page_number: int = 3,
    quote_text: str = "This is the cited passage.",
    section_title: Optional[str] = None,
    validation_status: str = "valid",
) -> CitationRecord:
    return CitationRecord(
        doc_id="doc-a",
        file_name=file_name,
        page_number=page_number,
        quote_text=quote_text,
        section_title=section_title,
        validation_status=validation_status,
    )


def _make_response(
    *,
    answer_text: str = "The answer is 42.",
    sources: Optional[List[CitationRecord]] = None,
    validation_flags: Optional[List[str]] = None,
    model_used: str = "test-model",
) -> AnswerResponse:
    return AnswerResponse(
        query="test query",
        answer_text=answer_text,
        model_used=model_used,
        sources=sources if sources is not None else [],
        validation_flags=validation_flags if validation_flags is not None else [],
    )


# ---------------------------------------------------------------------------
# TestFormatIndexResult
# ---------------------------------------------------------------------------


class TestFormatIndexResult:
    def test_contains_parent_count(self) -> None:
        result = _format_index_result(_make_manifest(parent_count=5))
        assert "5" in result

    def test_contains_child_count(self) -> None:
        result = _format_index_result(_make_manifest(child_count=15))
        assert "15" in result

    def test_contains_doc_id(self) -> None:
        result = _format_index_result(_make_manifest(doc_ids=["my-doc-xyz"]))
        assert "my-doc-xyz" in result

    def test_contains_embedding_model(self) -> None:
        result = _format_index_result(_make_manifest(embedding_model="Qwen3Embedding"))
        assert "Qwen3Embedding" in result

    def test_contains_run_id(self) -> None:
        result = _format_index_result(_make_manifest(run_id="run-9999"))
        assert "run-9999" in result

    def test_empty_doc_ids_shows_placeholder(self) -> None:
        result = _format_index_result(_make_manifest(doc_ids=[]))
        assert "(none)" in result

    def test_multiple_doc_ids_all_present(self) -> None:
        result = _format_index_result(_make_manifest(doc_ids=["doc-a", "doc-b", "doc-c"]))
        assert "doc-a" in result
        assert "doc-b" in result
        assert "doc-c" in result

    def test_indexed_successfully_label_present(self) -> None:
        result = _format_index_result(_make_manifest())
        assert "Indexed successfully" in result


# ---------------------------------------------------------------------------
# TestFormatCitations
# ---------------------------------------------------------------------------


class TestFormatCitations:
    def test_empty_sources_shows_placeholder(self) -> None:
        result = _format_citations(_make_response(sources=[]))
        assert "(No citations)" in result

    def test_single_citation_includes_file_name(self) -> None:
        result = _format_citations(_make_response(sources=[_make_citation(file_name="annual_report.pdf")]))
        assert "annual_report.pdf" in result

    def test_single_citation_includes_page_number(self) -> None:
        result = _format_citations(_make_response(sources=[_make_citation(page_number=7)]))
        assert "7" in result

    def test_validation_status_uppercased(self) -> None:
        result = _format_citations(_make_response(sources=[_make_citation(validation_status="valid")]))
        assert "VALID" in result

    def test_invalid_status_uppercased(self) -> None:
        result = _format_citations(_make_response(sources=[_make_citation(validation_status="invalid")]))
        assert "INVALID" in result

    def test_section_title_included_when_present(self) -> None:
        result = _format_citations(_make_response(sources=[_make_citation(section_title="Executive Summary")]))
        assert "Executive Summary" in result

    def test_section_title_omitted_when_absent(self) -> None:
        result = _format_citations(_make_response(sources=[_make_citation(section_title=None)]))
        assert "Section:" not in result

    def test_long_quote_truncated_with_ellipsis(self) -> None:
        result = _format_citations(_make_response(sources=[_make_citation(quote_text="A" * 200)]))
        assert "\u2026" in result

    def test_short_quote_not_truncated(self) -> None:
        result = _format_citations(_make_response(sources=[_make_citation(quote_text="Short passage.")]))
        assert "\u2026" not in result

    def test_multiple_citations_numbered_sequentially(self) -> None:
        citations = [_make_citation(file_name="doc1.pdf"), _make_citation(file_name="doc2.pdf")]
        result = _format_citations(_make_response(sources=citations))
        assert "[1]" in result
        assert "[2]" in result


# ---------------------------------------------------------------------------
# TestFormatFlags
# ---------------------------------------------------------------------------


class TestFormatFlags:
    def test_no_flags_shows_clean_message(self) -> None:
        result = _format_flags(_make_response(validation_flags=[]))
        assert "No validation issues" in result

    def test_single_flag_included(self) -> None:
        result = _format_flags(_make_response(validation_flags=["Citation not verbatim."]))
        assert "Citation not verbatim." in result

    def test_multiple_flags_each_on_own_line(self) -> None:
        result = _format_flags(_make_response(validation_flags=["Flag A", "Flag B", "Flag C"]))
        assert len(result.splitlines()) == 3

    def test_flags_prefixed_with_bullet(self) -> None:
        result = _format_flags(_make_response(validation_flags=["Some flag"]))
        assert result.startswith("\u2022")


# ---------------------------------------------------------------------------
# TestHandleIndex
# ---------------------------------------------------------------------------


class TestHandleIndex:
    def test_none_path_returns_no_file_message(self) -> None:
        result = _handle_index(None)
        assert "No file selected" in result

    def test_calls_index_document_with_exact_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: list[str] = []

        def fake_index(fp, **_kw):
            captured.append(fp)
            return _make_manifest()

        monkeypatch.setattr("app.ui.index_document", fake_index)
        _handle_index("/tmp/myfile.txt")
        assert captured == ["/tmp/myfile.txt"]

    def test_success_result_contains_manifest_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.ui.index_document",
            lambda *a, **kw: _make_manifest(parent_count=4, child_count=12),
        )
        result = _handle_index("/tmp/doc.pdf")
        assert "4" in result
        assert "12" in result

    def test_service_error_returns_clean_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def failing(*a, **kw):
            raise ServiceError("index directory not writable")

        monkeypatch.setattr("app.ui.index_document", failing)
        result = _handle_index("/tmp/doc.pdf")
        assert "Indexing failed" in result
        assert "index directory not writable" in result
        assert "Traceback" not in result

    def test_service_error_does_not_propagate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def failing(*a, **kw):
            raise ServiceError("boom")

        monkeypatch.setattr("app.ui.index_document", failing)
        result = _handle_index("/tmp/x.txt")
        assert isinstance(result, str)

    def test_unexpected_exception_returns_generic_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def crashing(*a, **kw):
            raise RuntimeError("disk full")

        monkeypatch.setattr("app.ui.index_document", crashing)
        result = _handle_index("/tmp/doc.pdf")
        assert "Unexpected error" in result
        assert "disk full" in result
        assert "Traceback" not in result

    def test_unexpected_exception_does_not_propagate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def crashing(*a, **kw):
            raise OSError("permission denied")

        monkeypatch.setattr("app.ui.index_document", crashing)
        result = _handle_index("/tmp/x.txt")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# TestHandleAnswer
# ---------------------------------------------------------------------------


class TestHandleAnswer:
    def test_success_returns_answer_text(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.ui.answer_query",
            lambda *a, **kw: _make_response(answer_text="Forty-two."),
        )
        answer, _, _ = _handle_answer("What is 42?", 10, 5, "")
        assert answer == "Forty-two."

    def test_success_returns_citations_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.ui.answer_query",
            lambda *a, **kw: _make_response(sources=[_make_citation(file_name="policy.txt")]),
        )
        _, citations, _ = _handle_answer("Query?", 10, 5, "")
        assert "policy.txt" in citations

    def test_success_returns_flags_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.ui.answer_query",
            lambda *a, **kw: _make_response(validation_flags=["Unverified citation"]),
        )
        _, _, flags = _handle_answer("Query?", 10, 5, "")
        assert "Unverified citation" in flags

    def test_empty_model_string_passes_none_to_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict = {}

        def fake_answer(q, **kw):
            captured.update(kw)
            return _make_response()

        monkeypatch.setattr("app.ui.answer_query", fake_answer)
        _handle_answer("Q?", 10, 5, "")
        assert captured["model"] is None

    def test_whitespace_model_string_passes_none_to_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict = {}

        def fake_answer(q, **kw):
            captured.update(kw)
            return _make_response()

        monkeypatch.setattr("app.ui.answer_query", fake_answer)
        _handle_answer("Q?", 10, 5, "   ")
        assert captured["model"] is None

    def test_nonempty_model_string_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict = {}

        def fake_answer(q, **kw):
            captured.update(kw)
            return _make_response()

        monkeypatch.setattr("app.ui.answer_query", fake_answer)
        _handle_answer("Q?", 10, 5, "qwen3:8b")
        assert captured["model"] == "qwen3:8b"

    def test_retrieval_top_k_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict = {}

        def fake_answer(q, **kw):
            captured.update(kw)
            return _make_response()

        monkeypatch.setattr("app.ui.answer_query", fake_answer)
        _handle_answer("Q?", 20, 5, "")
        assert captured["retrieval_top_k"] == 20

    def test_rerank_top_k_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict = {}

        def fake_answer(q, **kw):
            captured.update(kw)
            return _make_response()

        monkeypatch.setattr("app.ui.answer_query", fake_answer)
        _handle_answer("Q?", 10, 8, "")
        assert captured["rerank_top_k"] == 8

    def test_service_error_in_first_element(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def failing(*a, **kw):
            raise ServiceError("query engine offline")

        monkeypatch.setattr("app.ui.answer_query", failing)
        answer, citations, flags = _handle_answer("Q?", 10, 5, "")
        assert "query engine offline" in answer
        assert "Query failed" in answer
        assert citations == ""
        assert flags == ""

    def test_unexpected_exception_in_first_element(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def crashing(*a, **kw):
            raise ValueError("bad input")

        monkeypatch.setattr("app.ui.answer_query", crashing)
        answer, citations, flags = _handle_answer("Q?", 10, 5, "")
        assert "Unexpected error" in answer
        assert "bad input" in answer
        assert citations == ""
        assert flags == ""

    def test_returns_three_string_elements(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.ui.answer_query",
            lambda *a, **kw: _make_response(),
        )
        result = _handle_answer("Q?", 10, 5, "")
        assert len(result) == 3
        assert all(isinstance(x, str) for x in result)


# ---------------------------------------------------------------------------
# TestBuildUi
# ---------------------------------------------------------------------------


class TestBuildUi:
    def test_returns_blocks_instance(self) -> None:
        gr = pytest.importorskip("gradio")
        from app.ui import build_ui

        demo = build_ui()
        assert isinstance(demo, gr.Blocks)
