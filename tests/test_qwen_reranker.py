"""
Tests for src/reranking/qwen_reranker.py — cross-encoder reranking layer.

Test strategy
-------------
All unit tests inject a ``_FakeReranker`` via the ``_model`` parameter —
no network access, no model download, no sentence-transformers inference.
The fake reranker is constructed with a predetermined list of scores so
each test can assert exact ordering and score values.

Integration test (``TestIntegrationQwenReranker``) calls the real
``Qwen/Qwen3-Reranker-0.6B`` model. Gated by ``RERANKER_INTEGRATION_TESTS=1``
and skipped by default.
"""
from __future__ import annotations

import os
from typing import List, Sequence

import numpy as np
import pytest

from src.reranking.qwen_reranker import _DEFAULT_MODEL, _MODEL_CACHE, rerank
from src.schema.models import RetrievedChunk


# ---------------------------------------------------------------------------
# Fake model and chunk factory
# ---------------------------------------------------------------------------


class _FakeReranker:
    """
    Minimal cross-encoder stand-in for unit testing.

    ``predict`` returns the pre-configured score list, truncated to the
    number of pairs supplied so callers with fewer chunks than scores work.
    """

    def __init__(self, scores: Sequence[float]) -> None:
        self._scores = list(scores)

    def predict(self, sentence_pairs: list) -> np.ndarray:
        return np.array(self._scores[: len(sentence_pairs)], dtype=float)


def _make_chunk(
    chunk_id: str,
    *,
    text: str = "sample text",
    retrieval_method: str = "hybrid",
    vector_score: float | None = 0.8,
    bm25_score: float | None = 5.0,
    fusion_score: float | None = 0.025,
    rerank_score: float | None = None,
    parent_chunk_id: str | None = "parent-1",
    file_type: str | None = "txt",
    doc_id: str = "doc1",
    page_id: str = "page1",
    file_name: str = "test.txt",
    page_number: int = 1,
    section_title: str | None = None,
) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id=page_id,
        file_name=file_name,
        page_number=page_number,
        text=text,
        word_count=len(text.split()),
        retrieval_method=retrieval_method,  # type: ignore[arg-type]
        vector_score=vector_score,
        bm25_score=bm25_score,
        fusion_score=fusion_score,
        rerank_score=rerank_score,
        parent_chunk_id=parent_chunk_id,
        file_type=file_type,
        section_title=section_title,
    )


def _three_chunks() -> List[RetrievedChunk]:
    """Three chunks ordered by descending fusion_score (typical hybrid output)."""
    return [
        _make_chunk("c1", text="alpha text", fusion_score=0.030),
        _make_chunk("c2", text="beta text",  fusion_score=0.020),
        _make_chunk("c3", text="gamma text", fusion_score=0.010),
    ]


# ---------------------------------------------------------------------------
# TestRerankContract
# ---------------------------------------------------------------------------


class TestRerankContract:
    """Basic output contract of rerank()."""

    def test_returns_list(self) -> None:
        chunks = _three_chunks()
        result = rerank("query", chunks, _model=_FakeReranker([1.0, 0.5, 0.2]))
        assert isinstance(result, list)

    def test_output_is_list_of_retrieved_chunks(self) -> None:
        chunks = _three_chunks()
        result = rerank("query", chunks, _model=_FakeReranker([1.0, 0.5, 0.2]))
        assert all(isinstance(c, RetrievedChunk) for c in result)

    def test_rerank_score_populated_on_all_chunks(self) -> None:
        chunks = _three_chunks()
        result = rerank("query", chunks, _model=_FakeReranker([1.0, 0.5, 0.2]))
        assert all(c.rerank_score is not None for c in result)

    def test_rerank_scores_match_model_output(self) -> None:
        chunks = _three_chunks()
        # Model will score c1=1.0, c2=0.5, c3=0.2 → sorted desc → c1, c2, c3
        result = rerank("query", chunks, _model=_FakeReranker([1.0, 0.5, 0.2]))
        scores = [c.rerank_score for c in result]
        assert scores == pytest.approx([1.0, 0.5, 0.2])

    def test_sorted_descending_by_rerank_score(self) -> None:
        chunks = _three_chunks()
        result = rerank("query", chunks, _model=_FakeReranker([0.3, 0.9, 0.6]))
        scores = [c.rerank_score for c in result]
        assert scores == sorted(scores, reverse=True)

    def test_ordering_changes_from_input_order(self) -> None:
        """When model reverses relevance, output order should reverse."""
        chunks = _three_chunks()
        # Input order: c1, c2, c3 — model scores lowest-to-highest → reverses
        result = rerank("query", chunks, _model=_FakeReranker([0.1, 0.5, 0.9]))
        chunk_ids = [c.chunk_id for c in result]
        assert chunk_ids == ["c3", "c2", "c1"]

    def test_output_length_equals_input_without_top_k(self) -> None:
        chunks = _three_chunks()
        result = rerank("query", chunks, _model=_FakeReranker([1.0, 0.5, 0.2]))
        assert len(result) == 3

    def test_tie_breaking_preserves_input_order(self) -> None:
        """Equal rerank scores → stable sort preserves input (fusion) order."""
        chunks = _three_chunks()
        # All same rerank score
        result = rerank("query", chunks, _model=_FakeReranker([0.5, 0.5, 0.5]))
        chunk_ids = [c.chunk_id for c in result]
        assert chunk_ids == ["c1", "c2", "c3"]


# ---------------------------------------------------------------------------
# TestScorePreservation
# ---------------------------------------------------------------------------


class TestScorePreservation:
    """All pre-existing score fields and metadata must survive reranking."""

    def test_vector_score_preserved(self) -> None:
        chunk = _make_chunk("c1", vector_score=0.91)
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].vector_score == pytest.approx(0.91)

    def test_bm25_score_preserved(self) -> None:
        chunk = _make_chunk("c1", bm25_score=7.42)
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].bm25_score == pytest.approx(7.42)

    def test_fusion_score_preserved(self) -> None:
        chunk = _make_chunk("c1", fusion_score=0.031)
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].fusion_score == pytest.approx(0.031)

    def test_retrieval_method_preserved(self) -> None:
        chunk = _make_chunk("c1", retrieval_method="hybrid")
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].retrieval_method == "hybrid"

    def test_chunk_id_preserved(self) -> None:
        chunk = _make_chunk("unique-id-42")
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].chunk_id == "unique-id-42"

    def test_parent_chunk_id_preserved(self) -> None:
        chunk = _make_chunk("c1", parent_chunk_id="parent-99")
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].parent_chunk_id == "parent-99"

    def test_none_parent_chunk_id_preserved(self) -> None:
        chunk = _make_chunk("c1", parent_chunk_id=None)
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].parent_chunk_id is None

    def test_file_type_preserved(self) -> None:
        chunk = _make_chunk("c1", file_type="pdf")
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].file_type == "pdf"

    def test_section_title_preserved(self) -> None:
        chunk = _make_chunk("c1", section_title="Introduction")
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].section_title == "Introduction"

    def test_none_scores_on_dense_only_chunk_preserved(self) -> None:
        """Dense-only chunk has bm25_score=None and fusion_score=None."""
        chunk = _make_chunk("c1", bm25_score=None, fusion_score=None,
                             retrieval_method="vector")
        result = rerank("q", [chunk], _model=_FakeReranker([0.7]))
        assert result[0].bm25_score is None
        assert result[0].fusion_score is None

    def test_input_chunks_not_mutated(self) -> None:
        """rerank must not mutate the input list or its chunk objects."""
        chunks = _three_chunks()
        original_ids = [id(c) for c in chunks]
        original_scores = [c.rerank_score for c in chunks]
        rerank("q", chunks, _model=_FakeReranker([0.1, 0.5, 0.9]))
        assert [id(c) for c in chunks] == original_ids
        assert [c.rerank_score for c in chunks] == original_scores


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Boundary and edge cases."""

    def test_empty_input_returns_empty_list(self) -> None:
        result = rerank("query", [], _model=_FakeReranker([]))
        assert result == []

    def test_single_chunk_input(self) -> None:
        chunk = _make_chunk("c1", text="only chunk")
        result = rerank("query", [chunk], _model=_FakeReranker([2.5]))
        assert len(result) == 1
        assert result[0].chunk_id == "c1"
        assert result[0].rerank_score == pytest.approx(2.5)

    def test_top_k_respected(self) -> None:
        chunks = _three_chunks()
        result = rerank("q", chunks, top_k=2, _model=_FakeReranker([1.0, 0.5, 0.2]))
        assert len(result) == 2

    def test_top_k_keeps_highest_scores(self) -> None:
        chunks = _three_chunks()
        result = rerank("q", chunks, top_k=2, _model=_FakeReranker([0.3, 0.9, 0.6]))
        # sorted: c2=0.9, c3=0.6, c1=0.3 → top_k=2 → c2, c3
        assert result[0].chunk_id == "c2"
        assert result[1].chunk_id == "c3"

    def test_top_k_larger_than_chunks_returns_all(self) -> None:
        chunks = _three_chunks()
        result = rerank("q", chunks, top_k=100, _model=_FakeReranker([1.0, 0.5, 0.2]))
        assert len(result) == 3

    def test_top_k_zero_returns_empty(self) -> None:
        chunks = _three_chunks()
        result = rerank("q", chunks, top_k=0, _model=_FakeReranker([1.0, 0.5, 0.2]))
        assert result == []

    def test_negative_rerank_scores_allowed(self) -> None:
        """Reranker logits can be negative; still sorted and preserved."""
        chunks = _three_chunks()
        result = rerank("q", chunks, _model=_FakeReranker([-0.1, -0.5, -0.9]))
        scores = [c.rerank_score for c in result]
        assert scores == sorted(scores, reverse=True)
        assert all(s is not None for s in scores)

    def test_rerank_score_is_float(self) -> None:
        chunk = _make_chunk("c1")
        result = rerank("q", [chunk], _model=_FakeReranker([3]))
        assert isinstance(result[0].rerank_score, float)


# ---------------------------------------------------------------------------
# TestQueryPairing
# ---------------------------------------------------------------------------


class TestQueryPairing:
    """Verify the (query, chunk.text) pairs sent to the model are correct."""

    def test_query_appears_in_each_pair(self) -> None:
        captured: list = []

        class _CapturingReranker:
            def predict(self, pairs):
                captured.extend(pairs)
                return np.array([1.0] * len(pairs))

        query = "test query string"
        chunks = [_make_chunk("c1", text="first"), _make_chunk("c2", text="second")]
        rerank(query, chunks, _model=_CapturingReranker())

        assert len(captured) == 2
        assert all(pair[0] == query for pair in captured)

    def test_chunk_text_appears_in_pairs(self) -> None:
        captured: list = []

        class _CapturingReranker:
            def predict(self, pairs):
                captured.extend(pairs)
                return np.array([1.0] * len(pairs))

        chunks = [
            _make_chunk("c1", text="document alpha"),
            _make_chunk("c2", text="document beta"),
        ]
        rerank("q", chunks, _model=_CapturingReranker())

        texts = [pair[1] for pair in captured]
        assert texts == ["document alpha", "document beta"]

    def test_pair_order_matches_chunk_order(self) -> None:
        """Pairs are submitted in input chunk order so scores align correctly."""
        captured: list = []

        class _CapturingReranker:
            def predict(self, pairs):
                captured.extend(pairs)
                return np.array([0.5] * len(pairs))

        chunks = [
            _make_chunk("c1", text="first chunk"),
            _make_chunk("c2", text="second chunk"),
            _make_chunk("c3", text="third chunk"),
        ]
        rerank("myquery", chunks, _model=_CapturingReranker())

        assert captured[0] == ("myquery", "first chunk")
        assert captured[1] == ("myquery", "second chunk")
        assert captured[2] == ("myquery", "third chunk")


# ---------------------------------------------------------------------------
# TestIntegrationQwenReranker (gated — requires RERANKER_INTEGRATION_TESTS=1)
# ---------------------------------------------------------------------------


RERANKER_INTEGRATION = os.getenv("RERANKER_INTEGRATION_TESTS", "0") == "1"


@pytest.mark.skipif(
    not RERANKER_INTEGRATION,
    reason="Set RERANKER_INTEGRATION_TESTS=1 to run real model tests",
)
class TestIntegrationQwenReranker:
    """
    End-to-end test using the real Qwen3-Reranker-0.6B cross-encoder.

    Requires network access and ~1.2 GB model download on first run.
    The model is cached in ~/.cache/huggingface/hub/ after the first run.
    """

    def test_rerank_with_real_model_returns_scored_chunks(self) -> None:
        # Clear cache to ensure fresh load in integration context
        _MODEL_CACHE.clear()

        chunks = [
            _make_chunk("c1", text="The quarterly revenue increased by 12 percent."),
            _make_chunk("c2", text="The weather forecast predicts rain tomorrow."),
            _make_chunk("c3", text="Annual profit margins showed strong improvement."),
        ]

        result = rerank(
            query="financial performance",
            chunks=chunks,
            model_name=_DEFAULT_MODEL,
        )

        assert len(result) == 3
        assert all(isinstance(c, RetrievedChunk) for c in result)
        assert all(c.rerank_score is not None for c in result)

        # Financial chunks should score higher than weather chunk
        financial_ids = {"c1", "c3"}
        weather_id = "c2"
        top_two_ids = {result[0].chunk_id, result[1].chunk_id}
        assert top_two_ids == financial_ids, (
            f"Expected financial chunks at top, got: {[c.chunk_id for c in result]}"
        )
