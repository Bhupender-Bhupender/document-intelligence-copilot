"""
Tests for src/retrieval/hybrid_retriever.py — RRF fusion and deduplication.

Test strategy
-------------
Unit tests (the majority) use synthetic ``RetrievedChunk`` objects built
with a factory helper. No index is needed; ``_rrf_fuse`` is a pure function
tested directly.

Integration tests (``TestIntegrationHybridRetrieval``) call the full
``retrieve_hybrid`` public API against a real index built in a temp
directory. These are gated by the ``INTEGRATION_TESTS=1`` environment
variable and skipped by default in CI.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List

import pytest

from src.retrieval.hybrid_retriever import _rrf_fuse, retrieve_hybrid
from src.retrieval.vector_retriever import lookup_parents
from src.schema.models import RetrievedChunk


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_chunk(
    chunk_id: str,
    *,
    doc_id: str = "doc1",
    page_id: str = "page1",
    file_name: str = "test.txt",
    page_number: int = 1,
    text: str = "sample text",
    retrieval_method: str = "vector",
    vector_score: float | None = None,
    bm25_score: float | None = None,
    parent_chunk_id: str | None = "parent-1",
    file_type: str | None = "txt",
    section_title: str | None = None,
) -> RetrievedChunk:
    """Build a synthetic RetrievedChunk for testing."""
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
        parent_chunk_id=parent_chunk_id,
        file_type=file_type,
        section_title=section_title,
    )


def _make_dense_list(chunk_ids: List[str]) -> List[RetrievedChunk]:
    """Build a dense-style ranked list from a sequence of chunk IDs."""
    return [
        _make_chunk(cid, retrieval_method="vector", vector_score=1.0 - i * 0.1)
        for i, cid in enumerate(chunk_ids)
    ]


def _make_sparse_list(chunk_ids: List[str]) -> List[RetrievedChunk]:
    """Build a sparse-style ranked list from a sequence of chunk IDs."""
    return [
        _make_chunk(cid, retrieval_method="bm25", bm25_score=10.0 - i * 1.0)
        for i, cid in enumerate(chunk_ids)
    ]


_RRF_K = 60  # default constant used throughout tests


# ---------------------------------------------------------------------------
# TestRrfFuseContract
# ---------------------------------------------------------------------------


class TestRrfFuseContract:
    """Basic output contract of _rrf_fuse."""

    def test_returns_list(self) -> None:
        dense = _make_dense_list(["c1", "c2"])
        sparse = _make_sparse_list(["c3", "c4"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        assert isinstance(result, list)

    def test_all_retrieval_method_hybrid(self) -> None:
        dense = _make_dense_list(["c1", "c2"])
        sparse = _make_sparse_list(["c3", "c4"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        assert all(c.retrieval_method == "hybrid" for c in result)

    def test_all_fusion_score_positive(self) -> None:
        dense = _make_dense_list(["c1", "c2"])
        sparse = _make_sparse_list(["c3", "c4"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        assert all(c.fusion_score is not None and c.fusion_score > 0.0 for c in result)

    def test_sorted_descending_by_fusion_score(self) -> None:
        dense = _make_dense_list(["c1", "c2", "c3"])
        sparse = _make_sparse_list(["c4", "c5", "c6"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        scores = [c.fusion_score for c in result]
        assert scores == sorted(scores, reverse=True)

    def test_top_k_respected(self) -> None:
        dense = _make_dense_list(["c1", "c2", "c3", "c4", "c5"])
        sparse = _make_sparse_list(["c6", "c7", "c8", "c9", "c10"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=3)
        assert len(result) == 3

    def test_top_k_larger_than_total_returns_all(self) -> None:
        dense = _make_dense_list(["c1", "c2"])
        sparse = _make_sparse_list(["c3"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=100)
        # 3 unique chunk IDs total
        assert len(result) == 3

    def test_chunk_in_both_lists_outscores_single_path_chunk(self) -> None:
        # "shared" appears as rank 1 in both lists
        # "dense_only" appears only in dense at rank 2
        dense = _make_dense_list(["shared", "dense_only"])
        sparse = _make_sparse_list(["shared", "sparse_only"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        shared = next(c for c in result if c.chunk_id == "shared")
        dense_only = next(c for c in result if c.chunk_id == "dense_only")
        sparse_only = next(c for c in result if c.chunk_id == "sparse_only")
        assert shared.fusion_score > dense_only.fusion_score
        assert shared.fusion_score > sparse_only.fusion_score

    def test_rrf_score_arithmetic(self) -> None:
        # Rank 1 in both lists: score = 1/(60+1) + 1/(60+1) = 2/61
        dense = _make_dense_list(["c1"])
        sparse = _make_sparse_list(["c1"])
        result = _rrf_fuse(dense, sparse, rrf_k=60, top_k=10)
        assert len(result) == 1
        expected = 2.0 / 61.0
        assert abs(result[0].fusion_score - expected) < 1e-9

    def test_unique_chunk_ids_in_output(self) -> None:
        dense = _make_dense_list(["c1", "c2", "c3"])
        sparse = _make_sparse_list(["c2", "c3", "c4"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        ids = [c.chunk_id for c in result]
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# TestDeduplication
# ---------------------------------------------------------------------------


class TestDeduplication:
    """chunk_id-based deduplication and metadata preservation."""

    def test_duplicate_chunk_id_appears_once(self) -> None:
        dense = _make_dense_list(["c1", "c2"])
        sparse = _make_sparse_list(["c1", "c3"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        ids = [c.chunk_id for c in result]
        assert ids.count("c1") == 1

    def test_merged_chunk_preserves_vector_score(self) -> None:
        dense = [_make_chunk("c1", retrieval_method="vector", vector_score=0.9)]
        sparse = [_make_chunk("c1", retrieval_method="bm25", bm25_score=5.5)]
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        merged = result[0]
        assert merged.vector_score == pytest.approx(0.9)

    def test_merged_chunk_preserves_bm25_score(self) -> None:
        dense = [_make_chunk("c1", retrieval_method="vector", vector_score=0.9)]
        sparse = [_make_chunk("c1", retrieval_method="bm25", bm25_score=5.5)]
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        merged = result[0]
        assert merged.bm25_score == pytest.approx(5.5)

    def test_unique_dense_chunk_preserved(self) -> None:
        dense = _make_dense_list(["c1", "c2"])
        sparse = _make_sparse_list(["c3"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        ids = {c.chunk_id for c in result}
        assert "c2" in ids

    def test_unique_sparse_chunk_preserved(self) -> None:
        dense = _make_dense_list(["c1"])
        sparse = _make_sparse_list(["c1", "c2"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        ids = {c.chunk_id for c in result}
        assert "c2" in ids

    def test_file_type_preserved_on_merged_chunk(self) -> None:
        dense = [_make_chunk("c1", retrieval_method="vector", vector_score=0.8, file_type="pdf")]
        sparse = [_make_chunk("c1", retrieval_method="bm25", bm25_score=3.0, file_type="pdf")]
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        assert result[0].file_type == "pdf"

    def test_file_type_preserved_on_sparse_only_chunk(self) -> None:
        dense = _make_dense_list(["c1"])
        sparse = [_make_chunk("c2", retrieval_method="bm25", bm25_score=4.0, file_type="docx")]
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        c2 = next(c for c in result if c.chunk_id == "c2")
        assert c2.file_type == "docx"

    def test_parent_chunk_id_preserved_on_merged_chunk(self) -> None:
        dense = [_make_chunk("c1", retrieval_method="vector", vector_score=0.7, parent_chunk_id="parent-42")]
        sparse = [_make_chunk("c1", retrieval_method="bm25", bm25_score=2.0, parent_chunk_id="parent-42")]
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        assert result[0].parent_chunk_id == "parent-42"

    def test_parent_chunk_id_preserved_sparse_only(self) -> None:
        dense = _make_dense_list(["c1"])
        sparse = [_make_chunk("c2", retrieval_method="bm25", bm25_score=6.0, parent_chunk_id="parent-99")]
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        c2 = next(c for c in result if c.chunk_id == "c2")
        assert c2.parent_chunk_id == "parent-99"

    def test_section_title_preserved(self) -> None:
        dense = [_make_chunk("c1", retrieval_method="vector", vector_score=0.5, section_title="Intro")]
        sparse = _make_sparse_list(["c2"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        c1 = next(c for c in result if c.chunk_id == "c1")
        assert c1.section_title == "Intro"

    def test_dense_only_chunk_bm25_score_is_none(self) -> None:
        dense = _make_dense_list(["c1"])
        sparse = _make_sparse_list(["c2"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        c1 = next(c for c in result if c.chunk_id == "c1")
        assert c1.bm25_score is None

    def test_sparse_only_chunk_vector_score_is_none(self) -> None:
        dense = _make_dense_list(["c1"])
        sparse = _make_sparse_list(["c2"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        c2 = next(c for c in result if c.chunk_id == "c2")
        assert c2.vector_score is None


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge and boundary cases for _rrf_fuse."""

    def test_both_empty_returns_empty_list(self) -> None:
        result = _rrf_fuse([], [], rrf_k=_RRF_K, top_k=10)
        assert result == []

    def test_dense_empty_sparse_nonempty(self) -> None:
        sparse = _make_sparse_list(["c1", "c2"])
        result = _rrf_fuse([], sparse, rrf_k=_RRF_K, top_k=10)
        assert len(result) == 2
        assert all(c.retrieval_method == "hybrid" for c in result)

    def test_sparse_empty_dense_nonempty(self) -> None:
        dense = _make_dense_list(["c1", "c2"])
        result = _rrf_fuse(dense, [], rrf_k=_RRF_K, top_k=10)
        assert len(result) == 2
        assert all(c.retrieval_method == "hybrid" for c in result)

    def test_all_same_chunk_id_returns_one_result(self) -> None:
        dense = [
            _make_chunk("shared", retrieval_method="vector", vector_score=0.9),
            _make_chunk("shared", retrieval_method="vector", vector_score=0.8),
        ]
        sparse = [_make_chunk("shared", retrieval_method="bm25", bm25_score=7.0)]
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        assert len(result) == 1
        assert result[0].chunk_id == "shared"

    def test_top_k_zero_returns_empty(self) -> None:
        dense = _make_dense_list(["c1", "c2"])
        sparse = _make_sparse_list(["c3"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=0)
        assert result == []

    def test_single_dense_result(self) -> None:
        dense = [_make_chunk("solo", retrieval_method="vector", vector_score=0.95)]
        result = _rrf_fuse(dense, [], rrf_k=_RRF_K, top_k=10)
        assert len(result) == 1
        assert result[0].chunk_id == "solo"
        assert result[0].retrieval_method == "hybrid"
        expected_score = 1.0 / (_RRF_K + 1)
        assert abs(result[0].fusion_score - expected_score) < 1e-9

    def test_single_sparse_result(self) -> None:
        sparse = [_make_chunk("solo", retrieval_method="bm25", bm25_score=8.0)]
        result = _rrf_fuse([], sparse, rrf_k=_RRF_K, top_k=10)
        assert len(result) == 1
        assert result[0].chunk_id == "solo"
        assert result[0].retrieval_method == "hybrid"

    def test_output_is_list_of_retrieved_chunks(self) -> None:
        dense = _make_dense_list(["c1"])
        sparse = _make_sparse_list(["c2"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        assert all(isinstance(c, RetrievedChunk) for c in result)


# ---------------------------------------------------------------------------
# TestLookupParentsCompatibility
# ---------------------------------------------------------------------------


class TestLookupParentsCompatibility:
    """Fused output must remain passable to lookup_parents()."""

    def test_parent_chunk_id_survives_fusion(self) -> None:
        """parent_chunk_id on merged chunk must not be None after fusion."""
        dense = [_make_chunk("c1", retrieval_method="vector", vector_score=0.7, parent_chunk_id="p-10")]
        sparse = [_make_chunk("c1", retrieval_method="bm25", bm25_score=3.0, parent_chunk_id="p-10")]
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        assert result[0].parent_chunk_id == "p-10"

    def test_none_parent_chunk_id_preserved(self) -> None:
        """Flat chunks (parent_chunk_id=None) must remain None after fusion."""
        dense = [_make_chunk("c1", retrieval_method="vector", vector_score=0.7, parent_chunk_id=None)]
        sparse = _make_sparse_list(["c2"])
        result = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)
        c1 = next(c for c in result if c.chunk_id == "c1")
        assert c1.parent_chunk_id is None

    def test_lookup_parents_accepts_fused_results(self, tmp_path: Path) -> None:
        """
        lookup_parents() must accept fused results without type errors.

        Uses a nonexistent index dir. lookup_parents raises FileNotFoundError
        when the parent store is absent — that is the expected contract.
        The test verifies the fused result list is structurally compatible
        (no AttributeError or TypeError from the fused RetrievedChunk fields).
        """
        fake_index_dir = tmp_path / "no_index"
        dense = [_make_chunk("c1", retrieval_method="vector", vector_score=0.8, parent_chunk_id="p-1")]
        sparse = [_make_chunk("c2", retrieval_method="bm25", bm25_score=5.0, parent_chunk_id="p-2")]
        fused = _rrf_fuse(dense, sparse, rrf_k=_RRF_K, top_k=10)

        # FileNotFoundError is the correct contract when no index exists;
        # a TypeError or AttributeError would indicate a schema incompatibility.
        with pytest.raises(FileNotFoundError):
            lookup_parents(fused, index_dir=fake_index_dir)


# ---------------------------------------------------------------------------
# TestIntegrationHybridRetrieval (gated — requires INTEGRATION_TESTS=1)
# ---------------------------------------------------------------------------


INTEGRATION = os.getenv("INTEGRATION_TESTS", "0") == "1"


@pytest.mark.skipif(not INTEGRATION, reason="Set INTEGRATION_TESTS=1 to run")
class TestIntegrationHybridRetrieval:
    """
    End-to-end tests against a real persisted index.

    These tests call retrieve_hybrid() against data/index/ and require
    a fully built index (run build_indexes() first).
    """

    def test_retrieve_hybrid_returns_list(self) -> None:
        result = retrieve_hybrid("policy", top_k=5)
        assert isinstance(result, list)

    def test_retrieve_hybrid_all_hybrid_method(self) -> None:
        result = retrieve_hybrid("policy", top_k=5)
        assert all(c.retrieval_method == "hybrid" for c in result)

    def test_retrieve_hybrid_fusion_scores_positive(self) -> None:
        result = retrieve_hybrid("policy", top_k=5)
        assert all(c.fusion_score is not None and c.fusion_score > 0 for c in result)
