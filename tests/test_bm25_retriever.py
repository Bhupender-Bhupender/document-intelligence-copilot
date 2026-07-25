"""
Tests for src/retrieval/bm25_retriever.py

All tests use isolated output directories (tmp_path / tmp_path_factory) and
build the child index with MockEmbedding. No test writes to the project index
directory (data/index/) and no test triggers a real model download unless
INTEGRATION_TESTS=1 is set.

Test classes
------------
    TestRetrieveChildrenBm25Contract  — shape, top_k, scoring, metadata fields
    TestBm25LookupParentsCompatibility — sparse results pass through lookup_parents()
    TestBm25RetrieveEdgeCases         — oversized top_k, zero-match, FileNotFoundError
    TestIntegrationBm25RealCorpus     — gated by INTEGRATION_TESTS=1; real corpus
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import pytest

from llama_index.core.embeddings.mock_embed_model import MockEmbedding

from src.indexing.index_builder import build_indexes
from src.schema.models import DocumentChunk, RetrievedChunk
from src.retrieval.bm25_retriever import retrieve_children_bm25
from src.retrieval.vector_retriever import lookup_parents

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EMBED = MockEmbedding(embed_dim=384)

_SAMPLE_TXT = (
    Path(__file__).resolve().parent.parent / "docs" / "sample_docs" / "company_policy.txt"
)

_INTEGRATION = bool(os.environ.get("INTEGRATION_TESTS"))

# Distinctive tokens used in fixture chunks so BM25 queries reliably match.
_UNIQUE_TERMS = ["zyphron", "valquix", "mordecai"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_parent(n: int) -> DocumentChunk:
    """Build a minimal parent DocumentChunk with a distinctive term in the text."""
    cid = f"bm25-parent-{n}"
    return DocumentChunk(
        chunk_id=cid,
        doc_id="bm25-doc-001",
        page_id=f"bm25-page-{n}",
        page_number=n,
        file_name="bm25_test.txt",
        file_type="txt",
        section_title=f"BM25 Section {n}",
        text=(
            f"Parent chunk {n} discusses the concept of "
            f"{_UNIQUE_TERMS[(n - 1) % len(_UNIQUE_TERMS)]} in detail. "
            f"This passage contains enough words to be a meaningful context."
        ),
        word_count=20,
        chunk_index=n - 1,
        chunk_level="parent",
        parent_chunk_id=None,
    )


def _make_child(n: int, parent_id: str, parent_n: int) -> DocumentChunk:
    """Build a child DocumentChunk with the same distinctive term as its parent."""
    return DocumentChunk(
        chunk_id=f"bm25-child-{n}",
        doc_id="bm25-doc-001",
        page_id=f"bm25-page-{parent_n}",
        page_number=parent_n,
        file_name="bm25_test.txt",
        file_type="txt",
        section_title=f"BM25 Section {parent_n}",
        text=(
            f"Child chunk {n} is a detailed sub-passage about "
            f"{_UNIQUE_TERMS[(parent_n - 1) % len(_UNIQUE_TERMS)]}. "
            f"It expands on the parent context with specific information."
        ),
        word_count=18,
        chunk_index=n - 1,
        chunk_level="child",
        parent_chunk_id=parent_id,
    )


# ---------------------------------------------------------------------------
# Module-scope fixture: build index once, reuse across all test classes
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def built_bm25_index(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    Build a small child + parent index once for the whole module.

    Returns the index_dir (a tmp_path scoped to the module).
    Three parents, six children (two children per parent).
    Each parent/child pair shares a distinctive term for reliable BM25 matching.
    """
    index_dir = tmp_path_factory.mktemp("bm25_index")

    parents: List[DocumentChunk] = [_make_parent(i) for i in range(1, 4)]
    children: List[DocumentChunk] = []
    for i, p in enumerate(parents, start=1):
        children.append(_make_child(2 * i - 1, p.chunk_id, i))
        children.append(_make_child(2 * i, p.chunk_id, i))

    build_indexes(
        parent_chunks=parents,
        child_chunks=children,
        index_dir=index_dir,
        embed_model=_EMBED,
    )
    return index_dir


# ---------------------------------------------------------------------------
# TestRetrieveChildrenBm25Contract
# ---------------------------------------------------------------------------


class TestRetrieveChildrenBm25Contract:
    """
    Verify the shape, types, and field coverage of retrieve_children_bm25().
    """

    def test_returns_list(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index)
        assert isinstance(result, list)

    def test_items_are_retrieved_chunk_instances(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index)
        for item in result:
            assert isinstance(item, RetrievedChunk)

    def test_retrieval_method_is_bm25(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index)
        assert len(result) > 0
        for item in result:
            assert item.retrieval_method == "bm25"

    def test_bm25_score_is_positive_float(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index)
        assert len(result) > 0
        for item in result:
            assert isinstance(item.bm25_score, float)
            assert item.bm25_score > 0.0

    def test_vector_score_is_none(self, built_bm25_index: Path) -> None:
        """BM25 path never sets vector_score."""
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index)
        assert len(result) > 0
        for item in result:
            assert item.vector_score is None

    def test_fusion_rerank_scores_are_none(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index)
        for item in result:
            assert item.fusion_score is None
            assert item.rerank_score is None

    def test_top_k_limits_results(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=1)
        assert len(result) <= 1

    def test_top_k_one_returns_one_result(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=1)
        assert len(result) == 1

    def test_default_top_k_returns_up_to_five(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index)
        assert len(result) <= 5

    def test_chunk_id_populated(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        for item in result:
            assert item.chunk_id

    def test_doc_id_populated(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        for item in result:
            assert item.doc_id == "bm25-doc-001"

    def test_page_id_populated(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        for item in result:
            assert item.page_id

    def test_page_number_is_int(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        for item in result:
            assert isinstance(item.page_number, int)

    def test_file_name_populated(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        for item in result:
            assert item.file_name == "bm25_test.txt"

    def test_file_type_populated(self, built_bm25_index: Path) -> None:
        """file_type must survive the metadata round-trip through the docstore."""
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        assert len(result) > 0
        for item in result:
            assert item.file_type == "txt"

    def test_section_title_string_or_none(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        for item in result:
            assert item.section_title is None or isinstance(item.section_title, str)

    def test_text_is_nonempty_string(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        for item in result:
            assert isinstance(item.text, str)
            assert len(item.text) > 0

    def test_word_count_matches_text(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        for item in result:
            assert item.word_count == len(item.text.split())

    def test_parent_chunk_id_populated(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        assert len(result) > 0
        for item in result:
            assert item.parent_chunk_id is not None
            assert item.parent_chunk_id.startswith("bm25-parent-")

    def test_chunk_ids_are_unique(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=6)
        ids = [r.chunk_id for r in result]
        assert len(ids) == len(set(ids))

    def test_results_ordered_by_descending_score(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=6)
        if len(result) >= 2:
            scores = [r.bm25_score for r in result]
            assert scores == sorted(scores, reverse=True)

    def test_lexical_specificity_zyphron(self, built_bm25_index: Path) -> None:
        """Query with distinctive term from group 1 should return matching chunks."""
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=6)
        assert any("zyphron" in r.text.lower() for r in result)

    def test_lexical_specificity_valquix(self, built_bm25_index: Path) -> None:
        """Query with distinctive term from group 2 should return matching chunks."""
        result = retrieve_children_bm25("valquix", index_dir=built_bm25_index, top_k=6)
        assert any("valquix" in r.text.lower() for r in result)


# ---------------------------------------------------------------------------
# TestBm25LookupParentsCompatibility
# ---------------------------------------------------------------------------


class TestBm25LookupParentsCompatibility:
    """
    Verify sparse-retrieved child chunks are compatible with lookup_parents().
    """

    def test_lookup_parents_accepts_bm25_results(self, built_bm25_index: Path) -> None:
        children = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        parents = lookup_parents(children, index_dir=built_bm25_index)
        assert isinstance(parents, list)

    def test_output_length_equals_input_length(self, built_bm25_index: Path) -> None:
        children = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        parents = lookup_parents(children, index_dir=built_bm25_index)
        assert len(parents) == len(children)

    def test_each_parent_is_document_chunk_or_none(self, built_bm25_index: Path) -> None:
        children = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        parents = lookup_parents(children, index_dir=built_bm25_index)
        for p in parents:
            assert p is None or isinstance(p, DocumentChunk)

    def test_parent_found_for_matching_children(self, built_bm25_index: Path) -> None:
        """All fixture children have valid parent_chunk_ids → parents should be found."""
        children = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        parents = lookup_parents(children, index_dir=built_bm25_index)
        assert all(p is not None for p in parents)

    def test_parent_chunk_level_is_parent(self, built_bm25_index: Path) -> None:
        children = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        parents = lookup_parents(children, index_dir=built_bm25_index)
        for p in parents:
            assert p is not None
            assert p.chunk_level == "parent"

    def test_parent_chunk_id_matches_child_parent_chunk_id(
        self, built_bm25_index: Path
    ) -> None:
        children = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=3)
        parents = lookup_parents(children, index_dir=built_bm25_index)
        for child, parent in zip(children, parents):
            assert parent is not None
            assert parent.chunk_id == child.parent_chunk_id

    def test_none_returned_for_missing_parent_chunk_id(
        self, built_bm25_index: Path
    ) -> None:
        """An orphan RetrievedChunk with parent_chunk_id=None → None in output."""
        orphan = RetrievedChunk(
            chunk_id="bm25-orphan-1",
            doc_id="bm25-doc-001",
            page_id="bm25-page-1",
            file_name="bm25_test.txt",
            page_number=1,
            text="Orphan BM25 chunk.",
            word_count=3,
            retrieval_method="bm25",
            bm25_score=1.5,
            parent_chunk_id=None,
        )
        result = lookup_parents([orphan], index_dir=built_bm25_index)
        assert result == [None]

    def test_none_returned_for_nonexistent_parent(self, built_bm25_index: Path) -> None:
        """parent_chunk_id pointing to a non-existent key → None."""
        ghost = RetrievedChunk(
            chunk_id="bm25-ghost-1",
            doc_id="bm25-doc-001",
            page_id="bm25-page-1",
            file_name="bm25_test.txt",
            page_number=1,
            text="Ghost BM25 chunk.",
            word_count=3,
            retrieval_method="bm25",
            bm25_score=1.5,
            parent_chunk_id="nonexistent-bm25-parent",
        )
        result = lookup_parents([ghost], index_dir=built_bm25_index)
        assert result == [None]


# ---------------------------------------------------------------------------
# TestBm25RetrieveEdgeCases
# ---------------------------------------------------------------------------


class TestBm25RetrieveEdgeCases:
    """Edge cases for retrieve_children_bm25()."""

    def test_top_k_larger_than_corpus_returns_all_matching(
        self, built_bm25_index: Path
    ) -> None:
        """top_k=100 with a query that matches all 6 children returns all 6."""
        # "chunk" appears in every fixture child text
        result = retrieve_children_bm25("chunk", index_dir=built_bm25_index, top_k=100)
        assert len(result) == 6

    def test_zero_match_query_returns_empty_list(self, built_bm25_index: Path) -> None:
        """A query with no lexical overlap with any chunk returns an empty list."""
        result = retrieve_children_bm25(
            "zzzznonexistentterm9999", index_dir=built_bm25_index, top_k=5
        )
        assert result == []

    def test_raises_file_not_found_when_index_missing(self, tmp_path: Path) -> None:
        """FileNotFoundError when child_index/ directory doesn't exist."""
        empty = tmp_path / "no_index_here"
        with pytest.raises(FileNotFoundError):
            retrieve_children_bm25("query", index_dir=empty)

    def test_empty_query_returns_empty_list(self, built_bm25_index: Path) -> None:
        """An empty query has no tokens → all BM25 scores are zero → empty list."""
        result = retrieve_children_bm25("", index_dir=built_bm25_index)
        assert result == []

    def test_top_k_two_returns_at_most_two(self, built_bm25_index: Path) -> None:
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=2)
        assert len(result) <= 2

    def test_all_results_have_positive_bm25_score(self, built_bm25_index: Path) -> None:
        """No zero-score results should appear in any output."""
        result = retrieve_children_bm25("zyphron", index_dir=built_bm25_index, top_k=100)
        for item in result:
            assert item.bm25_score is not None
            assert item.bm25_score > 0.0


# ---------------------------------------------------------------------------
# TestIntegrationBm25RealCorpus — skipped unless INTEGRATION_TESTS=1
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _INTEGRATION, reason="Set INTEGRATION_TESTS=1 to run")
class TestIntegrationBm25RealCorpus:
    """
    End-to-end BM25 retrieval on a real .txt corpus indexed with MockEmbedding.

    Validates that lexical matches on exact terms from the document text
    are reflected in the BM25 rankings.

    Requires: INTEGRATION_TESTS=1 env var.
    """

    def test_sample_file_exists(self) -> None:
        assert _SAMPLE_TXT.exists(), f"Sample file not found: {_SAMPLE_TXT}"

    def test_real_corpus_retrieves_exact_term(self, tmp_path: Path) -> None:
        from src.indexing.indexing_pipeline import run_indexing_pipeline

        run_indexing_pipeline(
            file_path=_SAMPLE_TXT,
            index_dir=tmp_path,
            embed_model=_EMBED,
        )
        result = retrieve_children_bm25("leave", index_dir=tmp_path, top_k=5)
        assert len(result) >= 1
        for item in result:
            assert item.retrieval_method == "bm25"
            assert isinstance(item.bm25_score, float)
            assert item.bm25_score > 0.0

    def test_zero_match_on_real_corpus_returns_empty(self, tmp_path: Path) -> None:
        from src.indexing.indexing_pipeline import run_indexing_pipeline

        run_indexing_pipeline(
            file_path=_SAMPLE_TXT,
            index_dir=tmp_path,
            embed_model=_EMBED,
        )
        result = retrieve_children_bm25(
            "xylophone42zzz_nonexistent", index_dir=tmp_path, top_k=5
        )
        assert result == []

    def test_bm25_parent_lookup_on_real_corpus(self, tmp_path: Path) -> None:
        from src.indexing.indexing_pipeline import run_indexing_pipeline

        run_indexing_pipeline(
            file_path=_SAMPLE_TXT,
            index_dir=tmp_path,
            embed_model=_EMBED,
        )
        children = retrieve_children_bm25("policy", index_dir=tmp_path, top_k=3)
        parents = lookup_parents(children, index_dir=tmp_path)
        assert len(parents) == len(children)
        for p in parents:
            if p is not None:
                assert isinstance(p, DocumentChunk)
                assert p.chunk_level == "parent"
