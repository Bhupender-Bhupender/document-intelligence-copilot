"""
Tests for src/retrieval/vector_retriever.py

All tests use isolated output directories (tmp_path / tmp_path_factory) and
MockEmbedding — no test writes to the project index directory (data/index/)
and no test triggers a real model download unless INTEGRATION_TESTS=1.

Test classes
------------
    TestRetrieveChildrenContract  — shape, top_k, scoring, metadata fields
    TestLookupParents             — parent returned, None cases, metadata
    TestRetrieveEdgeCases         — oversized top_k, FileNotFoundError
    TestIntegrationRealRetrieval  — gated by INTEGRATION_TESTS=1; real embedder
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import pytest

from llama_index.core.embeddings.mock_embed_model import MockEmbedding

from src.indexing.index_builder import build_indexes
from src.schema.models import DocumentChunk, RetrievedChunk
from src.retrieval.vector_retriever import lookup_parents, retrieve_children

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EMBED = MockEmbedding(embed_dim=384)

_SAMPLE_TXT = (
    Path(__file__).resolve().parent.parent / "docs" / "sample_docs" / "company_policy.txt"
)

_INTEGRATION = bool(os.environ.get("INTEGRATION_TESTS"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_parent(n: int) -> DocumentChunk:
    """Build a minimal parent DocumentChunk for test fixture purposes."""
    cid = f"parent-{n}"
    return DocumentChunk(
        chunk_id=cid,
        doc_id="doc-001",
        page_id=f"page-{n}",
        page_number=n,
        file_name="test.txt",
        file_type="txt",
        section_title=f"Section {n}",
        text=f"Parent chunk number {n} with enough words for embedding tests.",
        word_count=10,
        chunk_index=n - 1,
        chunk_level="parent",
        parent_chunk_id=None,
    )


def _make_child(n: int, parent_id: str) -> DocumentChunk:
    """Build a minimal child DocumentChunk linked to a parent."""
    return DocumentChunk(
        chunk_id=f"child-{n}",
        doc_id="doc-001",
        page_id=f"page-{(n - 1) // 2 + 1}",
        page_number=(n - 1) // 2 + 1,
        file_name="test.txt",
        file_type="txt",
        section_title=f"Section {(n - 1) // 2 + 1}",
        text=f"Child chunk {n} has detailed text about topic {n}.",
        word_count=9,
        chunk_index=n - 1,
        chunk_level="child",
        parent_chunk_id=parent_id,
    )


# ---------------------------------------------------------------------------
# Module-scope fixture: build index once, reuse across all test classes
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def built_index(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    Build a small child + parent index once for the whole module.

    Returns the index_dir (a tmp_path scoped to the module).
    Three parents, six children (two children per parent).
    """
    index_dir = tmp_path_factory.mktemp("retrieval_index")

    parents: List[DocumentChunk] = [_make_parent(i) for i in range(1, 4)]
    children: List[DocumentChunk] = []
    for i, p in enumerate(parents, start=1):
        children.append(_make_child(2 * i - 1, p.chunk_id))
        children.append(_make_child(2 * i, p.chunk_id))

    build_indexes(
        parent_chunks=parents,
        child_chunks=children,
        index_dir=index_dir,
        embed_model=_EMBED,
    )
    return index_dir


# ---------------------------------------------------------------------------
# TestRetrieveChildrenContract
# ---------------------------------------------------------------------------


class TestRetrieveChildrenContract:
    """
    Verify the shape and contract of retrieve_children() return values.

    These tests use the module-scope fixture so the index is built once.
    """

    def test_returns_list(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED)
        assert isinstance(result, list)

    def test_items_are_retrieved_chunk_instances(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED)
        for item in result:
            assert isinstance(item, RetrievedChunk)

    def test_top_k_limits_results(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=2)
        assert len(result) <= 2

    def test_default_top_k_returns_up_to_five(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED)
        assert len(result) <= 5

    def test_top_k_one_returns_one_result(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=1)
        assert len(result) == 1

    def test_retrieval_method_is_vector(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert item.retrieval_method == "vector"

    def test_vector_score_is_float_or_none(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert item.vector_score is None or isinstance(item.vector_score, float)

    def test_chunk_id_populated(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert item.chunk_id

    def test_doc_id_populated(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert item.doc_id == "doc-001"

    def test_page_number_is_int(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert isinstance(item.page_number, int)

    def test_file_name_populated(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert item.file_name == "test.txt"

    def test_text_is_string(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert isinstance(item.text, str)

    def test_word_count_is_positive_int(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert isinstance(item.word_count, int)
            assert item.word_count > 0

    def test_parent_chunk_id_populated(self, built_index: Path) -> None:
        """Children have parent_chunk_id set; it should survive the round-trip."""
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert item.parent_chunk_id is not None
            assert item.parent_chunk_id.startswith("parent-")

    def test_section_title_string_or_none(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        for item in result:
            assert item.section_title is None or isinstance(item.section_title, str)

    def test_chunk_ids_are_unique(self, built_index: Path) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=6)
        ids = [r.chunk_id for r in result]
        assert len(ids) == len(set(ids))

    def test_bm25_hybrid_scores_are_none_for_vector_retrieval(
        self, built_index: Path
    ) -> None:
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=2)
        for item in result:
            assert item.bm25_score is None
            assert item.fusion_score is None
            assert item.rerank_score is None


# ---------------------------------------------------------------------------
# TestLookupParents
# ---------------------------------------------------------------------------


class TestLookupParents:
    """
    Verify lookup_parents() returns parallel Optional[DocumentChunk] list.
    """

    def test_returns_list(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=2)
        result = lookup_parents(children, index_dir=built_index)
        assert isinstance(result, list)

    def test_output_length_equals_input_length(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        result = lookup_parents(children, index_dir=built_index)
        assert len(result) == len(children)

    def test_each_item_is_document_chunk_or_none(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        result = lookup_parents(children, index_dir=built_index)
        for item in result:
            assert item is None or isinstance(item, DocumentChunk)

    def test_parent_found_for_every_child(self, built_index: Path) -> None:
        """All children have valid parent_chunk_ids → all lookups should succeed."""
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=6)
        result = lookup_parents(children, index_dir=built_index)
        assert all(p is not None for p in result)

    def test_parent_chunk_level_is_parent(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        parents = lookup_parents(children, index_dir=built_index)
        for p in parents:
            assert p is not None
            assert p.chunk_level == "parent"

    def test_parent_chunk_id_matches_child_parent_chunk_id(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        parents = lookup_parents(children, index_dir=built_index)
        for child, parent in zip(children, parents):
            assert parent is not None
            assert parent.chunk_id == child.parent_chunk_id

    def test_parent_text_is_nonempty_string(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        parents = lookup_parents(children, index_dir=built_index)
        for p in parents:
            assert p is not None
            assert isinstance(p.text, str)
            assert len(p.text) > 0

    def test_parent_word_count_is_positive(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        parents = lookup_parents(children, index_dir=built_index)
        for p in parents:
            assert p is not None
            assert p.word_count > 0

    def test_parent_word_count_matches_text(self, built_index: Path) -> None:
        """word_count must equal len(text.split()) since it's derived at load time."""
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        parents = lookup_parents(children, index_dir=built_index)
        for p in parents:
            assert p is not None
            assert p.word_count == len(p.text.split())

    def test_parent_file_name_preserved(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        parents = lookup_parents(children, index_dir=built_index)
        for p in parents:
            assert p is not None
            assert p.file_name == "test.txt"

    def test_parent_doc_id_preserved(self, built_index: Path) -> None:
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=3)
        parents = lookup_parents(children, index_dir=built_index)
        for p in parents:
            assert p is not None
            assert p.doc_id == "doc-001"

    def test_none_returned_when_parent_chunk_id_is_none(self, built_index: Path) -> None:
        """RetrievedChunk with parent_chunk_id=None → None in output."""
        orphan = RetrievedChunk(
            chunk_id="orphan-1",
            doc_id="doc-001",
            page_id="page-1",
            file_name="test.txt",
            page_number=1,
            text="Orphan text.",
            word_count=2,
            parent_chunk_id=None,
        )
        result = lookup_parents([orphan], index_dir=built_index)
        assert result == [None]

    def test_none_returned_when_parent_not_in_store(self, built_index: Path) -> None:
        """parent_chunk_id that doesn't exist in the store → None."""
        ghost = RetrievedChunk(
            chunk_id="ghost-1",
            doc_id="doc-001",
            page_id="page-1",
            file_name="test.txt",
            page_number=1,
            text="Ghost text.",
            word_count=2,
            parent_chunk_id="nonexistent-parent-id",
        )
        result = lookup_parents([ghost], index_dir=built_index)
        assert result == [None]

    def test_empty_input_returns_empty_list(self, built_index: Path) -> None:
        result = lookup_parents([], index_dir=built_index)
        assert result == []

    def test_mixed_valid_and_missing_parents(self, built_index: Path) -> None:
        """Mix of a valid child and an orphan chunk → [DocumentChunk, None]."""
        children = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=1)
        orphan = RetrievedChunk(
            chunk_id="orphan-mix",
            doc_id="doc-001",
            page_id="page-1",
            file_name="test.txt",
            page_number=1,
            text="Mixed orphan.",
            word_count=2,
            parent_chunk_id=None,
        )
        mixed = children + [orphan]
        result = lookup_parents(mixed, index_dir=built_index)
        assert len(result) == 2
        assert isinstance(result[0], DocumentChunk)
        assert result[1] is None


# ---------------------------------------------------------------------------
# TestRetrieveEdgeCases
# ---------------------------------------------------------------------------


class TestRetrieveEdgeCases:
    """Edge cases for retrieve_children() and lookup_parents()."""

    def test_top_k_larger_than_index_size_returns_all_nodes(
        self, built_index: Path
    ) -> None:
        """Index has 6 children; top_k=100 should return all 6."""
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED, top_k=100)
        assert len(result) == 6

    def test_retrieve_children_raises_on_missing_index(
        self, tmp_path: Path
    ) -> None:
        """FileNotFoundError when the index directory doesn't exist."""
        empty = tmp_path / "no_index_here"
        with pytest.raises(FileNotFoundError):
            retrieve_children("query", index_dir=empty, embed_model=_EMBED)

    def test_lookup_parents_raises_on_missing_store(self, tmp_path: Path) -> None:
        """FileNotFoundError when the parent store doesn't exist."""
        empty = tmp_path / "no_store_here"
        orphan = RetrievedChunk(
            chunk_id="c-1",
            doc_id="doc-001",
            page_id="page-1",
            file_name="test.txt",
            page_number=1,
            text="text",
            word_count=1,
            parent_chunk_id="p-1",
        )
        with pytest.raises(FileNotFoundError):
            lookup_parents([orphan], index_dir=empty)

    def test_lookup_parents_empty_string_parent_chunk_id_treated_as_none(
        self, built_index: Path
    ) -> None:
        """An empty-string parent_chunk_id should behave the same as None."""
        chunk = RetrievedChunk(
            chunk_id="c-empty",
            doc_id="doc-001",
            page_id="page-1",
            file_name="test.txt",
            page_number=1,
            text="text with empty parent",
            word_count=4,
            parent_chunk_id="",
        )
        result = lookup_parents([chunk], index_dir=built_index)
        assert result == [None]


# ---------------------------------------------------------------------------
# TestEmbedModelFallback — query path must use local embedder, never OpenAI
# ---------------------------------------------------------------------------


class TestEmbedModelFallback:
    """
    Regression: at query time the retrieval path must load the project's local
    embedding model instead of falling back to LlamaIndex's default OpenAI
    embedder (which raises "No API key found for OpenAI").

    These tests never download a real model — the project embedding loader
    (get_embed_model) is monkeypatched to return a MockEmbedding.
    """

    def test_retrieve_children_without_embed_model_uses_local_loader(
        self, built_index: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list = []

        def _fake_get_embed_model(model_name: Optional[str] = None) -> MockEmbedding:
            calls.append(model_name)
            return MockEmbedding(embed_dim=384)

        # load_child_index imports get_embed_model lazily from embed_config at
        # call time, so patching the attribute here is picked up.
        import src.indexing.embed_config as embed_config

        monkeypatch.setattr(embed_config, "get_embed_model", _fake_get_embed_model)

        # No embed_model passed → must resolve the local loader, not OpenAI.
        result = retrieve_children("topic", index_dir=built_index)

        assert calls == [None]  # local loader invoked exactly once
        assert isinstance(result, list)

    def test_load_child_index_attaches_local_embed_model(
        self, built_index: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from src.indexing.index_builder import load_child_index

        sentinel = MockEmbedding(embed_dim=384)

        def _fake_get_embed_model(model_name: Optional[str] = None) -> MockEmbedding:
            return sentinel

        import src.indexing.embed_config as embed_config

        monkeypatch.setattr(embed_config, "get_embed_model", _fake_get_embed_model)

        index = load_child_index(index_dir=built_index)

        # The loaded index must carry the resolved local embedder — never the
        # OpenAI default. This proves no OpenAI embedding is initialized.
        assert index._embed_model is sentinel

    def test_explicit_embed_model_bypasses_local_loader(
        self, built_index: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        called = {"count": 0}

        def _fake_get_embed_model(model_name: Optional[str] = None) -> MockEmbedding:
            called["count"] += 1
            return MockEmbedding(embed_dim=384)

        import src.indexing.embed_config as embed_config

        monkeypatch.setattr(embed_config, "get_embed_model", _fake_get_embed_model)

        # When an embed_model is passed explicitly, the local loader must NOT
        # be called (explicit dependency passing takes precedence).
        result = retrieve_children("topic", index_dir=built_index, embed_model=_EMBED)

        assert called["count"] == 0
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# TestIntegrationRealRetrieval — skipped unless INTEGRATION_TESTS=1
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _INTEGRATION, reason="Set INTEGRATION_TESTS=1 to run")
class TestIntegrationRealRetrieval:
    """
    End-to-end retrieval using a real HuggingFace embedding model.

    Builds an index with sentence-transformers/all-MiniLM-L6-v2 and runs
    retrieve_children + lookup_parents on the real company_policy.txt sample.

    Requires: INTEGRATION_TESTS=1 env var; internet access for first run.
    """

    def test_sample_file_exists(self) -> None:
        assert _SAMPLE_TXT.exists(), f"Sample file not found: {_SAMPLE_TXT}"

    def test_retrieve_children_real_model(self, tmp_path: Path) -> None:
        from src.indexing.indexing_pipeline import run_indexing_pipeline

        _real_embed_model = "sentence-transformers/all-MiniLM-L6-v2"
        from src.indexing.embed_config import get_embed_model

        real_embed = get_embed_model(_real_embed_model)
        run_indexing_pipeline(
            file_path=_SAMPLE_TXT,
            index_dir=tmp_path,
            embed_model=real_embed,
        )
        result = retrieve_children(
            "leave policy vacation days",
            index_dir=tmp_path,
            embed_model=real_embed,
            top_k=3,
        )
        assert len(result) >= 1
        assert all(isinstance(r, RetrievedChunk) for r in result)
        for r in result:
            assert r.retrieval_method == "vector"
            assert isinstance(r.vector_score, float)

    def test_lookup_parents_real_model(self, tmp_path: Path) -> None:
        from src.indexing.embed_config import get_embed_model
        from src.indexing.indexing_pipeline import run_indexing_pipeline

        _real_embed_model = "sentence-transformers/all-MiniLM-L6-v2"
        real_embed = get_embed_model(_real_embed_model)
        run_indexing_pipeline(
            file_path=_SAMPLE_TXT,
            index_dir=tmp_path,
            embed_model=real_embed,
        )
        children = retrieve_children(
            "employee benefits",
            index_dir=tmp_path,
            embed_model=real_embed,
            top_k=3,
        )
        parents = lookup_parents(children, index_dir=tmp_path)
        assert len(parents) == len(children)
        for p in parents:
            if p is not None:
                assert isinstance(p, DocumentChunk)
                assert p.chunk_level == "parent"
