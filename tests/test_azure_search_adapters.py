"""
Tests for Azure AI Search indexer and retriever adapters, backend gateways,
and config-driven backend dispatch.

All tests are mocked — no live Azure credentials or SDK installation required.
Heavy LlamaIndex / torch / HuggingFace modules are never imported.

Test classes
------------
    TestAzureSearchIndexerUpload      (7)  — adapter upload path
    TestAzureSearchRetriever          (9)  — adapter retrieve path + score semantics
    TestAzureSearchRetrieverParents   (4)  — parent lookup via get_document
    TestIndexGatewaySwitch            (3)  — index_gateway backend dispatch
    TestRetrievalGatewaySwitch        (4)  — retrieval_gateway backend dispatch

Total: 27 tests
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.indexing.azure_search_indexer import AzureSearchIndexError, AzureSearchIndexer
from src.retrieval.azure_search_retriever import (
    AzureSearchRetrievalError,
    AzureSearchRetriever,
)
from src.schema.models import DocumentChunk, RetrievedChunk


# ===========================================================================
# Stub SDK model classes
# Used by _make_indexer() to satisfy ensure_index() without the Azure package.
# ===========================================================================

class _FakeField:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _FakeFieldDataType:
    String = "Edm.String"
    Int32 = "Edm.Int32"


class _FakeSearchIndex:
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields


# ===========================================================================
# Helper factories
# ===========================================================================

def _make_indexer(mock_index_client=None, mock_search_client=None):
    """Build AzureSearchIndexer bypassing __init__ (no Azure SDK needed)."""
    if mock_index_client is None:
        mock_index_client = MagicMock()
    if mock_search_client is None:
        mock_search_client = MagicMock()
    adapter = object.__new__(AzureSearchIndexer)
    adapter._index_client = mock_index_client
    adapter._search_client = mock_search_client
    adapter._endpoint = "https://test.search.windows.net"
    adapter._index_name = "test-index"
    adapter._SearchIndex = _FakeSearchIndex
    adapter._SimpleField = _FakeField
    adapter._SearchableField = _FakeField
    adapter._SearchFieldDataType = _FakeFieldDataType
    return adapter


def _make_retriever(mock_client=None):
    """Build AzureSearchRetriever bypassing __init__ (no Azure SDK needed)."""
    if mock_client is None:
        mock_client = MagicMock()
    adapter = object.__new__(AzureSearchRetriever)
    adapter._client = mock_client
    adapter._index_name = "test-index"
    return adapter


def _make_child_chunk(chunk_id="chunk-001", parent_chunk_id=None):
    return DocumentChunk(
        chunk_id=chunk_id,
        doc_id="doc-001",
        page_id="page-001",
        page_number=1,
        file_name="test.pdf",
        file_type="pdf",
        section_title="Introduction",
        text="Sample child chunk text.",
        word_count=4,
        chunk_index=0,
        chunk_level="child",
        parent_chunk_id=parent_chunk_id,
    )


def _make_parent_chunk(chunk_id="parent-001"):
    return DocumentChunk(
        chunk_id=chunk_id,
        doc_id="doc-001",
        page_id="page-001",
        page_number=1,
        file_name="test.pdf",
        file_type="pdf",
        section_title="Introduction",
        text="Parent chunk with broader synthesis context.",
        word_count=6,
        chunk_index=0,
        chunk_level="parent",
        parent_chunk_id=None,
    )


def _make_search_result(
    chunk_id="chunk-001", score=0.95, parent_chunk_id="parent-001"
):
    return {
        "chunk_id": chunk_id,
        "doc_id": "doc-001",
        "page_id": "page-001",
        "file_name": "test.pdf",
        "page_number": 1,
        "section_title": "Introduction",
        "text": "Sample child chunk text.",
        "word_count": 4,
        "chunk_level": "child",
        "parent_chunk_id": parent_chunk_id,
        "file_type": "pdf",
        "@search.score": score,
    }


def _make_parent_doc(chunk_id="parent-001"):
    return {
        "chunk_id": chunk_id,
        "doc_id": "doc-001",
        "page_id": "page-001",
        "file_name": "test.pdf",
        "page_number": 1,
        "section_title": "Introduction",
        "text": "Parent chunk with broader synthesis context.",
        "word_count": 6,
        "chunk_level": "parent",
        "parent_chunk_id": "",
        "file_type": "pdf",
    }


def _make_retrieved_child(parent_chunk_id="parent-001"):
    return RetrievedChunk(
        chunk_id="chunk-001",
        doc_id="doc-001",
        page_id="page-001",
        file_name="test.pdf",
        page_number=1,
        text="Sample child text.",
        word_count=3,
        retrieval_method="bm25",
        parent_chunk_id=parent_chunk_id,
    )


# ===========================================================================
# TestAzureSearchIndexerUpload
# ===========================================================================

class TestAzureSearchIndexerUpload:

    def test_index_chunks_returns_manifest(self):
        from src.indexing.index_builder import IndexManifest

        indexer = _make_indexer()
        result = indexer.index_chunks([_make_child_chunk()])
        assert isinstance(result, IndexManifest)

    def test_index_chunks_calls_upload_documents(self):
        mock_sc = MagicMock()
        indexer = _make_indexer(mock_search_client=mock_sc)
        indexer.index_chunks([_make_child_chunk()])
        mock_sc.upload_documents.assert_called_once()
        docs = mock_sc.upload_documents.call_args.kwargs["documents"]
        assert len(docs) == 1

    def test_index_chunks_document_fields(self):
        mock_sc = MagicMock()
        indexer = _make_indexer(mock_search_client=mock_sc)
        indexer.index_chunks([_make_child_chunk(chunk_id="c-001")])
        doc = mock_sc.upload_documents.call_args.kwargs["documents"][0]
        assert doc["chunk_id"] == "c-001"
        assert doc["doc_id"] == "doc-001"
        assert doc["chunk_level"] == "child"
        assert doc["text"] == "Sample child chunk text."
        assert doc["word_count"] == 4

    def test_index_chunks_empty_input_zero_counts(self):
        from src.indexing.index_builder import IndexManifest

        indexer = _make_indexer()
        result = indexer.index_chunks([])
        assert isinstance(result, IndexManifest)
        assert result.parent_count == 0
        assert result.child_count == 0
        assert result.doc_ids == []

    def test_index_chunks_manifest_index_dir_is_endpoint(self):
        indexer = _make_indexer()
        result = indexer.index_chunks([_make_child_chunk()])
        assert result.index_dir == "https://test.search.windows.net"

    def test_index_chunks_sdk_exception_raises_error(self):
        mock_sc = MagicMock()
        mock_sc.upload_documents.side_effect = RuntimeError("Service unavailable")
        indexer = _make_indexer(mock_search_client=mock_sc)
        with pytest.raises(AzureSearchIndexError):
            indexer.index_chunks([_make_child_chunk()])

    def test_index_chunks_parent_and_child_counts(self):
        parent = _make_parent_chunk()
        child = _make_child_chunk()
        indexer = _make_indexer()
        result = indexer.index_chunks([parent, child])
        assert result.parent_count == 1
        assert result.child_count == 1


# ===========================================================================
# TestAzureSearchRetriever
# ===========================================================================

class TestAzureSearchRetriever:

    def test_retrieve_calls_search_with_child_filter(self):
        mock_client = MagicMock()
        mock_client.search.return_value = iter([])
        retriever = _make_retriever(mock_client)
        retriever.retrieve("test query", top_k=5)
        mock_client.search.assert_called_once()
        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["filter"] == "chunk_level eq 'child'"

    def test_retrieve_result_to_retrieved_chunk(self):
        mock_client = MagicMock()
        mock_client.search.return_value = iter([_make_search_result()])
        retriever = _make_retriever(mock_client)
        results = retriever.retrieve("query")
        assert len(results) == 1
        chunk = results[0]
        assert isinstance(chunk, RetrievedChunk)
        assert chunk.chunk_id == "chunk-001"
        assert chunk.doc_id == "doc-001"

    def test_retrieve_bm25_score_from_search_score(self):
        mock_client = MagicMock()
        mock_client.search.return_value = iter([_make_search_result(score=0.87)])
        retriever = _make_retriever(mock_client)
        results = retriever.retrieve("query")
        assert results[0].bm25_score == pytest.approx(0.87)

    def test_retrieve_vector_score_is_none(self):
        mock_client = MagicMock()
        mock_client.search.return_value = iter([_make_search_result()])
        retriever = _make_retriever(mock_client)
        results = retriever.retrieve("query")
        assert results[0].vector_score is None

    def test_retrieve_fusion_score_is_none(self):
        mock_client = MagicMock()
        mock_client.search.return_value = iter([_make_search_result()])
        retriever = _make_retriever(mock_client)
        results = retriever.retrieve("query")
        assert results[0].fusion_score is None

    def test_retrieve_retrieval_method_is_bm25(self):
        mock_client = MagicMock()
        mock_client.search.return_value = iter([_make_search_result()])
        retriever = _make_retriever(mock_client)
        results = retriever.retrieve("query")
        assert results[0].retrieval_method == "bm25"

    def test_retrieve_empty_results_returns_empty_list(self):
        mock_client = MagicMock()
        mock_client.search.return_value = iter([])
        retriever = _make_retriever(mock_client)
        results = retriever.retrieve("query")
        assert results == []

    def test_retrieve_top_k_forwarded(self):
        mock_client = MagicMock()
        mock_client.search.return_value = iter([])
        retriever = _make_retriever(mock_client)
        retriever.retrieve("query", top_k=7)
        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["top"] == 7

    def test_retrieve_sdk_exception_raises_error(self):
        mock_client = MagicMock()
        mock_client.search.side_effect = RuntimeError("Connection refused")
        retriever = _make_retriever(mock_client)
        with pytest.raises(AzureSearchRetrievalError):
            retriever.retrieve("query")


# ===========================================================================
# TestAzureSearchRetrieverParents
# ===========================================================================

class TestAzureSearchRetrieverParents:

    def test_lookup_parents_returns_document_chunk(self):
        mock_client = MagicMock()
        mock_client.get_document.return_value = _make_parent_doc()
        retriever = _make_retriever(mock_client)
        parents = retriever.lookup_parents([_make_retrieved_child()])
        assert len(parents) == 1
        assert isinstance(parents[0], DocumentChunk)
        assert parents[0].chunk_id == "parent-001"

    def test_lookup_parents_none_when_no_parent_chunk_id(self):
        mock_client = MagicMock()
        retriever = _make_retriever(mock_client)
        parents = retriever.lookup_parents([_make_retrieved_child(parent_chunk_id=None)])
        assert parents == [None]
        mock_client.get_document.assert_not_called()

    def test_lookup_parents_none_on_fetch_failure(self):
        mock_client = MagicMock()
        mock_client.get_document.side_effect = RuntimeError("Not found")
        retriever = _make_retriever(mock_client)
        parents = retriever.lookup_parents([_make_retrieved_child()])
        assert parents == [None]  # error absorbed, not re-raised

    def test_lookup_parents_chunk_index_reconstructed_as_zero(self):
        mock_client = MagicMock()
        mock_client.get_document.return_value = _make_parent_doc()
        retriever = _make_retriever(mock_client)
        parents = retriever.lookup_parents([_make_retrieved_child()])
        assert parents[0].chunk_index == 0


# ===========================================================================
# TestIndexGatewaySwitch
# ===========================================================================

class TestIndexGatewaySwitch:

    def _chunks(self):
        return [_make_parent_chunk()], [_make_child_chunk()]

    def test_route_index_azure_branch_when_configured(self, monkeypatch):
        from src.indexing import index_gateway

        mock_manifest = MagicMock()
        monkeypatch.setattr(
            index_gateway, "_run_azure_search_indexing", lambda chunks: mock_manifest
        )
        monkeypatch.setattr(index_gateway.config, "search_backend", "azure_search")
        parents, children = self._chunks()
        result = index_gateway.route_index(
            parents, children, index_dir=None, embed_model=None
        )
        assert result is mock_manifest

    def test_route_index_local_branch_when_default(self, monkeypatch):
        from src.indexing import index_gateway

        mock_manifest = MagicMock()
        monkeypatch.setattr(
            index_gateway,
            "_run_local_indexing",
            lambda *a, **kw: mock_manifest,
        )
        monkeypatch.setattr(index_gateway.config, "search_backend", "local")
        parents, children = self._chunks()
        result = index_gateway.route_index(
            parents, children, index_dir=None, embed_model=None
        )
        assert result is mock_manifest

    def test_route_index_azure_receives_all_chunks(self, monkeypatch):
        from src.indexing import index_gateway

        received: dict = {}

        def _fake_azure(chunks):
            received["chunks"] = chunks
            return MagicMock()

        monkeypatch.setattr(index_gateway, "_run_azure_search_indexing", _fake_azure)
        monkeypatch.setattr(index_gateway.config, "search_backend", "azure_search")
        parents, children = self._chunks()
        index_gateway.route_index(parents, children, index_dir=None, embed_model=None)
        # 1 parent + 1 child = 2 total
        assert len(received["chunks"]) == 2


# ===========================================================================
# TestRetrievalGatewaySwitch
# ===========================================================================

class TestRetrievalGatewaySwitch:

    def test_route_retrieve_azure_branch_when_configured(self, monkeypatch):
        from src.retrieval import retrieval_gateway

        mock_chunks = [MagicMock()]
        monkeypatch.setattr(
            retrieval_gateway,
            "_retrieve_azure",
            lambda q, top_k: mock_chunks,
        )
        monkeypatch.setattr(retrieval_gateway.config, "search_backend", "azure_search")
        result = retrieval_gateway.route_retrieve("test query", top_k=5)
        assert result is mock_chunks

    def test_route_retrieve_local_branch_when_default(self, monkeypatch):
        from src.retrieval import retrieval_gateway

        mock_chunks = [MagicMock()]
        monkeypatch.setattr(
            retrieval_gateway,
            "_retrieve_local",
            lambda q, index_dir, top_k: mock_chunks,
        )
        monkeypatch.setattr(retrieval_gateway.config, "search_backend", "local")
        result = retrieval_gateway.route_retrieve("test query", top_k=5)
        assert result is mock_chunks

    def test_route_lookup_parents_azure_branch(self, monkeypatch):
        from src.retrieval import retrieval_gateway

        mock_parents = [MagicMock()]
        monkeypatch.setattr(
            retrieval_gateway,
            "_lookup_parents_azure",
            lambda r: mock_parents,
        )
        monkeypatch.setattr(retrieval_gateway.config, "search_backend", "azure_search")
        result = retrieval_gateway.route_lookup_parents([MagicMock()])
        assert result is mock_parents

    def test_route_lookup_parents_local_branch(self, monkeypatch):
        from src.retrieval import retrieval_gateway

        mock_parents = [None]
        monkeypatch.setattr(
            retrieval_gateway,
            "_lookup_parents_local",
            lambda r, index_dir: mock_parents,
        )
        monkeypatch.setattr(retrieval_gateway.config, "search_backend", "local")
        result = retrieval_gateway.route_lookup_parents([MagicMock()])
        assert result is mock_parents
