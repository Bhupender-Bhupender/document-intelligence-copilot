"""
Azure AI Search indexing adapter.

Provides AzureSearchIndexer, which uploads DocumentChunk records to an
Azure AI Search index and returns a project-native IndexManifest.

All azure.search.documents SDK imports are deferred to __init__ so the
module is import-light and testable without the Azure package installed.

Public API
----------
    class AzureSearchIndexError(Exception)

    class AzureSearchIndexer:
        __init__(endpoint, index_name, credential=None)
        ensure_index() -> None
        index_chunks(chunks: List[DocumentChunk]) -> IndexManifest

Design notes
------------
Index schema
    chunk_id        — key field (Edm.String)
    doc_id          — filterable string
    page_id         — filterable string
    page_number     — filterable Int32
    file_name       — filterable string
    file_type       — filterable string
    section_title   — filterable string (empty string when None)
    chunk_level     — filterable string ("parent" | "child" | "flat")
    parent_chunk_id — filterable string (empty string when None)
    text            — searchable string (full-text BM25)
    word_count      — Int32

No vector field is included in this version. True hybrid/vector indexing
(uploading embedding vectors) is deferred to a future hardening step.

Parent chunks are indexed alongside child chunks. Child-only filtering
during retrieval is applied by the retriever (``chunk_level eq 'child'``),
not the indexer. Parent chunks remain available for point-lookup.

Batch upload
    All documents are uploaded in a single batch via upload_documents().
    For very large corpora a future hardening step should split into
    batches of 1000 (Azure Search service limit per request).

Manifest
    IndexManifest.index_dir is set to the endpoint URL (no local path).
    IndexManifest.embedding_model is set to "azure-search".

SDK class injection for tests
    All SDK model classes needed by ensure_index() are imported once inside
    __init__ and stored as instance attributes (_SearchIndex, _SimpleField,
    _SearchableField, _SearchFieldDataType). This mirrors the pattern used
    by AzureDiOcrAdapter._AnalyzeDocumentRequest and keeps ensure_index()
    import-free, enabling the test bypass below.

Test bypass pattern
-------------------
    class _FakeField:
        def __init__(self, **kwargs): [setattr(self, k, v) for k, v in kwargs.items()]

    class _FakeFieldDataType:
        String = "Edm.String"
        Int32  = "Edm.Int32"

    class _FakeSearchIndex:
        def __init__(self, name, fields): ...

    adapter = object.__new__(AzureSearchIndexer)
    adapter._index_client  = mock_index_client
    adapter._search_client = mock_search_client
    adapter._endpoint      = "https://test.search.windows.net"
    adapter._index_name    = "test-index"
    adapter._SearchIndex       = _FakeSearchIndex
    adapter._SimpleField       = _FakeField
    adapter._SearchableField   = _FakeField
    adapter._SearchFieldDataType = _FakeFieldDataType
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional

from src.schema.models import DocumentChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class AzureSearchIndexError(Exception):
    """Raised when an Azure AI Search indexing operation fails."""


class AzureSearchIndexer:
    """
    Adapter that indexes DocumentChunk records into Azure AI Search.

    Azure SDK objects are fully internal to this class. Only project-native
    types cross the adapter boundary (DocumentChunk in, IndexManifest out).
    """

    def __init__(
        self,
        endpoint: str,
        index_name: str,
        credential: Optional[Any] = None,
    ) -> None:
        try:
            from azure.search.documents import SearchClient
            from azure.search.documents.indexes import SearchIndexClient
            from azure.search.documents.indexes.models import (
                SearchFieldDataType,
                SearchIndex,
                SearchableField,
                SimpleField,
            )
            from azure.identity import DefaultAzureCredential
        except ImportError as exc:
            raise AzureSearchIndexError(
                "azure-search-documents and azure-identity packages are required. "
                "Install them via: pip install 'azure-search-documents>=11.0,<12' azure-identity"
            ) from exc

        self._endpoint = endpoint
        self._index_name = index_name

        # Store SDK model classes so ensure_index() needs no further imports.
        self._SearchIndex = SearchIndex
        self._SimpleField = SimpleField
        self._SearchableField = SearchableField
        self._SearchFieldDataType = SearchFieldDataType

        cred = credential or DefaultAzureCredential()
        self._index_client = SearchIndexClient(endpoint=endpoint, credential=cred)
        self._search_client = SearchClient(
            endpoint=endpoint,
            index_name=index_name,
            credential=cred,
        )
        logger.debug(
            "azure_search_indexer_init",
            endpoint=endpoint,
            index_name=index_name,
        )

    def ensure_index(self) -> None:
        """
        Create or update the Azure AI Search index schema.

        Uses create_or_update_index so the call is idempotent — safe to
        call on every indexing run. Raises AzureSearchIndexError on failure.
        """
        SearchIndex = self._SearchIndex
        SimpleField = self._SimpleField
        SearchableField = self._SearchableField
        SearchFieldDataType = self._SearchFieldDataType

        fields = [
            SimpleField(name="chunk_id", type=SearchFieldDataType.String, key=True),
            SimpleField(
                name="doc_id", type=SearchFieldDataType.String, filterable=True
            ),
            SimpleField(
                name="page_id", type=SearchFieldDataType.String, filterable=True
            ),
            SimpleField(
                name="page_number", type=SearchFieldDataType.Int32, filterable=True
            ),
            SimpleField(
                name="file_name", type=SearchFieldDataType.String, filterable=True
            ),
            SimpleField(
                name="file_type", type=SearchFieldDataType.String, filterable=True
            ),
            SimpleField(
                name="section_title",
                type=SearchFieldDataType.String,
                filterable=True,
            ),
            SimpleField(
                name="chunk_level",
                type=SearchFieldDataType.String,
                filterable=True,
            ),
            SimpleField(
                name="parent_chunk_id",
                type=SearchFieldDataType.String,
                filterable=True,
            ),
            SearchableField(name="text", type=SearchFieldDataType.String),
            SimpleField(name="word_count", type=SearchFieldDataType.Int32),
        ]

        index = SearchIndex(name=self._index_name, fields=fields)

        try:
            self._index_client.create_or_update_index(index)
            logger.debug("azure_search_index_ensured", index_name=self._index_name)
        except Exception as exc:
            raise AzureSearchIndexError(
                f"Failed to create or update index '{self._index_name}': {exc}"
            ) from exc

    def index_chunks(self, chunks: List[DocumentChunk]) -> "IndexManifest":
        """
        Upload DocumentChunk records to Azure AI Search.

        Calls ensure_index() before uploading to guarantee the index schema
        exists. Parent and child chunks are both uploaded; the retriever
        filters to child-level chunks during search queries, while parent
        chunks remain available for point-lookup by chunk_id.

        Args:
            chunks: Mixed list of parent and child DocumentChunks.

        Returns:
            IndexManifest with upload statistics and provenance metadata.

        Raises:
            AzureSearchIndexError: if index creation or document upload fails.
        """
        # Deferred import — avoids pulling llama_index into module scope.
        from src.indexing.index_builder import IndexManifest

        self.ensure_index()

        documents = [self._chunk_to_document(chunk) for chunk in chunks]

        if documents:
            try:
                self._search_client.upload_documents(documents=documents)
                logger.info(
                    "azure_search_chunks_uploaded",
                    count=len(documents),
                    index_name=self._index_name,
                )
            except Exception as exc:
                raise AzureSearchIndexError(
                    f"Failed to upload {len(documents)} documents to "
                    f"'{self._index_name}': {exc}"
                ) from exc

        parent_count = sum(1 for c in chunks if c.chunk_level == "parent")
        child_count = sum(
            1 for c in chunks if c.chunk_level in ("child", "flat")
        )
        doc_ids = sorted({c.doc_id for c in chunks})

        return IndexManifest(
            run_id=uuid.uuid4().hex,
            built_at=datetime.now(timezone.utc),
            index_dir=self._endpoint,
            embedding_model="azure-search",
            parent_count=parent_count,
            child_count=child_count,
            doc_ids=doc_ids,
        )

    @staticmethod
    def _chunk_to_document(chunk: DocumentChunk) -> dict:
        """Convert a DocumentChunk to an Azure Search document dict."""
        return {
            "chunk_id": chunk.chunk_id,
            "doc_id": chunk.doc_id,
            "page_id": chunk.page_id,
            "page_number": chunk.page_number,
            "file_name": chunk.file_name,
            "file_type": chunk.file_type or "",
            "section_title": chunk.section_title or "",
            "chunk_level": chunk.chunk_level,
            "parent_chunk_id": chunk.parent_chunk_id or "",
            "text": chunk.text,
            "word_count": chunk.word_count,
        }
