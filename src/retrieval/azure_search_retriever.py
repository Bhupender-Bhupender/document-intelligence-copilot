"""
Azure AI Search retrieval adapter.

Provides AzureSearchRetriever for child-chunk search queries and
parent-chunk point-lookup against an Azure AI Search index.

All azure.search.documents SDK imports are deferred to __init__ so the
module is import-light and testable without the Azure package installed.

Public API
----------
    class AzureSearchRetrievalError(Exception)

    class AzureSearchRetriever:
        __init__(endpoint, index_name, credential=None)
        retrieve(query, top_k=10) -> List[RetrievedChunk]
        lookup_parents(retrieved)  -> List[Optional[DocumentChunk]]

Design notes
------------
Child-only retrieval
    retrieve() applies ``filter="chunk_level eq 'child'"`` so the returned
    results are always child-level chunks. This ensures reranker, citation
    builder, and synthesis receive child-level retrieval units, consistent
    with the local hybrid path.

Score semantics (honest for text/BM25-only mode)
    retrieval_method = "bm25"
    bm25_score       = result["@search.score"]  (Azure BM25 relevance score)
    vector_score     = None  (no vector field in this version)
    fusion_score     = None  (single scoring path, no fusion)
    rerank_score     = None  (set by the reranker downstream)

    True Azure hybrid/vector retrieval (uploading embedding vectors,
    using Azure's vector search profile) is deferred to a future
    hardening step.

Parent lookup
    lookup_parents() performs a point-lookup via
    ``get_document(key=parent_chunk_id)`` for each RetrievedChunk that
    carries a parent_chunk_id. Returns project-native DocumentChunk objects.
    No Azure SDK types cross the adapter boundary.

    chunk_index is reconstructed as 0 — not stored in the index. This
    matches the rationale in vector_retriever.py for the local path.

    If the parent document is not found (key absent, network error, etc.),
    None is returned for that position and no exception is raised. This
    mirrors the behaviour of lookup_parents() in vector_retriever.py.

Test bypass pattern
-------------------
    adapter = object.__new__(AzureSearchRetriever)
    adapter._client     = mock_search_client
    adapter._index_name = "test-index"
"""
from __future__ import annotations

from typing import Any, List, Optional

from src.schema.models import DocumentChunk, RetrievedChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class AzureSearchRetrievalError(Exception):
    """Raised when an Azure AI Search retrieval operation fails."""


class AzureSearchRetriever:
    """
    Adapter for child-chunk retrieval and parent-chunk lookup via Azure AI Search.

    Azure SDK objects are fully internal to this class. Only project-native
    types cross the adapter boundary (RetrievedChunk and DocumentChunk out).
    """

    def __init__(
        self,
        endpoint: str,
        index_name: str,
        credential: Optional[Any] = None,
    ) -> None:
        try:
            from azure.search.documents import SearchClient
            from azure.identity import DefaultAzureCredential
        except ImportError as exc:
            raise AzureSearchRetrievalError(
                "azure-search-documents and azure-identity packages are required. "
                "Install them via: pip install 'azure-search-documents>=11.0,<12' azure-identity"
            ) from exc

        self._index_name = index_name
        cred = credential or DefaultAzureCredential()
        self._client = SearchClient(
            endpoint=endpoint,
            index_name=index_name,
            credential=cred,
        )
        logger.debug(
            "azure_search_retriever_init",
            endpoint=endpoint,
            index_name=index_name,
        )

    def retrieve(self, query: str, top_k: int = 10) -> List[RetrievedChunk]:
        """
        Search for child chunks matching query using Azure AI Search BM25.

        Applies ``filter="chunk_level eq 'child'"`` so only child-level
        chunks are returned, keeping retrieval units consistent with the
        local hybrid path (reranker, citations, and synthesis expect
        child-level chunks as primary results).

        Score fields set on each RetrievedChunk:
            retrieval_method = "bm25"
            bm25_score       = @search.score (Azure BM25 relevance score)
            vector_score     = None
            fusion_score     = None
            rerank_score     = None

        Args:
            query:  Natural language query string.
            top_k:  Maximum number of results to return.

        Returns:
            List[RetrievedChunk] — child-level only, ordered by bm25_score
            descending (Azure Search default ordering).

        Raises:
            AzureSearchRetrievalError: on SDK failure.
        """
        try:
            results = self._client.search(
                search_text=query,
                top=top_k,
                filter="chunk_level eq 'child'",
                select="*",
            )
            chunks = [self._to_retrieved_chunk(r) for r in results]
        except Exception as exc:
            raise AzureSearchRetrievalError(
                f"Azure AI Search query failed: {exc}"
            ) from exc

        logger.debug("azure_search_retrieved", count=len(chunks))
        return chunks

    def lookup_parents(
        self, retrieved: List[RetrievedChunk]
    ) -> List[Optional[DocumentChunk]]:
        """
        Fetch parent DocumentChunks for a list of RetrievedChunks.

        For each RetrievedChunk, if ``parent_chunk_id`` is present, a
        point-lookup is performed via ``get_document(key=parent_chunk_id)``.
        Returns None for chunks without a parent or when the parent document
        is not found (error is absorbed, not re-raised).

        No Azure SDK objects cross the return boundary — all results are
        project-native DocumentChunk instances.

        Args:
            retrieved: Reranked child-level RetrievedChunk list.

        Returns:
            List[Optional[DocumentChunk]] aligned to the input list.
        """
        parents: List[Optional[DocumentChunk]] = []
        for chunk in retrieved:
            if not chunk.parent_chunk_id:
                parents.append(None)
                continue
            try:
                doc = self._client.get_document(key=chunk.parent_chunk_id)
                parents.append(self._to_document_chunk(doc))
            except Exception:
                logger.debug(
                    "azure_search_parent_not_found",
                    parent_chunk_id=chunk.parent_chunk_id,
                )
                parents.append(None)
        return parents

    @staticmethod
    def _to_retrieved_chunk(result: Any) -> RetrievedChunk:
        """Convert an Azure Search result dict to a project-native RetrievedChunk."""
        return RetrievedChunk(
            chunk_id=result["chunk_id"],
            doc_id=result["doc_id"],
            page_id=result["page_id"],
            file_name=result["file_name"],
            page_number=result["page_number"],
            section_title=result.get("section_title") or None,
            text=result["text"],
            word_count=result["word_count"],
            retrieval_method="bm25",
            bm25_score=result["@search.score"],
            vector_score=None,
            fusion_score=None,
            rerank_score=None,
            parent_chunk_id=result.get("parent_chunk_id") or None,
            file_type=result.get("file_type") or None,
        )

    @staticmethod
    def _to_document_chunk(doc: Any) -> DocumentChunk:
        """
        Convert an Azure Search document dict to a project-native DocumentChunk.

        ``chunk_index`` is reconstructed as 0 — it is not stored in the
        index (same rationale as vector_retriever.py for the local path).
        """
        return DocumentChunk(
            chunk_id=doc["chunk_id"],
            doc_id=doc["doc_id"],
            page_id=doc["page_id"],
            page_number=doc["page_number"],
            file_name=doc["file_name"],
            file_type=doc.get("file_type") or "",
            section_title=doc.get("section_title") or None,
            text=doc["text"],
            word_count=doc.get("word_count") or len(doc["text"].split()),
            chunk_index=0,
            chunk_level=doc.get("chunk_level", "parent"),
            parent_chunk_id=doc.get("parent_chunk_id") or None,
        )
