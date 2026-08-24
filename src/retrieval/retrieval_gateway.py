"""
Retrieval backend gateway.

Lightweight dispatch module that routes retrieval queries and parent-chunk
lookups to either the local LlamaIndex/BM25 backend or the Azure AI Search
backend, based on ``config.search_backend``.

All heavy SDK and LlamaIndex imports are deferred inside the wrapper
functions so this module is import-light and usable in the fast test suite.

``answer_pipeline.py`` calls this gateway exclusively for both retrieval
and parent lookup — it has no direct knowledge of which backend is active.
This centralises all backend-selection logic and keeps orchestration code
backend-agnostic.

Public API
----------
    route_retrieve(
        query:     str,
        index_dir: Path | None = None,
        top_k:     int = 10,
    ) -> List[RetrievedChunk]

    route_lookup_parents(
        retrieved: List[RetrievedChunk],
        index_dir: Path | None = None,
    ) -> List[Optional[DocumentChunk]]

Backend wrappers (internal)
---------------------------
    _retrieve_local          — delegates to retrieve_hybrid
    _retrieve_azure          — delegates to AzureSearchRetriever.retrieve
    _lookup_parents_local    — delegates to lookup_parents (vector_retriever)
    _lookup_parents_azure    — delegates to AzureSearchRetriever.lookup_parents

These wrappers are module-level functions so tests can monkeypatch them
without importing any heavy dependency.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from src.core.config import config
from src.schema.models import DocumentChunk, RetrievedChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Retrieval wrappers — each defers its heavy SDK / LlamaIndex import
# ---------------------------------------------------------------------------


def _retrieve_local(
    query: str, index_dir: Optional[Path], top_k: int
) -> List[RetrievedChunk]:
    """Local LlamaIndex + BM25 hybrid path (default)."""
    from src.retrieval.hybrid_retriever import retrieve_hybrid  # deferred — llama_index heavy
    return retrieve_hybrid(query, index_dir=index_dir, top_k=top_k)


def _retrieve_azure(query: str, top_k: int) -> List[RetrievedChunk]:
    """Azure AI Search BM25 path — child-level results only."""
    from src.retrieval.azure_search_retriever import AzureSearchRetriever  # deferred
    retriever = AzureSearchRetriever(
        endpoint=config.azure_search_endpoint,
        index_name=config.azure_search_index_name,
    )
    return retriever.retrieve(query, top_k=top_k)

def _retrieve_databricks(
    query: str,
    top_k: int,
    filters: Optional[dict] = None,
) -> List[RetrievedChunk]:
    """Databricks AI Search hybrid child-chunk retrieval."""
    from src.retrieval.databricks_search_retriever import (
        DatabricksSearchRetriever,
    )

    retriever = DatabricksSearchRetriever(
        endpoint_name=(
            config.databricks_ai_search_endpoint_name or None
        ),
        index_name=config.databricks_ai_search_index_name,
    )

    return retriever.retrieve(
        query,
        top_k=top_k,
        filters=filters,
    )
# ---------------------------------------------------------------------------
# Parent lookup wrappers
# ---------------------------------------------------------------------------


def _lookup_parents_local(
    retrieved: List[RetrievedChunk], index_dir: Optional[Path]
) -> List[Optional[DocumentChunk]]:
    """Local SimpleDocumentStore path (default)."""
    from src.retrieval.vector_retriever import lookup_parents  # deferred — llama_index heavy
    return lookup_parents(retrieved, index_dir=index_dir)


def _lookup_parents_azure(
    retrieved: List[RetrievedChunk],
) -> List[Optional[DocumentChunk]]:
    """Azure AI Search parent point-lookup path."""
    from src.retrieval.azure_search_retriever import AzureSearchRetriever  # deferred
    retriever = AzureSearchRetriever(
        endpoint=config.azure_search_endpoint,
        index_name=config.azure_search_index_name,
    )
    return retriever.lookup_parents(retrieved)

def _lookup_parents_databricks(
    retrieved: List[RetrievedChunk],
) -> List[Optional[DocumentChunk]]:
    """Databricks Gold Delta parent-context lookup."""
    from src.retrieval.databricks_search_retriever import (
        DatabricksSearchRetriever,
    )

    retriever = DatabricksSearchRetriever(
        endpoint_name=(
            config.databricks_ai_search_endpoint_name or None
        ),
        index_name=config.databricks_ai_search_index_name,
        parent_table_name=config.databricks_parent_chunks_table,
    )

    return retriever.lookup_parents(retrieved)
# ---------------------------------------------------------------------------
# Public gateway functions
# ---------------------------------------------------------------------------


def route_retrieve(
    query: str,
    index_dir: Optional[Path] = None,
    top_k: int = 10,
    filters: Optional[dict] = None,
) -> List[RetrievedChunk]:
    """
    Route a retrieval query to the configured backend.

    Metadata filters are currently supported
    only by the Databricks retrieval backend.

    Args:
        query:
            Natural-language retrieval query.

        index_dir:
            Local index path. Used only by the
            local retrieval backend.

        top_k:
            Maximum number of results.

        filters:
            Optional backend metadata filters.
            Currently supported only for
            Databricks AI Search.

    Returns:
        List[RetrievedChunk]
    """

    if (
        filters
        and config.search_backend
        != "databricks"
    ):
        raise ValueError(
            "Metadata filters are currently "
            "supported only by the Databricks "
            "retrieval backend."
        )

    if config.search_backend == "azure_search":
        logger.info(
            "retrieval_gateway_azure",
            query_chars=len(query),
            top_k=top_k,
        )

        return _retrieve_azure(
            query,
            top_k=top_k,
        )

    if config.search_backend == "databricks":
        logger.info(
            "retrieval_gateway_databricks",
            query_chars=len(query),
            top_k=top_k,
            filtered=bool(filters),
        )

        # Preserve the historical internal
        # call contract when no filters exist.
        # This also keeps older test doubles
        # and callers compatible.
        if filters:
            return _retrieve_databricks(
                query=query,
                top_k=top_k,
                filters=filters,
            )

        return _retrieve_databricks(
            query=query,
            top_k=top_k,
        )

    if config.search_backend == "local":
        logger.debug(
            "retrieval_gateway_local",
            query_chars=len(query),
            top_k=top_k,
        )

        return _retrieve_local(
            query,
            index_dir=index_dir,
            top_k=top_k,
        )

    raise ValueError(
        "Unsupported search backend: "
        f"{config.search_backend}"
    )


def route_lookup_parents(
    retrieved: List[RetrievedChunk],
    index_dir: Optional[Path] = None,
) -> List[Optional[DocumentChunk]]:
    """
    Route parent-chunk lookup to the configured backend.

    Azure path (``search_backend='azure_search'``):
        Performs point-lookup by parent_chunk_id via
        AzureSearchRetriever.lookup_parents. Returns project-native
        DocumentChunk objects. ``index_dir`` is unused.

    Local path (``search_backend='local'``, default):
        Loads the SimpleDocumentStore from index_dir and looks up
        parent_chunk_id entries.

    Both paths return project-native DocumentChunk objects. No SDK types
    cross this boundary.

    Args:
        retrieved:  Reranked child-level chunks.
        index_dir:  Local index directory (local path only).

    Returns:
        List[Optional[DocumentChunk]] aligned to the input list.
    """
    if config.search_backend == "azure_search":
        logger.info(
            "parent_lookup_gateway_azure",
            count=len(retrieved),
        )
        return _lookup_parents_azure(retrieved)

    if config.search_backend == "databricks":
        logger.info(
            "parent_lookup_gateway_databricks",
            count=len(retrieved),
        )
        return _lookup_parents_databricks(retrieved)

    if config.search_backend == "local":
        logger.debug(
            "parent_lookup_gateway_local",
            count=len(retrieved),
        )
        return _lookup_parents_local(
            retrieved,
            index_dir=index_dir,
        )

    raise ValueError(
        f"Unsupported search backend: {config.search_backend}"
    )


