"""
Indexing backend gateway.

Lightweight dispatch module that routes DocumentChunk indexing to either
the local LlamaIndex backend or the Azure AI Search backend, based on
``config.search_backend``.

All heavy SDK and LlamaIndex imports are deferred inside the wrapper
functions so this module is import-light and usable in the fast test suite
without triggering model downloads or Azure SDK imports.

Public API
----------
    route_index(
        parent_chunks: List[DocumentChunk],
        child_chunks:  List[DocumentChunk],
        index_dir:     Path | None,
        embed_model:   Any | None,
    ) -> IndexManifest

Backend wrappers (internal)
---------------------------
    _run_local_indexing  — delegates to build_indexes (LlamaIndex)
    _run_azure_search_indexing — delegates to AzureSearchIndexer.index_chunks

These wrappers are module-level functions so tests can monkeypatch them
without importing any heavy dependency.
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, List

from src.core.config import config
from src.schema.models import DocumentChunk
from src.utils.logging_utils import get_logger

if TYPE_CHECKING:
    from src.indexing.index_builder import IndexManifest

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Backend wrappers — each defers its heavy SDK import
# ---------------------------------------------------------------------------


def _run_local_indexing(
    parent_chunks: List[DocumentChunk],
    child_chunks: List[DocumentChunk],
    index_dir: Path | None,
    embed_model: Any,
) -> "IndexManifest":
    """Local LlamaIndex + SimpleVectorStore path (default)."""
    from src.indexing.index_builder import build_indexes  # deferred — llama_index heavy
    return build_indexes(
        parent_chunks,
        child_chunks,
        index_dir=index_dir,
        embed_model=embed_model,
    )


def _run_azure_search_indexing(chunks: List[DocumentChunk]) -> "IndexManifest":
    """Azure AI Search path — uploads all chunks, returns IndexManifest."""
    from src.indexing.azure_search_indexer import AzureSearchIndexer  # deferred
    indexer = AzureSearchIndexer(
        endpoint=config.azure_search_endpoint,
        index_name=config.azure_search_index_name,
    )
    return indexer.index_chunks(chunks)


# ---------------------------------------------------------------------------
# Public gateway
# ---------------------------------------------------------------------------


def route_index(
    parent_chunks: List[DocumentChunk],
    child_chunks: List[DocumentChunk],
    index_dir: Path | None,
    embed_model: Any,
) -> "IndexManifest":
    """
    Route chunk indexing to the configured backend.

    When ``search_backend='azure_search'``, all chunks (parents and
    children) are forwarded to AzureSearchIndexer. The indexer stores
    parent chunks for point-lookup and child chunks for search queries.
    ``index_dir`` and ``embed_model`` are unused in this path.

    When ``search_backend='local'`` (default), delegates to build_indexes
    using the LlamaIndex/SimpleVectorStore pipeline.

    Args:
        parent_chunks: Parent-level DocumentChunks.
        child_chunks:  Child-level DocumentChunks.
        index_dir:     Local index directory (local path only).
        embed_model:   Embedding model instance (local path only).

    Returns:
        IndexManifest with build statistics.
    """
    if config.search_backend == "azure_search":
        logger.info(
            "index_gateway_azure",
            chunk_count=len(parent_chunks) + len(child_chunks),
        )
        return _run_azure_search_indexing(
            parent_chunks + child_chunks
        )

    if config.search_backend == "databricks":
        raise RuntimeError(
            "Databricks AI Search indexing is managed through the "
            "Gold Delta table and Delta Sync. "
            "The application index gateway must not build a separate index."
        )

    if config.search_backend == "local":
        logger.debug(
            "index_gateway_local",
            parent_count=len(parent_chunks),
            child_count=len(child_chunks),
        )
        return _run_local_indexing(
            parent_chunks,
            child_chunks,
            index_dir,
            embed_model,
        )

    raise ValueError(
        f"Unsupported search backend: {config.search_backend}"
    )
