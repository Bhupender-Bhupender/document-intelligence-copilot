"""
Indexing layer: builds and persists a LlamaIndex child VectorStoreIndex
and a parent document store from hierarchical DocumentChunk lists.

Public API
----------
    build_indexes(
        parent_chunks: list[DocumentChunk],
        child_chunks: list[DocumentChunk],
        index_dir: Path | None = None,
        embed_model: BaseEmbedding | None = None,
    ) -> IndexManifest

    load_child_index(
        index_dir: Path | None = None,
        embed_model: BaseEmbedding | None = None,
    ) -> VectorStoreIndex

    load_parent_store(
        index_dir: Path | None = None,
    ) -> SimpleDocumentStore

Storage layout under index_dir (default: data/index/)
------------------------------------------------------
    child_index/
        default__vector_store.json  ← embedded child chunk vectors
        docstore.json               ← child chunk text and metadata
        index_store.json            ← VectorStoreIndex internal state
        graph_store.json            ← empty (LlamaIndex artefact)
        image__vector_store.json    ← empty (LlamaIndex artefact)
    parent_store/
        docstore.json               ← parent chunk text and metadata
    build_manifest.json             ← build statistics

Design
------
Two separate stores, one per chunk level:

    child_index/ — VectorStoreIndex (SimpleVectorStore).
        Each child DocumentChunk becomes a TextNode with id_=chunk.chunk_id.
        All required metadata fields are stored in node.metadata.
        This store is the retrieval surface for Phase 5+.

    parent_store/ — SimpleDocumentStore (no embeddings).
        Each parent DocumentChunk is stored as a Document with
        doc_id=chunk.chunk_id. Looked up by parent_chunk_id after child
        retrieval in Phase 5+. No vector indexing is needed for parents
        at this step.

Embedding model (pluggable, never hardwired)
--------------------------------------------
    The embed_model parameter accepts any BaseEmbedding subclass.
    When None, LlamaIndex defers to llama_index.core.Settings.embed_model.
    This module never imports a concrete runtime embedder — the caller
    decides which embedder to use.

        Tests:       inject MockEmbedding(embed_dim=384)
        Production:  configure Settings.embed_model externally (e.g.
                     Qwen/Qwen3-Embedding-0.6B via HuggingFaceEmbedding)
                     and call build_indexes() with embed_model=None.

Rebuild behaviour
-----------------
    Default: overwrites existing stores at index_dir.
    Determinism: chunk_ids are SHA-256 derived (Phase 3A), so the same
    input always produces identical node/document IDs, enabling idempotent
    future upserts.
    Empty input: build_indexes([], [], ...) succeeds and writes an empty
    index, an empty parent store, and a manifest with counts = 0.

Metadata fields preserved on every node and document
------------------------------------------------------
    chunk_id, doc_id, page_id, page_number, file_name, file_type,
    section_title, chunk_level, parent_chunk_id
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field

from llama_index.core import (
    StorageContext,
    VectorStoreIndex,
    load_index_from_storage,
)
from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.schema import Document, TextNode
from llama_index.core.storage.docstore import SimpleDocumentStore

from src.core.config import config
from src.schema.models import DocumentChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Storage sub-directory names (not versioned; overwrite on rebuild)
# ---------------------------------------------------------------------------

_CHILD_SUBDIR = "child_index"
_PARENT_SUBDIR = "parent_store"
_MANIFEST_FILE = "build_manifest.json"

# Metadata field names written to every node / document
_META_KEYS = (
    "chunk_id",
    "doc_id",
    "page_id",
    "page_number",
    "file_name",
    "file_type",
    "section_title",
    "chunk_level",
    "parent_chunk_id",
)


# ---------------------------------------------------------------------------
# IndexManifest — persisted build record
# ---------------------------------------------------------------------------


class IndexManifest(BaseModel):
    """
    Metadata record written after every successful build_indexes() call.

    Saved as build_manifest.json under index_dir.

    Fields:
        run_id:          Unique identifier for this build run.
        built_at:        UTC timestamp of the build.
        index_dir:       Absolute path to the root index directory.
        embedding_model: Class name (tests) or config model ID (production).
        parent_count:    Number of parent DocumentChunks indexed.
        child_count:     Number of child DocumentChunks indexed.
        doc_ids:         Sorted, deduplicated list of source doc_ids.
    """

    run_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    built_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    index_dir: str
    embedding_model: str
    parent_count: int
    child_count: int
    doc_ids: List[str]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _metadata(chunk: DocumentChunk) -> dict:
    """Build the metadata dict stored on every LlamaIndex node/document."""
    return {
        "chunk_id": chunk.chunk_id,
        "doc_id": chunk.doc_id,
        "page_id": chunk.page_id,
        "page_number": chunk.page_number,
        "file_name": chunk.file_name,
        "file_type": chunk.file_type,
        "section_title": chunk.section_title or "",
        "chunk_level": chunk.chunk_level,
        "parent_chunk_id": chunk.parent_chunk_id or "",
    }


def _to_text_node(chunk: DocumentChunk) -> TextNode:
    """Convert a child DocumentChunk to a LlamaIndex TextNode."""
    return TextNode(
        id_=chunk.chunk_id,
        text=chunk.text,
        metadata=_metadata(chunk),
    )


def _to_document(chunk: DocumentChunk) -> Document:
    """Convert a parent DocumentChunk to a LlamaIndex Document."""
    return Document(
        doc_id=chunk.chunk_id,
        text=chunk.text,
        metadata=_metadata(chunk),
    )


def _resolve_dir(index_dir: Optional[Path]) -> Path:
    return index_dir if index_dir is not None else config.index_dir


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_indexes(
    parent_chunks: List[DocumentChunk],
    child_chunks: List[DocumentChunk],
    index_dir: Optional[Path] = None,
    embed_model: Optional[BaseEmbedding] = None,
) -> IndexManifest:
    """
    Build and persist the child VectorStoreIndex and parent document store.

    Overwrites any existing stores at index_dir. Chunk IDs are deterministic
    (SHA-256, Phase 3A) so rebuilding with identical input produces the same
    node IDs — enabling idempotent future upserts.

    Args:
        parent_chunks: Parent-level DocumentChunks (synthesis context).
        child_chunks:  Child-level DocumentChunks (retrieval units).
        index_dir:     Root output directory. Defaults to config.index_dir.
        embed_model:   Any BaseEmbedding subclass, or None to defer to
                       llama_index.core.Settings.embed_model. Never import a
                       concrete embedder here — inject from the call site.

    Returns:
        IndexManifest with build statistics written to build_manifest.json.
    """
    root = _resolve_dir(index_dir)
    child_dir = root / _CHILD_SUBDIR
    parent_dir = root / _PARENT_SUBDIR
    child_dir.mkdir(parents=True, exist_ok=True)
    parent_dir.mkdir(parents=True, exist_ok=True)

    # --- Child VectorStoreIndex ---
    child_nodes = [_to_text_node(c) for c in child_chunks]
    index_kwargs: dict = {"nodes": child_nodes, "show_progress": False}
    if embed_model is not None:
        index_kwargs["embed_model"] = embed_model

    child_index = VectorStoreIndex(**index_kwargs)
    child_index.storage_context.persist(persist_dir=str(child_dir))
    logger.info(
        "index_builder: child index persisted",
        path=str(child_dir),
        node_count=len(child_nodes),
    )

    # --- Parent SimpleDocumentStore ---
    parent_docs = [_to_document(c) for c in parent_chunks]
    parent_store = SimpleDocumentStore()
    if parent_docs:
        parent_store.add_documents(parent_docs)
    parent_store_path = parent_dir / "docstore.json"
    parent_store.persist(str(parent_store_path))
    logger.info(
        "index_builder: parent store persisted",
        path=str(parent_store_path),
        doc_count=len(parent_docs),
    )

    # --- Manifest ---
    all_chunks = list(parent_chunks) + list(child_chunks)
    doc_ids = sorted({c.doc_id for c in all_chunks})
    embed_label = (
        embed_model.__class__.__name__
        if embed_model is not None
        else config.embedding_model
    )
    manifest = IndexManifest(
        index_dir=str(root),
        embedding_model=embed_label,
        parent_count=len(parent_chunks),
        child_count=len(child_chunks),
        doc_ids=doc_ids,
    )
    manifest_path = root / _MANIFEST_FILE
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    logger.info(
        "index_builder: manifest written",
        path=str(manifest_path),
        parent_count=manifest.parent_count,
        child_count=manifest.child_count,
    )
    return manifest


def load_child_index(
    index_dir: Optional[Path] = None,
    embed_model: Optional[BaseEmbedding] = None,
) -> VectorStoreIndex:
    """
    Load the persisted child VectorStoreIndex from disk.

    Args:
        index_dir:   Root directory containing index artifacts.
                     Defaults to config.index_dir.
        embed_model: Embedding model to attach to the reloaded index.
                     Should match the model used at build time. When None,
                     the project's local embedder (get_embed_model()) is used
                     so the query path never falls back to OpenAI defaults.

    Returns:
        The reloaded VectorStoreIndex.

    Raises:
        FileNotFoundError: If child_index/ directory does not exist.
    """
    root = _resolve_dir(index_dir)
    child_dir = root / _CHILD_SUBDIR
    if not child_dir.exists():
        raise FileNotFoundError(
            f"Child index directory not found: {child_dir}. "
            "Run build_indexes() first."
        )
    storage_context = StorageContext.from_defaults(persist_dir=str(child_dir))
    if embed_model is None:
        # Resolve the project's local embedding model so query-time embedding
        # never falls back to LlamaIndex's default OpenAI embedder (which would
        # raise "No API key found for OpenAI"). The import is deferred so this
        # module stays import-light and the HuggingFace model is only loaded
        # when an index is actually loaded without an explicit embedder.
        from src.indexing.embed_config import get_embed_model  # deferred — lazy HF import

        embed_model = get_embed_model()
    return load_index_from_storage(storage_context, embed_model=embed_model)


def load_parent_store(
    index_dir: Optional[Path] = None,
) -> SimpleDocumentStore:
    """
    Load the persisted parent document store from disk.

    Args:
        index_dir: Root directory containing index artifacts.
                   Defaults to config.index_dir.

    Returns:
        The reloaded SimpleDocumentStore.

    Raises:
        FileNotFoundError: If parent_store/docstore.json does not exist.
    """
    root = _resolve_dir(index_dir)
    store_path = root / _PARENT_SUBDIR / "docstore.json"
    if not store_path.exists():
        raise FileNotFoundError(
            f"Parent store not found: {store_path}. "
            "Run build_indexes() first."
        )
    return SimpleDocumentStore.from_persist_path(str(store_path))
