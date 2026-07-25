"""
End-to-end indexing pipeline.

Wires together the ingestion router, hierarchical chunker, and index builder
into a single callable for indexing a single document file.

Public API
----------
    run_indexing_pipeline(
        file_path: Path,
        index_dir: Path | None = None,
        embed_model: BaseEmbedding | None = None,
    ) -> IndexManifest

Scope
-----
This module covers: file routing → parsing/OCR → hierarchical chunking →
index building. Retrieval, reranking, and generation are out of scope here.

Embedding injection
-------------------
Pass ``embed_model`` explicitly (recommended for production and mandatory
in tests). If None, ``get_embed_model()`` is called automatically — this
requires llama-index-embeddings-huggingface installed and will download
the configured model on first use.

In tests, always pass ``embed_model=MockEmbedding(...)`` to keep tests
fast and avoid any model downloads.

Index output isolation
----------------------
``index_dir`` defaults to config.index_dir (data/index/) when None.
In tests, always pass ``index_dir=tmp_path`` (or a subdirectory thereof)
so that no test ever writes to the shared project index directory.
"""
from __future__ import annotations

from pathlib import Path

from llama_index.core.embeddings import BaseEmbedding

from src.chunking.hierarchical_chunker import build_hierarchical_chunks
from src.indexing.embed_config import get_embed_model
from src.indexing.index_builder import IndexManifest
from src.ingestion.router import route_file


def run_indexing_pipeline(
    file_path: Path,
    index_dir: Path | None = None,
    embed_model: BaseEmbedding | None = None,
) -> IndexManifest:
    """
    Index a single document file end to end.

    Steps:
        1. Route the file to the appropriate reader/parser (text, PDF, DOCX).
        2. Build hierarchical parent and child chunks.
        3. Build and persist the child VectorStoreIndex and parent document
           store under index_dir.

    Args:
        file_path:   Path to the source document (.txt, .md, .pdf, .docx).
        index_dir:   Directory to persist index files. Defaults to
                     config.index_dir (data/index/) when None.
                     **Always override in tests** by passing a tmp_path-based
                     directory to keep test output isolated.
        embed_model: LlamaIndex BaseEmbedding instance used for child chunk
                     embedding. If None, get_embed_model() is called, which
                     loads the HuggingFace model from config.embedding_model.
                     **Always pass an explicit model in tests** (e.g.
                     MockEmbedding) to prevent model downloads.

    Returns:
        IndexManifest with build statistics and provenance metadata.
    """
    file_path = Path(file_path)

    if embed_model is None:
        embed_model = get_embed_model()

    raw_doc, pages = route_file(file_path)
    parent_chunks, child_chunks = build_hierarchical_chunks(raw_doc, pages)
    from src.indexing.index_gateway import route_index  # deferred — keeps module import-neutral
    return route_index(
        parent_chunks,
        child_chunks,
        index_dir=index_dir,
        embed_model=embed_model,
    )
