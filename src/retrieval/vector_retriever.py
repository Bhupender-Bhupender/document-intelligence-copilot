"""
Dense retrieval layer: vector similarity search over the child chunk index
with parent-context recovery from the parent document store.

Public API
----------
    retrieve_children(
        query: str,
        index_dir: Path | None = None,
        embed_model: BaseEmbedding | None = None,
        top_k: int = 5,
    ) -> List[RetrievedChunk]

    lookup_parents(
        retrieved: List[RetrievedChunk],
        index_dir: Path | None = None,
    ) -> List[Optional[DocumentChunk]]

Design
------
Both functions are stateless and load their stores from disk on each call.
No LlamaIndex type crosses the module boundary: ``NodeWithScore`` and
``Document`` are used internally and converted to project-level types before
returning.

Retrieval flow
--------------
    retrieve_children(query, top_k)
        │  loads child VectorStoreIndex (Phase 4 index_builder)
        │  runs similarity_top_k search
        │  converts NodeWithScore → RetrievedChunk
        ▼
    List[RetrievedChunk]   (each has parent_chunk_id populated)
        │
    lookup_parents(retrieved)
        │  loads parent SimpleDocumentStore (Phase 4 index_builder)
        │  looks up each parent_chunk_id
        │  converts Document → DocumentChunk
        ▼
    List[Optional[DocumentChunk]]  (None where parent_chunk_id is absent)

Embedding injection
-------------------
Pass ``embed_model`` explicitly in tests (use MockEmbedding) to avoid model
downloads. In production, pass the same HuggingFaceEmbedding instance that
was used at index build time, or leave None if Settings.embed_model is set.

Index isolation
---------------
``index_dir`` defaults to config.index_dir (data/index/) when None.
In tests, always pass ``index_dir=tmp_path`` so no test touches the project
index directory.

Reconstruction compatibility notes
-----------------------------------
When converting a stored parent Document back to DocumentChunk, two fields
cannot be recovered from the stored metadata:

    chunk_index  — not stored in index metadata; reconstructed as 0.
                   This field is an intra-page ordering hint used during
                   chunking and is not required for parent-context synthesis.

    word_count   — not stored in index metadata; derived from
                   ``len(text.split())``. This is the same derivation used
                   for child chunks in ``_to_retrieved_chunk()`` and is
                   accurate to within whitespace normalisation differences.

All other DocumentChunk fields (chunk_id, doc_id, page_id, page_number,
file_name, file_type, section_title, chunk_level, parent_chunk_id, text)
are recovered exactly from stored metadata.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.schema import NodeWithScore

from src.indexing.index_builder import load_child_index, load_parent_store
from src.schema.models import DocumentChunk, RetrievedChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Internal conversion helpers
# ---------------------------------------------------------------------------


def _to_retrieved_chunk(nws: NodeWithScore) -> RetrievedChunk:
    """
    Convert a LlamaIndex NodeWithScore to a project-native RetrievedChunk.

    Empty-string metadata values for section_title and parent_chunk_id
    (stored as "" when the original field was None) are normalised back
    to None.
    """
    meta = nws.node.metadata
    text = nws.node.text or ""
    section_title = meta.get("section_title") or None
    parent_chunk_id = meta.get("parent_chunk_id") or None

    return RetrievedChunk(
        chunk_id=meta["chunk_id"],
        doc_id=meta["doc_id"],
        page_id=meta["page_id"],
        file_name=meta["file_name"],
        page_number=int(meta["page_number"]),
        section_title=section_title,
        text=text,
        word_count=len(text.split()),
        retrieval_method="vector",
        vector_score=float(nws.score) if nws.score is not None else None,
        parent_chunk_id=parent_chunk_id,
        file_type=meta.get("file_type") or None,
    )


def _document_to_chunk(doc) -> DocumentChunk:
    """
    Reconstruct a parent DocumentChunk from a stored LlamaIndex Document.

    See module docstring for reconstruction compatibility notes on chunk_index
    and word_count.
    """
    meta = doc.metadata
    text = doc.text or ""
    section_title = meta.get("section_title") or None
    parent_chunk_id_raw = meta.get("parent_chunk_id") or None

    return DocumentChunk(
        chunk_id=meta["chunk_id"],
        doc_id=meta["doc_id"],
        page_id=meta["page_id"],
        page_number=int(meta["page_number"]),
        file_name=meta["file_name"],
        file_type=meta["file_type"],
        section_title=section_title,
        text=text,
        word_count=len(text.split()),
        chunk_index=0,          # not stored in index metadata; see module docstring
        chunk_level="parent",   # always parent — these are stored parent chunks
        parent_chunk_id=parent_chunk_id_raw,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def retrieve_children(
    query: str,
    index_dir: Path | None = None,
    embed_model: BaseEmbedding | None = None,
    top_k: int = 5,
) -> List[RetrievedChunk]:
    """
    Run vector similarity search over the child chunk index.

    Loads the persisted child VectorStoreIndex, embeds the query, and returns
    the top-k most similar child chunks as project-native RetrievedChunk
    objects.

    Args:
        query:       Natural language query string.
        index_dir:   Root index directory. Defaults to config.index_dir.
                     In tests, always pass ``index_dir=tmp_path``.
        embed_model: Embedding model to use for query encoding. Should match
                     the model used at index build time. Pass MockEmbedding
                     in tests. Defaults to Settings.embed_model when None.
        top_k:       Maximum number of child chunks to return. Actual count
                     may be lower if the index contains fewer nodes.

    Returns:
        List of RetrievedChunk ordered by descending similarity score.
        Each result has parent_chunk_id populated when available.

    Raises:
        FileNotFoundError: if child_index/ does not exist under index_dir.
    """
    index = load_child_index(index_dir=index_dir, embed_model=embed_model)
    retriever = index.as_retriever(similarity_top_k=top_k)
    node_results: List[NodeWithScore] = retriever.retrieve(query)

    results = [_to_retrieved_chunk(nws) for nws in node_results]
    logger.debug(
        "vector_retriever: retrieved children",
        query_len=len(query),
        top_k=top_k,
        returned=len(results),
    )
    return results


def lookup_parents(
    retrieved: List[RetrievedChunk],
    index_dir: Path | None = None,
) -> List[Optional[DocumentChunk]]:
    """
    Load the parent DocumentChunk for each retrieved child chunk.

    Uses the parent_chunk_id stored on each RetrievedChunk to look up the
    corresponding parent from the persisted SimpleDocumentStore. Returns None
    in any position where the parent_chunk_id is absent or not found in the
    store.

    The returned list is parallel to ``retrieved``: index i of the output
    corresponds to index i of the input.

    Args:
        retrieved:  Child chunks from retrieve_children(), each with a
                    parent_chunk_id field.
        index_dir:  Root index directory. Defaults to config.index_dir.
                    In tests, always pass ``index_dir=tmp_path``.

    Returns:
        List of Optional[DocumentChunk], same length as retrieved.
        Each entry is the parent chunk or None.

    Raises:
        FileNotFoundError: if parent_store/docstore.json does not exist.
    """
    parent_store = load_parent_store(index_dir=index_dir)
    results: List[Optional[DocumentChunk]] = []

    for rc in retrieved:
        if not rc.parent_chunk_id:
            results.append(None)
            continue
        doc = parent_store.get_document(rc.parent_chunk_id, raise_error=False)
        if doc is not None:
            results.append(_document_to_chunk(doc))
        else:
            logger.debug(
                "vector_retriever: parent not found",
                parent_chunk_id=rc.parent_chunk_id,
            )
            results.append(None)

    logger.debug(
        "vector_retriever: parent lookup complete",
        requested=len(retrieved),
        found=sum(1 for p in results if p is not None),
    )
    return results
