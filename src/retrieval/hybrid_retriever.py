"""
Hybrid retrieval: Reciprocal Rank Fusion of dense vector and sparse BM25 results.

Public API
----------
    retrieve_hybrid(
        query: str,
        index_dir: Path | None = None,
        top_k: int = 10,
        vector_top_k: int = 10,
        bm25_top_k: int = 10,
        rrf_k: int = 60,
    ) -> List[RetrievedChunk]

Design
------
``retrieve_hybrid`` is a thin coordinator: it calls ``retrieve_children``
(dense vector path) and ``retrieve_children_bm25`` (sparse BM25 path) in
sequence, then delegates to ``_rrf_fuse`` — a pure stateless function that
performs RRF scoring, deduplication, and ranking.

Fusion strategy: Reciprocal Rank Fusion (RRF)
----------------------------------------------
RRF (Cormack et al. 2009) assigns each chunk a score:

    score(chunk) = Σ  1 / (k + rank_i)

summed over every ranked list in which the chunk appears, where ``rank_i``
is the 1-based rank in that list and ``k`` is a smoothing constant
(default 60, from the original paper).

Properties:
- Deterministic and parameter-stable across datasets.
- Naturally rewards chunks that appear in *both* retrieval paths.
- Robust to score-scale differences between BM25 and vector cosine scores —
  only rank positions matter.
- No normalisation step required.

Deduplication
-------------
Primary key: ``chunk_id``.
When the same ``chunk_id`` appears in both result lists:
- The dense-path ``RetrievedChunk`` is used as the base record.
- ``bm25_score`` from the sparse-path record is injected via
  ``model_copy(update=...)``, which returns a new Pydantic instance without
  mutating the original.
- All other metadata fields (``parent_chunk_id``, ``file_type``,
  ``section_title``, etc.) are preserved from the dense-path record, which
  has identical metadata values (both paths draw from the same docstore).

Tie-breaking
------------
- Primary sort: ``fusion_score`` descending (numerically deterministic).
- Secondary: Python's ``list.sort`` is stable, so equal ``fusion_score``
  values (mathematically possible only when two chunks have identical rank
  in every shared list) preserve dense-first insertion order.

Output contract
---------------
Every returned ``RetrievedChunk`` has:
    - ``retrieval_method = "hybrid"``
    - ``fusion_score``  set and positive
    - ``vector_score``  set when the chunk appeared in the dense path
    - ``bm25_score``    set when the chunk appeared in the sparse path
    - ``parent_chunk_id`` preserved (compatible with ``lookup_parents()``)
    - ``file_type``     preserved

FileNotFoundError propagation
------------------------------
Both underlying retrievers raise ``FileNotFoundError`` when ``child_index/``
is absent under ``index_dir``. ``retrieve_hybrid`` does not suppress these.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from src.retrieval.bm25_retriever import retrieve_children_bm25
from src.retrieval.vector_retriever import lookup_parents, retrieve_children
from src.schema.models import RetrievedChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Internal fusion helper
# ---------------------------------------------------------------------------


def _rrf_fuse(
    dense_results: List[RetrievedChunk],
    sparse_results: List[RetrievedChunk],
    rrf_k: int,
    top_k: int,
) -> List[RetrievedChunk]:
    """
    Fuse two ranked result lists using Reciprocal Rank Fusion.

    Pure function — no I/O, no index access. Accepts pre-fetched result
    lists from any retrieval paths and returns a deduplicated, RRF-ranked
    list of project-native RetrievedChunk objects.

    Args:
        dense_results:  Ranked results from the dense vector retrieval path.
                        May be empty.
        sparse_results: Ranked results from the sparse BM25 retrieval path.
                        May be empty.
        rrf_k:          RRF smoothing constant. Default 60 (Cormack 2009).
        top_k:          Maximum number of results to return.

    Returns:
        List of RetrievedChunk ordered by descending fusion_score, length
        at most top_k. All items have retrieval_method="hybrid" and a
        positive fusion_score.
    """
    # Accumulate RRF scores: chunk_id → accumulated score
    scores: Dict[str, float] = {}
    # Canonical chunk record per chunk_id (dense-first insertion order)
    chunks: Dict[str, RetrievedChunk] = {}

    # --- Dense path (1-indexed rank) ---
    for rank, chunk in enumerate(dense_results, start=1):
        cid = chunk.chunk_id
        scores[cid] = scores.get(cid, 0.0) + 1.0 / (rrf_k + rank)
        if cid not in chunks:
            chunks[cid] = chunk

    # --- Sparse path (1-indexed rank) ---
    for rank, chunk in enumerate(sparse_results, start=1):
        cid = chunk.chunk_id
        scores[cid] = scores.get(cid, 0.0) + 1.0 / (rrf_k + rank)
        if cid not in chunks:
            # Chunk appears only in sparse path — store as-is
            chunks[cid] = chunk
        else:
            # Chunk appears in both paths — inject bm25_score into
            # the dense-path record (which already has vector_score)
            chunks[cid] = chunks[cid].model_copy(
                update={"bm25_score": chunk.bm25_score}
            )

    # Apply retrieval_method="hybrid" and fusion_score to all records
    merged: List[RetrievedChunk] = [
        chunks[cid].model_copy(
            update={
                "retrieval_method": "hybrid",
                "fusion_score": scores[cid],
            }
        )
        for cid in chunks
    ]

    # Sort descending by fusion_score; Python sort is stable (tie-break:
    # dense-first insertion order preserved)
    merged.sort(key=lambda c: c.fusion_score, reverse=True)  # type: ignore[arg-type]

    logger.debug(
        "hybrid_retriever: rrf_fuse complete",
        dense_in=len(dense_results),
        sparse_in=len(sparse_results),
        unique_chunks=len(merged),
        top_k=top_k,
        returned=min(len(merged), top_k),
    )

    return merged[:top_k]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def retrieve_hybrid(
    query: str,
    index_dir: Path | None = None,
    top_k: int = 10,
    vector_top_k: int = 10,
    bm25_top_k: int = 10,
    rrf_k: int = 60,
) -> List[RetrievedChunk]:
    """
    Run hybrid retrieval: RRF fusion of dense vector and sparse BM25 paths.

    Calls the dense vector retriever and the BM25 sparse retriever in
    sequence, then fuses both result lists with Reciprocal Rank Fusion.
    Duplicate chunk IDs are merged and scored with their combined RRF
    contribution. The result is a single ranked list of project-native
    RetrievedChunk objects.

    Args:
        query:        Natural language or keyword query string.
        index_dir:    Root index directory. Defaults to config.index_dir.
                      In tests, always pass ``index_dir=tmp_path``.
        top_k:        Maximum number of results to return after fusion.
        vector_top_k: Maximum results fetched from the dense path before
                      fusion. Defaults to 10.
        bm25_top_k:   Maximum results fetched from the sparse path before
                      fusion. Defaults to 10.
        rrf_k:        RRF smoothing constant (default 60).

    Returns:
        List of RetrievedChunk ordered by descending fusion_score.
        - retrieval_method is "hybrid" for all results.
        - fusion_score is populated and positive for all results.
        - vector_score is populated for chunks from the dense path.
        - bm25_score is populated for chunks from the sparse path.
        - Chunks appearing in both paths carry both scores.
        - parent_chunk_id is preserved (compatible with lookup_parents()).
        - file_type is preserved.
        Empty list when both paths return no results.

    Raises:
        FileNotFoundError: propagated from either retriever when
            child_index/ is absent under index_dir.
    """
    dense = retrieve_children(query, index_dir=index_dir, top_k=vector_top_k)
    sparse = retrieve_children_bm25(query, index_dir=index_dir, top_k=bm25_top_k)

    logger.debug(
        "hybrid_retriever: retrieved from both paths",
        dense_count=len(dense),
        sparse_count=len(sparse),
    )

    return _rrf_fuse(dense, sparse, rrf_k=rrf_k, top_k=top_k)
