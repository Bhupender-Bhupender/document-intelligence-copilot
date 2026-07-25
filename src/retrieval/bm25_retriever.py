"""
Sparse retrieval layer: BM25 lexical search over the child chunk corpus.

Public API
----------
    retrieve_children_bm25(
        query: str,
        index_dir: Path | None = None,
        top_k: int = 5,
    ) -> List[RetrievedChunk]

Design
------
BM25Plus is built in-memory from the persisted child chunk text corpus on
every call. No BM25 structure is persisted to disk.

Corpus source
-------------
Child chunks are already persisted in ``child_index/docstore.json`` as
LlamaIndex TextNode objects (written by Phase 4 ``build_indexes()``). This
module reads that same store via ``StorageContext.from_defaults``, avoiding
any new storage format or separate corpus file.

    StorageContext (child_index/)
        docstore.docs → Dict[str, BaseNode]
            each node has .text and .metadata with all _META_KEYS fields

BM25 corpus rebuild cost
------------------------
BM25Plus over a corpus of short text chunks (typically < 10 000 nodes) builds
in tens of milliseconds. Persisting a pickled BM25 object would introduce a
cache invalidation problem whenever the corpus changes and adds serialisation
dependencies. Rebuild-on-demand is correct at this scale.

Tokenisation
------------
Tokens are extracted with ``re.findall(r'\\b\\w+\\b', text.lower())``:
word-boundary matching that strips all punctuation and preserves alphanumeric
plus underscore word characters. This is stdlib-only (no NLTK or spaCy), is
consistent with how users naturally phrase queries, and correctly handles
punctuation-adjacent terms such as "zyphron." or "policy," which would
otherwise fail to match the unpunctuated query token "zyphron" / "policy".

Zero-match behaviour
--------------------
``BM25Plus.get_scores()`` returns a float array; a score of 0.0 means no
lexical overlap. Zero-score results are excluded from the output. A query
that produces all-zero scores returns an **empty list**.

Parent lookup compatibility
---------------------------
Every returned ``RetrievedChunk`` has ``parent_chunk_id`` populated (or None
for flat chunks). Sparse-retrieved results can therefore be passed directly to
``lookup_parents()`` from ``src.retrieval.vector_retriever`` without any
adaptation.

Output alignment with dense retrieval
--------------------------------------
Both paths return ``List[RetrievedChunk]`` with identical field coverage.
The only deliberate differences:

    Dense path  → retrieval_method="vector", vector_score set, bm25_score=None
    Sparse path → retrieval_method="bm25",   bm25_score set, vector_score=None

All metadata fields (chunk_id, doc_id, page_id, page_number, file_name,
file_type, section_title, parent_chunk_id) are populated identically.

FileNotFoundError contract
--------------------------
Raised when ``child_index/`` directory is absent under ``index_dir``.
Matches the contract of ``load_child_index()`` in index_builder.

Index isolation
---------------
``index_dir`` defaults to config.index_dir (data/index/) when None.
In tests, always pass ``index_dir=tmp_path`` so no test touches the
project index directory.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple

import numpy as np
from rank_bm25 import BM25Plus

from llama_index.core import StorageContext

from src.core.config import config
from src.schema.models import RetrievedChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

_CHILD_SUBDIR = "child_index"

# Compiled regex for tokenisation: extract word-boundary-delimited tokens,
# stripping all punctuation. Stdlib-only, no NLTK required.
_TOKEN_RE = re.compile(r"\b\w+\b")


def _tokenize(text: str) -> List[str]:
    """
    Extract lowercase word tokens from text, stripping punctuation.

    Uses word-boundary regex (\\b\\w+\\b) so that punctuation-adjacent terms
    such as "policy," or "zyphron." are reduced to their clean forms "policy"
    and "zyphron", enabling reliable exact-term matching in BM25.
    """
    return _TOKEN_RE.findall(text.lower())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_dir(index_dir: Path | None) -> Path:
    return index_dir if index_dir is not None else config.index_dir


def _load_child_corpus(child_dir: Path) -> List[Tuple[str, dict]]:
    """
    Load all child chunk texts and metadata from the persisted docstore.

    Returns a list of (text, metadata) tuples in stable iteration order.
    The position of each tuple corresponds to the BM25 corpus index.

    Args:
        child_dir: Path to the child_index/ directory (must exist).

    Returns:
        List of (text, metadata) pairs, one per child node.
    """
    storage_context = StorageContext.from_defaults(persist_dir=str(child_dir))
    nodes = storage_context.docstore.docs  # Dict[str, BaseNode]
    corpus = []
    for node in nodes.values():
        text = node.text or ""
        corpus.append((text, node.metadata))
    return corpus


def _node_to_retrieved_chunk(text: str, meta: dict, bm25_score: float) -> RetrievedChunk:
    """
    Convert a child node's text and metadata to a project-native RetrievedChunk.

    Empty-string metadata values for section_title and parent_chunk_id are
    normalised back to None (they are stored as "" when the original field
    was None — LlamaIndex metadata requirement).

    Args:
        text:       Node text content.
        meta:       Node metadata dict (contains all _META_KEYS fields).
        bm25_score: BM25Plus score for this node against the query.

    Returns:
        RetrievedChunk with retrieval_method="bm25" and bm25_score set.
    """
    section_title = meta.get("section_title") or None
    parent_chunk_id = meta.get("parent_chunk_id") or None
    file_type = meta.get("file_type") or None

    return RetrievedChunk(
        chunk_id=meta["chunk_id"],
        doc_id=meta["doc_id"],
        page_id=meta["page_id"],
        file_name=meta["file_name"],
        page_number=int(meta["page_number"]),
        section_title=section_title,
        text=text,
        word_count=len(text.split()),
        retrieval_method="bm25",
        bm25_score=float(bm25_score),
        parent_chunk_id=parent_chunk_id,
        file_type=file_type,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def retrieve_children_bm25(
    query: str,
    index_dir: Path | None = None,
    top_k: int = 5,
) -> List[RetrievedChunk]:
    """
    Run BM25 lexical search over the child chunk corpus.

    Loads the persisted child chunk texts from the child index docstore,
    builds a BM25Plus model in-memory, scores all chunks against the query,
    and returns the top-k highest-scoring chunks as project-native
    RetrievedChunk objects.

    Zero-score results (no lexical overlap with query) are excluded. A query
    that produces all-zero scores returns an empty list.

    Args:
        query:     Natural language or keyword query string.
        index_dir: Root index directory. Defaults to config.index_dir.
                   In tests, always pass ``index_dir=tmp_path``.
        top_k:     Maximum number of results to return. Actual count may be
                   lower if fewer chunks have non-zero BM25 scores or the
                   corpus contains fewer nodes than top_k.

    Returns:
        List of RetrievedChunk ordered by descending BM25 score.
        Each result has parent_chunk_id populated when available.
        Empty list when no chunk lexically matches the query.

    Raises:
        FileNotFoundError: if child_index/ does not exist under index_dir.
    """
    root = _resolve_dir(index_dir)
    child_dir = root / _CHILD_SUBDIR

    if not child_dir.exists():
        raise FileNotFoundError(
            f"Child index directory not found: {child_dir}. "
            "Run build_indexes() first."
        )

    corpus = _load_child_corpus(child_dir)

    if not corpus:
        logger.debug("bm25_retriever: corpus is empty, returning empty list")
        return []

    texts, metas = zip(*corpus)
    tokenized_corpus = [_tokenize(text) for text in texts]
    tokenized_query = _tokenize(query)

    # Empty query (no tokens after tokenisation) → no match possible
    if not tokenized_query:
        logger.debug("bm25_retriever: empty tokenized query, returning empty list")
        return []

    bm25 = BM25Plus(tokenized_corpus)
    scores: np.ndarray = bm25.get_scores(tokenized_query)

    # Pair each score with its corpus index; keep only non-zero scores
    scored_indices = [
        (float(score), idx)
        for idx, score in enumerate(scores)
        if score > 0.0
    ]

    # Sort descending by score and take top_k
    scored_indices.sort(key=lambda x: x[0], reverse=True)
    top_indices = scored_indices[:top_k]

    results = [
        _node_to_retrieved_chunk(texts[idx], metas[idx], score)
        for score, idx in top_indices
    ]

    logger.debug(
        "bm25_retriever: retrieved children",
        query_len=len(query),
        top_k=top_k,
        corpus_size=len(corpus),
        non_zero=len(scored_indices),
        returned=len(results),
    )
    return results
