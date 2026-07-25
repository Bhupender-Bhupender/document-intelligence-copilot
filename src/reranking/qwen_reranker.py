"""
Reranking layer: cross-encoder re-scoring of retrieved chunks.

Public API
----------
    rerank(
        query: str,
        chunks: List[RetrievedChunk],
        top_k: int | None = None,
        model_name: str | None = None,
        *,
        _model: Any = None,
    ) -> List[RetrievedChunk]

Design
------
``rerank`` is a pure post-processing step. It accepts the project-native
``List[RetrievedChunk]`` produced by any retrieval path (dense, sparse, or
hybrid) and re-scores each chunk against the query using a cross-encoder.

This layer sits *after* retrieval and *before* answer synthesis:

    retrieve_hybrid(query) → List[RetrievedChunk]
           │
           ▼
    rerank(query, chunks)  → List[RetrievedChunk]  ← this module
           │
           ▼
    answer synthesis (Phase 6)

Cross-encoder model
-------------------
Default model: ``Qwen/Qwen3-Reranker-0.6B``
Loaded via ``sentence_transformers.CrossEncoder``, which handles the
model-specific prompt format internally. The model takes
``(query, passage)`` pairs and returns a relevance score per pair.

Score semantics
---------------
``rerank_score`` is the **raw logit** output of the cross-encoder — an
unbounded floating-point value (not a probability). Higher values indicate
higher relevance. Callers that need a probability can apply sigmoid, but
this is not required for ranking. The field name is deliberately not
``rerank_probability`` to avoid implying normalisation.

The existing ``vector_score``, ``bm25_score``, and ``fusion_score`` fields
are **never modified** — they preserve the full retrieval provenance.

Lazy model loading
------------------
``from sentence_transformers import CrossEncoder`` is inside the body of
``_load_model()``. Importing this module does not trigger a model download.
The model is downloaded and cached in ``_MODEL_CACHE`` on the first call to
``rerank()`` with a real model. In tests, pass a fake model via the
``_model`` keyword argument — no network access occurs.

Test injection
--------------
The ``_model`` keyword-only parameter bypasses ``_load_model`` entirely.
Any object with a ``predict(sentence_pairs) -> Sequence[float]`` method is
accepted. Prefix ``_`` signals this is an internal seam, not a public
configuration option.

    class _FakeReranker:
        def predict(self, pairs):
            return [1.0, 0.5, 0.2]   # synthetic scores

    result = rerank(query, chunks, _model=_FakeReranker())

Tie-breaking
------------
``list.sort`` is stable. Equal ``rerank_score`` values preserve the
incoming order (which is fusion-score descending when the input came from
``retrieve_hybrid``).

LlamaIndex BaseNodePostprocessor alternative
--------------------------------------------
The LlamaIndex ecosystem provides ``BaseNodePostprocessor`` for integrating
postprocessors into LlamaIndex query pipelines. That interface uses
``List[NodeWithScore]`` — LlamaIndex-native types. The project-native API
here is preferred at module boundaries, but a thin ``BaseNodePostprocessor``
adapter can be added later (wrapping this function) without changing this
module.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from src.schema.models import RetrievedChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

_DEFAULT_MODEL = "Qwen/Qwen3-Reranker-0.6B"

# Module-level cache: avoids reloading the model on every call.
# Keyed by model_name string. Tests never populate this cache because
# they inject _model directly.
_MODEL_CACHE: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_model(model_name: str) -> Any:
    """
    Load and cache a CrossEncoder model by name.

    The ``sentence_transformers`` import is deferred inside this function so
    that importing ``qwen_reranker`` does not trigger any model download or
    require sentence-transformers to be installed in environments that only
    use the project schema and other lightweight modules.

    Args:
        model_name: HuggingFace model ID or local path.

    Returns:
        A ``CrossEncoder`` instance, cached for subsequent calls.
    """
    if model_name not in _MODEL_CACHE:
        from sentence_transformers import CrossEncoder  # noqa: PLC0415

        logger.debug("qwen_reranker: loading cross-encoder model", model_name=model_name)
        _MODEL_CACHE[model_name] = CrossEncoder(model_name)
        logger.debug("qwen_reranker: model loaded", model_name=model_name)

    return _MODEL_CACHE[model_name]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def rerank(
    query: str,
    chunks: List[RetrievedChunk],
    top_k: Optional[int] = None,
    model_name: Optional[str] = None,
    *,
    _model: Any = None,
) -> List[RetrievedChunk]:
    """
    Re-score retrieved chunks against a query using a cross-encoder model.

    Accepts any ``List[RetrievedChunk]`` — from dense, sparse, or hybrid
    retrieval — and returns the same chunks re-sorted by ``rerank_score``.
    All existing score fields (``vector_score``, ``bm25_score``,
    ``fusion_score``) are preserved unchanged.

    Args:
        query:      The query string. Each chunk is scored as the pair
                    ``(query, chunk.text)``.
        chunks:     Retrieved chunks to rerank. May be empty.
        top_k:      If provided, return only the top-k highest-scoring
                    chunks after reranking. None returns all chunks.
        model_name: Cross-encoder model to use. Defaults to
                    ``"Qwen/Qwen3-Reranker-0.6B"``. Ignored when
                    ``_model`` is supplied.
        _model:     Internal test injection point. Any object that
                    implements ``predict(sentence_pairs) -> Sequence[float]``
                    is accepted. When provided, ``model_name`` and
                    ``_MODEL_CACHE`` are bypassed entirely.

    Returns:
        ``List[RetrievedChunk]`` ordered by descending ``rerank_score``.
        Each item has ``rerank_score`` populated (a raw logit — higher
        means more relevant). ``vector_score``, ``bm25_score``, and
        ``fusion_score`` are preserved from the input. Equal ``rerank_score``
        values preserve the incoming order (stable sort).
        Empty list when ``chunks`` is empty.

    Note:
        ``rerank_score`` is a raw cross-encoder logit, not a probability.
        Values are unbounded floats. Do not compare magnitudes across
        different models or queries.
    """
    if not chunks:
        logger.debug("qwen_reranker: empty input, returning empty list")
        return []

    model = _model if _model is not None else _load_model(model_name or _DEFAULT_MODEL)

    sentence_pairs = [(query, chunk.text) for chunk in chunks]
    raw_scores = model.predict(sentence_pairs)

    # Apply rerank_score to each chunk; originals are never mutated.
    reranked: List[RetrievedChunk] = [
        chunk.model_copy(update={"rerank_score": float(score)})
        for chunk, score in zip(chunks, raw_scores)
    ]

    # Sort descending by rerank_score. Python sort is stable: equal scores
    # preserve the incoming order (fusion-score descending from retrieve_hybrid).
    reranked.sort(key=lambda c: c.rerank_score, reverse=True)  # type: ignore[arg-type]

    result = reranked[:top_k] if top_k is not None else reranked

    logger.debug(
        "qwen_reranker: reranked chunks",
        query_len=len(query),
        input_count=len(chunks),
        top_k=top_k,
        returned=len(result),
    )

    return result
