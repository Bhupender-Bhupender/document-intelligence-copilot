"""
End-to-end answer pipeline: hybrid retrieval → reranking → parent lookup → synthesis.

Public API
----------
    run_pipeline(
        query: str,
        index_dir: Path | None = None,
        retrieval_top_k: int = 10,
        rerank_top_k: int = 5,
        model: str | None = None,
        *,
        _retriever:     Callable[[str], List[RetrievedChunk]] | None = None,
        _reranker:      Callable[[str, List[RetrievedChunk]], List[RetrievedChunk]] | None = None,
        _parent_lookup: Callable[[List[RetrievedChunk]], List[Optional[DocumentChunk]]] | None = None,
        _generator:     Callable[[List[dict]], str] | None = None,
    ) -> AnswerResponse

Design
------
``run_pipeline`` is the single entry point for the complete answer pipeline. It
coordinates the four existing stage functions in a fixed, deterministic order:

    1. retrieve_hybrid  — hybrid dense + BM25 retrieval with RRF fusion
    2. rerank           — cross-encoder reranking of fused results
    3. lookup_parents   — load parent chunks aligned to reranked child list
    4. synthesise       — grounded answer generation with parent-context enrichment

Each stage is individually injectable via keyword-only parameters (_retriever,
_reranker, _parent_lookup, _generator). When an injectable is provided, the
corresponding real function is bypassed entirely — no I/O, no model load. This
keeps all unit tests lightweight.

Stage order
-----------
The order is non-negotiable and intentional:
- Retrieval before reranking: you can only rerank what you have retrieved.
- Reranking before parent lookup: parent lookup is keyed on parent_chunk_id
  fields carried by RetrievedChunk; performing it before reranking wastes
  lookups for chunks that will not survive the top-k cut.
- Parent lookup before synthesis: synthesise uses the parallel parents list
  for context enrichment. The list must be aligned with the reranked chunk list.

Empty retrieval
---------------
When retrieval returns an empty list, rerank returns [] and lookup_parents
returns []. synthesise handles [] correctly: it generates a response using the
"no context provided" placeholder. No special-casing in this module.

Injection signatures
--------------------
The injected callables use simplified signatures that match the coordination
contract, not the full signatures of the underlying functions:

    _retriever(query: str)           -> List[RetrievedChunk]
    _reranker(query, chunks)         -> List[RetrievedChunk]
    _parent_lookup(chunks)           -> List[Optional[DocumentChunk]]
    _generator(messages)             -> str   (forwarded to synthesise)

In unit tests the caller closes over index_dir / top_k / model_name in the
fake's own scope — the injected callable needs only the runtime data that
flows through the pipeline.

Citations
---------
After synthesis, build_citations(reranked) is called to populate
AnswerResponse.sources with deterministic CitationRecord objects — one per
reranked chunk. The enriched response is returned via model_copy(update=...).

Annotation
----------
AnswerResponse.validation_flags is always []. Populated in Phase 8.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, List, Optional

from src.schema.models import AnswerResponse, CitationRecord, DocumentChunk, RetrievedChunk, RoutingPlan
from src.core.config import config
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_pipeline(
    query: str,
    index_dir: Optional[Path] = None,
    retrieval_top_k: int = 10,
    rerank_top_k: int = 5,
    model: Optional[str] = None,
    routing_plan: Optional[RoutingPlan] = None,
    *,
    _retriever: Optional[Callable[[str], List[RetrievedChunk]]] = None,
    _reranker: Optional[
        Callable[[str, List[RetrievedChunk]], List[RetrievedChunk]]
    ] = None,
    _parent_lookup: Optional[
        Callable[[List[RetrievedChunk]], List[Optional[DocumentChunk]]]
    ] = None,
    _generator: Optional[Callable[[List[Dict[str, str]]], str]] = None,
    _citation_builder: Optional[
        Callable[[List[RetrievedChunk]], List[CitationRecord]]
    ] = None,
    _validator: Optional[Callable[["AnswerResponse"], "AnswerResponse"]] = None,
) -> AnswerResponse:
    """
    Run the complete answer pipeline for a user query.

    Executes the four pipeline stages in order:
        retrieve_hybrid → rerank → lookup_parents → synthesise

    Each stage can be replaced by an injected callable for testing.
    When injected, the real function and any associated I/O are bypassed.

    Parameters
    ----------
    query:
        The user's natural language question.
    index_dir:
        Root index directory for retrieval and parent lookup.
        Defaults to config.index_dir. Ignored when _retriever and
        _parent_lookup are both injected.
    retrieval_top_k:
        Maximum number of candidates returned by hybrid retrieval before
        reranking. Passed to retrieve_hybrid(top_k=...).
    rerank_top_k:
        Maximum number of results returned by the reranker. The reranked
        list is used as-is for both synthesis context and
        AnswerResponse.supporting_chunks.
    model:
        Ollama model tag override for generation. Passed to synthesise().
        Defaults to config.generation_model.
    routing_plan:
        Optional deterministic routing plan from route_query(). When
        provided, its retrieval_top_k and rerank_top_k override the
        corresponding parameters above. emphasize_parent_context controls
        whether parent chunks are forwarded to synthesise() (True) or
        suppressed so synthesis uses child text only (False).
    _retriever:
        Test injection. Replaces retrieve_hybrid.
        Signature: (query: str) -> List[RetrievedChunk]
    _reranker:
        Test injection. Replaces rerank.
        Signature: (query: str, chunks: List[RetrievedChunk]) -> List[RetrievedChunk]
    _parent_lookup:
        Test injection. Replaces lookup_parents.
        Signature: (chunks: List[RetrievedChunk]) -> List[Optional[DocumentChunk]]
    _generator:
        Test injection. Forwarded to synthesise as _generator.
        Signature: (messages: List[dict]) -> str
    _citation_builder:
        Test injection. Replaces build_citations.
        Signature: (chunks: List[RetrievedChunk]) -> List[CitationRecord]
    _validator:
        Test injection. Replaces validate_response.
        Signature: (response: AnswerResponse) -> AnswerResponse

    Returns
    -------
    AnswerResponse
        Project-native answer result. Always has:
        - query: the original query string
        - answer_text: grounded response from the language model
        - model_used: Ollama model tag that generated the answer
        - supporting_chunks: the reranked chunk list (authoritative provenance)
        - sources: deterministic CitationRecord list (one per reranked chunk)
        - validation_flags: diagnostic strings from rule-based validation
        - latency_ms: generation time in milliseconds (synthesis stage only)
        - run_id: auto-generated UUID for this pipeline run
    """
    resolved_dir = index_dir or config.index_dir

    # ------------------------------------------------------------------
    # Apply routing plan overrides (when provided)
    # ------------------------------------------------------------------
    if routing_plan is not None:
        retrieval_top_k = routing_plan.retrieval_top_k
        rerank_top_k = routing_plan.rerank_top_k
        logger.info(
            "pipeline_routed",
            query_type=routing_plan.query_type,
            retrieval_top_k=retrieval_top_k,
            rerank_top_k=rerank_top_k,
            emphasize_parent=routing_plan.emphasize_parent_context,
        )

    logger.info(
        "pipeline_start",
        query_chars=len(query),
        retrieval_top_k=retrieval_top_k,
        rerank_top_k=rerank_top_k,
    )

    # ------------------------------------------------------------------
    # Stage 1: Hybrid retrieval
    # ------------------------------------------------------------------
    if _retriever is not None:
        retrieved = _retriever(query)
    else:
        from src.retrieval.retrieval_gateway import route_retrieve
        retrieved = route_retrieve(
            query,
            index_dir=resolved_dir,
            top_k=retrieval_top_k,
        )

    logger.debug("pipeline_retrieved", count=len(retrieved))

    # ------------------------------------------------------------------
    # Stage 2: Reranking
    # ------------------------------------------------------------------
    if _reranker is not None:
        reranked = _reranker(query, retrieved)
    else:
        from src.reranking.qwen_reranker import rerank
        reranked = rerank(query, retrieved, top_k=rerank_top_k)

    logger.debug("pipeline_reranked", count=len(reranked))

    # ------------------------------------------------------------------
    # Stage 3: Parent lookup
    # ------------------------------------------------------------------
    if _parent_lookup is not None:
        parents = _parent_lookup(reranked)
    else:
        from src.retrieval.retrieval_gateway import route_lookup_parents
        parents = route_lookup_parents(reranked, index_dir=resolved_dir)

    logger.debug(
        "pipeline_parents_loaded",
        requested=len(reranked),
        found=sum(1 for p in parents if p is not None),
    )

    # ------------------------------------------------------------------
    # Parent-context gating (routing plan)
    # ------------------------------------------------------------------
    # When a routing plan is present and emphasize_parent_context is False,
    # suppress parent chunks so synthesise() uses child text only.
    # When no routing plan is provided, parent chunks are always forwarded
    # (preserves the existing default behaviour).
    if routing_plan is not None and not routing_plan.emphasize_parent_context:
        synthesis_parents = None
    else:
        synthesis_parents = parents

    # ------------------------------------------------------------------
    # Stage 4: Answer synthesis
    # ------------------------------------------------------------------
    from src.generation.answer_engine import synthesise

    response = synthesise(
        query,
        reranked,
        parents=synthesis_parents,
        model=model,
        _generator=_generator,
    )

    # ------------------------------------------------------------------
    # Stage 5: Citation construction
    # ------------------------------------------------------------------
    if _citation_builder is not None:
        citations = _citation_builder(reranked)
    else:
        from src.citations.citation_builder import build_citations
        citations = build_citations(reranked)

    response = response.model_copy(update={"sources": citations})

    # ------------------------------------------------------------------
    # Stage 6: Validation
    # ------------------------------------------------------------------
    if _validator is not None:
        response = _validator(response)
    else:
        from src.validation.validators import validate_response
        response = validate_response(response)

    logger.info(
        "pipeline_done",
        answer_chars=len(response.answer_text),
        supporting_chunks=len(response.supporting_chunks),
        citations=len(response.sources),
        validation_flags=len(response.validation_flags),
    )

    return response
