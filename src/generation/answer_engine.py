"""
Answer synthesis layer: grounded answer generation from retrieved context.

Public API
----------
    synthesise(
        query: str,
        chunks: List[RetrievedChunk],
        parents: List[Optional[DocumentChunk]] | None = None,
        top_k: int | None = None,
        model: str | None = None,
        *,
        _generator: Callable[[List[dict]], str] | None = None,
    ) -> AnswerResponse

Design
------
``synthesise`` is the single entry point for answer generation. It:
    1. Applies top_k truncation to the chunk list for context assembly.
    2. Assembles context blocks using parent-context enrichment (see below).
    3. Builds a structured Ollama message list via build_grounded_messages.
    4. Calls generate() (or an injected _generator) to produce the answer.
    5. Returns an AnswerResponse with all pipeline provenance preserved.

Parent-context enrichment
-------------------------
When ``parents`` is provided, the answer engine uses parent chunk text as the
context passage for each retrieved child chunk. Parent text provides a broader
synthesis window than the child text alone, which improves answer coherence for
hierarchical-chunked documents.

Fallback rules — child text is used for position i when ANY of these hold:
    - parents is None
    - i >= len(parents)  (parents list is shorter than chunks)
    - parents[i] is None  (no parent was found for this child)

Citation contract
-----------------
AnswerResponse.sources is always returned as an empty list. Deterministic
citations are produced by the citation builder in Phase 7. This module
never generates or validates citation records.

AnswerResponse.validation_flags is always returned as an empty list.
Validation is deferred to Phase 8.

Test injection
--------------
Pass any callable(List[dict]) -> str via ``_generator`` to avoid Ollama calls
in unit tests. When _generator is provided, generate() is never imported or
called.

    _generator: Callable[[List[dict]], str]
        Receives the full message list built by build_grounded_messages.
        Returns a reply string, which becomes AnswerResponse.answer_text.
"""
from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional

from src.schema.models import AnswerResponse, DocumentChunk, RetrievedChunk
from src.core.config import config
from src.utils.logging_utils import get_logger
from src.generation.prompt_templates import build_grounded_messages
from src.generation.ollama_llm import generate

logger = get_logger(__name__)


def synthesise(
    query: str,
    chunks: List[RetrievedChunk],
    parents: Optional[List[Optional[DocumentChunk]]] = None,
    top_k: Optional[int] = None,
    model: Optional[str] = None,
    *,
    _generator: Optional[Callable[[List[Dict[str, str]]], str]] = None,
) -> AnswerResponse:
    """
    Generate a grounded answer from retrieved/reranked context.

    Parameters
    ----------
    query:
        The user's question.
    chunks:
        Retrieved (and optionally reranked) child chunks. These are always
        preserved in full as AnswerResponse.supporting_chunks regardless of
        the top_k setting.
    parents:
        Optional list of parent DocumentChunks aligned to ``chunks`` by index.
        Obtain via ``lookup_parents()`` from the retrieval layer. Where a
        parent is available, its broader text is used as the synthesis context
        for that chunk position; child text is used otherwise.
    top_k:
        If given, only the first top_k chunks are used for context assembly.
        Does not affect supporting_chunks, which always reflects the full input.
    model:
        Ollama model tag override. Defaults to config.generation_model.
    _generator:
        Optional callable(List[dict]) -> str injected for testing. When
        provided, generate() is never called and no Ollama connection is made.

    Returns
    -------
    AnswerResponse
        Project-native answer result.
        - supporting_chunks: full input chunk list (untruncated by top_k)
        - sources: empty list (Phase 7 citation builder)
        - validation_flags: empty list (Phase 8 validator)
        - latency_ms: wall-clock time for the generation call only
    """
    resolved_model = model or config.generation_model

    # top_k limits context assembly only — supporting_chunks keeps full list
    active_chunks = chunks[:top_k] if top_k is not None else chunks

    # Parent-context enrichment with per-position fallback to child text
    context_blocks: List[str] = []
    for i, child in enumerate(active_chunks):
        if (
            parents is not None
            and i < len(parents)
            and parents[i] is not None
        ):
            context_blocks.append(parents[i].text)  # type: ignore[union-attr]
        else:
            context_blocks.append(child.text)

    logger.info(
        "synthesise_start",
        query_chars=len(query),
        context_blocks=len(context_blocks),
        model=resolved_model,
    )

    messages = build_grounded_messages(query, context_blocks)

    t0 = time.perf_counter()
    if _generator is not None:
        answer_text = _generator(messages)
    else:
        answer_text = generate(messages, model=resolved_model)
    latency_ms = (time.perf_counter() - t0) * 1000.0

    logger.info(
        "synthesise_done",
        answer_chars=len(answer_text),
        latency_ms=round(latency_ms, 1),
    )

    return AnswerResponse(
        query=query,
        answer_text=answer_text,
        model_used=resolved_model,
        sources=[],
        supporting_chunks=list(chunks),  # full input, not active_chunks
        validation_flags=[],
        latency_ms=latency_ms,
    )
