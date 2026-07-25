"""
Judge prompt construction for semantic evaluation.

Public API
----------
    build_judge_messages(
        query: str,
        context: str,
        answer: str,
    ) -> List[dict]

Design
------
Returns a structured Ollama /api/chat message list that instructs an LLM
judge to score a (query, context, answer) triple on four semantic dimensions.

The judge is instructed to return ONLY a JSON object — no prose, no markdown
fences, no chain-of-thought reasoning outside the JSON. This keeps parsing
deterministic.

Scoring dimensions
------------------
    groundedness      — are all factual claims in the answer supported by the
                        context? (1.0 = fully grounded, 0.0 = unsupported)
    answer_relevance  — does the answer directly address the question?
                        (1.0 = fully on-topic, 0.0 = off-topic or empty)
    context_relevance — does the supplied context appear sufficient and
                        relevant to answer the question?
                        (1.0 = highly relevant, 0.0 = irrelevant)
    completeness      — does the answer cover the core aspects of the
                        question given the available context?
                        (1.0 = comprehensive, 0.0 = core question missed)

Expected judge output
---------------------
    {"groundedness": 0.8, "answer_relevance": 0.9,
     "context_relevance": 0.7, "completeness": 0.6}

Empty inputs
------------
``context`` may be ``"[No context provided.]"`` (the canonical placeholder
used across this codebase). ``answer`` may be an empty string. The prompt
is structured so the judge can score these edge cases without a malformed
prompt.

This module has no dependencies on the evaluation runtime — it is a pure
prompt-assembly library.
"""
from __future__ import annotations

from typing import List

_JUDGE_SYSTEM_INSTRUCTION = (
    "You are a strict factual evaluation judge. "
    "You will be given a question, a set of context passages, and an answer. "
    "Score the answer on four dimensions using a float between 0.0 and 1.0 "
    "(inclusive) for each dimension:\n\n"
    "  groundedness      : Every factual claim in the answer is directly "
    "supported by the provided context. "
    "Score 1.0 if fully grounded; 0.0 if no claims are supported.\n"
    "  answer_relevance  : The answer directly addresses the user's question. "
    "Score 1.0 if fully on-topic; 0.0 if off-topic or empty.\n"
    "  context_relevance : The supplied context appears sufficient and "
    "relevant to answer the question. "
    "Score 1.0 if highly relevant; 0.0 if irrelevant or missing.\n"
    "  completeness      : The answer covers the core aspects of the question "
    "given what the context provides. "
    "Score 1.0 if comprehensive; 0.0 if the core question is missed.\n\n"
    "IMPORTANT: Respond with ONLY a JSON object in this exact format. "
    "Do not include any prose, markdown, code fences, or reasoning text:\n"
    '{"groundedness": <float>, "answer_relevance": <float>, '
    '"context_relevance": <float>, "completeness": <float>}'
)

_CONTEXT_HEADER = "=== Context ==="
_ANSWER_HEADER = "=== Answer ==="
_QUESTION_HEADER = "=== Question ==="
_NO_CONTEXT_PLACEHOLDER = "[No context provided.]"


def build_judge_messages(
    query: str,
    context: str,
    answer: str,
) -> List[dict]:
    """
    Build a structured Ollama /api/chat message list for semantic scoring.

    Parameters
    ----------
    query:
        The original user question.
    context:
        Concatenated context passages retrieved for the query. Pass
        ``"[No context provided.]"`` when no chunks were retrieved.
    answer:
        The generated answer text to be scored.

    Returns
    -------
    List[dict]
        Two-element list: system instruction and user turn.
        Structure::

            [
                {"role": "system", "content": <judge instruction>},
                {"role": "user",   "content": <question + context + answer>},
            ]
    """
    effective_context = context.strip() if context.strip() else _NO_CONTEXT_PLACEHOLDER

    user_content = (
        f"{_QUESTION_HEADER}\n{query}\n\n"
        f"{_CONTEXT_HEADER}\n{effective_context}\n\n"
        f"{_ANSWER_HEADER}\n{answer}"
    )

    return [
        {"role": "system", "content": _JUDGE_SYSTEM_INSTRUCTION},
        {"role": "user", "content": user_content},
    ]
