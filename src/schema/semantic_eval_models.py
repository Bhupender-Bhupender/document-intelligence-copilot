"""
Project-native semantic evaluation data contracts.

These models are intentionally separate from both the pipeline schema
(models.py) and the deterministic evaluation schema (eval_models.py)
so that semantic and deterministic evaluation contracts are independently
versionable and clearly distinguishable at the type level.

Public classes
--------------
    SemanticScore       — per-example semantic scoring result from an LLM judge
    SemanticEvalReport  — aggregate result of one semantic evaluation run
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List

from pydantic import BaseModel, Field


def _new_semantic_id() -> str:
    return str(uuid.uuid4())


# --------------------------------------------------------------------------- #
# Per-example semantic score                                                   #
# --------------------------------------------------------------------------- #


class SemanticScore(BaseModel):
    """
    LLM-judge scores for a single (query, context, answer) triple.

    All four score fields are floats in [0.0, 1.0].

    When the judge's raw output cannot be parsed as valid JSON after
    minimal normalization (whitespace trimming + single ````json``` fence
    removal), all four scores are set to 0.0, ``parse_failed`` is set to
    True, and ``judge_notes`` is set to ``"parse_error"``.

    Parse failures are included in aggregate means so that a broken judge
    call penalises the overall report honestly rather than being silently
    skipped.

    Fields
    ------
    example_id:
        The ``EvalExample.example_id`` this score corresponds to.
    query:
        The original query, carried here so the report is self-contained.
    groundedness_score:
        How well every factual claim in the answer is supported by the
        supplied context. 1.0 = fully grounded; 0.0 = entirely unsupported.
    answer_relevance_score:
        How directly the answer addresses the user's question. 1.0 = fully
        on-topic; 0.0 = completely off-topic or empty.
    context_relevance_score:
        How sufficient and relevant the retrieved context appears for
        answering the question. 1.0 = highly relevant; 0.0 = irrelevant.
    completeness_score:
        How thoroughly the answer covers the core aspects of the question
        given what the context provides. 1.0 = comprehensive; 0.0 = missing
        the core answer.
    judge_notes:
        Short diagnostic note. Set to ``"parse_error"`` when ``parse_failed``
        is True; empty string otherwise.
    parse_failed:
        True when the judge's raw output could not be parsed as valid JSON
        after minimal normalization. All four scores will be 0.0.
    """

    example_id: str
    query: str
    groundedness_score: float = 0.0
    answer_relevance_score: float = 0.0
    context_relevance_score: float = 0.0
    completeness_score: float = 0.0
    judge_notes: str = ""
    parse_failed: bool = False


# --------------------------------------------------------------------------- #
# Aggregate semantic evaluation report                                        #
# --------------------------------------------------------------------------- #


class SemanticEvalReport(BaseModel):
    """
    Aggregate result of one semantic evaluation run.

    All mean fields are floats in [0.0, 1.0]. Zero-denominator handling:
    when ``total == 0`` all means and rates are 0.0.

    Parse failures are included in means (as 0.0 scores) and counted
    separately in ``parse_failure_count`` so callers can distinguish a
    genuinely poor score from a broken judge call.

    Fields
    ------
    report_id:
        Auto-generated unique identifier for this report.
    total:
        Total number of (example, response) pairs evaluated.
    threshold:
        The pass/fail threshold used for ``above_threshold_count``.
        An example is "above threshold" when all four scores >= threshold.
    mean_groundedness:
        Mean groundedness score across all examples.
    mean_answer_relevance:
        Mean answer relevance score across all examples.
    mean_context_relevance:
        Mean context relevance score across all examples.
    mean_completeness:
        Mean completeness score across all examples.
    above_threshold_count:
        Number of examples where all four scores >= ``threshold``.
    above_threshold_rate:
        ``above_threshold_count / total``, or 0.0 when ``total == 0``.
    parse_failure_count:
        Number of examples where the judge's output could not be parsed.
    per_example:
        List of individual ``SemanticScore`` objects, one per example.
    """

    report_id: str = Field(default_factory=_new_semantic_id)
    total: int
    threshold: float

    mean_groundedness: float
    mean_answer_relevance: float
    mean_context_relevance: float
    mean_completeness: float

    above_threshold_count: int
    above_threshold_rate: float

    parse_failure_count: int

    per_example: List[SemanticScore]
