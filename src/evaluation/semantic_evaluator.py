"""
Semantic evaluation harness for the answer pipeline.

Public API
----------
    run_semantic_evaluation(
        examples,
        responses,
        *,
        _judge,
        judge_model,
        threshold,
    ) -> SemanticEvalReport

Design
------
``run_semantic_evaluation`` accepts a pre-collected list of EvalExamples
and a corresponding list of AnswerResponse objects (one per example).

Caller alignment rule
---------------------
``len(examples)`` MUST equal ``len(responses)``. If they differ,
``run_semantic_evaluation`` raises ``ValueError`` immediately before any
scoring occurs. No silent truncation. No partial scoring.

Separation from deterministic harness
--------------------------------------
This module scores existing responses; it does not run the production
pipeline. The caller is responsible for pipeline execution and for
passing aligned lists. This keeps semantic evaluation clearly separate
from both the production pipeline and the deterministic evaluator.

Judge injection
---------------
The ``_judge`` keyword parameter accepts any callable with signature:
    (messages: List[dict]) -> str

When ``_judge`` is None, the module falls back to
``src.generation.ollama_llm.generate``. The fallback is a lazy import so
that importing this module does not open a connection to Ollama.

Parse strategy
--------------
``_parse_scores`` applies minimal deterministic normalization before
JSON parsing:
    1. Strip surrounding whitespace.
    2. Strip a single outer ```json ... ``` fence if present.
    3. ``json.loads`` the result.
    4. On any parse failure: parse_failed=True, all scores=0.0,
       judge_notes="parse_error".
    5. On success: clamp each expected key to [0.0, 1.0]; missing keys → 0.0.

No broad heuristic extraction from arbitrary prose.

Aggregate means
---------------
Parse failures score 0.0 on all dimensions and are included in aggregate
means. They are also counted separately in ``parse_failure_count`` so
callers can distinguish a genuinely poor score from a broken judge call.

Zero-denominator rule
---------------------
When ``total == 0``, all means and rates are 0.0.
"""
from __future__ import annotations

import json
from typing import Callable, List, Optional

from src.evaluation.judge_prompts import build_judge_messages
from src.schema.eval_models import EvalExample
from src.schema.models import AnswerResponse
from src.schema.semantic_eval_models import SemanticEvalReport, SemanticScore
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

_NO_CONTEXT_PLACEHOLDER = "[No context provided.]"
_SCORE_KEYS = ("groundedness", "answer_relevance", "context_relevance", "completeness")


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #


def run_semantic_evaluation(
    examples: List[EvalExample],
    responses: List[AnswerResponse],
    *,
    _judge: Optional[Callable[[List[dict]], str]] = None,
    judge_model: Optional[str] = None,
    threshold: float = 0.7,
) -> SemanticEvalReport:
    """
    Score aligned (example, response) pairs using an LLM judge.

    Parameters
    ----------
    examples:
        EvalExamples that were used to generate the responses.
    responses:
        AnswerResponse objects produced by the pipeline, one per example.
        Must have the same length as ``examples``.
    _judge:
        Test injection. When provided, replaces the Ollama ``generate``
        call entirely.
        Signature: (messages: List[dict]) -> str
    judge_model:
        Ollama model tag to use for judging (e.g. ``"qwen3:8b"``).
        Ignored when ``_judge`` is injected. Defaults to
        ``config.generation_model``.
    threshold:
        Pass/fail threshold for ``above_threshold_count``. An example is
        "above threshold" when all four scores >= threshold.

    Returns
    -------
    SemanticEvalReport
        Aggregate semantic evaluation result with per-example scores.

    Raises
    ------
    ValueError
        If ``len(examples) != len(responses)``.
    """
    if len(examples) != len(responses):
        raise ValueError(
            f"examples and responses must have the same length; "
            f"got {len(examples)} examples and {len(responses)} responses."
        )

    judge_callable = _judge if _judge is not None else _make_default_judge(judge_model)

    scores: List[SemanticScore] = []
    for ex, resp in zip(examples, responses):
        logger.debug("semantic_eval_example_start", example_id=ex.example_id)
        score = _score_one(ex, resp, judge_callable)
        scores.append(score)

    report = _aggregate_scores(scores, threshold=threshold)

    logger.info(
        "semantic_evaluation_done",
        total=report.total,
        mean_groundedness=report.mean_groundedness,
        mean_answer_relevance=report.mean_answer_relevance,
        above_threshold_rate=report.above_threshold_rate,
        parse_failures=report.parse_failure_count,
    )

    return report


# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #


def _make_default_judge(
    judge_model: Optional[str],
) -> Callable[[List[dict]], str]:
    """
    Return a callable that wraps ``ollama_llm.generate`` with the given model.

    The import is deferred so that loading this module does not open a
    connection to Ollama.
    """
    def _call(messages: List[dict]) -> str:
        from src.generation.ollama_llm import generate  # lazy import
        return generate(messages, model=judge_model)

    return _call


def _build_context_text(response: AnswerResponse) -> str:
    """
    Join the text of all supporting chunks into a single context string.

    Returns the canonical no-context placeholder when the response has no
    supporting chunks.
    """
    if not response.supporting_chunks:
        return _NO_CONTEXT_PLACEHOLDER
    return "\n---\n".join(
        chunk.text for chunk in response.supporting_chunks if chunk.text
    ) or _NO_CONTEXT_PLACEHOLDER


def _parse_scores(raw: str) -> dict:
    """
    Parse the judge's raw output into a score dictionary.

    Normalization sequence (deterministic, minimal):
        1. Strip surrounding whitespace.
        2. Strip a single outer ```json ... ``` fence if present.
        3. ``json.loads`` the result.

    On any parse failure:
        Returns {"parse_failed": True} with all score keys absent (callers
        treat absent keys as 0.0).

    On success:
        Returns a dict with the four score keys; values are NOT yet clamped
        (clamping is applied in ``_score_one``).

    Parameters
    ----------
    raw:
        The raw string returned by the judge.

    Returns
    -------
    dict
        Parsed score dict, or {"parse_failed": True} on failure.
    """
    text = raw.strip()

    # Strip a single outer ```json ... ``` fence.
    if text.startswith("```json"):
        # Remove the opening fence line and the closing ``` line.
        lines = text.splitlines()
        # Drop the first line (```json) and the last line if it is ```
        start = 1
        end = len(lines)
        if lines[-1].strip() == "```":
            end = len(lines) - 1
        text = "\n".join(lines[start:end]).strip()
    elif text.startswith("```"):
        # Bare ``` fence without language tag — strip the same way.
        lines = text.splitlines()
        start = 1
        end = len(lines)
        if lines[-1].strip() == "```":
            end = len(lines) - 1
        text = "\n".join(lines[start:end]).strip()

    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return {"parse_failed": True}

    if not isinstance(parsed, dict):
        return {"parse_failed": True}

    return parsed


def _clamp(value: object) -> float:
    """Coerce value to float and clamp to [0.0, 1.0]."""
    try:
        return max(0.0, min(1.0, float(value)))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _score_one(
    example: EvalExample,
    response: AnswerResponse,
    judge_callable: Callable[[List[dict]], str],
) -> SemanticScore:
    """
    Call the judge for one (example, response) pair and return a SemanticScore.

    Catches all exceptions from the judge callable so that one failing call
    does not abort the entire evaluation run. A caught exception is treated
    as a parse failure.
    """
    context = _build_context_text(response)
    messages = build_judge_messages(
        query=example.query,
        context=context,
        answer=response.answer_text,
    )

    try:
        raw = judge_callable(messages)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "semantic_judge_call_failed",
            example_id=example.example_id,
            error=str(exc),
        )
        return SemanticScore(
            example_id=example.example_id,
            query=example.query,
            groundedness_score=0.0,
            answer_relevance_score=0.0,
            context_relevance_score=0.0,
            completeness_score=0.0,
            judge_notes="judge_call_error",
            parse_failed=True,
        )

    parsed = _parse_scores(raw)

    if parsed.get("parse_failed"):
        return SemanticScore(
            example_id=example.example_id,
            query=example.query,
            groundedness_score=0.0,
            answer_relevance_score=0.0,
            context_relevance_score=0.0,
            completeness_score=0.0,
            judge_notes="parse_error",
            parse_failed=True,
        )

    return SemanticScore(
        example_id=example.example_id,
        query=example.query,
        groundedness_score=_clamp(parsed.get("groundedness", 0.0)),
        answer_relevance_score=_clamp(parsed.get("answer_relevance", 0.0)),
        context_relevance_score=_clamp(parsed.get("context_relevance", 0.0)),
        completeness_score=_clamp(parsed.get("completeness", 0.0)),
        judge_notes="",
        parse_failed=False,
    )


def _mean(values: List[float]) -> float:
    """Return arithmetic mean of values, or 0.0 for an empty list."""
    return sum(values) / len(values) if values else 0.0


def _rate(count: int, denominator: int) -> float:
    """Return count / denominator, or 0.0 when denominator == 0."""
    return count / denominator if denominator > 0 else 0.0


def _aggregate_scores(
    scores: List[SemanticScore],
    *,
    threshold: float,
) -> SemanticEvalReport:
    """
    Aggregate a list of SemanticScores into a SemanticEvalReport.

    Pure function — no side effects.

    Parameters
    ----------
    scores:
        Per-example SemanticScore objects.
    threshold:
        Pass/fail threshold for all four dimensions.

    Returns
    -------
    SemanticEvalReport
    """
    total = len(scores)

    groundedness_vals = [s.groundedness_score for s in scores]
    answer_relevance_vals = [s.answer_relevance_score for s in scores]
    context_relevance_vals = [s.context_relevance_score for s in scores]
    completeness_vals = [s.completeness_score for s in scores]

    above_threshold_count = sum(
        1
        for s in scores
        if (
            s.groundedness_score >= threshold
            and s.answer_relevance_score >= threshold
            and s.context_relevance_score >= threshold
            and s.completeness_score >= threshold
        )
    )

    parse_failure_count = sum(1 for s in scores if s.parse_failed)

    return SemanticEvalReport(
        total=total,
        threshold=threshold,
        mean_groundedness=_mean(groundedness_vals),
        mean_answer_relevance=_mean(answer_relevance_vals),
        mean_context_relevance=_mean(context_relevance_vals),
        mean_completeness=_mean(completeness_vals),
        above_threshold_count=above_threshold_count,
        above_threshold_rate=_rate(above_threshold_count, total),
        parse_failure_count=parse_failure_count,
        per_example=scores,
    )
