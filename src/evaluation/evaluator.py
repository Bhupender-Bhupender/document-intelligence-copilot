"""
Deterministic evaluation harness for the answer pipeline.

Public API
----------
    run_evaluation(examples, *, _pipeline, ...) -> EvalReport

Design
------
``run_evaluation`` is the sole public function. It:
    1. Calls ``run_pipeline(query, ...)`` (or an injected ``_pipeline``)
       for each EvalExample.
    2. Passes the collected AnswerResponse list to ``_compute_metrics``.
    3. Returns an ``EvalReport``.

``_compute_metrics`` is a pure function: same inputs → same outputs.
No I/O, no model calls. All metrics are deterministic.

Metric semantics
----------------
See ``src/schema/eval_models.EvalReport`` docstring for full details.

Zero-denominator rule
---------------------
Every rate is computed as:
    count / denominator if denominator > 0 else 0.0

No fuzzy fallbacks.
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Callable, List, Optional

from src.schema.eval_models import EvalExample, EvalReport
from src.schema.models import AnswerResponse
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #


def run_evaluation(
    examples: List[EvalExample],
    *,
    _pipeline: Optional[Callable[[str], AnswerResponse]] = None,
    index_dir: Optional[Path] = None,
    retrieval_top_k: int = 10,
    rerank_top_k: int = 5,
    model: Optional[str] = None,
) -> EvalReport:
    """
    Run the evaluation harness over a list of EvalExamples.

    For each example, the answer pipeline is invoked and the response is
    collected. Deterministic metrics are computed over the full batch and
    returned as an EvalReport.

    Parameters
    ----------
    examples:
        Evaluation examples to run.
    _pipeline:
        Test injection. When provided, replaces ``run_pipeline`` entirely.
        Signature: (query: str) -> AnswerResponse
    index_dir:
        Forwarded to ``run_pipeline`` when ``_pipeline`` is not injected.
    retrieval_top_k:
        Forwarded to ``run_pipeline``.
    rerank_top_k:
        Forwarded to ``run_pipeline``.
    model:
        Forwarded to ``run_pipeline``.

    Returns
    -------
    EvalReport
        Structured evaluation result with deterministic metrics.
        All rates are 0.0 when their denominator is 0.
    """
    responses: List[AnswerResponse] = []

    for i, example in enumerate(examples):
        logger.debug("eval_example_start", index=i, example_id=example.example_id)

        if _pipeline is not None:
            response = _pipeline(example.query)
        else:
            from src.generation.answer_pipeline import run_pipeline
            response = run_pipeline(
                query=example.query,
                index_dir=index_dir,
                retrieval_top_k=retrieval_top_k,
                rerank_top_k=rerank_top_k,
                model=model,
            )

        responses.append(response)

    report = _compute_metrics(examples, responses)

    logger.info(
        "evaluation_done",
        total=report.total,
        answer_non_empty_rate=report.answer_non_empty_rate,
        citation_valid_rate=report.citation_valid_rate,
        source_hit_rate=report.source_hit_rate,
    )

    return report


# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #


def _rate(count: int, denominator: int) -> float:
    """Return count / denominator, or 0.0 when denominator == 0."""
    return count / denominator if denominator > 0 else 0.0


def _compute_metrics(
    examples: List[EvalExample],
    responses: List[AnswerResponse],
) -> EvalReport:
    """
    Compute deterministic metrics from paired (example, response) lists.

    Parameters
    ----------
    examples:
        The original EvalExamples, in the same order as responses.
    responses:
        AnswerResponse objects, one per example.

    Returns
    -------
    EvalReport
        All counts and rates. Pure function — no side effects.
    """
    total = len(examples)

    # Total-based counters
    answer_non_empty_count = 0
    no_source_count = 0
    no_supporting_chunk_count = 0
    total_citations = 0
    citation_valid_count = 0
    invalid_citation_count = 0
    flag_counter: Counter[str] = Counter()

    # Restricted-denominator counters
    source_hit_count = 0
    source_denom = 0
    file_hit_count = 0
    file_denom = 0
    page_hit_count = 0
    page_denom = 0
    citations_all_valid_count = 0
    citations_all_valid_denom = 0

    per_example = []

    for ex, resp in zip(examples, responses):
        # ---- total-based ------------------------------------------------- #

        non_empty = bool(resp.answer_text.strip())
        if non_empty:
            answer_non_empty_count += 1

        if not resp.sources:
            no_source_count += 1

        if not resp.supporting_chunks:
            no_supporting_chunk_count += 1

        for citation in resp.sources:
            total_citations += 1
            if citation.validation_status == "valid":
                citation_valid_count += 1
            elif citation.validation_status == "invalid":
                invalid_citation_count += 1

        flag_counter.update(resp.validation_flags)

        # ---- restricted-denominator -------------------------------------- #

        # Source hit: at least one expected chunk ID in response sources
        if ex.expected_source_chunk_ids:
            source_denom += 1
            actual_ids = {
                c.source_chunk_id
                for c in resp.sources
                if c.source_chunk_id is not None
            }
            if actual_ids & set(ex.expected_source_chunk_ids):
                source_hit_count += 1

        # File hit: at least one expected file name in response sources
        if ex.expected_file_names:
            file_denom += 1
            actual_files = {c.file_name for c in resp.sources}
            if actual_files & set(ex.expected_file_names):
                file_hit_count += 1

        # Page hit: at least one (file_name, page_number) pair from the
        # cartesian product of expected_file_names × expected_page_numbers
        # appears in response sources. Only scored when both lists are
        # non-empty.
        if ex.expected_file_names and ex.expected_page_numbers:
            page_denom += 1
            actual_pairs = {(c.file_name, c.page_number) for c in resp.sources}
            expected_pairs = {
                (f, p)
                for f in ex.expected_file_names
                for p in ex.expected_page_numbers
            }
            if actual_pairs & expected_pairs:
                page_hit_count += 1

        # Citations-all-valid: only scored when expect_citations_valid=True.
        # Hit = at least one citation exists AND all have status "valid".
        if ex.expect_citations_valid:
            citations_all_valid_denom += 1
            if resp.sources and all(
                c.validation_status == "valid" for c in resp.sources
            ):
                citations_all_valid_count += 1

        per_example.append(
            {
                "example_id": ex.example_id,
                "query": ex.query,
                "answer_non_empty": non_empty,
                "source_count": len(resp.sources),
                "supporting_chunk_count": len(resp.supporting_chunks),
                "validation_flags": list(resp.validation_flags),
            }
        )

    return EvalReport(
        total=total,
        # Total-based
        answer_non_empty_count=answer_non_empty_count,
        answer_non_empty_rate=_rate(answer_non_empty_count, total),
        citation_valid_count=citation_valid_count,
        citation_valid_rate=_rate(citation_valid_count, total_citations),
        invalid_citation_count=invalid_citation_count,
        invalid_citation_rate=_rate(invalid_citation_count, total_citations),
        no_source_count=no_source_count,
        no_source_rate=_rate(no_source_count, total),
        no_supporting_chunk_count=no_supporting_chunk_count,
        no_supporting_chunk_rate=_rate(no_supporting_chunk_count, total),
        # Restricted-denominator
        source_hit_count=source_hit_count,
        source_hit_rate=_rate(source_hit_count, source_denom),
        file_hit_count=file_hit_count,
        file_hit_rate=_rate(file_hit_count, file_denom),
        page_hit_count=page_hit_count,
        page_hit_rate=_rate(page_hit_count, page_denom),
        citations_all_valid_count=citations_all_valid_count,
        citations_all_valid_rate=_rate(citations_all_valid_count, citations_all_valid_denom),
        # Aggregates
        flag_frequency=dict(flag_counter),
        per_example=per_example,
    )
