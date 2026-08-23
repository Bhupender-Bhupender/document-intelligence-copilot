from __future__ import annotations

import math
import statistics
import time

from collections.abc import (
    Callable,
    Sequence,
)
from typing import Any

from src.schema.retrieval_eval_models import (
    RetrievalEvalCaseResult,
    RetrievalEvalExample,
    RetrievalEvalReport,
)


RetrieveFn = Callable[
    [str, int],
    Sequence[Any],
]


def _default_retrieve(
    query: str,
    top_k: int,
) -> Sequence[Any]:
    # Deferred import keeps this module
    # lightweight for unit testing.
    from src.retrieval.retrieval_gateway import (
        route_retrieve,
    )

    return route_retrieve(
        query=query,
        top_k=top_k,
    )


def _has_value(
    value: Any,
) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
    )


def _score_present(
    chunk: Any,
) -> bool:
    return any(
        getattr(
            chunk,
            field,
            None,
        )
        is not None
        for field in (
            "rerank_score",
            "fusion_score",
            "vector_score",
            "bm25_score",
        )
    )


def _chunk_metadata_valid(
    chunk: Any,
) -> bool:
    """
    Validate only the project-native retrieval
    contract. No content is logged or returned.
    """

    required_text_fields = (
        "chunk_id",
        "doc_id",
        "page_id",
        "file_name",
        "text",
        "parent_chunk_id",
        "file_type",
        "retrieval_method",
    )

    if not all(
        _has_value(
            getattr(
                chunk,
                field,
                None,
            )
        )
        for field
        in required_text_fields
    ):
        return False

    try:
        page_number = int(
            getattr(
                chunk,
                "page_number",
            )
        )

        word_count = int(
            getattr(
                chunk,
                "word_count",
            )
        )

    except (
        TypeError,
        ValueError,
        AttributeError,
    ):
        return False

    if page_number < 1:
        return False

    if word_count < 0:
        return False

    if not _score_present(
        chunk
    ):
        return False

    return True


def _rate(
    count: int,
    total: int,
) -> float:
    if total == 0:
        return 0.0

    return count / total


def _nearest_rank_p95(
    values: Sequence[float],
) -> float:
    if not values:
        return 0.0

    ordered = sorted(
        values
    )

    index = max(
        0,
        math.ceil(
            0.95
            * len(ordered)
        )
        - 1,
    )

    return float(
        ordered[index]
    )


def run_retrieval_evaluation(
    examples: Sequence[
        RetrievalEvalExample
    ],
    *,
    top_k: int = 10,
    _retrieve: RetrieveFn | None = None,
    _clock: Callable[[], float] = (
        time.perf_counter
    ),
) -> tuple[
    RetrievalEvalReport,
    list[RetrievalEvalCaseResult],
]:
    """
    Evaluate ranked child-chunk retrieval only.

    Errors remain in the strict denominator
    and therefore count as retrieval misses.

    Results are evaluated in returned rank
    order. Document IDs are not deduplicated.
    """

    if top_k < 10:
        raise ValueError(
            "top_k must be at least 10 "
            "to score Hit@10."
        )

    case_ids = [
        example.case_id
        for example in examples
    ]

    if (
        len(case_ids)
        != len(set(case_ids))
    ):
        raise ValueError(
            "Duplicate retrieval evaluation "
            "case IDs."
        )

    retrieve = (
        _retrieve
        or _default_retrieve
    )

    case_results: list[
        RetrievalEvalCaseResult
    ] = []

    for example in examples:

        started = _clock()

        try:
            retrieved = list(
                retrieve(
                    example.query,
                    top_k,
                )
            )

            latency_ms = max(
                0.0,
                (
                    _clock()
                    - started
                )
                * 1000,
            )

            document_ids = [
                getattr(
                    chunk,
                    "doc_id",
                    None,
                )
                for chunk in retrieved
            ]

            metadata_valid = (
                len(retrieved) > 0
                and all(
                    _chunk_metadata_valid(
                        chunk
                    )
                    for chunk
                    in retrieved
                )
            )

            expected = (
                example
                .expected_document_id
            )

            case_results.append(
                RetrievalEvalCaseResult(
                    case_id=(
                        example.case_id
                    ),
                    result_count=(
                        len(retrieved)
                    ),
                    latency_ms=(
                        latency_ms
                    ),
                    zero_result=(
                        len(retrieved)
                        == 0
                    ),
                    metadata_valid=(
                        metadata_valid
                    ),
                    hit_at_1=(
                        expected
                        in document_ids[:1]
                    ),
                    hit_at_3=(
                        expected
                        in document_ids[:3]
                    ),
                    hit_at_5=(
                        expected
                        in document_ids[:5]
                    ),
                    hit_at_10=(
                        expected
                        in document_ids[:10]
                    ),
                )
            )

        except Exception as exc:

            latency_ms = max(
                0.0,
                (
                    _clock()
                    - started
                )
                * 1000,
            )

            case_results.append(
                RetrievalEvalCaseResult(
                    case_id=(
                        example.case_id
                    ),
                    result_count=0,
                    latency_ms=(
                        latency_ms
                    ),
                    zero_result=True,
                    metadata_valid=False,
                    hit_at_1=False,
                    hit_at_3=False,
                    hit_at_5=False,
                    hit_at_10=False,
                    error_type=(
                        type(exc).__name__
                    ),
                )
            )

    total = len(
        case_results
    )

    hit_1 = sum(
        result.hit_at_1
        for result in case_results
    )

    hit_3 = sum(
        result.hit_at_3
        for result in case_results
    )

    hit_5 = sum(
        result.hit_at_5
        for result in case_results
    )

    hit_10 = sum(
        result.hit_at_10
        for result in case_results
    )

    zero_results = sum(
        result.zero_result
        for result in case_results
    )

    errors = sum(
        result.error_type
        is not None
        for result in case_results
    )

    metadata_valid = sum(
        result.metadata_valid
        for result in case_results
    )

    latencies = [
        result.latency_ms
        for result in case_results
    ]

    result_counts = [
        result.result_count
        for result in case_results
    ]

    operational_pass = (
        total > 0
        and errors == 0
        and zero_results == 0
        and metadata_valid == total
    )

    report = RetrievalEvalReport(
        cases_evaluated=total,
        top_k=top_k,

        hit_at_1_count=hit_1,
        hit_at_1=_rate(
            hit_1,
            total,
        ),

        hit_at_3_count=hit_3,
        hit_at_3=_rate(
            hit_3,
            total,
        ),

        hit_at_5_count=hit_5,
        hit_at_5=_rate(
            hit_5,
            total,
        ),

        hit_at_10_count=hit_10,
        hit_at_10=_rate(
            hit_10,
            total,
        ),

        zero_result_count=(
            zero_results
        ),
        zero_result_rate=_rate(
            zero_results,
            total,
        ),

        retrieval_error_count=errors,
        retrieval_error_rate=_rate(
            errors,
            total,
        ),

        metadata_valid_count=(
            metadata_valid
        ),
        metadata_valid_rate=_rate(
            metadata_valid,
            total,
        ),

        min_results_returned=(
            min(result_counts)
            if result_counts
            else 0
        ),

        max_results_returned=(
            max(result_counts)
            if result_counts
            else 0
        ),

        mean_results_returned=(
            statistics.mean(
                result_counts
            )
            if result_counts
            else 0.0
        ),

        mean_latency_ms=(
            statistics.mean(
                latencies
            )
            if latencies
            else 0.0
        ),

        median_latency_ms=(
            statistics.median(
                latencies
            )
            if latencies
            else 0.0
        ),

        p95_latency_ms=(
            _nearest_rank_p95(
                latencies
            )
        ),

        operational_retrieval_pass=(
            operational_pass
        ),
    )

    return (
        report,
        case_results,
    )
