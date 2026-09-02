from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.llmops.evaluation_dataset import (
    EvaluationDatasetBundle,
)


PredictQueryFn = Callable[
    [str],
    Any,
]


def build_mlflow_evaluation_data(
    bundle: EvaluationDatasetBundle,
    *,
    include_query: bool = False,
) -> list[dict[str, Any]]:
    """
    Project the validated evaluation bundle into
    MLflow GenAI evaluation rows.

    By default, raw queries are excluded from the
    MLflow dataset. The prediction function can
    resolve case_id back to the query locally.

    Reference answers are never synthesized.
    """
    rows: list[
        dict[str, Any]
    ] = []

    for (
        case,
        eval_example,
        retrieval_example,
    ) in zip(
        bundle.dataset.active_cases,
        bundle.eval_examples,
        bundle.retrieval_examples,
        strict=True,
    ):
        if (
            case.case_id
            != eval_example.example_id
        ):
            raise ValueError(
                "EvalExample identity drift "
                "detected."
            )

        if (
            case.case_id
            != retrieval_example.case_id
        ):
            raise ValueError(
                "RetrievalEvalExample identity "
                "drift detected."
            )

        inputs: dict[
            str,
            Any,
        ] = {
            "case_id":
                case.case_id,
        }

        if include_query:
            inputs[
                "query"
            ] = case.query

        expectations: dict[
            str,
            Any,
        ] = {
            "expected_document_id":
                retrieval_example
                .expected_document_id,

            "expect_non_empty_answer":
                eval_example
                .expect_non_empty_answer,
        }

        if (
            eval_example
            .expected_source_chunk_ids
        ):
            expectations[
                "expected_source_chunk_ids"
            ] = list(
                eval_example
                .expected_source_chunk_ids
            )

        if (
            eval_example
            .expected_file_names
        ):
            expectations[
                "expected_file_names"
            ] = list(
                eval_example
                .expected_file_names
            )

        if (
            eval_example
            .expected_page_numbers
        ):
            expectations[
                "expected_page_numbers"
            ] = list(
                eval_example
                .expected_page_numbers
            )

        if (
            eval_example
            .expect_citations_valid
        ):
            expectations[
                "expect_citations_valid"
            ] = True

        rows.append(
            {
                "inputs": inputs,
                "expectations":
                    expectations,
            }
        )

    return rows


def build_case_query_lookup(
    bundle: EvaluationDatasetBundle,
) -> dict[str, str]:
    """
    Build the local case_id -> query lookup used
    by privacy-preserving MLflow prediction.

    This mapping is intentionally not suitable
    for experiment metadata logging.
    """
    lookup = {
        case.case_id:
            case.query
        for case
        in bundle.dataset.active_cases
    }

    if (
        len(lookup)
        != bundle.active_case_count
    ):
        raise ValueError(
            "Duplicate case IDs detected while "
            "building query lookup."
        )

    return lookup


def make_case_predict_fn(
    bundle: EvaluationDatasetBundle,
    *,
    predict_query: PredictQueryFn,
) -> Callable[..., Any]:
    """
    Adapt a query-based application to MLflow's
    named-input prediction contract.

    Default evaluation rows contain only case_id.
    When an explicitly query-inclusive dataset is
    used, the provided query must exactly match
    the canonical query for that case.
    """
    query_lookup = (
        build_case_query_lookup(
            bundle
        )
    )

    def predict_fn(
        case_id: str,
        query: str | None = None,
    ) -> Any:
        if case_id not in query_lookup:
            raise ValueError(
                "Unknown evaluation case ID."
            )

        canonical_query = (
            query_lookup[
                case_id
            ]
        )

        if (
            query is not None
            and query
                != canonical_query
        ):
            raise ValueError(
                "Evaluation query does not "
                "match canonical case."
            )

        return predict_query(
            canonical_query
        )

    return predict_fn