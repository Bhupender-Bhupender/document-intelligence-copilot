from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.llmops.evaluation_dataset import (
    EvaluationDatasetBundle,
)
from src.llmops.evaluation_output import (
    normalize_serving_response,
)
from src.schema.serving_models import (
    ServingAnswerRequest,
    ServingAnswerResponse,
)


ServingRunner = Callable[
    [ServingAnswerRequest],
    ServingAnswerResponse,
]


def _default_serving_runner(
    request: ServingAnswerRequest,
) -> ServingAnswerResponse:
    """
    Deferred import keeps evaluation modules
    independent from the application runtime
    until prediction is actually executed.
    """
    from app.serving_service import (
        answer_with_evidence,
    )

    return answer_with_evidence(
        request
    )


def build_case_query_lookup(
    bundle: EvaluationDatasetBundle,
) -> dict[str, str]:
    """
    Build the private local case-to-query map.

    This mapping must not be logged as MLflow
    experiment metadata.
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
            "Duplicate evaluation case IDs."
        )

    return lookup


def make_serving_evaluation_predict_fn(
    bundle: EvaluationDatasetBundle,
    *,
    _serving_runner: ServingRunner | None = None,
) -> Callable[..., dict[str, Any]]:
    """
    Adapt case-id based MLflow evaluation input
    to the real serving request/response boundary.

    Raw canonical queries stay local to this
    function and are not returned in normalized
    scorer output.
    """
    lookup = build_case_query_lookup(
        bundle
    )

    serving_runner = (
        _serving_runner
        or _default_serving_runner
    )

    def predict_fn(
        case_id: str,
    ) -> dict[str, Any]:
        canonical_query = lookup.get(
            case_id
        )

        if canonical_query is None:
            raise ValueError(
                "Unknown evaluation case ID."
            )

        request = ServingAnswerRequest(
            query=canonical_query
        )

        response = serving_runner(
            request
        )

        if not isinstance(
            response,
            ServingAnswerResponse,
        ):
            raise TypeError(
                "Serving evaluation runner must "
                "return ServingAnswerResponse."
            )

        return normalize_serving_response(
            response
        )

    return predict_fn