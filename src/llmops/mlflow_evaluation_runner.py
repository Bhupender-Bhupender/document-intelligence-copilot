from __future__ import annotations

import math

from collections.abc import (
    Callable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from typing import Any

import mlflow

from src.llmops.evaluation_dataset import (
    EvaluationDatasetBundle,
)
from src.llmops.mlflow_safe_evaluation import (
    build_mlflow_safe_evaluation_data,
)
from src.llmops.mlflow_safe_scorers import (
    MLFLOW_SAFE_DETERMINISTIC_SCORERS,
)
from src.llmops.mlflow_tracking import (
    MLflowExperimentConfig,
    log_params,
    start_llmops_run,
)
from src.llmops.versioning import (
    LLMOpsVersionContext,
)


CasePredictFn = Callable[
    [str],
    Mapping[str, Any],
]


REQUIRED_DETERMINISTIC_METRICS = (
    "answer_expectation_met/mean",
    "expected_document_fingerprint_hit/mean",
    "evidence_present/mean",
    "citation_present/mean",
)


@dataclass(frozen=True)
class DeterministicEvaluationRunResult:
    """
    Stable aggregate result returned by the
    Phase 15 MLflow deterministic evaluator.
    """

    run_id: str

    metrics: dict[
        str,
        float,
    ]

    evaluated_case_count: int

    evaluation_dataset_version: str


def _collect_local_outputs(
    bundle: EvaluationDatasetBundle,
    *,
    predict_case: CasePredictFn,
) -> dict[
    str,
    Mapping[str, Any],
]:
    """
    Execute canonical cases locally.

    Raw queries and serving responses remain
    outside MLflow at this stage.
    """
    outputs: dict[
        str,
        Mapping[str, Any],
    ] = {}

    for case in (
        bundle.dataset.active_cases
    ):
        result = predict_case(
            case.case_id
        )

        if not isinstance(
            result,
            Mapping,
        ):
            raise TypeError(
                "Evaluation prediction must "
                "return a mapping."
            )

        outputs[
            case.case_id
        ] = result

    if (
        len(outputs)
        != bundle.active_case_count
    ):
        raise RuntimeError(
            "Evaluation prediction count does "
            "not match active canonical cases."
        )

    return outputs


def _normalize_required_metrics(
    metrics: Mapping[
        str,
        Any,
    ],
) -> dict[str, float]:
    normalized: dict[
        str,
        float,
    ] = {}

    for metric_name in (
        REQUIRED_DETERMINISTIC_METRICS
    ):
        if metric_name not in metrics:
            raise ValueError(
                "MLflow evaluation result is "
                "missing required metric: "
                f"{metric_name}"
            )

        value = metrics[
            metric_name
        ]

        if isinstance(
            value,
            bool,
        ):
            raise TypeError(
                "MLflow evaluation metric "
                f"{metric_name} must be numeric."
            )

        try:
            numeric = float(
                value
            )

        except (
            TypeError,
            ValueError,
        ) as exc:
            raise TypeError(
                "MLflow evaluation metric "
                f"{metric_name} must be numeric."
            ) from exc

        if not math.isfinite(
            numeric
        ):
            raise ValueError(
                "MLflow evaluation metric "
                f"{metric_name} must be finite."
            )

        if not (
            0.0
            <= numeric
            <= 1.0
        ):
            raise ValueError(
                "MLflow deterministic metric "
                f"{metric_name} must be in "
                "[0.0, 1.0]."
            )

        normalized[
            metric_name
        ] = numeric

    return normalized


def run_mlflow_deterministic_evaluation(
    bundle: EvaluationDatasetBundle,
    *,
    predict_case: CasePredictFn,
    config: MLflowExperimentConfig,
    version_context: LLMOpsVersionContext,
    run_name: str,
    document_fingerprint_key: (
        bytes | None
    ) = None,
    scorers: Sequence[Any] | None = None,
    _mlflow=None,
) -> DeterministicEvaluationRunResult:
    """
    Execute deterministic canonical evaluation
    while preserving the MLflow privacy boundary.

    Flow:
      local case execution
      -> local normalized outputs
      -> privacy-safe projection
      -> MLflow GenAI evaluation
      -> stable aggregate metrics

    Raw queries, answers, case IDs, and document
    IDs are not passed to MLflow.
    """
    if (
        version_context
        .evaluation_dataset_version
        != bundle.evaluation_dataset_version
    ):
        raise ValueError(
            "LLMOps version context evaluation "
            "dataset does not match the loaded "
            "canonical dataset."
        )

    mlflow_module = (
        _mlflow
        if _mlflow is not None
        else mlflow
    )

    local_outputs = (
        _collect_local_outputs(
            bundle,
            predict_case=predict_case,
        )
    )

    safe_rows = (
        build_mlflow_safe_evaluation_data(
            bundle,
            local_outputs,
            document_fingerprint_key=(
                document_fingerprint_key
            ),
        )
    )

    if (
        len(safe_rows)
        != bundle.active_case_count
    ):
        raise RuntimeError(
            "MLflow-safe evaluation row count "
            "does not match active cases."
        )

    scorer_set = (
        tuple(scorers)
        if scorers is not None
        else (
            MLFLOW_SAFE_DETERMINISTIC_SCORERS
        )
    )

    if not scorer_set:
        raise ValueError(
            "At least one MLflow scorer "
            "is required."
        )

    run_params = dict(
        bundle.safe_metadata()
    )

    run_params[
        "mlflow_safe_projection"
    ] = True

    run_params[
        "deterministic_scorer_count"
    ] = len(
        scorer_set
    )

    with start_llmops_run(
        config=config,
        version_context=version_context,
        run_name=run_name,
        _mlflow=mlflow_module,
    ):
        log_params(
            run_params,
            _mlflow=mlflow_module,
        )

        evaluation_result = (
            mlflow_module
            .genai
            .evaluate(
                data=safe_rows,
                scorers=list(
                    scorer_set
                ),
            )
        )

    run_id = str(
        getattr(
            evaluation_result,
            "run_id",
            "",
        )
        or ""
    ).strip()

    if not run_id:
        raise RuntimeError(
            "MLflow evaluation returned no "
            "run ID."
        )

    metrics = (
        _normalize_required_metrics(
            evaluation_result.metrics
        )
    )

    return DeterministicEvaluationRunResult(
        run_id=run_id,
        metrics=metrics,
        evaluated_case_count=(
            bundle.active_case_count
        ),
        evaluation_dataset_version=(
            bundle
            .evaluation_dataset_version
        ),
    )