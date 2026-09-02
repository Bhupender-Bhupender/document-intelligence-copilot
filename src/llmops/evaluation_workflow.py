from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from src.llmops.evaluation_dataset import (
    EvaluationDatasetBundle,
)
from src.llmops.evaluation_predictor import (
    ServingRunner,
    make_serving_evaluation_predict_fn,
)
from src.llmops.mlflow_evaluation_runner import (
    DeterministicEvaluationRunResult,
    run_mlflow_deterministic_evaluation,
)
from src.llmops.mlflow_tracking import (
    MLflowExperimentConfig,
)
from src.llmops.versioning import (
    LLMOpsVersionContext,
)


def run_serving_mlflow_evaluation(
    bundle: EvaluationDatasetBundle,
    *,
    config: MLflowExperimentConfig,
    version_context: LLMOpsVersionContext,
    run_name: str,
    serving_runner: ServingRunner | None = None,
    document_fingerprint_key: (
        bytes | None
    ) = None,
    scorers: Sequence[Any] | None = None,
    _mlflow=None,
) -> DeterministicEvaluationRunResult:
    """
    Execute canonical evaluation through the
    production serving contract.

    Serving execution and raw evaluation output
    remain local. The downstream MLflow runner
    applies the validated privacy-safe projection
    before MLflow persistence.
    """
    predict_case = (
        make_serving_evaluation_predict_fn(
            bundle,
            _serving_runner=serving_runner,
        )
    )

    return run_mlflow_deterministic_evaluation(
        bundle,
        predict_case=predict_case,
        config=config,
        version_context=version_context,
        run_name=run_name,
        document_fingerprint_key=(
            document_fingerprint_key
        ),
        scorers=scorers,
        _mlflow=_mlflow,
    )