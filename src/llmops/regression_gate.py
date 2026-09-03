from __future__ import annotations

import math

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import mlflow

from src.llmops.mlflow_evaluation_runner import (
    REQUIRED_DETERMINISTIC_METRICS,
)


COMPATIBILITY_TAGS = (
    "application",
    "phase",
    "evaluation_contract_version",
    "evaluation_dataset_version",
)


_FLOAT_EPSILON = 1e-12


@dataclass(frozen=True)
class EvaluationRunSnapshot:
    """
    Validated MLflow evaluation run used by the
    regression gate.
    """

    run_id: str

    metrics: dict[
        str,
        float,
    ]

    compatibility_tags: dict[
        str,
        str,
    ]


@dataclass(frozen=True)
class MetricRegressionResult:
    metric_name: str

    baseline_value: float
    candidate_value: float

    tolerance: float
    required_minimum: float

    delta: float
    passed: bool


@dataclass(frozen=True)
class RegressionGateResult:
    baseline_run_id: str
    candidate_run_id: str

    passed: bool

    metric_results: tuple[
        MetricRegressionResult,
        ...,
    ]

    compatibility_tags: dict[
        str,
        str,
    ]

    @property
    def failed_metrics(
        self,
    ) -> tuple[str, ...]:
        return tuple(
            result.metric_name
            for result
            in self.metric_results
            if not result.passed
        )


def _require_run_id(
    run_id: str,
    *,
    label: str,
) -> str:
    normalized = str(
        run_id
        or ""
    ).strip()

    if not normalized:
        raise ValueError(
            f"{label} run ID is required."
        )

    return normalized


def _normalize_metric_value(
    value: Any,
    *,
    metric_name: str,
) -> float:
    if isinstance(
        value,
        bool,
    ):
        raise TypeError(
            f"Metric {metric_name} "
            "must be numeric."
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
            f"Metric {metric_name} "
            "must be numeric."
        ) from exc

    if not math.isfinite(
        numeric
    ):
        raise ValueError(
            f"Metric {metric_name} "
            "must be finite."
        )

    if not (
        0.0
        <= numeric
        <= 1.0
    ):
        raise ValueError(
            f"Metric {metric_name} "
            "must be in [0.0, 1.0]."
        )

    return numeric


def _normalize_tolerance(
    value: Any,
    *,
    label: str,
) -> float:
    if isinstance(
        value,
        bool,
    ):
        raise TypeError(
            f"{label} must be numeric."
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
            f"{label} must be numeric."
        ) from exc

    if not math.isfinite(
        numeric
    ):
        raise ValueError(
            f"{label} must be finite."
        )

    if not (
        0.0
        <= numeric
        <= 1.0
    ):
        raise ValueError(
            f"{label} must be in [0.0, 1.0]."
        )

    return numeric


def load_evaluation_run_snapshot(
    run_id: str,
    *,
    _mlflow=None,
) -> EvaluationRunSnapshot:
    """
    Load and validate one explicit MLflow run.

    No run discovery or "latest run" behavior
    is permitted here.
    """
    normalized_run_id = (
        _require_run_id(
            run_id,
            label="Evaluation",
        )
    )

    mlflow_module = (
        _mlflow
        if _mlflow is not None
        else mlflow
    )

    run = mlflow_module.get_run(
        normalized_run_id
    )

    retrieved_run_id = str(
        getattr(
            run.info,
            "run_id",
            "",
        )
        or ""
    ).strip()

    if (
        retrieved_run_id
        and retrieved_run_id
        != normalized_run_id
    ):
        raise ValueError(
            "MLflow returned a different "
            "run ID than requested."
        )

    status = str(
        getattr(
            run.info,
            "status",
            "",
        )
        or ""
    ).strip()

    if status != "FINISHED":
        raise ValueError(
            "Evaluation run must have "
            "FINISHED status."
        )

    raw_metrics = (
        run.data.metrics
    )

    metrics: dict[
        str,
        float,
    ] = {}

    for metric_name in (
        REQUIRED_DETERMINISTIC_METRICS
    ):
        if metric_name not in raw_metrics:
            raise ValueError(
                "Evaluation run is missing "
                "required metric: "
                f"{metric_name}"
            )

        metrics[
            metric_name
        ] = _normalize_metric_value(
            raw_metrics[
                metric_name
            ],
            metric_name=metric_name,
        )

    raw_tags = (
        run.data.tags
    )

    compatibility_tags: dict[
        str,
        str,
    ] = {}

    for tag_name in (
        COMPATIBILITY_TAGS
    ):
        value = str(
            raw_tags.get(
                tag_name
            )
            or ""
        ).strip()

        if not value:
            raise ValueError(
                "Evaluation run is missing "
                "compatibility tag: "
                f"{tag_name}"
            )

        compatibility_tags[
            tag_name
        ] = value

    safe_projection = str(
        run.data.params.get(
            "mlflow_safe_projection"
        )
        or ""
    ).strip().lower()

    if safe_projection != "true":
        raise ValueError(
            "Evaluation run is not marked "
            "as an MLflow-safe projection."
        )

    return EvaluationRunSnapshot(
        run_id=normalized_run_id,
        metrics=metrics,
        compatibility_tags=(
            compatibility_tags
        ),
    )


def _resolve_metric_tolerances(
    *,
    default_tolerance: float,
    per_metric_tolerances: (
        Mapping[str, float]
        | None
    ),
) -> dict[str, float]:
    default_value = (
        _normalize_tolerance(
            default_tolerance,
            label="Default tolerance",
        )
    )

    overrides = dict(
        per_metric_tolerances
        or {}
    )

    unknown = (
        set(overrides)
        - set(
            REQUIRED_DETERMINISTIC_METRICS
        )
    )

    if unknown:
        raise ValueError(
            "Tolerance supplied for unknown "
            "deterministic metric."
        )

    resolved: dict[
        str,
        float,
    ] = {}

    for metric_name in (
        REQUIRED_DETERMINISTIC_METRICS
    ):
        value = overrides.get(
            metric_name,
            default_value,
        )

        resolved[
            metric_name
        ] = _normalize_tolerance(
            value,
            label=(
                "Tolerance for "
                f"{metric_name}"
            ),
        )

    return resolved


def compare_evaluation_run_snapshots(
    baseline: EvaluationRunSnapshot,
    candidate: EvaluationRunSnapshot,
    *,
    default_tolerance: float = 0.0,
    per_metric_tolerances: (
        Mapping[str, float]
        | None
    ) = None,
) -> RegressionGateResult:
    """
    Compare one explicit candidate against one
    explicit compatible baseline.

    Candidate passes a metric when:

        candidate >= baseline - tolerance
    """
    if (
        baseline.run_id
        == candidate.run_id
    ):
        raise ValueError(
            "Baseline and candidate must be "
            "different MLflow runs."
        )

    for tag_name in (
        COMPATIBILITY_TAGS
    ):
        baseline_value = (
            baseline
            .compatibility_tags
            .get(
                tag_name
            )
        )

        candidate_value = (
            candidate
            .compatibility_tags
            .get(
                tag_name
            )
        )

        if (
            baseline_value
            != candidate_value
        ):
            raise ValueError(
                "Baseline and candidate are "
                "evaluation-incompatible for "
                f"tag: {tag_name}"
            )

    tolerances = (
        _resolve_metric_tolerances(
            default_tolerance=(
                default_tolerance
            ),
            per_metric_tolerances=(
                per_metric_tolerances
            ),
        )
    )

    results: list[
        MetricRegressionResult
    ] = []

    for metric_name in (
        REQUIRED_DETERMINISTIC_METRICS
    ):
        baseline_value = (
            baseline.metrics[
                metric_name
            ]
        )

        candidate_value = (
            candidate.metrics[
                metric_name
            ]
        )

        tolerance = tolerances[
            metric_name
        ]

        required_minimum = (
            baseline_value
            - tolerance
        )

        passed = (
            candidate_value
            + _FLOAT_EPSILON
            >= required_minimum
        )

        results.append(
            MetricRegressionResult(
                metric_name=metric_name,
                baseline_value=(
                    baseline_value
                ),
                candidate_value=(
                    candidate_value
                ),
                tolerance=tolerance,
                required_minimum=(
                    required_minimum
                ),
                delta=(
                    candidate_value
                    - baseline_value
                ),
                passed=passed,
            )
        )

    metric_results = tuple(
        results
    )

    return RegressionGateResult(
        baseline_run_id=(
            baseline.run_id
        ),
        candidate_run_id=(
            candidate.run_id
        ),
        passed=all(
            result.passed
            for result
            in metric_results
        ),
        metric_results=(
            metric_results
        ),
        compatibility_tags=dict(
            baseline.compatibility_tags
        ),
    )


def run_regression_gate(
    *,
    baseline_run_id: str,
    candidate_run_id: str,
    default_tolerance: float = 0.0,
    per_metric_tolerances: (
        Mapping[str, float]
        | None
    ) = None,
    _mlflow=None,
) -> RegressionGateResult:
    """
    Load two explicit MLflow run IDs and execute
    the deterministic promotion regression gate.
    """
    baseline_id = _require_run_id(
        baseline_run_id,
        label="Baseline",
    )

    candidate_id = _require_run_id(
        candidate_run_id,
        label="Candidate",
    )

    if baseline_id == candidate_id:
        raise ValueError(
            "Baseline and candidate must be "
            "different MLflow runs."
        )

    baseline = (
        load_evaluation_run_snapshot(
            baseline_id,
            _mlflow=_mlflow,
        )
    )

    candidate = (
        load_evaluation_run_snapshot(
            candidate_id,
            _mlflow=_mlflow,
        )
    )

    return (
        compare_evaluation_run_snapshots(
            baseline,
            candidate,
            default_tolerance=(
                default_tolerance
            ),
            per_metric_tolerances=(
                per_metric_tolerances
            ),
        )
    )