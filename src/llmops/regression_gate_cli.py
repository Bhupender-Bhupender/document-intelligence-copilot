from __future__ import annotations

import argparse

from collections.abc import Sequence
from typing import Any

import mlflow

from src.llmops.mlflow_evaluation_runner import (
    REQUIRED_DETERMINISTIC_METRICS,
)
from src.llmops.regression_gate import (
    RegressionGateResult,
    run_regression_gate,
)


PASS_EXIT_CODE = 0
REGRESSION_EXIT_CODE = 2


def _parse_metric_tolerance(
    value: str,
) -> tuple[str, float]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(
            "Metric tolerance must use "
            "METRIC=VALUE."
        )

    metric_name, raw_tolerance = (
        value.split(
            "=",
            1,
        )
    )

    metric_name = (
        metric_name.strip()
    )

    if (
        metric_name
        not in REQUIRED_DETERMINISTIC_METRICS
    ):
        raise argparse.ArgumentTypeError(
            "Unknown deterministic metric: "
            f"{metric_name}"
        )

    try:
        tolerance = float(
            raw_tolerance
        )

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Metric tolerance must be numeric."
        ) from exc

    if not (
        0.0
        <= tolerance
        <= 1.0
    ):
        raise argparse.ArgumentTypeError(
            "Metric tolerance must be "
            "in [0.0, 1.0]."
        )

    return (
        metric_name,
        tolerance,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare one explicit candidate "
            "MLflow evaluation run against one "
            "explicit baseline run."
        )
    )

    parser.add_argument(
        "--baseline-run-id",
        required=True,
        help=(
            "Explicit MLflow run ID used as "
            "the evaluation baseline."
        ),
    )

    parser.add_argument(
        "--candidate-run-id",
        required=True,
        help=(
            "Explicit MLflow run ID being "
            "evaluated for promotion."
        ),
    )

    parser.add_argument(
        "--tracking-uri",
        default=None,
        help=(
            "Optional MLflow tracking URI."
        ),
    )

    parser.add_argument(
        "--default-tolerance",
        type=float,
        default=0.0,
        help=(
            "Maximum permitted absolute "
            "regression for every metric unless "
            "overridden. Default: 0.0."
        ),
    )

    parser.add_argument(
        "--metric-tolerance",
        action="append",
        default=[],
        type=_parse_metric_tolerance,
        metavar="METRIC=VALUE",
        help=(
            "Per-metric tolerance override. "
            "May be supplied multiple times."
        ),
    )

    return parser


def _build_tolerance_overrides(
    values: Sequence[
        tuple[str, float]
    ],
) -> dict[str, float]:
    overrides: dict[
        str,
        float,
    ] = {}

    for metric_name, tolerance in values:
        if metric_name in overrides:
            raise ValueError(
                "Duplicate tolerance override "
                f"for metric: {metric_name}"
            )

        overrides[
            metric_name
        ] = tolerance

    return overrides


def _print_result(
    result: RegressionGateResult,
) -> None:
    print(
        "BASELINE_RUN_ID:",
        result.baseline_run_id,
    )

    print(
        "CANDIDATE_RUN_ID:",
        result.candidate_run_id,
    )

    print(
        "EVALUATION_DATASET_VERSION:",
        result.compatibility_tags[
            "evaluation_dataset_version"
        ],
    )

    print(
        "EVALUATION_CONTRACT_VERSION:",
        result.compatibility_tags[
            "evaluation_contract_version"
        ],
    )

    print(
        "METRIC_RESULTS:"
    )

    for metric in (
        result.metric_results
    ):
        print(
            " ",
            metric.metric_name,
        )

        print(
            "    baseline:",
            metric.baseline_value,
        )

        print(
            "    candidate:",
            metric.candidate_value,
        )

        print(
            "    delta:",
            metric.delta,
        )

        print(
            "    tolerance:",
            metric.tolerance,
        )

        print(
            "    required_minimum:",
            metric.required_minimum,
        )

        print(
            "    passed:",
            metric.passed,
        )

    print(
        "FAILED_METRIC_COUNT:",
        len(
            result.failed_metrics
        ),
    )

    print(
        "REGRESSION_GATE_PASS:",
        result.passed,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    _mlflow: Any = None,
) -> int:
    args = build_parser().parse_args(
        argv
    )

    if not (
        0.0
        <= args.default_tolerance
        <= 1.0
    ):
        raise ValueError(
            "Default tolerance must be "
            "in [0.0, 1.0]."
        )

    overrides = (
        _build_tolerance_overrides(
            args.metric_tolerance
        )
    )

    mlflow_module = (
        _mlflow
        if _mlflow is not None
        else mlflow
    )

    if args.tracking_uri:
        mlflow_module.set_tracking_uri(
            args.tracking_uri
        )

    result = run_regression_gate(
        baseline_run_id=(
            args.baseline_run_id
        ),
        candidate_run_id=(
            args.candidate_run_id
        ),
        default_tolerance=(
            args.default_tolerance
        ),
        per_metric_tolerances=(
            overrides
        ),
        _mlflow=mlflow_module,
    )

    _print_result(
        result
    )

    if result.passed:
        return PASS_EXIT_CODE

    return REGRESSION_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(
        main()
    )