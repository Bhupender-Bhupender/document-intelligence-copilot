from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.llmops.mlflow_evaluation_runner import (
    REQUIRED_DETERMINISTIC_METRICS,
)
from src.llmops.regression_gate import (
    COMPATIBILITY_TAGS,
    compare_evaluation_run_snapshots,
    EvaluationRunSnapshot,
    load_evaluation_run_snapshot,
    run_regression_gate,
)


def _metrics(
    **overrides,
):
    values = {
        metric_name: 1.0
        for metric_name
        in REQUIRED_DETERMINISTIC_METRICS
    }

    values.update(
        overrides
    )

    return values


def _tags(
    *,
    dataset=(
        "evaluation_cases_v1"
    ),
    contract=(
        "evaluation-contract-v1"
    ),
    **extra,
):
    values = {
        "application":
            "document-intelligence-copilot",

        "phase":
            "15",

        "evaluation_contract_version":
            contract,

        "evaluation_dataset_version":
            dataset,

        "generation_model":
            "generation-a",

        "embedding_model":
            "embedding-a",

        "index_name":
            "index-a",

        "retrieval_config_version":
            "retrieval-a",

        "prompt_contract_version":
            "prompt-a",

        "chunking_contract_version":
            "chunking-a",

        "code_revision":
            "revision-a",

        "environment":
            "baseline",
    }

    values.update(
        extra
    )

    return values


def _snapshot(
    run_id,
    *,
    metrics=None,
    tags=None,
):
    selected_tags = (
        tags
        if tags is not None
        else _tags()
    )

    compatibility = {
        key:
            selected_tags[key]
        for key
        in COMPATIBILITY_TAGS
    }

    return EvaluationRunSnapshot(
        run_id=run_id,
        metrics=(
            metrics
            if metrics is not None
            else _metrics()
        ),
        compatibility_tags=(
            compatibility
        ),
    )


class _FakeMLflow:

    def __init__(
        self,
        runs,
    ):
        self.runs = runs

    def get_run(
        self,
        run_id,
    ):
        return self.runs[
            run_id
        ]


def _fake_run(
    run_id,
    *,
    metrics=None,
    tags=None,
    status="FINISHED",
    safe_projection="True",
):
    return SimpleNamespace(
        info=SimpleNamespace(
            run_id=run_id,
            status=status,
        ),
        data=SimpleNamespace(
            metrics=(
                metrics
                if metrics is not None
                else _metrics()
            ),
            tags=(
                tags
                if tags is not None
                else _tags()
            ),
            params={
                "mlflow_safe_projection":
                    safe_projection,
            },
        ),
    )


def test_identical_metrics_pass():
    result = (
        compare_evaluation_run_snapshots(
            _snapshot(
                "baseline"
            ),
            _snapshot(
                "candidate"
            ),
        )
    )

    assert result.passed is True
    assert result.failed_metrics == ()


def test_metric_regression_fails():
    baseline = _snapshot(
        "baseline"
    )

    candidate_metrics = _metrics()

    candidate_metrics[
        "expected_document_fingerprint_hit/mean"
    ] = 0.90

    candidate = _snapshot(
        "candidate",
        metrics=candidate_metrics,
    )

    result = (
        compare_evaluation_run_snapshots(
            baseline,
            candidate,
        )
    )

    assert result.passed is False

    assert result.failed_metrics == (
        "expected_document_fingerprint_hit/mean",
    )


def test_default_tolerance_allows_small_regression():
    baseline_metrics = _metrics()

    baseline_metrics[
        "expected_document_fingerprint_hit/mean"
    ] = 0.95

    candidate_metrics = _metrics()

    candidate_metrics[
        "expected_document_fingerprint_hit/mean"
    ] = 0.94

    result = (
        compare_evaluation_run_snapshots(
            _snapshot(
                "baseline",
                metrics=baseline_metrics,
            ),
            _snapshot(
                "candidate",
                metrics=candidate_metrics,
            ),
            default_tolerance=0.02,
        )
    )

    assert result.passed is True


def test_per_metric_tolerance_overrides_default():
    baseline_metrics = _metrics()

    baseline_metrics[
        "expected_document_fingerprint_hit/mean"
    ] = 0.95

    candidate_metrics = _metrics()

    candidate_metrics[
        "expected_document_fingerprint_hit/mean"
    ] = 0.93

    result = (
        compare_evaluation_run_snapshots(
            _snapshot(
                "baseline",
                metrics=baseline_metrics,
            ),
            _snapshot(
                "candidate",
                metrics=candidate_metrics,
            ),
            default_tolerance=0.0,
            per_metric_tolerances={
                "expected_document_fingerprint_hit/mean":
                    0.03,
            },
        )
    )

    assert result.passed is True


def test_different_dataset_is_rejected():
    baseline = _snapshot(
        "baseline"
    )

    candidate = _snapshot(
        "candidate",
        tags=_tags(
            dataset=(
                "evaluation_cases_v2"
            )
        ),
    )

    with pytest.raises(
        ValueError,
        match=(
            "evaluation_dataset_version"
        ),
    ):
        compare_evaluation_run_snapshots(
            baseline,
            candidate,
        )


def test_different_evaluation_contract_is_rejected():
    baseline = _snapshot(
        "baseline"
    )

    candidate = _snapshot(
        "candidate",
        tags=_tags(
            contract=(
                "evaluation-contract-v2"
            )
        ),
    )

    with pytest.raises(
        ValueError,
        match=(
            "evaluation_contract_version"
        ),
    ):
        compare_evaluation_run_snapshots(
            baseline,
            candidate,
        )


def test_candidate_runtime_identity_may_change():
    baseline_tags = _tags()

    candidate_tags = _tags(
        generation_model=(
            "generation-b"
        ),
        embedding_model=(
            "embedding-b"
        ),
        index_name=(
            "index-b"
        ),
        retrieval_config_version=(
            "retrieval-b"
        ),
        prompt_contract_version=(
            "prompt-b"
        ),
        chunking_contract_version=(
            "chunking-b"
        ),
        code_revision=(
            "revision-b"
        ),
        environment=(
            "candidate"
        ),
    )

    fake_mlflow = _FakeMLflow(
        {
            "baseline":
                _fake_run(
                    "baseline",
                    tags=baseline_tags,
                ),

            "candidate":
                _fake_run(
                    "candidate",
                    tags=candidate_tags,
                ),
        }
    )

    result = run_regression_gate(
        baseline_run_id="baseline",
        candidate_run_id="candidate",
        _mlflow=fake_mlflow,
    )

    assert result.passed is True


def test_missing_required_metric_is_rejected():
    metrics = _metrics()

    metrics.pop(
        "citation_present/mean"
    )

    fake_mlflow = _FakeMLflow(
        {
            "run":
                _fake_run(
                    "run",
                    metrics=metrics,
                )
        }
    )

    with pytest.raises(
        ValueError,
        match="missing required metric",
    ):
        load_evaluation_run_snapshot(
            "run",
            _mlflow=fake_mlflow,
        )


def test_unsafe_projection_is_rejected():
    fake_mlflow = _FakeMLflow(
        {
            "run":
                _fake_run(
                    "run",
                    safe_projection="False",
                )
        }
    )

    with pytest.raises(
        ValueError,
        match="MLflow-safe projection",
    ):
        load_evaluation_run_snapshot(
            "run",
            _mlflow=fake_mlflow,
        )


def test_non_finished_run_is_rejected():
    fake_mlflow = _FakeMLflow(
        {
            "run":
                _fake_run(
                    "run",
                    status="RUNNING",
                )
        }
    )

    with pytest.raises(
        ValueError,
        match="FINISHED",
    ):
        load_evaluation_run_snapshot(
            "run",
            _mlflow=fake_mlflow,
        )


def test_same_run_cannot_be_baseline_and_candidate():
    fake_mlflow = _FakeMLflow(
        {}
    )

    with pytest.raises(
        ValueError,
        match="different MLflow runs",
    ):
        run_regression_gate(
            baseline_run_id="same",
            candidate_run_id="same",
            _mlflow=fake_mlflow,
        )


def test_unknown_metric_tolerance_is_rejected():
    with pytest.raises(
        ValueError,
        match="unknown deterministic metric",
    ):
        compare_evaluation_run_snapshots(
            _snapshot(
                "baseline"
            ),
            _snapshot(
                "candidate"
            ),
            per_metric_tolerances={
                "unknown/mean":
                    0.1,
            },
        )