from __future__ import annotations

import subprocess
import sys

from pathlib import Path

import pytest

import src.llmops.regression_gate_cli as cli

from src.llmops.regression_gate import (
    MetricRegressionResult,
    RegressionGateResult,
)


def _result(
    *,
    passed: bool,
) -> RegressionGateResult:
    metric_results = (
        MetricRegressionResult(
            metric_name=(
                "answer_expectation_met/mean"
            ),
            baseline_value=1.0,
            candidate_value=(
                1.0
                if passed
                else 0.9
            ),
            tolerance=0.0,
            required_minimum=1.0,
            delta=(
                0.0
                if passed
                else -0.1
            ),
            passed=passed,
        ),
        MetricRegressionResult(
            metric_name=(
                "expected_document_"
                "fingerprint_hit/mean"
            ),
            baseline_value=1.0,
            candidate_value=1.0,
            tolerance=0.0,
            required_minimum=1.0,
            delta=0.0,
            passed=True,
        ),
        MetricRegressionResult(
            metric_name=(
                "evidence_present/mean"
            ),
            baseline_value=1.0,
            candidate_value=1.0,
            tolerance=0.0,
            required_minimum=1.0,
            delta=0.0,
            passed=True,
        ),
        MetricRegressionResult(
            metric_name=(
                "citation_present/mean"
            ),
            baseline_value=1.0,
            candidate_value=1.0,
            tolerance=0.0,
            required_minimum=1.0,
            delta=0.0,
            passed=True,
        ),
    )

    return RegressionGateResult(
        baseline_run_id="baseline-run",
        candidate_run_id="candidate-run",
        passed=passed,
        metric_results=metric_results,
        compatibility_tags={
            "application":
                "document-intelligence-copilot",
            "phase":
                "15",
            "evaluation_contract_version":
                "evaluation-contract-v1",
            "evaluation_dataset_version":
                "evaluation_cases_v1",
        },
    )


def test_cli_requires_explicit_run_ids():
    parser = cli.build_parser()

    with pytest.raises(
        SystemExit,
    ):
        parser.parse_args(
            []
        )


def test_cli_pass_returns_zero(
    monkeypatch,
):
    monkeypatch.setattr(
        cli,
        "run_regression_gate",
        lambda **kwargs:
            _result(
                passed=True
            ),
    )

    exit_code = cli.main(
        [
            "--baseline-run-id",
            "baseline-run",
            "--candidate-run-id",
            "candidate-run",
        ]
    )

    assert (
        exit_code
        == cli.PASS_EXIT_CODE
    )


def test_cli_regression_returns_two(
    monkeypatch,
):
    monkeypatch.setattr(
        cli,
        "run_regression_gate",
        lambda **kwargs:
            _result(
                passed=False
            ),
    )

    exit_code = cli.main(
        [
            "--baseline-run-id",
            "baseline-run",
            "--candidate-run-id",
            "candidate-run",
        ]
    )

    assert (
        exit_code
        == cli.REGRESSION_EXIT_CODE
    )


def test_cli_forwards_global_tolerance(
    monkeypatch,
):
    captured = {}

    def fake_gate(
        **kwargs,
    ):
        captured.update(
            kwargs
        )

        return _result(
            passed=True
        )

    monkeypatch.setattr(
        cli,
        "run_regression_gate",
        fake_gate,
    )

    exit_code = cli.main(
        [
            "--baseline-run-id",
            "baseline-run",
            "--candidate-run-id",
            "candidate-run",
            "--default-tolerance",
            "0.02",
        ]
    )

    assert exit_code == 0

    assert (
        captured[
            "default_tolerance"
        ]
        == 0.02
    )


def test_cli_forwards_metric_override(
    monkeypatch,
):
    captured = {}

    def fake_gate(
        **kwargs,
    ):
        captured.update(
            kwargs
        )

        return _result(
            passed=True
        )

    monkeypatch.setattr(
        cli,
        "run_regression_gate",
        fake_gate,
    )

    metric = (
        "expected_document_"
        "fingerprint_hit/mean"
    )

    exit_code = cli.main(
        [
            "--baseline-run-id",
            "baseline-run",
            "--candidate-run-id",
            "candidate-run",
            "--metric-tolerance",
            f"{metric}=0.03",
        ]
    )

    assert exit_code == 0

    assert (
        captured[
            "per_metric_tolerances"
        ][
            metric
        ]
        == 0.03
    )


def test_duplicate_metric_override_is_rejected():
    metric = (
        "citation_present/mean"
    )

    with pytest.raises(
        ValueError,
        match="Duplicate tolerance",
    ):
        cli.main(
            [
                "--baseline-run-id",
                "baseline-run",
                "--candidate-run-id",
                "candidate-run",
                "--metric-tolerance",
                f"{metric}=0.01",
                "--metric-tolerance",
                f"{metric}=0.02",
            ]
        )


def test_unknown_metric_override_is_rejected():
    parser = cli.build_parser()

    with pytest.raises(
        SystemExit,
    ):
        parser.parse_args(
            [
                "--baseline-run-id",
                "baseline-run",
                "--candidate-run-id",
                "candidate-run",
                "--metric-tolerance",
                "unknown/mean=0.1",
            ]
        )


def test_direct_script_entrypoint_help():
    repo_root = (
        Path(__file__)
        .resolve()
        .parents[1]
    )

    script = (
        repo_root
        / "scripts"
        / "run_llmops_regression_gate.py"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--help",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0

    assert (
        "--baseline-run-id"
        in completed.stdout
    )

    assert (
        "--candidate-run-id"
        in completed.stdout
    )

    assert (
        "--default-tolerance"
        in completed.stdout
    )

    assert (
        "--metric-tolerance"
        in completed.stdout
    )