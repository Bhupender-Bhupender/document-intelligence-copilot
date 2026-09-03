from __future__ import annotations

import csv
import json

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

import src.llmops.mlflow_evaluation_runner as runner_module

from src.llmops.evaluation_dataset import (
    build_version_context_for_dataset,
    load_evaluation_dataset_bundle,
)
from src.llmops.mlflow_evaluation_runner import (
    REQUIRED_DETERMINISTIC_METRICS,
    run_mlflow_deterministic_evaluation,
)
from src.llmops.mlflow_tracking import (
    MLflowExperimentConfig,
)


_PRIVATE_QUERY_1 = (
    "PRIVATE_QUERY_RUNNER_1"
)

_PRIVATE_QUERY_2 = (
    "PRIVATE_QUERY_RUNNER_2"
)

_PRIVATE_ANSWER = (
    "PRIVATE_ANSWER_RUNNER"
)


def _bundle(
    tmp_path: Path,
):
    canonical = (
        tmp_path
        / "evaluation_cases_v1.jsonl"
    )

    manifest = (
        tmp_path
        / "manifest.csv"
    )

    rows = [
        {
            "case_id":
                "case-1",
            "dataset_id":
                "source-a",
            "version":
                "1.0",
            "query":
                _PRIVATE_QUERY_1,
            "target_document_id":
                "baseline-1",
            "is_active":
                True,
            "comment":
                "",
        },
        {
            "case_id":
                "case-2",
            "dataset_id":
                "source-b",
            "version":
                "1.0",
            "query":
                _PRIVATE_QUERY_2,
            "target_document_id":
                "baseline-2",
            "is_active":
                True,
            "comment":
                "",
        },
    ]

    canonical.write_text(
        "\n".join(
            json.dumps(row)
            for row in rows
        )
        + "\n",
        encoding="utf-8",
    )

    with manifest.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "document_id",
                "sha256",
            ],
        )

        writer.writeheader()

        writer.writerows(
            [
                {
                    "document_id":
                        "baseline-1",
                    "sha256":
                        "a" * 64,
                },
                {
                    "document_id":
                        "baseline-2",
                    "sha256":
                        "b" * 64,
                },
            ]
        )

    return (
        load_evaluation_dataset_bundle(
            canonical,
            manifest,
        )
    )


def _context(
    bundle,
):
    return (
        build_version_context_for_dataset(
            bundle,
            generation_model="generation",
            embedding_model="embedding",
            index_name="index",
            code_revision="abc123",
        )
    )


def _config():
    return MLflowExperimentConfig(
        experiment_name=(
            "phase15f-test"
        ),
        environment="test",
    )


def _metrics():
    return {
        name: 1.0
        for name
        in REQUIRED_DETERMINISTIC_METRICS
    }


class _FakeGenAI:

    def __init__(
        self,
        *,
        metrics=None,
    ):
        self.calls = []

        self.metrics = (
            metrics
            if metrics is not None
            else _metrics()
        )

    def evaluate(
        self,
        *,
        data,
        scorers,
    ):
        self.calls.append(
            {
                "data": data,
                "scorers": scorers,
            }
        )

        return SimpleNamespace(
            run_id="run-123",
            metrics=self.metrics,
        )


class _FakeMLflow:

    def __init__(
        self,
        *,
        metrics=None,
    ):
        self.genai = _FakeGenAI(
            metrics=metrics
        )


def _patch_tracking(
    monkeypatch,
):
    captured = {
        "runs": [],
        "params": [],
    }

    @contextmanager
    def fake_start_llmops_run(
        *,
        config,
        version_context,
        run_name,
        extra_tags=None,
        _mlflow=None,
    ):
        captured["runs"].append(
            {
                "config": config,
                "version_context":
                    version_context,
                "run_name":
                    run_name,
            }
        )

        yield SimpleNamespace()

    def fake_log_params(
        params,
        *,
        _mlflow=None,
    ):
        captured[
            "params"
        ].append(
            dict(params)
        )

    monkeypatch.setattr(
        runner_module,
        "start_llmops_run",
        fake_start_llmops_run,
    )

    monkeypatch.setattr(
        runner_module,
        "log_params",
        fake_log_params,
    )

    return captured


def test_runner_executes_each_case_once(
    tmp_path,
    monkeypatch,
):
    bundle = _bundle(
        tmp_path
    )

    captured = _patch_tracking(
        monkeypatch
    )

    fake_mlflow = _FakeMLflow()

    seen = []

    expected_by_case = {
        example.case_id:
            example.expected_document_id
        for example
        in bundle.retrieval_examples
    }

    def predict_case(
        case_id,
    ):
        seen.append(
            case_id
        )

        return {
            "answer_text":
                _PRIVATE_ANSWER,

            "retrieved_document_ids":
                [
                    expected_by_case[
                        case_id
                    ]
                ],

            "evidence_count":
                1,

            "citation_count":
                1,
        }

    result = (
        run_mlflow_deterministic_evaluation(
            bundle,
            predict_case=predict_case,
            config=_config(),
            version_context=(
                _context(bundle)
            ),
            run_name="runner-test",
            document_fingerprint_key=(
                b"k" * 32
            ),
            _mlflow=fake_mlflow,
        )
    )

    assert seen == [
        "case-1",
        "case-2",
    ]

    assert (
        result.evaluated_case_count
        == 2
    )

    assert result.run_id == "run-123"

    assert len(
        captured["runs"]
    ) == 1

    assert len(
        captured["params"]
    ) == 1


def test_runner_sends_only_safe_rows_to_mlflow(
    tmp_path,
    monkeypatch,
):
    bundle = _bundle(
        tmp_path
    )

    _patch_tracking(
        monkeypatch
    )

    fake_mlflow = _FakeMLflow()

    expected_by_case = {
        example.case_id:
            example.expected_document_id
        for example
        in bundle.retrieval_examples
    }

    def predict_case(
        case_id,
    ):
        return {
            "answer_text":
                _PRIVATE_ANSWER,

            "retrieved_document_ids":
                [
                    expected_by_case[
                        case_id
                    ]
                ],

            "evidence_count":
                1,

            "citation_count":
                1,
        }

    run_mlflow_deterministic_evaluation(
        bundle,
        predict_case=predict_case,
        config=_config(),
        version_context=(
            _context(bundle)
        ),
        run_name="privacy-test",
        document_fingerprint_key=(
            b"k" * 32
        ),
        _mlflow=fake_mlflow,
    )

    call = (
        fake_mlflow
        .genai
        .calls[0]
    )

    serialized = repr(
        call["data"]
    )

    assert (
        _PRIVATE_QUERY_1
        not in serialized
    )

    assert (
        _PRIVATE_QUERY_2
        not in serialized
    )

    assert (
        _PRIVATE_ANSWER
        not in serialized
    )

    assert "case-1" not in serialized
    assert "case-2" not in serialized

    for expected_id in (
        expected_by_case.values()
    ):
        assert (
            expected_id
            not in serialized
        )

    assert [
        row["inputs"]
        for row in call["data"]
    ] == [
        {
            "evaluation_case_index": 1,
        },
        {
            "evaluation_case_index": 2,
        },
    ]


def test_runner_returns_required_metrics(
    tmp_path,
    monkeypatch,
):
    bundle = _bundle(
        tmp_path
    )

    _patch_tracking(
        monkeypatch
    )

    fake_mlflow = _FakeMLflow()

    expected_by_case = {
        example.case_id:
            example.expected_document_id
        for example
        in bundle.retrieval_examples
    }

    result = (
        run_mlflow_deterministic_evaluation(
            bundle,
            predict_case=(
                lambda case_id: {
                    "answer_text":
                        "answer",

                    "retrieved_document_ids":
                        [
                            expected_by_case[
                                case_id
                            ]
                        ],

                    "evidence_count":
                        1,

                    "citation_count":
                        1,
                }
            ),
            config=_config(),
            version_context=(
                _context(bundle)
            ),
            run_name="metric-test",
            document_fingerprint_key=(
                b"k" * 32
            ),
            _mlflow=fake_mlflow,
        )
    )

    assert result.metrics == _metrics()

    assert (
        result.evaluation_dataset_version
        == "evaluation_cases_v1"
    )


def test_runner_rejects_version_drift(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    context = _context(
        bundle
    )

    wrong_context = (
        context.__class__(
            **{
                **context.__dict__,
                "evaluation_dataset_version":
                    "different_dataset",
            }
        )
    )

    with pytest.raises(
        ValueError,
        match="does not match",
    ):
        run_mlflow_deterministic_evaluation(
            bundle,
            predict_case=(
                lambda case_id: {}
            ),
            config=_config(),
            version_context=(
                wrong_context
            ),
            run_name="drift-test",
        )


def test_runner_rejects_non_mapping_prediction(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    with pytest.raises(
        TypeError,
        match="return a mapping",
    ):
        run_mlflow_deterministic_evaluation(
            bundle,
            predict_case=(
                lambda case_id:
                    "not-a-mapping"
            ),
            config=_config(),
            version_context=(
                _context(bundle)
            ),
            run_name="bad-output",
        )


def test_runner_rejects_missing_metric(
    tmp_path,
    monkeypatch,
):
    bundle = _bundle(
        tmp_path
    )

    _patch_tracking(
        monkeypatch
    )

    metrics = _metrics()

    metrics.pop(
        "citation_present/mean"
    )

    fake_mlflow = _FakeMLflow(
        metrics=metrics
    )

    expected_by_case = {
        example.case_id:
            example.expected_document_id
        for example
        in bundle.retrieval_examples
    }

    with pytest.raises(
        ValueError,
        match="missing required metric",
    ):
        run_mlflow_deterministic_evaluation(
            bundle,
            predict_case=(
                lambda case_id: {
                    "answer_text":
                        "answer",

                    "retrieved_document_ids":
                        [
                            expected_by_case[
                                case_id
                            ]
                        ],

                    "evidence_count":
                        1,

                    "citation_count":
                        1,
                }
            ),
            config=_config(),
            version_context=(
                _context(bundle)
            ),
            run_name="missing-metric",
            document_fingerprint_key=(
                b"k" * 32
            ),
            _mlflow=fake_mlflow,
        )