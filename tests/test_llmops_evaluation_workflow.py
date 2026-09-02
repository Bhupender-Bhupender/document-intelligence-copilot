from __future__ import annotations

import csv
import json

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import src.llmops.mlflow_evaluation_runner as runner_module

from src.llmops.evaluation_dataset import (
    build_version_context_for_dataset,
    load_evaluation_dataset_bundle,
)
from src.llmops.evaluation_workflow import (
    run_serving_mlflow_evaluation,
)
from src.llmops.mlflow_evaluation_runner import (
    REQUIRED_DETERMINISTIC_METRICS,
)
from src.llmops.mlflow_tracking import (
    MLflowExperimentConfig,
)
from src.schema.retrieval_service_models import (
    RetrievalEvidence,
)
from src.schema.serving_models import (
    ServingAnswerResponse,
)


PRIVATE_QUERY = (
    "PRIVATE_WORKFLOW_QUERY"
)

PRIVATE_ANSWER = (
    "PRIVATE_WORKFLOW_ANSWER"
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

    canonical.write_text(
        json.dumps(
            {
                "case_id":
                    "case-1",
                "dataset_id":
                    "source-a",
                "version":
                    "1.0",
                "query":
                    PRIVATE_QUERY,
                "target_document_id":
                    "baseline-1",
                "is_active":
                    True,
                "comment":
                    "",
            }
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

        writer.writerow(
            {
                "document_id":
                    "baseline-1",
                "sha256":
                    "a" * 64,
            }
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
        experiment_name="workflow-test",
        environment="test",
    )


class _FakeGenAI:

    def __init__(self):
        self.data = None

    def evaluate(
        self,
        *,
        data,
        scorers,
    ):
        self.data = data

        return SimpleNamespace(
            run_id="workflow-run",
            metrics={
                name: 1.0
                for name
                in REQUIRED_DETERMINISTIC_METRICS
            },
        )


class _FakeMLflow:

    def __init__(self):
        self.genai = _FakeGenAI()


def _patch_tracking(
    monkeypatch,
):
    @contextmanager
    def fake_start_llmops_run(
        **kwargs,
    ):
        yield SimpleNamespace()

    def fake_log_params(
        params,
        *,
        _mlflow=None,
    ):
        return None

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


def test_workflow_uses_real_serving_request_boundary(
    tmp_path,
    monkeypatch,
):
    bundle = _bundle(
        tmp_path
    )

    _patch_tracking(
        monkeypatch
    )

    expected_document_id = (
        bundle
        .retrieval_examples[0]
        .expected_document_id
    )

    seen_queries = []

    def serving_runner(
        request,
    ):
        seen_queries.append(
            request.query
        )

        return (
            ServingAnswerResponse.model_construct(
                answer_text=(
                    PRIVATE_ANSWER
                ),
                evidence=[
                    RetrievalEvidence.model_construct(
                        document_id=(
                            expected_document_id
                        ),
                    )
                ],
                sources=[
                    object(),
                ],
            )
        )

    fake_mlflow = _FakeMLflow()

    result = (
        run_serving_mlflow_evaluation(
            bundle,
            config=_config(),
            version_context=(
                _context(bundle)
            ),
            run_name="workflow",
            serving_runner=(
                serving_runner
            ),
            document_fingerprint_key=(
                b"k" * 32
            ),
            _mlflow=fake_mlflow,
        )
    )

    assert seen_queries == [
        PRIVATE_QUERY,
    ]

    assert (
        result.evaluated_case_count
        == 1
    )

    assert result.run_id == (
        "workflow-run"
    )


def test_workflow_sends_no_raw_rag_content_to_mlflow(
    tmp_path,
    monkeypatch,
):
    bundle = _bundle(
        tmp_path
    )

    _patch_tracking(
        monkeypatch
    )

    expected_document_id = (
        bundle
        .retrieval_examples[0]
        .expected_document_id
    )

    def serving_runner(
        request,
    ):
        return (
            ServingAnswerResponse.model_construct(
                answer_text=(
                    PRIVATE_ANSWER
                ),
                evidence=[
                    RetrievalEvidence.model_construct(
                        document_id=(
                            expected_document_id
                        ),
                    )
                ],
                sources=[
                    object(),
                ],
            )
        )

    fake_mlflow = _FakeMLflow()

    run_serving_mlflow_evaluation(
        bundle,
        config=_config(),
        version_context=(
            _context(bundle)
        ),
        run_name="privacy",
        serving_runner=serving_runner,
        document_fingerprint_key=(
            b"k" * 32
        ),
        _mlflow=fake_mlflow,
    )

    persisted_boundary = repr(
        fake_mlflow.genai.data
    )

    assert (
        PRIVATE_QUERY
        not in persisted_boundary
    )

    assert (
        PRIVATE_ANSWER
        not in persisted_boundary
    )

    assert (
        expected_document_id
        not in persisted_boundary
    )

    assert (
        "case-1"
        not in persisted_boundary
    )


def test_workflow_returns_four_metrics(
    tmp_path,
    monkeypatch,
):
    bundle = _bundle(
        tmp_path
    )

    _patch_tracking(
        monkeypatch
    )

    expected_document_id = (
        bundle
        .retrieval_examples[0]
        .expected_document_id
    )

    fake_mlflow = _FakeMLflow()

    result = (
        run_serving_mlflow_evaluation(
            bundle,
            config=_config(),
            version_context=(
                _context(bundle)
            ),
            run_name="metrics",
            serving_runner=(
                lambda request:
                    ServingAnswerResponse
                    .model_construct(
                        answer_text="answer",
                        evidence=[
                            RetrievalEvidence
                            .model_construct(
                                document_id=(
                                    expected_document_id
                                ),
                            )
                        ],
                        sources=[
                            object(),
                        ],
                    )
            ),
            document_fingerprint_key=(
                b"k" * 32
            ),
            _mlflow=fake_mlflow,
        )
    )

    assert len(
        result.metrics
    ) == 4

    assert all(
        value == 1.0
        for value
        in result.metrics.values()
    )