from __future__ import annotations

import math

import pytest

from src.llmops.mlflow_tracking import (
    DEFAULT_ENVIRONMENT,
    DEFAULT_EXPERIMENT_NAME,
    MLflowExperimentConfig,
    build_run_tags,
    configure_tracking,
    log_metrics,
    log_params,
    start_llmops_run,
)
from src.llmops.versioning import LLMOpsVersionContext


class _FakeRun:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type,
        exc,
        traceback,
    ) -> bool:
        self.exited = True
        return False


class _FakeMLflow:
    def __init__(self) -> None:
        self.tracking_uris: list[str] = []
        self.experiments: list[str] = []
        self.start_run_calls: list[dict] = []
        self.metric_calls: list[tuple[dict, int | None]] = []
        self.param_calls: list[dict] = []
        self.run = _FakeRun()

    def set_tracking_uri(self, uri: str) -> None:
        self.tracking_uris.append(uri)

    def set_experiment(self, name: str):
        self.experiments.append(name)
        return {"name": name}

    def start_run(self, **kwargs):
        self.start_run_calls.append(kwargs)
        return self.run

    def log_metrics(
        self,
        metrics: dict,
        step: int | None = None,
    ) -> None:
        self.metric_calls.append(
            (metrics, step)
        )

    def log_params(self, params: dict) -> None:
        self.param_calls.append(params)


def _version_context() -> LLMOpsVersionContext:
    return LLMOpsVersionContext(
        retrieval_config_version="retrieval-v1",
        prompt_contract_version="prompt-v1",
        chunking_contract_version="chunk-v1",
        evaluation_contract_version="eval-v1",
        generation_model="generation-model",
        embedding_model="embedding-model",
        index_name="catalog.schema.index",
        evaluation_dataset_version="dataset-v1",
        code_revision="abc123",
    )


def test_config_from_env_uses_safe_defaults():
    config = MLflowExperimentConfig.from_env({})

    assert (
        config.experiment_name
        == DEFAULT_EXPERIMENT_NAME
    )
    assert config.environment == DEFAULT_ENVIRONMENT
    assert config.tracking_uri is None


def test_config_from_env_accepts_overrides():
    config = MLflowExperimentConfig.from_env(
        {
            "MLFLOW_TRACKING_URI": "databricks",
            "MLFLOW_EXPERIMENT_NAME": "/Shared/docintel",
            "DOCINTEL_ENV": "development",
        }
    )

    assert config.tracking_uri == "databricks"
    assert config.experiment_name == "/Shared/docintel"
    assert config.environment == "development"


def test_build_run_tags_contains_version_identity():
    tags = build_run_tags(
        _version_context(),
        environment="test",
        extra_tags={"run_kind": "regression"},
    )

    assert tags["application"] == (
        "document-intelligence-copilot"
    )
    assert tags["phase"] == "15"
    assert tags["environment"] == "test"
    assert tags["code_revision"] == "abc123"
    assert tags["retrieval_config_version"] == (
        "retrieval-v1"
    )
    assert tags["run_kind"] == "regression"


@pytest.mark.parametrize(
    "unsafe_key",
    [
        "token",
        "api_key",
        "prompt_text",
        "query_text",
        "answer_text",
        "document_text",
    ],
)
def test_build_run_tags_rejects_content_or_secret_keys(
    unsafe_key: str,
):
    with pytest.raises(ValueError):
        build_run_tags(
            _version_context(),
            environment="test",
            extra_tags={
                unsafe_key: "must-not-log"
            },
        )


def test_build_run_tags_prevents_reserved_override():
    with pytest.raises(ValueError):
        build_run_tags(
            _version_context(),
            environment="test",
            extra_tags={
                "code_revision": "override"
            },
        )


def test_configure_tracking_sets_uri_and_experiment():
    fake = _FakeMLflow()
    config = MLflowExperimentConfig(
        experiment_name="/Shared/docintel",
        environment="test",
        tracking_uri="databricks",
    )

    result = configure_tracking(
        config,
        _mlflow=fake,
    )

    assert fake.tracking_uris == ["databricks"]
    assert fake.experiments == ["/Shared/docintel"]
    assert result == {"name": "/Shared/docintel"}


def test_start_llmops_run_carries_canonical_tags():
    fake = _FakeMLflow()
    config = MLflowExperimentConfig(
        experiment_name="docintel",
        environment="test",
    )

    with start_llmops_run(
        config=config,
        version_context=_version_context(),
        run_name="candidate-eval",
        extra_tags={
            "run_kind": "candidate"
        },
        _mlflow=fake,
    ) as active:
        assert active is fake.run
        assert fake.run.entered is True

    assert fake.run.exited is True
    assert fake.experiments == ["docintel"]
    assert len(fake.start_run_calls) == 1

    call = fake.start_run_calls[0]
    assert call["run_name"] == "candidate-eval"
    assert call["tags"]["code_revision"] == "abc123"
    assert call["tags"]["run_kind"] == "candidate"


def test_log_metrics_normalizes_values():
    fake = _FakeMLflow()

    result = log_metrics(
        {
            "hit_at_3": 1,
            "citation_valid_rate": 0.95,
        },
        step=2,
        _mlflow=fake,
    )

    assert result == {
        "hit_at_3": 1.0,
        "citation_valid_rate": 0.95,
    }
    assert fake.metric_calls == [
        (result, 2)
    ]


@pytest.mark.parametrize(
    "value",
    [
        math.inf,
        -math.inf,
        math.nan,
    ],
)
def test_log_metrics_rejects_non_finite_values(
    value: float,
):
    with pytest.raises(ValueError):
        log_metrics(
            {"metric": value},
            _mlflow=_FakeMLflow(),
        )


def test_log_metrics_rejects_bool():
    with pytest.raises(TypeError):
        log_metrics(
            {"metric": True},
            _mlflow=_FakeMLflow(),
        )


def test_log_params_stringifies_safe_values():
    fake = _FakeMLflow()

    result = log_params(
        {
            "retrieval_top_k": 10,
            "rerank_top_k": 3,
            "semantic_eval": False,
        },
        _mlflow=fake,
    )

    assert result == {
        "retrieval_top_k": "10",
        "rerank_top_k": "3",
        "semantic_eval": "False",
    }
    assert fake.param_calls == [result]


def test_log_params_rejects_sensitive_key():
    with pytest.raises(ValueError):
        log_params(
            {
                "access_token": "secret"
            },
            _mlflow=_FakeMLflow(),
        )
