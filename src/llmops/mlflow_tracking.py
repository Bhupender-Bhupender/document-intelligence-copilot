from __future__ import annotations

import math
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Mapping

from src.llmops.versioning import LLMOpsVersionContext


DEFAULT_EXPERIMENT_NAME = "document-intelligence-llmops"
DEFAULT_ENVIRONMENT = "development"

_FORBIDDEN_METADATA_KEYS = (
    "api_key",
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
    "prompt_text",
    "query_text",
    "answer_text",
    "response_text",
    "document_text",
)


def _required_text(name: str, value: object) -> str:
    resolved = str(value).strip()
    if not resolved:
        raise ValueError(f"{name} must be non-empty.")
    return resolved


def _metadata_key(name: object) -> str:
    key = _required_text("metadata key", name)
    lowered = key.lower()
    if any(fragment in lowered for fragment in _FORBIDDEN_METADATA_KEYS):
        raise ValueError(
            f"Unsafe MLflow metadata key is not allowed: {key}"
        )
    return key


@dataclass(frozen=True)
class MLflowExperimentConfig:
    """Runtime-neutral configuration for Phase 15 MLflow tracking."""

    experiment_name: str
    environment: str
    tracking_uri: str | None = None

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
    ) -> "MLflowExperimentConfig":
        source = os.environ if env is None else env
        tracking_uri = str(
            source.get("MLFLOW_TRACKING_URI", "")
        ).strip() or None

        return cls(
            experiment_name=_required_text(
                "experiment_name",
                source.get(
                    "MLFLOW_EXPERIMENT_NAME",
                    DEFAULT_EXPERIMENT_NAME,
                ),
            ),
            environment=_required_text(
                "environment",
                source.get(
                    "DOCINTEL_ENV",
                    DEFAULT_ENVIRONMENT,
                ),
            ),
            tracking_uri=tracking_uri,
        )


def _resolve_mlflow(_mlflow: Any | None = None) -> Any:
    if _mlflow is not None:
        return _mlflow

    import mlflow

    return mlflow


def configure_tracking(
    config: MLflowExperimentConfig,
    *,
    _mlflow: Any | None = None,
) -> Any:
    """Configure tracking only when explicitly called."""

    backend = _resolve_mlflow(_mlflow)

    if config.tracking_uri:
        backend.set_tracking_uri(
            config.tracking_uri
        )

    return backend.set_experiment(
        config.experiment_name
    )


def build_run_tags(
    version_context: LLMOpsVersionContext,
    *,
    environment: str,
    extra_tags: Mapping[str, object] | None = None,
) -> dict[str, str]:
    """Build non-content metadata for one evaluation/tracing run."""

    tags = {
        "application": "document-intelligence-copilot",
        "phase": "15",
        "environment": _required_text(
            "environment",
            environment,
        ),
        **version_context.as_tags(),
    }

    if extra_tags:
        for raw_key, raw_value in extra_tags.items():
            key = _metadata_key(raw_key)
            if key in tags:
                raise ValueError(
                    f"Reserved MLflow tag cannot be overridden: {key}"
                )
            tags[key] = _required_text(
                f"tag {key}",
                raw_value,
            )

    return tags


@contextmanager
def start_llmops_run(
    *,
    config: MLflowExperimentConfig,
    version_context: LLMOpsVersionContext,
    run_name: str,
    extra_tags: Mapping[str, object] | None = None,
    _mlflow: Any | None = None,
) -> Iterator[Any]:
    """Start one MLflow run carrying canonical Phase 15 version identity."""

    backend = _resolve_mlflow(_mlflow)
    configure_tracking(
        config,
        _mlflow=backend,
    )

    tags = build_run_tags(
        version_context,
        environment=config.environment,
        extra_tags=extra_tags,
    )

    with backend.start_run(
        run_name=_required_text(
            "run_name",
            run_name,
        ),
        tags=tags,
    ) as active_run:
        yield active_run


def log_metrics(
    metrics: Mapping[str, float | int],
    *,
    step: int | None = None,
    _mlflow: Any | None = None,
) -> dict[str, float]:
    """Log finite numeric aggregate metrics only."""

    normalized: dict[str, float] = {}

    for raw_key, raw_value in metrics.items():
        key = _metadata_key(raw_key)

        if isinstance(raw_value, bool):
            raise TypeError(
                f"Metric {key} must be numeric, not bool."
            )

        value = float(raw_value)
        if not math.isfinite(value):
            raise ValueError(
                f"Metric {key} must be finite."
            )

        normalized[key] = value

    if not normalized:
        return {}

    backend = _resolve_mlflow(_mlflow)

    if step is None:
        backend.log_metrics(normalized)
    else:
        if step < 0:
            raise ValueError(
                "step must be non-negative."
            )
        backend.log_metrics(
            normalized,
            step=step,
        )

    return normalized


def log_params(
    params: Mapping[str, object],
    *,
    _mlflow: Any | None = None,
) -> dict[str, str]:
    """Log small configuration values, never prompt/query/answer content."""

    normalized = {
        _metadata_key(raw_key): _required_text(
            f"parameter {raw_key}",
            raw_value,
        )
        for raw_key, raw_value in params.items()
    }

    if not normalized:
        return {}

    backend = _resolve_mlflow(_mlflow)
    backend.log_params(normalized)
    return normalized
