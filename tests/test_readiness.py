from __future__ import annotations

from app.serving_service import (
    get_readiness,
)
from src.core.config import config


def test_local_runtime_readiness(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "runtime_mode",
        "local",
    )

    monkeypatch.setattr(
        config,
        "search_backend",
        "local",
    )

    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    monkeypatch.setattr(
        config,
        "generation_model",
        "qwen3:8b",
    )

    monkeypatch.setattr(
        config,
        "ollama_base_url",
        "http://localhost:11434",
    )

    result = get_readiness()

    assert result.status == "ready"
    assert all(result.checks.values())


def test_databricks_runtime_ready_when_configured(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "runtime_mode",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "search_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "generation_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "databricks_ai_search_endpoint_name",
        "configured",
    )

    monkeypatch.setattr(
        config,
        "databricks_ai_search_index_name",
        "configured",
    )

    monkeypatch.setattr(
        config,
        "databricks_parent_chunks_table",
        "configured",
    )

    monkeypatch.setattr(
        config,
        "databricks_sql_warehouse_id",
        "configured",
    )

    monkeypatch.setattr(
        config,
        "databricks_generation_model",
        "configured",
    )

    result = get_readiness()

    assert result.status == "ready"
    assert all(result.checks.values())


def test_databricks_runtime_fails_closed(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "runtime_mode",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "search_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "generation_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "databricks_ai_search_endpoint_name",
        "",
    )

    monkeypatch.setattr(
        config,
        "databricks_ai_search_index_name",
        "",
    )

    monkeypatch.setattr(
        config,
        "databricks_parent_chunks_table",
        "",
    )

    monkeypatch.setattr(
        config,
        "databricks_sql_warehouse_id",
        "",
    )

    monkeypatch.setattr(
        config,
        "databricks_generation_model",
        "",
    )

    result = get_readiness()

    assert result.status == (
        "not_ready"
    )

    assert not all(
        result.checks.values()
    )


def test_readiness_does_not_expose_values(
    monkeypatch,
):
    secret_like_value = (
        "do-not-expose-this-value"
    )

    monkeypatch.setattr(
        config,
        "runtime_mode",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "search_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "generation_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "databricks_ai_search_endpoint_name",
        secret_like_value,
    )

    monkeypatch.setattr(
        config,
        "databricks_ai_search_index_name",
        secret_like_value,
    )

    monkeypatch.setattr(
        config,
        "databricks_parent_chunks_table",
        secret_like_value,
    )

    monkeypatch.setattr(
        config,
        "databricks_sql_warehouse_id",
        secret_like_value,
    )

    monkeypatch.setattr(
        config,
        "databricks_generation_model",
        secret_like_value,
    )

    result = get_readiness()

    serialized = result.model_dump_json()

    assert secret_like_value not in serialized
