from __future__ import annotations

from databricks.src.embedding_config import (
    get_databricks_embedding_model,
)


def test_databricks_embedding_model():
    """Validates that the Databricks configuration layer exposes the correct serving identifier."""
    assert (
        get_databricks_embedding_model()
        == "databricks-qwen3-embedding-0-6b"
    )