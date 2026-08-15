from __future__ import annotations

from src.core.config import config


def get_databricks_embedding_model() -> str:
    return config.databricks_embedding_model