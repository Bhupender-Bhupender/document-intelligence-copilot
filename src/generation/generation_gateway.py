"""
Generation backend gateway.

Keeps answer-generation orchestration independent from the model provider.

Current:
- ollama      -> local Qwen through Ollama

Future:
- databricks  -> Databricks managed/model-serving endpoint
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from src.core.config import config


class GenerationBackendError(RuntimeError):
    """Raised when the configured generation backend cannot be used."""


def generate(
    messages: List[Dict[str, str]],
    model: Optional[str] = None,
    *,
    _ollama_generate: Optional[Callable[..., str]] = None,
    _databricks_generate: Optional[Callable[..., str]] = None,
) -> str:
    """Route generation to the configured backend."""

    if config.generation_backend == "ollama":
        generator = _ollama_generate

        if generator is None:
            from src.generation.ollama_llm import generate as ollama_generate

            generator = ollama_generate

        return generator(messages, model)

    if config.generation_backend == "databricks":
        if _databricks_generate is None:
            raise GenerationBackendError(
                "Databricks generation backend is not configured yet."
            )

        return _databricks_generate(messages, model)

    raise GenerationBackendError(
        f"Unsupported generation backend: {config.generation_backend}"
    )
