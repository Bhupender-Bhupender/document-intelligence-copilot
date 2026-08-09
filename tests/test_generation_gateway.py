import pytest

from src.core.config import config
from src.generation.generation_gateway import (
    GenerationBackendError,
    generate,
)


def test_ollama_backend_routes_to_injected_generator(monkeypatch):
    monkeypatch.setattr(config, "generation_backend", "ollama")

    def fake_generator(messages, model=None):
        return "local-result"

    result = generate(
        [{"role": "user", "content": "test"}],
        "test-model",
        _ollama_generate=fake_generator,
    )

    assert result == "local-result"


def test_databricks_backend_routes_to_injected_generator(monkeypatch):
    monkeypatch.setattr(config, "generation_backend", "databricks")

    def fake_generator(messages, model=None):
        return "databricks-result"

    result = generate(
        [{"role": "user", "content": "test"}],
        "test-model",
        _databricks_generate=fake_generator,
    )

    assert result == "databricks-result"


def test_databricks_backend_requires_adapter(monkeypatch):
    monkeypatch.setattr(config, "generation_backend", "databricks")

    with pytest.raises(GenerationBackendError):
        generate(
            [{"role": "user", "content": "test"}],
            "test-model",
        )
