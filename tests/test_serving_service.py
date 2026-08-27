from __future__ import annotations

import pytest

from app.serving_service import (
    ServingServiceError,
    _resolve_generation_model,
    answer_with_evidence,
    retrieve_evidence,
)
from src.core.config import config
from src.schema.generation_service_models import (
    GenerationResponse,
)
from src.schema.retrieval_service_models import (
    RETRIEVAL_CONFIG_VERSION,
    RetrievalRequest,
    RetrievalResponse,
)
from src.schema.serving_models import (
    ServingAnswerRequest,
)


def _retrieval_response():
    return RetrievalResponse(
        query_id="query-test",
        results=[],
        latency_ms=10.0,
        index_version=None,
        retrieval_config_version=(
            RETRIEVAL_CONFIG_VERSION
        ),
        applied_filters=[],
    )


def _generation_response(
    query: str,
    model: str,
):
    return GenerationResponse(
        run_id="run-test",
        query=query,
        answer_text="Grounded answer.",
        model_used=model,
        generation_backend=(
            config.generation_backend
        ),
        sources=[],
        evidence=[],
        latency_ms=20.0,
    )


def test_retrieve_evidence_delegates():
    captured = []

    def fake(request):
        captured.append(request)
        return _retrieval_response()

    request = RetrievalRequest(
        query="question"
    )

    response = retrieve_evidence(
        request,
        _retrieval_runner=fake,
    )

    assert response.query_id == "query-test"
    assert captured == [request]


def test_retrieve_failure_is_wrapped():
    def failing(request):
        raise RuntimeError(
            "private details"
        )

    with pytest.raises(
        ServingServiceError,
        match="Retrieval service unavailable",
    ):
        retrieve_evidence(
            RetrievalRequest(
                query="question"
            ),
            _retrieval_runner=failing,
        )


def test_answer_retrieves_then_generates(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    events = []
    captured = {}

    def fake_retrieval(request):
        events.append("retrieve")
        captured[
            "retrieval"
        ] = request
        return _retrieval_response()

    def fake_generation(request):
        events.append("generate")
        captured[
            "generation"
        ] = request

        return _generation_response(
            request.query,
            request.model,
        )

    times = iter(
        [100.0, 100.050]
    )

    response = answer_with_evidence(
        ServingAnswerRequest(
            query="question",
            top_k=10,
            final_k=3,
        ),
        _retrieval_runner=(
            fake_retrieval
        ),
        _generation_runner=(
            fake_generation
        ),
        _clock=lambda: next(times),
    )

    assert events == [
        "retrieve",
        "generate",
    ]

    assert captured[
        "retrieval"
    ].top_k == 10

    assert captured[
        "retrieval"
    ].final_k == 3

    assert captured[
        "generation"
    ].evidence == []

    assert response.answer_text == (
        "Grounded answer."
    )

    assert abs(
        response.total_latency_ms
        - 50.0
    ) < 0.001


def test_local_default_model_used(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    monkeypatch.setattr(
        config,
        "generation_model",
        "local-test-model",
    )

    assert (
        _resolve_generation_model(None)
        == "local-test-model"
    )


def test_databricks_default_model_used(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "databricks_generation_model",
        "system.ai.test-model",
    )

    assert (
        _resolve_generation_model(None)
        == "system.ai.test-model"
    )


def test_databricks_never_falls_back_to_local_model(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "generation_model",
        "qwen3:8b",
    )

    monkeypatch.setattr(
        config,
        "databricks_generation_model",
        "",
    )

    with pytest.raises(
        ServingServiceError,
        match="Managed generation model",
    ):
        _resolve_generation_model(None)


def test_explicit_model_wins(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "databricks",
    )

    monkeypatch.setattr(
        config,
        "databricks_generation_model",
        "system.ai.default",
    )

    assert (
        _resolve_generation_model(
            "system.ai.override"
        )
        == "system.ai.override"
    )


def test_answer_failure_is_safe(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    def failing_retrieval(request):
        raise RuntimeError(
            "private retrieval failure"
        )

    with pytest.raises(
        ServingServiceError,
        match="Answer service unavailable",
    ):
        answer_with_evidence(
            ServingAnswerRequest(
                query="question"
            ),
            _retrieval_runner=(
                failing_retrieval
            ),
        )
