from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import create_app
from app.serving_service import (
    ServingServiceError,
)
from src.schema.retrieval_service_models import (
    RETRIEVAL_CONFIG_VERSION,
    RetrievalResponse,
)
from src.schema.serving_models import (
    ServingAnswerResponse,
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


def _answer_response():
    return ServingAnswerResponse(
        run_id="run-test",
        retrieval_query_id="query-test",
        query="question",
        answer_text="Grounded answer.",
        model_used="test-model",
        generation_backend="ollama",
        sources=[],
        evidence=[],
        retrieval_latency_ms=10.0,
        generation_latency_ms=20.0,
        total_latency_ms=30.0,
        retrieval_config_version=(
            RETRIEVAL_CONFIG_VERSION
        ),
        generation_contract_version=(
            "generation_service_v1"
        ),
        applied_filters=[],
    )


def test_health():
    response = TestClient(
        create_app()
    ).get(
        "/api/v1/health"
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_retrieve_delegates():
    calls = []

    def fake(request):
        calls.append(request)
        return _retrieval_response()

    client = TestClient(
        create_app(
            _retrieval_service=fake
        )
    )

    response = client.post(
        "/api/v1/retrieve",
        json={
            "query": "question",
            "top_k": 10,
            "final_k": 3,
        },
    )

    assert response.status_code == 200
    assert len(calls) == 1
    assert calls[0].final_k == 3


def test_answer_delegates():
    calls = []

    def fake(request):
        calls.append(request)
        return _answer_response()

    client = TestClient(
        create_app(
            _answer_service=fake
        )
    )

    response = client.post(
        "/api/v1/answer",
        json={
            "query": "question",
            "top_k": 10,
            "final_k": 3,
        },
    )

    assert response.status_code == 200
    assert len(calls) == 1
    assert response.json()[
        "answer_text"
    ] == "Grounded answer."


def test_retrieve_service_error_maps_to_503():
    def failing(request):
        raise ServingServiceError(
            "Retrieval service unavailable."
        )

    response = TestClient(
        create_app(
            _retrieval_service=failing
        )
    ).post(
        "/api/v1/retrieve",
        json={
            "query": "question"
        },
    )

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "Retrieval service unavailable."
    )


def test_answer_service_error_maps_to_503():
    def failing(request):
        raise ServingServiceError(
            "Answer service unavailable."
        )

    response = TestClient(
        create_app(
            _answer_service=failing
        )
    ).post(
        "/api/v1/answer",
        json={
            "query": "question"
        },
    )

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "Answer service unavailable."
    )


def test_blank_query_is_422():
    response = TestClient(
        create_app()
    ).post(
        "/api/v1/answer",
        json={
            "query": "   "
        },
    )

    assert response.status_code == 422


def test_blank_model_is_422():
    response = TestClient(
        create_app()
    ).post(
        "/api/v1/answer",
        json={
            "query": "question",
            "model": "   ",
        },
    )

    assert response.status_code == 422



def test_ready_returns_200_when_ready():
    from src.schema.serving_models import (
        ReadinessResponse,
    )

    def ready_service():
        return ReadinessResponse(
            status="ready",
            runtime_mode="local",
            search_backend="local",
            generation_backend="ollama",
            checks={
                "configuration": True,
            },
        )

    response = TestClient(
        create_app(
            _readiness_service=ready_service
        )
    ).get(
        "/api/v1/ready"
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_ready_returns_503_when_not_ready():
    from src.schema.serving_models import (
        ReadinessResponse,
    )

    def not_ready_service():
        return ReadinessResponse(
            status="not_ready",
            runtime_mode="databricks",
            search_backend="databricks",
            generation_backend="databricks",
            checks={
                "configuration": False,
            },
        )

    response = TestClient(
        create_app(
            _readiness_service=not_ready_service
        )
    ).get(
        "/api/v1/ready"
    )

    assert response.status_code == 503

    assert response.json()["status"] == (
        "not_ready"
    )
