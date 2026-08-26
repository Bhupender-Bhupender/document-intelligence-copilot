from __future__ import annotations

from app.serving_service import (
    ServingServiceError,
)
from app.ui import (
    _format_serving_metadata,
    _handle_served_answer,
)
from src.schema.serving_models import (
    ServingAnswerResponse,
)


def _response():
    return ServingAnswerResponse(
        run_id="run-test",
        retrieval_query_id="query-test",
        query="question",
        answer_text="Grounded answer.",
        model_used="test-model",
        generation_backend="ollama",
        sources=[],
        evidence=[],
        retrieval_latency_ms=12.0,
        generation_latency_ms=25.0,
        total_latency_ms=37.0,
        retrieval_config_version=(
            "retrieval_service_v1"
        ),
        generation_contract_version=(
            "generation_service_v1"
        ),
        applied_filters=[
            "document_ids"
        ],
    )


def test_served_answer_builds_request():
    captured = []

    def fake(request):
        captured.append(request)
        return _response()

    answer, citations, metadata = (
        _handle_served_answer(
            "question",
            10,
            3,
            "",
            _answer_service=fake,
        )
    )

    assert answer == "Grounded answer."
    assert citations == "(No citations)"
    assert "Backend: ollama" in metadata

    assert len(captured) == 1

    request = captured[0]

    assert request.query == "question"
    assert request.top_k == 10
    assert request.final_k == 3
    assert (
        request.include_parent_context
        is True
    )
    assert request.model is None


def test_explicit_model_forwarded():
    captured = []

    def fake(request):
        captured.append(request)
        return _response()

    _handle_served_answer(
        "question",
        10,
        3,
        "custom-model",
        _answer_service=fake,
    )

    assert captured[0].model == (
        "custom-model"
    )


def test_metadata_contains_latencies():
    result = _format_serving_metadata(
        _response()
    )

    assert "12.00 ms" in result
    assert "25.00 ms" in result
    assert "37.00 ms" in result


def test_metadata_contains_filters():
    result = _format_serving_metadata(
        _response()
    )

    assert "document_ids" in result


def test_service_failure_is_clean():
    def failing(request):
        raise ServingServiceError(
            "Answer service unavailable."
        )

    answer, citations, metadata = (
        _handle_served_answer(
            "question",
            10,
            3,
            "",
            _answer_service=failing,
        )
    )

    assert answer == (
        "Query failed: "
        "Answer service unavailable."
    )

    assert citations == ""
    assert metadata == ""


def test_unexpected_failure_does_not_leak_details():
    def failing(request):
        raise RuntimeError(
            "private provider information"
        )

    answer, citations, metadata = (
        _handle_served_answer(
            "question",
            10,
            3,
            "",
            _answer_service=failing,
        )
    )

    assert answer == (
        "Unexpected error while "
        "processing the request."
    )

    assert (
        "private provider information"
        not in answer
    )

    assert citations == ""
    assert metadata == ""
