from __future__ import annotations

import json

from types import SimpleNamespace

import pytest

import app.serving_service as module

from src.observability.emitter import (
    emit_operational_event,
)
from src.observability.events import (
    OperationalEvent,
)
from src.schema.retrieval_service_models import (
    RetrievalRequest,
    RetrievalResponse,
)
from src.schema.serving_models import (
    ServingAnswerRequest,
    ServingAnswerResponse,
)


def _answer_request():
    return (
        ServingAnswerRequest
        .model_construct(
            query="PRIVATE_QUERY_SENTINEL",
            model=None,
        )
    )


def _answer_response():
    return (
        ServingAnswerResponse
        .model_construct(
            run_id="run-safe",
            retrieval_query_id="query-safe",
            query="PRIVATE_QUERY_SENTINEL",
            answer_text=(
                "PRIVATE_ANSWER_SENTINEL"
            ),
            model_used="endpoint-v1",
            generation_backend="databricks",
            sources=[
                SimpleNamespace(
                    text=(
                        "PRIVATE_CITATION_SENTINEL"
                    )
                )
            ],
            evidence=[
                SimpleNamespace(
                    text=(
                        "PRIVATE_EVIDENCE_SENTINEL"
                    )
                ),
                SimpleNamespace(
                    text=(
                        "PRIVATE_EVIDENCE_2_SENTINEL"
                    )
                ),
            ],
            retrieval_latency_ms=10.0,
            generation_latency_ms=20.0,
            total_latency_ms=30.0,
            index_version="index-v1",
            retrieval_config_version=(
                "retrieval-v1"
            ),
            generation_contract_version=(
                "generation-v1"
            ),
            applied_filters={},
        )
    )


def test_answer_success_emits_one_safe_operational_event(
    monkeypatch,
):
    response = (
        _answer_response()
    )

    events = []


    def fake_core(
        request,
        **kwargs,
    ):
        return response


    monkeypatch.setattr(
        module,
        "_answer_with_evidence_core",
        fake_core,
    )


    actual = (
        module.answer_with_evidence(
            _answer_request(),
            _tracing_enabled=False,
            _event_emitter=events.append,
        )
    )


    assert actual is response

    assert len(events) == 1


    event = events[0]

    assert isinstance(
        event,
        OperationalEvent,
    )

    assert (
        event.event_name
        == "serving.answer.completed"
    )

    assert event.component == "serving"

    assert (
        event.operation
        == "answer_with_evidence"
    )

    assert event.status == "success"

    assert event.latency_ms == 30.0

    assert event.evidence_count == 2

    assert event.citation_count == 1

    assert (
        event.generation_model
        == "endpoint-v1"
    )

    assert (
        event.retrieval_config_version
        == "retrieval-v1"
    )


def test_answer_event_excludes_raw_rag_content(
    monkeypatch,
):
    response = (
        _answer_response()
    )

    events = []


    monkeypatch.setattr(
        module,
        "_answer_with_evidence_core",
        lambda request, **kwargs:
            response,
    )


    module.answer_with_evidence(
        _answer_request(),
        _tracing_enabled=False,
        _event_emitter=events.append,
    )


    serialized = json.dumps(
        events[0].to_record()
    )


    forbidden_values = (
        "PRIVATE_QUERY_SENTINEL",
        "PRIVATE_ANSWER_SENTINEL",
        "PRIVATE_EVIDENCE_SENTINEL",
        "PRIVATE_EVIDENCE_2_SENTINEL",
        "PRIVATE_CITATION_SENTINEL",
    )


    for value in forbidden_values:
        assert value not in serialized


def test_answer_failure_emits_safe_error_event(
    monkeypatch,
):
    events = []


    def failing_core(
        request,
        **kwargs,
    ):
        raise module.ServingServiceError(
            "safe boundary failure"
        )


    monkeypatch.setattr(
        module,
        "_answer_with_evidence_core",
        failing_core,
    )


    clock_values = iter(
        [
            10.0,
            10.25,
        ]
    )


    with pytest.raises(
        module.ServingServiceError
    ):
        module.answer_with_evidence(
            _answer_request(),
            _tracing_enabled=False,
            _event_emitter=events.append,
            _event_clock=lambda:
                next(
                    clock_values
                ),
        )


    assert len(events) == 1

    event = events[0]

    assert (
        event.event_name
        == "serving.answer.failed"
    )

    assert event.status == "error"

    assert (
        event.error_type
        == "ServingServiceError"
    )

    assert event.latency_ms == 250.0


def test_operational_emitter_failure_does_not_break_answer(
    monkeypatch,
):
    response = (
        _answer_response()
    )


    monkeypatch.setattr(
        module,
        "_answer_with_evidence_core",
        lambda request, **kwargs:
            response,
    )


    def broken_emitter(
        event,
    ):
        raise RuntimeError(
            "telemetry unavailable"
        )


    actual = (
        module.answer_with_evidence(
            _answer_request(),
            _tracing_enabled=False,
            _event_emitter=broken_emitter,
        )
    )


    assert actual is response


def test_retrieve_success_emits_one_safe_event():
    events = []


    request = (
        RetrievalRequest
        .model_construct(
            query=(
                "PRIVATE_RETRIEVAL_SENTINEL"
            )
        )
    )


    response = (
        RetrievalResponse
        .model_construct(
            query_id="query-safe",
            results=[
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
            ],
            latency_ms=12.5,
            index_version="index-v1",
            retrieval_config_version=(
                "retrieval-v1"
            ),
            applied_filters={},
        )
    )


    actual = (
        module.retrieve_evidence(
            request,
            _retrieval_runner=lambda _: (
                response
            ),
            _event_emitter=events.append,
        )
    )


    assert actual is response

    assert len(events) == 1


    event = events[0]

    assert (
        event.event_name
        == "serving.retrieve.completed"
    )

    assert event.status == "success"

    assert event.result_count == 3

    assert event.latency_ms == 12.5

    assert (
        event.retrieval_config_version
        == "retrieval-v1"
    )


    assert (
        "PRIVATE_RETRIEVAL_SENTINEL"
        not in json.dumps(
            event.to_record()
        )
    )


def test_retrieve_failure_emits_wrapped_boundary_error():
    events = []


    request = (
        RetrievalRequest
        .model_construct(
            query="PRIVATE_QUERY"
        )
    )


    def failing_runner(
        request,
    ):
        raise RuntimeError(
            "backend failure"
        )


    clock_values = iter(
        [
            2.0,
            2.2,
        ]
    )


    with pytest.raises(
        module.ServingServiceError
    ):
        module.retrieve_evidence(
            request,
            _retrieval_runner=(
                failing_runner
            ),
            _event_emitter=(
                events.append
            ),
            _event_clock=lambda:
                next(
                    clock_values
                ),
        )


    assert len(events) == 1

    event = events[0]

    assert (
        event.event_name
        == "serving.retrieve.failed"
    )

    assert event.status == "error"

    assert (
        event.error_type
        == "ServingServiceError"
    )

    assert pytest.approx(
        event.latency_ms
    ) == 200.0


def test_structured_log_sink_emits_only_event_record():
    captured = {}


    class FakeLogger:

        def info(
            self,
            *args,
            **kwargs,
        ):
            captured[
                "args"
            ] = args

            captured[
                "kwargs"
            ] = kwargs


    event = OperationalEvent(
        event_name=(
            "serving.answer.completed"
        ),
        component="serving",
        operation=(
            "answer_with_evidence"
        ),
        status="success",
        runtime_mode="databricks",
        backend="databricks",
        latency_ms=25.0,
        evidence_count=2,
        citation_count=2,
    )


    emit_operational_event(
        event,
        _logger=FakeLogger(),
    )


    assert captured[
        "args"
    ] == (
        "operational_event",
    )

    assert (
        captured[
            "kwargs"
        ]
        == event.to_record()
    )
