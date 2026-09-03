from __future__ import annotations

import json

from types import SimpleNamespace

import pytest

import src.generation.databricks_llm as provider
import src.generation.evidence_answer_engine as generation
import src.retrieval.retrieval_service as retrieval

from src.generation.databricks_llm import (
    DatabricksGenerationError,
    DatabricksGenerationResult,
)


def _clock(
    *values,
):
    iterator = iter(
        values
    )

    return lambda: next(
        iterator
    )


# ============================================================
# Retrieval
# ============================================================

def test_retrieval_component_success_event(
    monkeypatch,
):
    events = []


    response = SimpleNamespace(
        results=[
            SimpleNamespace(
                parent_text="parent",
            ),
            SimpleNamespace(
                parent_text=None,
            ),
        ],
        latency_ms=12.5,
        retrieval_config_version=(
            "retrieval-v1"
        ),
    )


    monkeypatch.setattr(
        retrieval,
        "_run_retrieval_service_core",
        lambda request, **kwargs:
            response,
    )


    actual = (
        retrieval.run_retrieval_service(
            SimpleNamespace(
                query="PRIVATE_QUERY"
            ),
            _event_emitter=events.append,
        )
    )


    assert actual is response
    assert len(events) == 1


    event = events[0]

    assert (
        event.event_name
        == "retrieval.request.completed"
    )

    assert event.component == "retrieval"

    assert (
        event.operation
        == "run_retrieval_service"
    )

    assert event.status == "success"

    assert event.result_count == 2

    assert (
        event.parent_context_count
        == 1
    )

    assert event.latency_ms == 12.5

    assert (
        event.retrieval_config_version
        == "retrieval-v1"
    )


    serialized = json.dumps(
        event.to_record()
    )

    assert (
        "PRIVATE_QUERY"
        not in serialized
    )


def test_retrieval_component_failure_event(
    monkeypatch,
):
    events = []


    def fail(
        request,
        **kwargs,
    ):
        raise RuntimeError(
            "PRIVATE_FAILURE_MESSAGE"
        )


    monkeypatch.setattr(
        retrieval,
        "_run_retrieval_service_core",
        fail,
    )


    with pytest.raises(
        RuntimeError
    ):
        retrieval.run_retrieval_service(
            SimpleNamespace(
                query="PRIVATE_QUERY"
            ),
            _event_emitter=events.append,
            _event_clock=_clock(
                1.0,
                1.2,
            ),
        )


    assert len(events) == 1

    event = events[0]

    assert (
        event.event_name
        == "retrieval.request.failed"
    )

    assert event.status == "error"

    assert (
        event.error_type
        == "RuntimeError"
    )

    assert event.latency_ms == pytest.approx(
        200.0
    )


    serialized = json.dumps(
        event.to_record()
    )

    assert (
        "PRIVATE_FAILURE_MESSAGE"
        not in serialized
    )

    assert (
        "PRIVATE_QUERY"
        not in serialized
    )


# ============================================================
# Generation engine
# ============================================================

def test_generation_component_success_event(
    monkeypatch,
):
    events = []


    response = SimpleNamespace(
        evidence=[
            SimpleNamespace(
                text="PRIVATE_EVIDENCE"
            ),
            SimpleNamespace(
                text="PRIVATE_EVIDENCE_2"
            ),
        ],
        sources=[
            SimpleNamespace(
                text="PRIVATE_CITATION"
            )
        ],
        model_used="generation-v1",
        generation_backend="databricks",
        latency_ms=25.0,
    )


    monkeypatch.setattr(
        generation,
        "_generate_from_evidence_core",
        lambda request, **kwargs:
            response,
    )


    actual = (
        generation.generate_from_evidence(
            SimpleNamespace(
                query="PRIVATE_QUERY",
                evidence=[],
            ),
            _event_emitter=events.append,
        )
    )


    assert actual is response
    assert len(events) == 1


    event = events[0]

    assert (
        event.event_name
        == "generation.answer.completed"
    )

    assert event.component == "generation"

    assert (
        event.operation
        == "generate_from_evidence"
    )

    assert event.status == "success"

    assert event.evidence_count == 2

    assert event.citation_count == 1

    assert event.latency_ms == 25.0

    assert (
        event.generation_model
        == "generation-v1"
    )


    serialized = json.dumps(
        event.to_record()
    )


    for private_value in (
        "PRIVATE_QUERY",
        "PRIVATE_EVIDENCE",
        "PRIVATE_EVIDENCE_2",
        "PRIVATE_CITATION",
    ):
        assert (
            private_value
            not in serialized
        )


def test_generation_component_failure_event(
    monkeypatch,
):
    events = []


    def fail(
        request,
        **kwargs,
    ):
        raise generation.EvidenceGenerationError(
            "PRIVATE_GENERATION_FAILURE"
        )


    monkeypatch.setattr(
        generation,
        "_generate_from_evidence_core",
        fail,
    )


    with pytest.raises(
        generation.EvidenceGenerationError
    ):
        generation.generate_from_evidence(
            SimpleNamespace(
                query="PRIVATE_QUERY",
                evidence=[],
            ),
            _event_emitter=events.append,
            _event_clock=_clock(
                5.0,
                5.1,
            ),
        )


    assert len(events) == 1

    event = events[0]

    assert (
        event.event_name
        == "generation.answer.failed"
    )

    assert (
        event.error_type
        == "EvidenceGenerationError"
    )

    assert event.latency_ms == pytest.approx(
        100.0
    )


    serialized = json.dumps(
        event.to_record()
    )

    assert (
        "PRIVATE_GENERATION_FAILURE"
        not in serialized
    )


# ============================================================
# Databricks provider
# ============================================================

def test_provider_success_event_contains_token_counts(
    monkeypatch,
):
    events = []


    result = DatabricksGenerationResult(
        text="PRIVATE_PROVIDER_ANSWER",
        model="endpoint-v1",
        prompt_tokens=100,
        completion_tokens=20,
        total_tokens=120,
        finish_reason="stop",
    )


    monkeypatch.setattr(
        provider,
        "_generate_with_metadata_core",
        lambda messages, **kwargs:
            result,
    )


    actual = (
        provider.generate_with_metadata(
            [
                {
                    "role": "user",
                    "content":
                        "PRIVATE_PROMPT",
                }
            ],
            model="endpoint-v1",
            _event_emitter=events.append,
            _event_clock=_clock(
                10.0,
                10.25,
            ),
        )
    )


    assert actual is result

    assert len(events) == 1


    event = events[0]

    assert (
        event.event_name
        == "generation.provider.completed"
    )

    assert (
        event.operation
        == "databricks_generation"
    )

    assert event.status == "success"

    assert event.prompt_tokens == 100

    assert (
        event.completion_tokens
        == 20
    )

    assert event.total_tokens == 120

    assert (
        event.generation_model
        == "endpoint-v1"
    )

    assert event.latency_ms == pytest.approx(
        250.0
    )


    serialized = json.dumps(
        event.to_record()
    )


    assert (
        "PRIVATE_PROMPT"
        not in serialized
    )

    assert (
        "PRIVATE_PROVIDER_ANSWER"
        not in serialized
    )


def test_provider_failure_uses_safe_cause_metadata(
    monkeypatch,
):
    events = []


    class ProviderUnavailableError(
        Exception
    ):
        status_code = 503


    def fail(
        messages,
        **kwargs,
    ):
        try:
            raise ProviderUnavailableError(
                "PRIVATE_PROVIDER_MESSAGE"
            )

        except ProviderUnavailableError as exc:
            raise DatabricksGenerationError(
                "PRIVATE_WRAPPER_MESSAGE"
            ) from exc


    monkeypatch.setattr(
        provider,
        "_generate_with_metadata_core",
        fail,
    )


    with pytest.raises(
        DatabricksGenerationError
    ):
        provider.generate_with_metadata(
            [
                {
                    "role": "user",
                    "content":
                        "PRIVATE_PROMPT",
                }
            ],
            model="endpoint-v1",
            _event_emitter=events.append,
            _event_clock=_clock(
                20.0,
                20.4,
            ),
        )


    assert len(events) == 1

    event = events[0]

    assert (
        event.event_name
        == "generation.provider.failed"
    )

    assert event.status == "error"

    assert (
        event.error_type
        == "ProviderUnavailableError"
    )

    assert (
        event.http_status_code
        == 503
    )

    assert event.latency_ms == pytest.approx(
        400.0
    )


    serialized = json.dumps(
        event.to_record()
    )


    for private_value in (
        "PRIVATE_PROVIDER_MESSAGE",
        "PRIVATE_WRAPPER_MESSAGE",
        "PRIVATE_PROMPT",
    ):
        assert (
            private_value
            not in serialized
        )


def test_component_event_emitter_is_fail_open(
    monkeypatch,
):
    result = DatabricksGenerationResult(
        text="answer",
        model="endpoint-v1",
        prompt_tokens=1,
        completion_tokens=1,
        total_tokens=2,
        finish_reason="stop",
    )


    monkeypatch.setattr(
        provider,
        "_generate_with_metadata_core",
        lambda messages, **kwargs:
            result,
    )


    def broken_emitter(
        event,
    ):
        raise RuntimeError(
            "telemetry unavailable"
        )


    actual = (
        provider.generate_with_metadata(
            [
                {
                    "role": "user",
                    "content": "private",
                }
            ],
            model="endpoint-v1",
            _event_emitter=broken_emitter,
        )
    )


    assert actual is result
