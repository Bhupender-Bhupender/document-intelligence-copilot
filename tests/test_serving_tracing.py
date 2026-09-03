from __future__ import annotations

from contextlib import contextmanager

import app.serving_service as service

from src.core.config import config
from src.schema.generation_service_models import (
    GenerationResponse,
)
from src.schema.retrieval_service_models import (
    RETRIEVAL_CONFIG_VERSION,
    RetrievalResponse,
)
from src.schema.serving_models import (
    ServingAnswerRequest,
)


class FakeSpan:
    def __init__(
        self,
        name: str,
        span_type: str,
        attributes: dict | None = None,
    ) -> None:
        self.name = name
        self.span_type = span_type
        self.attributes = dict(
            attributes or {}
        )

    def set_attributes(
        self,
        attributes,
    ) -> None:
        self.attributes.update(
            attributes
        )


def _retrieval_response() -> RetrievalResponse:
    return RetrievalResponse(
        query_id="trace-query-test",
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
) -> GenerationResponse:
    return GenerationResponse(
        run_id="trace-run-test",
        query=query,
        answer_text="PRIVATE_ANSWER_VALUE",
        model_used=model,
        generation_backend="ollama",
        sources=[],
        evidence=[],
        latency_ms=20.0,
    )


def test_tracing_disabled_emits_no_spans_and_preserves_behavior(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    emitted = []
    runner_events = []

    @contextmanager
    def fake_start_safe_span(
        *,
        name,
        span_type,
        attributes=None,
        enabled=True,
        **kwargs,
    ):
        if not enabled:
            yield None
            return

        span = FakeSpan(
            name,
            span_type,
            attributes,
        )
        emitted.append(span)
        yield span

    monkeypatch.setattr(
        service,
        "start_safe_span",
        fake_start_safe_span,
    )

    def fake_retrieval(request):
        runner_events.append("retrieve")
        return _retrieval_response()

    def fake_generation(request):
        runner_events.append("generate")
        return _generation_response(
            request.query,
            request.model,
        )

    times = iter(
        [100.0, 100.050]
    )

    response = service.answer_with_evidence(
        ServingAnswerRequest(
            query="PRIVATE_QUERY_VALUE",
            model="test-model",
            top_k=10,
            final_k=3,
        ),
        _retrieval_runner=fake_retrieval,
        _generation_runner=fake_generation,
        _tracing_enabled=False,
        _clock=lambda: next(times),
    )

    assert emitted == []
    assert runner_events == [
        "retrieve",
        "generate",
    ]
    assert response.answer_text == (
        "PRIVATE_ANSWER_VALUE"
    )
    assert abs(
        response.total_latency_ms
        - 50.0
    ) < 0.001


def test_enabled_tracing_emits_expected_nested_spans(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    spans = []
    events = []

    @contextmanager
    def fake_start_safe_span(
        *,
        name,
        span_type,
        attributes=None,
        enabled=True,
        **kwargs,
    ):
        if not enabled:
            yield None
            return

        span = FakeSpan(
            name,
            span_type,
            attributes,
        )

        spans.append(span)
        events.append(
            ("enter", name, span_type)
        )

        try:
            yield span
        finally:
            events.append(
                ("exit", name, span_type)
            )

    monkeypatch.setattr(
        service,
        "start_safe_span",
        fake_start_safe_span,
    )

    def fake_retrieval(request):
        return _retrieval_response()

    def fake_generation(request):
        return _generation_response(
            request.query,
            request.model,
        )

    times = iter(
        [100.0, 100.050]
    )

    response = service.answer_with_evidence(
        ServingAnswerRequest(
            query="PRIVATE_QUERY_VALUE",
            model="test-model",
            top_k=10,
            final_k=3,
        ),
        _retrieval_runner=fake_retrieval,
        _generation_runner=fake_generation,
        _tracing_enabled=True,
        _clock=lambda: next(times),
    )

    assert response.answer_text == (
        "PRIVATE_ANSWER_VALUE"
    )

    assert [
        span.name
        for span in spans
    ] == [
        "rag_request",
        "retrieval",
        "generation",
    ]

    assert [
        span.span_type
        for span in spans
    ] == [
        "CHAIN",
        "RETRIEVER",
        "CHAIN",
    ]

    assert events == [
        ("enter", "rag_request", "CHAIN"),
        ("enter", "retrieval", "RETRIEVER"),
        ("exit", "retrieval", "RETRIEVER"),
        ("enter", "generation", "CHAIN"),
        ("exit", "generation", "CHAIN"),
        ("exit", "rag_request", "CHAIN"),
    ]


def test_enabled_tracing_contains_metadata_only(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    spans = []

    @contextmanager
    def fake_start_safe_span(
        *,
        name,
        span_type,
        attributes=None,
        enabled=True,
        **kwargs,
    ):
        if not enabled:
            yield None
            return

        span = FakeSpan(
            name,
            span_type,
            attributes,
        )
        spans.append(span)
        yield span

    monkeypatch.setattr(
        service,
        "start_safe_span",
        fake_start_safe_span,
    )

    def fake_retrieval(request):
        return _retrieval_response()

    def fake_generation(request):
        return _generation_response(
            request.query,
            request.model,
        )

    times = iter(
        [100.0, 100.050]
    )

    service.answer_with_evidence(
        ServingAnswerRequest(
            query="PRIVATE_QUERY_VALUE",
            model="test-model",
        ),
        _retrieval_runner=fake_retrieval,
        _generation_runner=fake_generation,
        _tracing_enabled=True,
        _clock=lambda: next(times),
    )

    combined = repr(
        [
            span.attributes
            for span in spans
        ]
    )

    assert "PRIVATE_QUERY_VALUE" not in combined
    assert "PRIVATE_ANSWER_VALUE" not in combined

    forbidden_keys = (
        "query",
        "question",
        "prompt",
        "answer",
        "response_text",
        "document_text",
        "evidence_text",
        "api_key",
        "password",
        "secret",
        "authorization",
    )

    for span in spans:
        for key in span.attributes:
            lowered = str(key).lower()

            assert not any(
                fragment in lowered
                for fragment
                in forbidden_keys
            )
