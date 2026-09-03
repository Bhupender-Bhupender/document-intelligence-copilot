from __future__ import annotations

from contextlib import contextmanager

import src.generation.evidence_answer_engine as generation
import src.retrieval.retrieval_service as retrieval

from src.core.config import config
from src.schema.generation_service_models import (
    GenerationRequest,
)
from src.schema.retrieval_service_models import (
    RetrievalRequest,
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


def _span_recorder(spans, events):
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

    return fake_start_safe_span


def _assert_private_content_absent(
    spans,
) -> None:
    combined = repr(
        [
            span.attributes
            for span in spans
        ]
    )

    forbidden_values = (
        "PRIVATE_QUERY_VALUE",
        "PRIVATE_ANSWER_VALUE",
        "PRIVATE_DOCUMENT_TEXT",
        "PRIVATE_PROMPT_VALUE",
    )

    for value in forbidden_values:
        assert value not in combined

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


def test_retrieval_emits_real_evidence_build_span(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "llmops_tracing_enabled",
        True,
    )

    spans = []
    events = []

    monkeypatch.setattr(
        retrieval,
        "start_safe_span",
        _span_recorder(
            spans,
            events,
        ),
    )

    def fake_retrieve(
        query,
        top_k,
        filters,
    ):
        assert query == (
            "PRIVATE_QUERY_VALUE"
        )
        return []

    times = iter(
        [
            100.0,
            100.010,
        ]
    )

    response = retrieval.run_retrieval_service(
        RetrievalRequest(
            query="PRIVATE_QUERY_VALUE"
        ),
        _retrieve=fake_retrieve,
        _clock=lambda: next(times),
    )

    assert response.results == []

    assert [
        span.name
        for span in spans
    ] == [
        "evidence_build",
    ]

    assert [
        span.span_type
        for span in spans
    ] == [
        "CHAIN",
    ]

    assert events == [
        (
            "enter",
            "evidence_build",
            "CHAIN",
        ),
        (
            "exit",
            "evidence_build",
            "CHAIN",
        ),
    ]

    assert spans[0].attributes[
        "selected_count"
    ] == 0

    assert spans[0].attributes[
        "evidence_count"
    ] == 0

    assert spans[0].attributes[
        "citation_count"
    ] == 0

    _assert_private_content_absent(
        spans
    )


def test_generation_emits_prompt_and_llm_spans(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "llmops_tracing_enabled",
        True,
    )

    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    spans = []
    events = []

    monkeypatch.setattr(
        generation,
        "start_safe_span",
        _span_recorder(
            spans,
            events,
        ),
    )

    def fake_generator(messages):
        assert messages
        return "PRIVATE_ANSWER_VALUE"

    times = iter(
        [
            200.0,
            200.020,
        ]
    )

    response = generation.generate_from_evidence(
        GenerationRequest(
            query="PRIVATE_QUERY_VALUE",
            evidence=[],
            model="test-model",
        ),
        _generator=fake_generator,
        _clock=lambda: next(times),
    )

    assert response.answer_text == (
        "PRIVATE_ANSWER_VALUE"
    )

    assert [
        span.name
        for span in spans
    ] == [
        "prompt_build",
        "llm_call",
    ]

    assert [
        span.span_type
        for span in spans
    ] == [
        "CHAIN",
        "LLM",
    ]

    assert events == [
        (
            "enter",
            "prompt_build",
            "CHAIN",
        ),
        (
            "exit",
            "prompt_build",
            "CHAIN",
        ),
        (
            "enter",
            "llm_call",
            "LLM",
        ),
        (
            "exit",
            "llm_call",
            "LLM",
        ),
    ]

    prompt_span = spans[0]
    llm_span = spans[1]

    assert prompt_span.attributes[
        "evidence_count"
    ] == 0

    assert prompt_span.attributes[
        "message_count"
    ] > 0

    assert llm_span.attributes[
        "model"
    ] == "test-model"

    assert llm_span.attributes[
        "generation_backend"
    ] == "ollama"

    assert llm_span.attributes[
        "latency_ms"
    ] >= 0.0

    _assert_private_content_absent(
        spans
    )
