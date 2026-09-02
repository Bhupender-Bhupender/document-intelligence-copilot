from __future__ import annotations

from contextlib import contextmanager

import pytest

from src.llmops.tracing import (
    GENERATION_SPAN,
    GENERATION_SPAN_TYPE,
    RETRIEVAL_SPAN,
    RETRIEVAL_SPAN_TYPE,
    sanitise_trace_attributes,
    set_safe_span_attributes,
    start_safe_span,
)


class FakeSpan:
    def __init__(self) -> None:
        self.attributes = {}

    def set_attributes(
        self,
        attributes,
    ) -> None:
        self.attributes.update(
            attributes
        )


class FakeMLflow:
    def __init__(self) -> None:
        self.calls = []
        self.span = FakeSpan()

    @contextmanager
    def start_span(
        self,
        *,
        name,
        span_type,
        attributes,
    ):
        self.calls.append(
            {
                'name': name,
                'span_type': span_type,
                'attributes': attributes,
            }
        )

        yield self.span


def test_trace_contract_names() -> None:
    assert (
        RETRIEVAL_SPAN
        == 'retrieval'
    )

    assert (
        RETRIEVAL_SPAN_TYPE
        == 'RETRIEVER'
    )

    assert (
        GENERATION_SPAN
        == 'generation'
    )

    assert (
        GENERATION_SPAN_TYPE
        == 'CHAIN'
    )


def test_sanitise_trace_attributes_accepts_safe_metadata():
    result = sanitise_trace_attributes(
        {
            'evidence_count': 5,
            'latency_ms': 123.4,
            'provider': 'databricks',
            'enabled': True,
        }
    )

    assert result == {
        'evidence_count': 5,
        'latency_ms': 123.4,
        'provider': 'databricks',
        'enabled': True,
    }


@pytest.mark.parametrize(
    'key',
    [
        'query',
        'query_text',
        'question',
        'prompt',
        'prompt_text',
        'answer',
        'answer_text',
        'response_text',
        'document_text',
        'evidence_text',
        'token',
        'api_key',
        'password',
        'secret',
        'authorization',
    ],
)
def test_sensitive_trace_keys_are_rejected(
    key,
) -> None:
    with pytest.raises(
        ValueError
    ):
        sanitise_trace_attributes(
            {
                key: 'sensitive'
            }
        )


def test_complex_attribute_values_are_rejected():
    with pytest.raises(
        TypeError
    ):
        sanitise_trace_attributes(
            {
                'metadata': {
                    'unsafe':
                        'nested object'
                }
            }
        )


def test_start_safe_span_passes_safe_metadata():
    fake = FakeMLflow()

    with start_safe_span(
        name='retrieval',
        span_type='RETRIEVER',
        attributes={
            'top_k': 10,
        },
        _mlflow=fake,
    ) as span:
        assert span is fake.span

    assert fake.calls == [
        {
            'name': 'retrieval',
            'span_type': 'RETRIEVER',
            'attributes': {
                'top_k': 10,
            },
        }
    ]


def test_disabled_span_does_not_call_mlflow():
    fake = FakeMLflow()

    with start_safe_span(
        name='retrieval',
        span_type='RETRIEVER',
        enabled=False,
        _mlflow=fake,
    ) as span:
        assert span is None

    assert fake.calls == []


def test_set_safe_span_attributes_updates_live_span():
    span = FakeSpan()

    result = (
        set_safe_span_attributes(
            span,
            {
                'evidence_count': 4,
                'latency_ms': 10.0,
            },
        )
    )

    assert result == {
        'evidence_count': 4,
        'latency_ms': 10.0,
    }

    assert span.attributes == result


def test_set_safe_span_attributes_supports_disabled_span():
    result = (
        set_safe_span_attributes(
            None,
            {
                'evidence_count': 4,
            },
        )
    )

    assert result == {
        'evidence_count': 4,
    }


def test_start_safe_span_propagates_application_error():
    fake = FakeMLflow()

    with pytest.raises(
        RuntimeError,
        match='application failure',
    ):
        with start_safe_span(
            name='generation',
            span_type='LLM',
            _mlflow=fake,
        ):
            raise RuntimeError(
                'application failure'
            )
