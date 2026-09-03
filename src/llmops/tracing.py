from __future__ import annotations

from contextlib import contextmanager
from importlib import import_module
from typing import Any, Iterator, Mapping


RAG_REQUEST_SPAN = 'rag_request'
RETRIEVAL_SPAN = 'retrieval'
EVIDENCE_BUILD_SPAN = 'evidence_build'
GENERATION_SPAN = 'generation'
PROMPT_BUILD_SPAN = 'prompt_build'
LLM_CALL_SPAN = 'llm_call'
CITATION_VALIDATION_SPAN = 'citation_validation'


RAG_REQUEST_SPAN_TYPE = 'CHAIN'
RETRIEVAL_SPAN_TYPE = 'RETRIEVER'
EVIDENCE_BUILD_SPAN_TYPE = 'CHAIN'
GENERATION_SPAN_TYPE = 'CHAIN'
PROMPT_BUILD_SPAN_TYPE = 'CHAIN'
LLM_CALL_SPAN_TYPE = 'LLM'
CITATION_VALIDATION_SPAN_TYPE = 'CHAIN'


_BLOCKED_KEY_FRAGMENTS = (
    'api_key',
    'apikey',
    'authorization',
    'credential',
    'document_text',
    'evidence_text',
    'password',
    'prompt',
    'query',
    'question',
    'answer',
    'response_text',
    'secret',
    'token',
)


_ALLOWED_VALUE_TYPES = (
    str,
    int,
    float,
    bool,
    type(None),
)


def _load_mlflow() -> Any:
    return import_module(
        'mlflow'
    )


def _normalise_key(
    key: object,
) -> str:
    value = str(
        key
    ).strip()

    if not value:
        raise ValueError(
            'Trace attribute key cannot be empty.'
        )

    lowered = value.lower()

    for fragment in (
        _BLOCKED_KEY_FRAGMENTS
    ):
        if fragment in lowered:
            raise ValueError(
                'Unsafe trace attribute key: '
                + value
            )

    return value


def _normalise_value(
    value: object,
) -> str | int | float | bool | None:
    if not isinstance(
        value,
        _ALLOWED_VALUE_TYPES,
    ):
        raise TypeError(
            'Trace attributes must use '
            'scalar metadata values.'
        )

    return value


def sanitise_trace_attributes(
    attributes: (
        Mapping[str, object]
        | None
    ),
) -> dict[
    str,
    str | int | float | bool | None,
]:
    if not attributes:
        return {}

    safe: dict[
        str,
        str | int | float | bool | None,
    ] = {}

    for key, value in attributes.items():
        safe[
            _normalise_key(
                key
            )
        ] = _normalise_value(
            value
        )

    return safe


@contextmanager
def start_safe_span(
    *,
    name: str,
    span_type: str,
    attributes: (
        Mapping[str, object]
        | None
    ) = None,
    enabled: bool = True,
    _mlflow: Any | None = None,
) -> Iterator[Any | None]:
    if not enabled:
        yield None
        return

    safe_attributes = (
        sanitise_trace_attributes(
            attributes
        )
    )

    mlflow = (
        _mlflow
        if _mlflow is not None
        else _load_mlflow()
    )

    with mlflow.start_span(
        name=name,
        span_type=span_type,
        attributes=safe_attributes,
    ) as span:
        yield span


def set_safe_span_attributes(
    span: Any | None,
    attributes: Mapping[
        str,
        object,
    ],
) -> dict[
    str,
    str | int | float | bool | None,
]:
    safe_attributes = (
        sanitise_trace_attributes(
            attributes
        )
    )

    if span is not None:
        span.set_attributes(
            safe_attributes
        )

    return safe_attributes
