from __future__ import annotations

import json

from datetime import datetime, timezone

import pytest

from src.observability.events import (
    FORBIDDEN_CONTENT_FIELDS,
    OPERATIONAL_EVENT_SCHEMA_VERSION,
    SAFE_OPERATIONAL_EVENT_FIELDS,
    OperationalEvent,
)


def _valid_event(
    **overrides,
) -> OperationalEvent:
    values = {
        "event_name":
            "serving.answer.completed",

        "component":
            "serving",

        "operation":
            "answer_with_evidence",

        "status":
            "success",

        "occurred_at_utc":
            datetime(
                2026,
                9,
                3,
                12,
                0,
                0,
                tzinfo=timezone.utc,
            ),

        "runtime_mode":
            "databricks",

        "backend":
            "databricks",

        "latency_ms":
            125.5,

        "result_count":
            5,

        "evidence_count":
            5,

        "citation_count":
            5,

        "parent_context_count":
            3,

        "prompt_tokens":
            100,

        "completion_tokens":
            20,

        "total_tokens":
            120,

        "retry_count":
            0,

        "http_status_code":
            200,

        "generation_model":
            "generation-endpoint",

        "retrieval_config_version":
            "retrieval-v1",

        "prompt_contract_version":
            "prompt-v1",

        "chunking_contract_version":
            "chunking-v1",

        "code_revision":
            "abc123",
    }

    values.update(
        overrides
    )

    return OperationalEvent(
        **values
    )


def test_operational_event_serializes_fixed_safe_schema():
    event = _valid_event()

    record = event.to_record()

    assert set(
        record
    ) == SAFE_OPERATIONAL_EVENT_FIELDS

    assert (
        record[
            "event_schema_version"
        ]
        == OPERATIONAL_EVENT_SCHEMA_VERSION
    )

    assert (
        record[
            "occurred_at_utc"
        ]
        == "2026-09-03T12:00:00Z"
    )


def test_safe_schema_contains_no_forbidden_content_fields():
    assert (
        SAFE_OPERATIONAL_EVENT_FIELDS
        .isdisjoint(
            FORBIDDEN_CONTENT_FIELDS
        )
    )


def test_operational_event_is_json_serializable():
    payload = json.dumps(
        _valid_event().to_record()
    )

    assert payload


@pytest.mark.parametrize(
    "field_name",
    [
        "result_count",
        "evidence_count",
        "citation_count",
        "parent_context_count",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "retry_count",
    ],
)
def test_negative_counts_are_rejected(
    field_name,
):
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            **{
                field_name: -1,
            }
        )


def test_negative_latency_is_rejected():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            latency_ms=-0.01
        )


def test_naive_timestamp_is_rejected():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            occurred_at_utc=datetime(
                2026,
                9,
                3,
                12,
                0,
                0,
            )
        )


def test_invalid_component_is_rejected():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            component="unknown"
        )


def test_invalid_status_is_rejected():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            status="maybe"
        )


def test_error_event_requires_error_type():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            status="error",
            http_status_code=500,
        )


def test_error_event_accepts_exception_class_only():
    event = _valid_event(
        status="error",
        error_type="ServingServiceError",
        http_status_code=500,
    )

    assert (
        event.to_record()[
            "error_type"
        ]
        == "ServingServiceError"
    )


def test_success_event_rejects_error_type():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            error_type="UnexpectedError"
        )


def test_success_event_rejects_http_error_status():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            http_status_code=500
        )


@pytest.mark.parametrize(
    "field_name",
    [
        "event_name",
        "operation",
        "runtime_mode",
        "backend",
        "generation_model",
        "retrieval_config_version",
        "prompt_contract_version",
        "chunking_contract_version",
        "code_revision",
    ],
)
def test_identifier_fields_reject_free_text(
    field_name,
):
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            **{
                field_name:
                    "raw user text must not be here",
            }
        )


def test_error_type_rejects_free_text_error_message():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            status="error",
            error_type=(
                "Request failed because "
                "the user's document was missing"
            ),
            http_status_code=500,
        )


def test_record_does_not_support_arbitrary_metadata():
    record = _valid_event().to_record()

    assert "metadata" not in record
    assert "attributes" not in record
    assert "extra" not in record


def test_schema_version_cannot_drift_silently():
    with pytest.raises(
        ValueError
    ):
        _valid_event(
            event_schema_version="2.0"
        )
