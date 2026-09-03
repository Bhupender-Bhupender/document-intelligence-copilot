from __future__ import annotations

import re

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


OPERATIONAL_EVENT_SCHEMA_VERSION = "1.0"


ALLOWED_COMPONENTS = frozenset(
    {
        "serving",
        "retrieval",
        "generation",
        "readiness",
        "job",
    }
)


ALLOWED_STATUSES = frozenset(
    {
        "success",
        "error",
    }
)


SAFE_OPERATIONAL_EVENT_FIELDS = frozenset(
    {
        "event_schema_version",
        "occurred_at_utc",
        "event_name",
        "component",
        "operation",
        "status",
        "runtime_mode",
        "backend",
        "latency_ms",
        "result_count",
        "evidence_count",
        "citation_count",
        "parent_context_count",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "retry_count",
        "http_status_code",
        "error_type",
        "generation_model",
        "retrieval_config_version",
        "prompt_contract_version",
        "chunking_contract_version",
        "code_revision",
    }
)


FORBIDDEN_CONTENT_FIELDS = frozenset(
    {
        "query",
        "question",
        "answer",
        "answer_text",
        "evidence",
        "evidence_text",
        "parent_text",
        "prompt",
        "messages",
        "response_text",
        "document_id",
        "document_ids",
        "file_name",
        "file_path",
        "citation",
        "citations",
        "request_payload",
        "response_payload",
        "error_message",
        "stack_trace",
        "traceback",
        "credential",
        "credentials",
        "api_key",
        "access_token",
        "bearer_token",
    }
)


_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.:/@+\-]{0,255}$"
)


def _validate_identifier(
    name: str,
    value: str | None,
    *,
    required: bool = False,
) -> None:
    if value is None:
        if required:
            raise ValueError(
                f"{name} must not be empty"
            )

        return

    if not isinstance(
        value,
        str,
    ):
        raise TypeError(
            f"{name} must be a string"
        )

    if not value:
        raise ValueError(
            f"{name} must not be empty"
        )

    if not _IDENTIFIER_PATTERN.fullmatch(
        value
    ):
        raise ValueError(
            f"{name} must be a machine-readable identifier"
        )


def _validate_non_negative_number(
    name: str,
    value: float | int | None,
) -> None:
    if value is None:
        return

    if isinstance(
        value,
        bool,
    ):
        raise TypeError(
            f"{name} must be numeric"
        )

    if not isinstance(
        value,
        (
            int,
            float,
        ),
    ):
        raise TypeError(
            f"{name} must be numeric"
        )

    if value < 0:
        raise ValueError(
            f"{name} must be non-negative"
        )


def _validate_non_negative_integer(
    name: str,
    value: int | None,
) -> None:
    if value is None:
        return

    if (
        isinstance(
            value,
            bool,
        )
        or not isinstance(
            value,
            int,
        )
    ):
        raise TypeError(
            f"{name} must be an integer"
        )

    if value < 0:
        raise ValueError(
            f"{name} must be non-negative"
        )


@dataclass(
    frozen=True,
)
class OperationalEvent:
    event_name: str

    component: str

    operation: str

    status: str

    occurred_at_utc: datetime = field(
        default_factory=lambda: datetime.now(
            timezone.utc
        )
    )

    runtime_mode: str | None = None

    backend: str | None = None

    latency_ms: float | None = None

    result_count: int | None = None

    evidence_count: int | None = None

    citation_count: int | None = None

    parent_context_count: int | None = None

    prompt_tokens: int | None = None

    completion_tokens: int | None = None

    total_tokens: int | None = None

    retry_count: int | None = None

    http_status_code: int | None = None

    error_type: str | None = None

    generation_model: str | None = None

    retrieval_config_version: str | None = None

    prompt_contract_version: str | None = None

    chunking_contract_version: str | None = None

    code_revision: str | None = None

    event_schema_version: str = (
        OPERATIONAL_EVENT_SCHEMA_VERSION
    )


    def __post_init__(
        self,
    ) -> None:
        if (
            self.event_schema_version
            != OPERATIONAL_EVENT_SCHEMA_VERSION
        ):
            raise ValueError(
                "Unsupported operational event "
                "schema version."
            )


        _validate_identifier(
            "event_name",
            self.event_name,
            required=True,
        )

        _validate_identifier(
            "operation",
            self.operation,
            required=True,
        )


        if self.component not in ALLOWED_COMPONENTS:
            raise ValueError(
                "Unsupported operational event "
                f"component: {self.component!r}"
            )


        if self.status not in ALLOWED_STATUSES:
            raise ValueError(
                "Unsupported operational event "
                f"status: {self.status!r}"
            )


        if (
            self.occurred_at_utc.tzinfo
            is None
            or self.occurred_at_utc.utcoffset()
            is None
        ):
            raise ValueError(
                "occurred_at_utc must be "
                "timezone-aware"
            )


        for name in (
            "runtime_mode",
            "backend",
            "error_type",
            "generation_model",
            "retrieval_config_version",
            "prompt_contract_version",
            "chunking_contract_version",
            "code_revision",
        ):
            _validate_identifier(
                name,
                getattr(
                    self,
                    name,
                ),
            )


        _validate_non_negative_number(
            "latency_ms",
            self.latency_ms,
        )


        for name in (
            "result_count",
            "evidence_count",
            "citation_count",
            "parent_context_count",
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "retry_count",
        ):
            _validate_non_negative_integer(
                name,
                getattr(
                    self,
                    name,
                ),
            )


        if (
            self.http_status_code
            is not None
        ):
            if (
                isinstance(
                    self.http_status_code,
                    bool,
                )
                or not isinstance(
                    self.http_status_code,
                    int,
                )
            ):
                raise TypeError(
                    "http_status_code must "
                    "be an integer"
                )

            if not (
                100
                <= self.http_status_code
                <= 599
            ):
                raise ValueError(
                    "http_status_code must be "
                    "between 100 and 599"
                )


        if (
            self.status
            == "error"
            and not self.error_type
        ):
            raise ValueError(
                "error events must provide "
                "error_type"
            )


        if (
            self.status
            == "success"
            and self.error_type is not None
        ):
            raise ValueError(
                "successful events must not "
                "provide error_type"
            )


        if (
            self.status
            == "success"
            and self.http_status_code
            is not None
            and self.http_status_code
            >= 400
        ):
            raise ValueError(
                "successful events cannot use "
                "an HTTP error status"
            )


    def to_record(
        self,
    ) -> dict[str, Any]:
        occurred_at = (
            self.occurred_at_utc
            .astimezone(
                timezone.utc
            )
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )


        record = {
            "event_schema_version":
                self.event_schema_version,

            "occurred_at_utc":
                occurred_at,

            "event_name":
                self.event_name,

            "component":
                self.component,

            "operation":
                self.operation,

            "status":
                self.status,

            "runtime_mode":
                self.runtime_mode,

            "backend":
                self.backend,

            "latency_ms":
                self.latency_ms,

            "result_count":
                self.result_count,

            "evidence_count":
                self.evidence_count,

            "citation_count":
                self.citation_count,

            "parent_context_count":
                self.parent_context_count,

            "prompt_tokens":
                self.prompt_tokens,

            "completion_tokens":
                self.completion_tokens,

            "total_tokens":
                self.total_tokens,

            "retry_count":
                self.retry_count,

            "http_status_code":
                self.http_status_code,

            "error_type":
                self.error_type,

            "generation_model":
                self.generation_model,

            "retrieval_config_version":
                self.retrieval_config_version,

            "prompt_contract_version":
                self.prompt_contract_version,

            "chunking_contract_version":
                self.chunking_contract_version,

            "code_revision":
                self.code_revision,
        }


        if (
            set(record)
            != SAFE_OPERATIONAL_EVENT_FIELDS
        ):
            raise RuntimeError(
                "Operational event schema drift."
            )


        return record
