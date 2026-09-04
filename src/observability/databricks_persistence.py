from __future__ import annotations

import re
import threading
import time

from collections.abc import Sequence
from typing import Any, Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    StatementParameterListItem,
)

from src.observability.events import (
    SAFE_OPERATIONAL_EVENT_FIELDS,
    OperationalEvent,
)
from src.utils.logging_utils import (
    get_logger,
)


logger = get_logger(
    __name__
)


_TABLE_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*"
    r"\.[A-Za-z_][A-Za-z0-9_]*"
    r"\.[A-Za-z_][A-Za-z0-9_]*$"
)


_EVENT_COLUMNS = (
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
)


if set(
    _EVENT_COLUMNS
) != SAFE_OPERATIONAL_EVENT_FIELDS:
    raise RuntimeError(
        "Operational persistence schema drift."
    )


_PARAMETER_TYPES = {
    "event_schema_version": "STRING",
    "occurred_at_utc": "STRING",
    "event_name": "STRING",
    "component": "STRING",
    "operation": "STRING",
    "status": "STRING",
    "runtime_mode": "STRING",
    "backend": "STRING",
    "latency_ms": "DOUBLE",
    "result_count": "BIGINT",
    "evidence_count": "BIGINT",
    "citation_count": "BIGINT",
    "parent_context_count": "BIGINT",
    "prompt_tokens": "BIGINT",
    "completion_tokens": "BIGINT",
    "total_tokens": "BIGINT",
    "retry_count": "BIGINT",
    "http_status_code": "INT",
    "error_type": "STRING",
    "generation_model": "STRING",
    "retrieval_config_version": "STRING",
    "prompt_contract_version": "STRING",
    "chunking_contract_version": "STRING",
    "code_revision": "STRING",
}


def _validate_table_name(
    table_name: str,
) -> str:
    value = str(
        table_name
        or ""
    ).strip()

    if not _TABLE_NAME_PATTERN.fullmatch(
        value
    ):
        raise ValueError(
            "Monitoring table must be a valid "
            "three-part Unity Catalog name."
        )

    return value


def _schema_name(
    table_name: str,
) -> str:
    validated = (
        _validate_table_name(
            table_name
        )
    )

    catalog, schema, _ = (
        validated.split(
            "."
        )
    )

    return (
        f"{catalog}.{schema}"
    )


def _state_name(
    response: Any,
) -> str:
    status = getattr(
        response,
        "status",
        None,
    )

    state = getattr(
        status,
        "state",
        None,
    )

    if state is None:
        return ""

    value = getattr(
        state,
        "value",
        state,
    )

    return (
        str(value)
        .upper()
        .split(".")[-1]
    )


def _parameter_value(
    value: Any,
) -> str | None:
    if value is None:
        return None

    if isinstance(
        value,
        bool,
    ):
        raise TypeError(
            "Boolean operational parameters "
            "are not supported."
        )

    if isinstance(
        value,
        float,
    ):
        return format(
            value,
            ".17g",
        )

    return str(
        value
    )


def _execute_statement_and_wait(
    *,
    client: Any,
    warehouse_id: str,
    statement: str,
    parameters: list[
        StatementParameterListItem
    ] | None = None,
    timeout_seconds: float,
    _clock: Callable[
        [],
        float,
    ] = time.monotonic,
    _sleep: Callable[
        [float],
        None,
    ] = time.sleep,
) -> Any:
    warehouse = str(
        warehouse_id
        or ""
    ).strip()

    if not warehouse:
        raise RuntimeError(
            "Databricks SQL warehouse ID "
            "is not configured."
        )

    kwargs: dict[str, Any] = {
        "statement":
            statement,

        "warehouse_id":
            warehouse,

        "wait_timeout":
            "50s",
    }

    if parameters:
        kwargs[
            "parameters"
        ] = parameters


    response = (
        client
        .statement_execution
        .execute_statement(
            **kwargs
        )
    )


    deadline = (
        _clock()
        + timeout_seconds
    )

    state = (
        _state_name(
            response
        )
    )


    while state in {
        "PENDING",
        "RUNNING",
    }:
        if _clock() >= deadline:
            raise RuntimeError(
                "Timed out waiting for Databricks "
                "monitoring statement."
            )


        statement_id = getattr(
            response,
            "statement_id",
            None,
        )


        if not statement_id:
            raise RuntimeError(
                "Databricks monitoring statement "
                "did not return a statement ID."
            )


        _sleep(
            2.0
        )


        response = (
            client
            .statement_execution
            .get_statement(
                statement_id
            )
        )


        state = (
            _state_name(
                response
            )
        )


    if state != "SUCCEEDED":
        raise RuntimeError(
            "Databricks monitoring statement failed."
        )


    return response


def _build_insert_statement(
    *,
    table_name: str,
    records: Sequence[
        dict[str, Any]
    ],
) -> tuple[
    str,
    list[
        StatementParameterListItem
    ],
]:
    table = (
        _validate_table_name(
            table_name
        )
    )


    if not records:
        raise ValueError(
            "records must not be empty"
        )


    value_rows = []

    parameters: list[
        StatementParameterListItem
    ] = []


    for row_index, record in enumerate(
        records
    ):
        if set(
            record
        ) != set(
            _EVENT_COLUMNS
        ):
            raise RuntimeError(
                "Operational record schema drift."
            )


        placeholders = []


        for column in (
            _EVENT_COLUMNS
        ):
            parameter_name = (
                f"event_{row_index}_{column}"
            )


            if (
                column
                == "occurred_at_utc"
            ):
                placeholders.append(
                    "CAST("
                    f":{parameter_name}"
                    " AS TIMESTAMP)"
                )

            else:
                placeholders.append(
                    f":{parameter_name}"
                )


            parameters.append(
                StatementParameterListItem(
                    name=parameter_name,
                    value=_parameter_value(
                        record[
                            column
                        ]
                    ),
                    type=_PARAMETER_TYPES[
                        column
                    ],
                )
            )


        value_rows.append(
            "("
            + ", ".join(
                placeholders
            )
            + ")"
        )


    columns_sql = ", ".join(
        _EVENT_COLUMNS
    )


    statement = (
        f"INSERT INTO {table} "
        f"({columns_sql}) VALUES "
        + ", ".join(
            value_rows
        )
    )


    return (
        statement,
        parameters,
    )


def build_operational_events_table_ddl(
    table_name: str,
) -> str:
    table = (
        _validate_table_name(
            table_name
        )
    )


    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    event_schema_version STRING NOT NULL,
    occurred_at_utc TIMESTAMP NOT NULL,
    event_name STRING NOT NULL,
    component STRING NOT NULL,
    operation STRING NOT NULL,
    status STRING NOT NULL,
    runtime_mode STRING,
    backend STRING,
    latency_ms DOUBLE,
    result_count BIGINT,
    evidence_count BIGINT,
    citation_count BIGINT,
    parent_context_count BIGINT,
    prompt_tokens BIGINT,
    completion_tokens BIGINT,
    total_tokens BIGINT,
    retry_count BIGINT,
    http_status_code INT,
    error_type STRING,
    generation_model STRING,
    retrieval_config_version STRING,
    prompt_contract_version STRING,
    chunking_contract_version STRING,
    code_revision STRING
)
USING DELTA
""".strip()


def build_hourly_metrics_table_ddl(
    table_name: str,
) -> str:
    table = (
        _validate_table_name(
            table_name
        )
    )


    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    window_start_utc TIMESTAMP NOT NULL,
    component STRING NOT NULL,
    operation STRING NOT NULL,
    backend STRING NOT NULL,
    generation_model STRING NOT NULL,
    request_count BIGINT NOT NULL,
    success_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    error_rate DOUBLE NOT NULL,
    avg_latency_ms DOUBLE,
    p95_latency_ms DOUBLE,
    avg_result_count DOUBLE,
    avg_evidence_count DOUBLE,
    avg_citation_count DOUBLE,
    prompt_tokens BIGINT NOT NULL,
    completion_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,
    updated_at_utc TIMESTAMP NOT NULL
)
USING DELTA
""".strip()


def build_hourly_refresh_statement(
    *,
    events_table: str,
    metrics_table: str,
    lookback_hours: int = 48,
) -> str:
    source_table = (
        _validate_table_name(
            events_table
        )
    )

    target_table = (
        _validate_table_name(
            metrics_table
        )
    )


    if (
        isinstance(
            lookback_hours,
            bool,
        )
        or not isinstance(
            lookback_hours,
            int,
        )
        or lookback_hours <= 0
        or lookback_hours > 720
    ):
        raise ValueError(
            "lookback_hours must be an integer "
            "between 1 and 720."
        )


    return f"""
MERGE INTO {target_table} AS target
USING (
    SELECT
        date_trunc(
            'HOUR',
            occurred_at_utc
        ) AS window_start_utc,

        component,
        operation,

        COALESCE(
            backend,
            'unknown'
        ) AS backend,

        COALESCE(
            generation_model,
            'unknown'
        ) AS generation_model,

        COUNT(*) AS request_count,

        SUM(
            CASE
                WHEN status = 'success'
                THEN 1
                ELSE 0
            END
        ) AS success_count,

        SUM(
            CASE
                WHEN status = 'error'
                THEN 1
                ELSE 0
            END
        ) AS error_count,

        CAST(
            SUM(
                CASE
                    WHEN status = 'error'
                    THEN 1
                    ELSE 0
                END
            )
            AS DOUBLE
        ) / COUNT(*) AS error_rate,

        AVG(
            latency_ms
        ) AS avg_latency_ms,

        percentile_approx(
            latency_ms,
            0.95
        ) AS p95_latency_ms,

        AVG(
            result_count
        ) AS avg_result_count,

        AVG(
            evidence_count
        ) AS avg_evidence_count,

        AVG(
            citation_count
        ) AS avg_citation_count,

        COALESCE(
            SUM(
                prompt_tokens
            ),
            0
        ) AS prompt_tokens,

        COALESCE(
            SUM(
                completion_tokens
            ),
            0
        ) AS completion_tokens,

        COALESCE(
            SUM(
                total_tokens
            ),
            0
        ) AS total_tokens,

        current_timestamp()
            AS updated_at_utc

    FROM {source_table}

    WHERE occurred_at_utc
        >= current_timestamp()
            - INTERVAL {lookback_hours} HOURS

    GROUP BY
        date_trunc(
            'HOUR',
            occurred_at_utc
        ),
        component,
        operation,
        COALESCE(
            backend,
            'unknown'
        ),
        COALESCE(
            generation_model,
            'unknown'
        )
) AS source

ON
    target.window_start_utc
        = source.window_start_utc

    AND target.component
        = source.component

    AND target.operation
        = source.operation

    AND target.backend
        = source.backend

    AND target.generation_model
        = source.generation_model

WHEN MATCHED THEN
UPDATE SET
    request_count =
        source.request_count,

    success_count =
        source.success_count,

    error_count =
        source.error_count,

    error_rate =
        source.error_rate,

    avg_latency_ms =
        source.avg_latency_ms,

    p95_latency_ms =
        source.p95_latency_ms,

    avg_result_count =
        source.avg_result_count,

    avg_evidence_count =
        source.avg_evidence_count,

    avg_citation_count =
        source.avg_citation_count,

    prompt_tokens =
        source.prompt_tokens,

    completion_tokens =
        source.completion_tokens,

    total_tokens =
        source.total_tokens,

    updated_at_utc =
        source.updated_at_utc

WHEN NOT MATCHED THEN
INSERT (
    window_start_utc,
    component,
    operation,
    backend,
    generation_model,
    request_count,
    success_count,
    error_count,
    error_rate,
    avg_latency_ms,
    p95_latency_ms,
    avg_result_count,
    avg_evidence_count,
    avg_citation_count,
    prompt_tokens,
    completion_tokens,
    total_tokens,
    updated_at_utc
)
VALUES (
    source.window_start_utc,
    source.component,
    source.operation,
    source.backend,
    source.generation_model,
    source.request_count,
    source.success_count,
    source.error_count,
    source.error_rate,
    source.avg_latency_ms,
    source.p95_latency_ms,
    source.avg_result_count,
    source.avg_evidence_count,
    source.avg_citation_count,
    source.prompt_tokens,
    source.completion_tokens,
    source.total_tokens,
    source.updated_at_utc
)
""".strip()


class DatabricksOperationalEventWriter:
    def __init__(
        self,
        *,
        table_name: str,
        warehouse_id: str,
        workspace_client: Any = None,
        timeout_seconds: float = 120.0,
    ) -> None:
        self.table_name = (
            _validate_table_name(
                table_name
            )
        )

        self.warehouse_id = str(
            warehouse_id
            or ""
        ).strip()

        if not self.warehouse_id:
            raise ValueError(
                "warehouse_id must not be empty"
            )

        if timeout_seconds <= 0:
            raise ValueError(
                "timeout_seconds must be positive"
            )

        self.workspace_client = (
            workspace_client
        )

        self.timeout_seconds = (
            float(
                timeout_seconds
            )
        )


    def _client(
        self,
    ) -> Any:
        if (
            self.workspace_client
            is None
        ):
            self.workspace_client = (
                WorkspaceClient()
            )

        return self.workspace_client


    def write_events(
        self,
        events: Sequence[
            OperationalEvent
        ],
    ) -> int:
        if not events:
            return 0


        records = [
            event.to_record()
            for event
            in events
        ]


        statement, parameters = (
            _build_insert_statement(
                table_name=(
                    self.table_name
                ),
                records=records,
            )
        )


        _execute_statement_and_wait(
            client=self._client(),
            warehouse_id=(
                self.warehouse_id
            ),
            statement=statement,
            parameters=parameters,
            timeout_seconds=(
                self.timeout_seconds
            ),
        )


        return len(
            records
        )


class BufferedDatabricksOperationalEventSink:
    """
    Best-effort bounded batching for serving-time telemetry.

    The structured application log remains the primary event
    record. A failed Delta write is dropped rather than retried
    ambiguously or propagated into the user request.
    """

    def __init__(
        self,
        writer: DatabricksOperationalEventWriter,
        *,
        batch_size: int = 25,
        flush_interval_seconds: float = 30.0,
        _clock: Callable[
            [],
            float,
        ] = time.monotonic,
        _logger: Any = None,
    ) -> None:
        if (
            isinstance(
                batch_size,
                bool,
            )
            or not isinstance(
                batch_size,
                int,
            )
            or batch_size <= 0
        ):
            raise ValueError(
                "batch_size must be a positive integer"
            )

        if flush_interval_seconds <= 0:
            raise ValueError(
                "flush_interval_seconds must be positive"
            )


        self.writer = writer

        self.batch_size = (
            batch_size
        )

        self.flush_interval_seconds = (
            float(
                flush_interval_seconds
            )
        )

        self._clock = _clock

        self._logger = (
            _logger
            if _logger is not None
            else logger
        )

        self._buffer: list[
            OperationalEvent
        ] = []

        self._buffer_started_at: (
            float | None
        ) = None

        self._lock = (
            threading.Lock()
        )


    @property
    def pending_count(
        self,
    ) -> int:
        with self._lock:
            return len(
                self._buffer
            )


    def emit(
        self,
        event: OperationalEvent,
    ) -> None:
        now = (
            self._clock()
        )

        batch = None


        with self._lock:
            if not self._buffer:
                self._buffer_started_at = (
                    now
                )


            self._buffer.append(
                event
            )


            age = (
                0.0
                if self._buffer_started_at
                is None
                else max(
                    0.0,
                    now
                    - self._buffer_started_at,
                )
            )


            if (
                len(
                    self._buffer
                )
                >= self.batch_size

                or age
                >= self.flush_interval_seconds
            ):
                batch = self._drain_locked()


        if batch:
            self._persist_batch(
                batch
            )


    def flush(
        self,
    ) -> bool:
        with self._lock:
            batch = (
                self._drain_locked()
            )


        if not batch:
            return True


        return self._persist_batch(
            batch
        )


    def _drain_locked(
        self,
    ) -> list[
        OperationalEvent
    ]:
        batch = list(
            self._buffer
        )

        self._buffer.clear()

        self._buffer_started_at = (
            None
        )

        return batch


    def _persist_batch(
        self,
        batch: Sequence[
            OperationalEvent
        ],
    ) -> bool:
        try:
            self.writer.write_events(
                batch
            )

            return True

        except Exception as exc:
            try:
                self._logger.warning(
                    "operational_event_persist_failed",
                    error_type=type(
                        exc
                    ).__name__,
                    dropped_event_count=len(
                        batch
                    ),
                )

            except Exception:
                pass

            return False


def create_monitoring_tables(
    *,
    events_table: str,
    metrics_table: str,
    warehouse_id: str,
    workspace_client: Any = None,
    timeout_seconds: float = 120.0,
) -> None:
    events = (
        _validate_table_name(
            events_table
        )
    )

    metrics = (
        _validate_table_name(
            metrics_table
        )
    )


    if (
        _schema_name(
            events
        )
        != _schema_name(
            metrics
        )
    ):
        raise ValueError(
            "Monitoring tables must use the "
            "same catalog and schema."
        )


    client = (
        workspace_client
        or WorkspaceClient()
    )


    statements = [
        (
            "CREATE SCHEMA IF NOT EXISTS "
            + _schema_name(
                events
            )
        ),

        build_operational_events_table_ddl(
            events
        ),

        build_hourly_metrics_table_ddl(
            metrics
        ),
    ]


    for statement in statements:
        _execute_statement_and_wait(
            client=client,
            warehouse_id=warehouse_id,
            statement=statement,
            timeout_seconds=(
                timeout_seconds
            ),
        )


def refresh_hourly_operational_metrics(
    *,
    events_table: str,
    metrics_table: str,
    warehouse_id: str,
    workspace_client: Any = None,
    timeout_seconds: float = 120.0,
    lookback_hours: int = 48,
) -> None:
    statement = (
        build_hourly_refresh_statement(
            events_table=(
                events_table
            ),
            metrics_table=(
                metrics_table
            ),
            lookback_hours=(
                lookback_hours
            ),
        )
    )


    client = (
        workspace_client
        or WorkspaceClient()
    )


    _execute_statement_and_wait(
        client=client,
        warehouse_id=warehouse_id,
        statement=statement,
        timeout_seconds=(
            timeout_seconds
        ),
    )
