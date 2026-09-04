from __future__ import annotations

from datetime import (
    datetime,
    timezone,
)
from types import (
    SimpleNamespace,
)

import pytest

from src.observability.databricks_persistence import (
    BufferedDatabricksOperationalEventSink,
    DatabricksOperationalEventWriter,
    _build_insert_statement,
    build_hourly_refresh_statement,
    build_operational_events_table_ddl,
    create_monitoring_tables,
)
from src.observability.emitter import (
    emit_operational_event,
)
from src.observability.events import (
    OperationalEvent,
)


def _response(
    state,
    *,
    statement_id="statement-1",
):
    return SimpleNamespace(
        status=SimpleNamespace(
            state=state,
        ),
        statement_id=(
            statement_id
        ),
    )


class FakeStatementExecution:
    def __init__(
        self,
        responses,
    ):
        self.responses = list(
            responses
        )

        self.execute_calls = []

        self.get_calls = []


    def execute_statement(
        self,
        **kwargs,
    ):
        self.execute_calls.append(
            kwargs
        )

        return self.responses.pop(
            0
        )


    def get_statement(
        self,
        statement_id,
    ):
        self.get_calls.append(
            statement_id
        )

        return self.responses.pop(
            0
        )


class FakeWorkspace:
    def __init__(
        self,
        responses,
    ):
        self.statement_execution = (
            FakeStatementExecution(
                responses
            )
        )


def _event(
    *,
    name=(
        "serving.answer.completed"
    ),
):
    return OperationalEvent(
        event_name=name,
        component="serving",
        operation=(
            "answer_with_evidence"
        ),
        status="success",
        occurred_at_utc=datetime(
            2026,
            9,
            4,
            10,
            0,
            tzinfo=timezone.utc,
        ),
        runtime_mode="databricks",
        backend="databricks",
        latency_ms=25.0,
        evidence_count=2,
        citation_count=2,
        prompt_tokens=100,
        completion_tokens=20,
        total_tokens=120,
        generation_model="endpoint-v1",
    )


def test_insert_uses_parameterized_values():
    event = _event()

    statement, parameters = (
        _build_insert_statement(
            table_name=(
                "docintel_dev.monitoring."
                "operational_events"
            ),
            records=[
                event.to_record()
            ],
        )
    )


    assert (
        "endpoint-v1"
        not in statement
    )

    assert (
        "serving.answer.completed"
        not in statement
    )

    assert ":event_0_event_name" in (
        statement
    )

    assert len(parameters) == 24


def test_writer_batches_multiple_events_into_one_statement():
    workspace = FakeWorkspace(
        [
            _response(
                "SUCCEEDED"
            )
        ]
    )


    writer = (
        DatabricksOperationalEventWriter(
            table_name=(
                "docintel_dev.monitoring."
                "operational_events"
            ),
            warehouse_id="warehouse-1",
            workspace_client=workspace,
        )
    )


    written = writer.write_events(
        [
            _event(),
            _event(
                name=(
                    "serving.retrieve.completed"
                )
            ),
        ]
    )


    assert written == 2

    assert len(
        workspace
        .statement_execution
        .execute_calls
    ) == 1


    call = (
        workspace
        .statement_execution
        .execute_calls[0]
    )


    assert len(
        call["parameters"]
    ) == 48


def test_writer_polls_pending_statement(
    monkeypatch,
):
    workspace = FakeWorkspace(
        [
            _response(
                "PENDING"
            ),
            _response(
                "SUCCEEDED"
            ),
        ]
    )


    monkeypatch.setattr(
        "src.observability."
        "databricks_persistence."
        "time.sleep",
        lambda seconds: None,
    )


    writer = (
        DatabricksOperationalEventWriter(
            table_name=(
                "docintel_dev.monitoring."
                "operational_events"
            ),
            warehouse_id="warehouse-1",
            workspace_client=workspace,
        )
    )


    assert writer.write_events(
        [
            _event()
        ]
    ) == 1


    assert (
        workspace
        .statement_execution
        .get_calls
        == [
            "statement-1"
        ]
    )


def test_invalid_monitoring_table_is_rejected():
    with pytest.raises(
        ValueError
    ):
        build_operational_events_table_ddl(
            "not-a-three-part-name"
        )


def test_buffer_flushes_as_single_batch():
    batches = []


    class Writer:

        def write_events(
            self,
            events,
        ):
            batches.append(
                list(
                    events
                )
            )

            return len(
                events
            )


    sink = (
        BufferedDatabricksOperationalEventSink(
            Writer(),
            batch_size=2,
            flush_interval_seconds=60.0,
        )
    )


    sink.emit(
        _event()
    )

    assert sink.pending_count == 1


    sink.emit(
        _event(
            name=(
                "serving.retrieve.completed"
            )
        )
    )


    assert sink.pending_count == 0

    assert len(batches) == 1

    assert len(
        batches[0]
    ) == 2


def test_buffer_failure_is_fail_open_and_drops_attempted_batch():
    captured = {}


    class Writer:

        def write_events(
            self,
            events,
        ):
            raise RuntimeError(
                "warehouse unavailable"
            )


    class FakeLogger:

        def warning(
            self,
            event,
            **kwargs,
        ):
            captured[
                "event"
            ] = event

            captured[
                "kwargs"
            ] = kwargs


    sink = (
        BufferedDatabricksOperationalEventSink(
            Writer(),
            batch_size=1,
            flush_interval_seconds=60.0,
            _logger=FakeLogger(),
        )
    )


    sink.emit(
        _event()
    )


    assert sink.pending_count == 0

    assert (
        captured["event"]
        == "operational_event_persist_failed"
    )

    assert (
        captured["kwargs"][
            "dropped_event_count"
        ]
        == 1
    )


def test_structured_log_and_persistent_sink_fan_out():
    captured = {
        "logs": [],
        "events": [],
    }


    class FakeLogger:

        def info(
            self,
            event,
            **kwargs,
        ):
            captured[
                "logs"
            ].append(
                (
                    event,
                    kwargs,
                )
            )


    class FakeSink:

        def emit(
            self,
            event,
        ):
            captured[
                "events"
            ].append(
                event
            )


    event = _event()


    emit_operational_event(
        event,
        _logger=FakeLogger(),
        _persistent_sink=FakeSink(),
    )


    assert len(
        captured["logs"]
    ) == 1

    assert len(
        captured["events"]
    ) == 1


def test_hourly_refresh_is_idempotent_merge():
    sql = (
        build_hourly_refresh_statement(
            events_table=(
                "docintel_dev.monitoring."
                "operational_events"
            ),
            metrics_table=(
                "docintel_dev.monitoring."
                "operational_metrics_hourly"
            ),
            lookback_hours=48,
        )
    )


    assert sql.startswith(
        "MERGE INTO"
    )

    assert (
        "WHEN MATCHED THEN"
        in sql
    )

    assert (
        "WHEN NOT MATCHED THEN"
        in sql
    )

    assert (
        "percentile_approx"
        in sql
    )


def test_monitoring_bootstrap_creates_schema_and_two_tables():
    workspace = FakeWorkspace(
        [
            _response(
                "SUCCEEDED"
            ),
            _response(
                "SUCCEEDED"
            ),
            _response(
                "SUCCEEDED"
            ),
        ]
    )


    create_monitoring_tables(
        events_table=(
            "docintel_dev.monitoring."
            "operational_events"
        ),
        metrics_table=(
            "docintel_dev.monitoring."
            "operational_metrics_hourly"
        ),
        warehouse_id="warehouse-1",
        workspace_client=workspace,
    )


    calls = (
        workspace
        .statement_execution
        .execute_calls
    )


    assert len(calls) == 3

    assert (
        calls[0]["statement"]
        == (
            "CREATE SCHEMA IF NOT EXISTS "
            "docintel_dev.monitoring"
        )
    )
