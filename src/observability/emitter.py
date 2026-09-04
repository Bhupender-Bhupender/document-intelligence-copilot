from __future__ import annotations

from typing import Any

from src.core.config import (
    config,
)
from src.observability.events import (
    OperationalEvent,
)
from src.utils.logging_utils import (
    get_logger,
)


logger = get_logger(
    __name__
)


_default_persistent_sink = None

_default_persistent_signature = None


def _persistent_signature():
    return (
        config
        .databricks_operational_events_table,

        config
        .databricks_sql_warehouse_id,

        config
        .observability_event_batch_size,

        config
        .observability_flush_interval_seconds,

        config
        .observability_statement_timeout_seconds,
    )


def _get_default_persistent_sink():
    global _default_persistent_sink
    global _default_persistent_signature


    if not (
        config
        .observability_persistence_enabled
    ):
        return None


    table_name = str(
        config
        .databricks_operational_events_table
        or ""
    ).strip()


    warehouse_id = str(
        config
        .databricks_sql_warehouse_id
        or ""
    ).strip()


    if (
        not table_name
        or not warehouse_id
    ):
        logger.warning(
            "operational_persistence_not_configured"
        )

        return None


    signature = (
        _persistent_signature()
    )


    if (
        _default_persistent_sink
        is not None

        and _default_persistent_signature
        == signature
    ):
        return (
            _default_persistent_sink
        )


    try:
        from src.observability.databricks_persistence import (
            BufferedDatabricksOperationalEventSink,
            DatabricksOperationalEventWriter,
        )


        writer = (
            DatabricksOperationalEventWriter(
                table_name=(
                    table_name
                ),

                warehouse_id=(
                    warehouse_id
                ),

                timeout_seconds=(
                    config
                    .observability_statement_timeout_seconds
                ),
            )
        )


        _default_persistent_sink = (
            BufferedDatabricksOperationalEventSink(
                writer,

                batch_size=(
                    config
                    .observability_event_batch_size
                ),

                flush_interval_seconds=(
                    config
                    .observability_flush_interval_seconds
                ),
            )
        )


        _default_persistent_signature = (
            signature
        )


        return (
            _default_persistent_sink
        )


    except Exception as exc:
        logger.warning(
            "operational_persistence_init_failed",
            error_type=type(
                exc
            ).__name__,
        )

        return None


def emit_operational_event(
    event: OperationalEvent,
    *,
    _logger: Any = None,
    _persistent_sink: Any = None,
) -> None:
    """
    Emit one privacy-safe operational event.

    Structured logging is always attempted. Optional
    Databricks Delta persistence is best-effort.
    """
    sink_logger = (
        _logger
        if _logger is not None
        else logger
    )


    record = (
        event.to_record()
    )


    sink_logger.info(
        "operational_event",
        **record,
    )


    persistent_sink = (
        _persistent_sink
        if _persistent_sink is not None
        else _get_default_persistent_sink()
    )


    if persistent_sink is not None:
        try:
            persistent_sink.emit(
                event
            )

        except Exception as exc:
            try:
                sink_logger.warning(
                    "operational_event_persist_failed",
                    error_type=type(
                        exc
                    ).__name__,
                    dropped_event_count=1,
                )

            except Exception:
                pass


def flush_default_persistent_sink() -> bool:
    if (
        _default_persistent_sink
        is None
    ):
        return True


    try:
        return bool(
            _default_persistent_sink
            .flush()
        )

    except Exception as exc:
        logger.warning(
            "operational_event_flush_failed",
            error_type=type(
                exc
            ).__name__,
        )

        return False


def reset_default_persistent_sink() -> None:
    global _default_persistent_sink
    global _default_persistent_signature


    _default_persistent_sink = None

    _default_persistent_signature = None


def emit_operational_event_safely(
    event_kwargs: dict[str, Any],
    *,
    _emitter: Any = None,
    _logger: Any = None,
) -> None:
    """
    Build and emit one OperationalEvent without allowing
    observability failures to break production execution.
    """
    sink = (
        _emitter
        if _emitter is not None
        else emit_operational_event
    )

    sink_logger = (
        _logger
        if _logger is not None
        else logger
    )


    try:
        sink(
            OperationalEvent(
                **event_kwargs
            )
        )

    except Exception as exc:
        try:
            sink_logger.warning(
                "operational_event_emit_failed",
                error_type=type(
                    exc
                ).__name__,
            )

        except Exception:
            pass
