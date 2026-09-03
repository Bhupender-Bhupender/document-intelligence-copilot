from __future__ import annotations

from typing import Any

from src.observability.events import (
    OperationalEvent,
)
from src.utils.logging_utils import (
    get_logger,
)


logger = get_logger(
    __name__
)


def emit_operational_event(
    event: OperationalEvent,
    *,
    _logger: Any = None,
) -> None:
    """
    Emit one privacy-safe operational event.

    This first Phase 16 sink uses the existing structured
    logger. Persistent Delta storage is added separately
    in Phase 16E.
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
