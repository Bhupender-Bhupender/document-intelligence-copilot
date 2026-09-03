"""
Managed Databricks generation adapter.

Uses the OpenAI-compatible MLflow Chat Completions API exposed by
Unity AI Gateway model services.

No retrieval, Spark, Delta, or document-table access occurs here.
The adapter receives only messages prepared by the generation layer.
"""

from __future__ import annotations

import time

from src.observability.emitter import (
    emit_operational_event_safely,
)

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.core.config import config
from src.utils.logging_utils import get_logger


logger = get_logger(__name__)



class DatabricksGenerationError(RuntimeError):
    """Raised when managed Databricks generation cannot be completed."""


def _safe_generation_error_metadata(
    exc: Exception,
) -> dict:
    """
    Return non-sensitive provider error metadata.

    Inspect one chained cause when present because authentication
    helpers can wrap SDK/provider exceptions.

    Never includes exception messages, prompts, responses, URLs,
    credentials, tokens, or model output.
    """
    target = (
        getattr(exc, "__cause__", None)
        or exc
    )

    status_code = getattr(
        target,
        "status_code",
        None,
    )

    response = getattr(
        target,
        "response",
        None,
    )

    if (
        status_code is None
        and response is not None
    ):
        status_code = getattr(
            response,
            "status_code",
            None,
        )

    error_code = getattr(
        target,
        "code",
        None,
    )

    if error_code is None:
        error_code = getattr(
            target,
            "error_code",
            None,
        )

    return {
        "cause_type":
            type(target).__name__,

        "status_code":
            status_code,

        "error_code":
            (
                str(error_code)
                if error_code is not None
                else None
            ),
    }



@dataclass(frozen=True)
class DatabricksGenerationResult:
    """Provider response normalized for later evaluation/telemetry."""

    text: str
    model: str
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    finish_reason: Optional[str] = None


def _resolve_workspace_host() -> str:
    """
    Resolve the workspace base URL without logging or exposing credentials.
    """
    host = os.environ.get("DATABRICKS_HOST", "").strip()

    if not host:
        try:
            from databricks.sdk import WorkspaceClient

            workspace = WorkspaceClient()
            host = (workspace.config.host or "").strip()

        except Exception as exc:
            raise DatabricksGenerationError(
                "Databricks workspace host is not configured."
            ) from exc

    host = host.rstrip("/")

    if not (
        host.startswith("https://")
        or host.startswith("http://")
    ):
        raise DatabricksGenerationError(
            "Databricks workspace host is invalid."
        )

    return host


def _resolve_token(
    *,
    _workspace_client=None,
) -> str:
    """
    Resolve a Databricks OAuth bearer token.

    Local development:
        Prefer a transient DATABRICKS_TOKEN.

    Databricks Apps / production:
        Fall back to Databricks unified authentication through
        WorkspaceClient. No credential value is logged.
    """
    token = os.getenv(
        "DATABRICKS_TOKEN",
        "",
    ).strip()

    if token:
        return token

    try:
        if _workspace_client is None:
            from databricks.sdk import (
                WorkspaceClient,
            )

            workspace_client = (
                WorkspaceClient()
            )
        else:
            workspace_client = (
                _workspace_client
            )

        headers = (
            workspace_client
            .config
            .authenticate()
        )

    except Exception as exc:
        raise DatabricksGenerationError(
            "Databricks authentication "
            "is not configured."
        ) from exc

    authorization = (
        headers.get("Authorization")
        or headers.get("authorization")
        or ""
    ).strip()

    prefix = "Bearer "

    if not authorization.startswith(prefix):
        raise DatabricksGenerationError(
            "Databricks authentication did "
            "not provide a bearer token."
        )

    token = authorization[
        len(prefix):
    ].strip()

    if not token:
        raise DatabricksGenerationError(
            "Databricks authentication did "
            "not provide a bearer token."
        )

    return token


def _extract_text_content(content: Any) -> str:
    """
    Normalize OpenAI-compatible assistant content.

    Standard chat models usually return a string. Some managed/reasoning
    models can return structured content blocks. Only visible text blocks
    are treated as the final answer; reasoning blocks are ignored.
    """
    if isinstance(content, str):
        return content.strip()

    if not isinstance(content, list):
        return ""

    text_parts: List[str] = []

    for block in content:
        if isinstance(block, dict):
            block_type = block.get("type")
            block_text = block.get("text")

        else:
            block_type = getattr(block, "type", None)
            block_text = getattr(block, "text", None)

        if (
            block_type == "text"
            and isinstance(block_text, str)
            and block_text.strip()
        ):
            text_parts.append(block_text.strip())

    return "`n".join(text_parts).strip()

def _extract_usage(
    response: Any,
) -> tuple[Optional[int], Optional[int], Optional[int]]:
    usage = getattr(response, "usage", None)

    if usage is None:
        return None, None, None

    return (
        getattr(usage, "prompt_tokens", None),
        getattr(usage, "completion_tokens", None),
        getattr(usage, "total_tokens", None),
    )


def _is_databricks_app_runtime() -> bool:
    """Return True only inside a deployed Databricks App."""
    return bool(
        os.getenv(
            "DATABRICKS_APP_NAME",
            "",
        ).strip()
    )



def _generate_with_metadata_core(
    messages: List[Dict[str, str]],
    model: Optional[str] = None,
    *,
    max_tokens: Optional[int] = None,
    _client: Optional[Any] = None,
) -> DatabricksGenerationResult:
    """
    Generate one grounded answer using a Unity AI Gateway model service.

    The caller supplies the already-built prompt/messages. This adapter has
    no access to retrieval indexes, lakehouse tables, or source documents.
    """
    if not messages:
        raise ValueError("messages must not be empty")

    selected_model = (
        model
        or config.databricks_generation_model
    ).strip()

    if not selected_model:
        raise DatabricksGenerationError(
            "Databricks generation model is not configured."
        )

    requested_max_tokens = (
        max_tokens
        if max_tokens is not None
        else config.databricks_generation_max_tokens
    )

    if requested_max_tokens <= 0:
        raise ValueError(
            "max_tokens must be greater than zero"
        )

    client = _client

    if client is None:
        try:
            from databricks.sdk import (
                WorkspaceClient,
            )

            workspace_client = (
                WorkspaceClient()
            )

            client = (
                workspace_client
                .serving_endpoints
                .get_open_ai_client(
                    timeout=(
                        config
                        .databricks_generation_timeout_seconds
                    ),
                    max_retries=0,
                )
            )

        except Exception as exc:
            logger.warning(
                "databricks_generation_client_init_failed",
                **_safe_generation_error_metadata(
                    exc
                ),
            )

            raise DatabricksGenerationError(
                "Unable to initialize the Databricks "
                "generation client."
            ) from exc

    try:
        response = client.chat.completions.create(
            model=selected_model,
            messages=messages,
            max_tokens=requested_max_tokens,
        )

    except Exception as exc:
        logger.warning(
            "databricks_generation_provider_failed",
            **_safe_generation_error_metadata(
                exc
            ),
        )

        raise DatabricksGenerationError(
            "Managed Databricks generation request failed."
        ) from exc

    choices = getattr(response, "choices", None)

    if not choices:
        raise DatabricksGenerationError(
            "Managed Databricks generation returned no choices."
        )

    first_choice = choices[0]
    message = getattr(first_choice, "message", None)
    text = _extract_text_content(
        getattr(message, "content", None)
    )

    if not text:
        finish_reason = getattr(
            first_choice,
            "finish_reason",
            None,
        )

        raise DatabricksGenerationError(
            "Managed Databricks generation returned "
            "an empty answer: no visible answer text. "
            f"finish_reason={finish_reason!r}"
        )

    prompt_tokens, completion_tokens, total_tokens = (
        _extract_usage(response)
    )

    return DatabricksGenerationResult(
        text=text.strip(),
        model=selected_model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        finish_reason=getattr(
            first_choice,
            "finish_reason",
            None,
        ),
    )




def generate_with_metadata(
    messages: List[Dict[str, str]],
    model: Optional[str] = None,
    *,
    max_tokens: Optional[int] = None,
    _client: Optional[Any] = None,
    _event_emitter=None,
    _event_clock=time.perf_counter,
) -> DatabricksGenerationResult:
    """
    Generate through the existing Databricks adapter while
    emitting content-free provider operational telemetry.
    """
    event_started = (
        _event_clock()
    )

    configured_model = str(
        model
        or config.databricks_generation_model
        or ""
    ).strip()

    event_model = (
        configured_model
        or None
    )


    try:
        result = (
            _generate_with_metadata_core(
                messages,
                model=model,
                max_tokens=max_tokens,
                _client=_client,
            )
        )

    except Exception as exc:
        latency_ms = max(
            0.0,
            (
                _event_clock()
                - event_started
            )
            * 1000.0,
        )


        safe_metadata = (
            _safe_generation_error_metadata(
                exc
            )
        )


        error_type = str(
            safe_metadata.get(
                "cause_type",
                "",
            )
            or type(
                exc
            ).__name__
        )


        raw_status = (
            safe_metadata.get(
                "status_code"
            )
        )


        http_status_code = (
            raw_status
            if (
                isinstance(
                    raw_status,
                    int,
                )
                and not isinstance(
                    raw_status,
                    bool,
                )
                and 100
                <= raw_status
                <= 599
            )
            else None
        )


        emit_operational_event_safely(
            {
                "event_name":
                    "generation.provider.failed",

                "component":
                    "generation",

                "operation":
                    "databricks_generation",

                "status":
                    "error",

                "runtime_mode":
                    config.runtime_mode,

                "backend":
                    "databricks",

                "latency_ms":
                    latency_ms,

                "http_status_code":
                    http_status_code,

                "error_type":
                    error_type,

                "generation_model":
                    event_model,
            },
            _emitter=_event_emitter,
        )

        raise


    latency_ms = max(
        0.0,
        (
            _event_clock()
            - event_started
        )
        * 1000.0,
    )


    emit_operational_event_safely(
        {
            "event_name":
                "generation.provider.completed",

            "component":
                "generation",

            "operation":
                "databricks_generation",

            "status":
                "success",

            "runtime_mode":
                config.runtime_mode,

            "backend":
                "databricks",

            "latency_ms":
                latency_ms,

            "prompt_tokens":
                result.prompt_tokens,

            "completion_tokens":
                result.completion_tokens,

            "total_tokens":
                result.total_tokens,

            "generation_model":
                result.model,
        },
        _emitter=_event_emitter,
    )


    return result

def generate(
    messages: List[Dict[str, str]],
    model: Optional[str] = None,
) -> str:
    """
    Gateway-compatible text-only generation function.
    """
    return generate_with_metadata(
        messages,
        model=model,
    ).text
