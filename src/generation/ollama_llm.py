"""
Generation runtime: narrow wrapper around the local Ollama daemon.

Public API
----------
    generate(
        messages: List[dict],
        model: str | None = None,
        *,
        _client: Any = None,
    ) -> str

Design
------
``generate`` accepts a structured Ollama /api/chat message list and returns
the model's reply text. Callers build the message list via
``prompt_templates.build_grounded_messages``; this module handles only the
HTTP transport.

Endpoint
--------
POST http://localhost:11434/api/chat
Body:     {"model": <model>, "messages": <messages>, "stream": false}
Response: response.json()["message"]["content"]

Lazy client
-----------
A module-level ``httpx.Client`` is created on the first real call. Importing
this module does not open a connection to Ollama.

Test injection
--------------
The ``_client`` keyword-only parameter accepts any object with a
``post(url, json, timeout) -> response`` method where
``response.json()["message"]["content"]`` is a string. Pass a fake client
in unit tests to avoid all network contact.

Error handling
--------------
If Ollama is unreachable, ``generate`` raises ``RuntimeError`` with a
descriptive message. It never swallows the underlying exception silently.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx

from src.core.config import config
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

_CLIENT: Optional[httpx.Client] = None
_CHAT_PATH = "/api/chat"
_DEFAULT_TIMEOUT = 120.0


def _get_client() -> httpx.Client:
    """Return the module-level httpx.Client, creating it lazily on first call."""
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.Client(
            base_url=config.ollama_base_url,
            timeout=_DEFAULT_TIMEOUT,
        )
    return _CLIENT


def generate(
    messages: List[Dict[str, str]],
    model: Optional[str] = None,
    *,
    _client: Any = None,
) -> str:
    """
    Send a structured message list to the Ollama /api/chat endpoint and
    return the model's reply text.

    Parameters
    ----------
    messages:
        Ollama-format message list, e.g.
        [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}].
        Build this with ``prompt_templates.build_grounded_messages``.
    model:
        Ollama model tag (e.g. ``"qwen3:8b"``). Defaults to
        ``config.generation_model``.
    _client:
        Optional injected client for testing. Must expose
        ``.post(url, json=..., timeout=...)`` returning a response whose
        ``.json()["message"]["content"]`` is a string.

    Returns
    -------
    str
        The model's reply text, stripped of leading/trailing whitespace.

    Raises
    ------
    RuntimeError
        If the Ollama daemon is unreachable or returns an unexpected response.
    """
    resolved_model = model or config.generation_model
    client = _client if _client is not None else _get_client()

    payload: Dict[str, Any] = {
        "model": resolved_model,
        "messages": messages,
        "stream": False,
    }

    logger.info(
        "ollama_generate_start",
        model=resolved_model,
        message_count=len(messages),
    )

    try:
        response = client.post(_CHAT_PATH, json=payload, timeout=_DEFAULT_TIMEOUT)
        response.raise_for_status()
    except httpx.ConnectError as exc:
        raise RuntimeError(
            f"Ollama unreachable at {config.ollama_base_url}. "
            "Ensure the Ollama daemon is running (`ollama serve`)."
        ) from exc
    except httpx.HTTPStatusError as exc:
        raise RuntimeError(
            f"Ollama returned HTTP {exc.response.status_code}: {exc.response.text}"
        ) from exc

    try:
        text = response.json()["message"]["content"]
    except (KeyError, ValueError) as exc:
        raise RuntimeError(
            f"Unexpected Ollama response format: {response.text[:200]}"
        ) from exc

    logger.info(
        "ollama_generate_done",
        model=resolved_model,
        answer_chars=len(text),
    )
    return text.strip()
