"""
Prompt construction for grounded answer synthesis.

Public API
----------
    build_grounded_messages(
        query: str,
        context_blocks: List[str],
    ) -> List[dict]

Design
------
Returns a structured Ollama /api/chat message list. The prompt enforces three
grounding rules:
    1. Answer only from the provided context passages.
    2. Do not invent facts not found in the context.
    3. If the context is insufficient, say so explicitly.

No citation formatting, quote extraction, or validation logic is included.
Those are deferred to Phase 7 (citations) and Phase 8 (validation).

Message structure
-----------------
[
    {"role": "system", "content": SYSTEM_INSTRUCTION},
    {"role": "user",   "content": <assembled context + query>},
]

Empty context_blocks
--------------------
When context_blocks is an empty list, the user turn contains a
"[No context provided.]" placeholder. This allows the model to respond
gracefully ("The provided context does not contain sufficient information...")
rather than receiving a malformed prompt.
"""
from __future__ import annotations

from typing import List

_SYSTEM_INSTRUCTION = (
    "You are a precise document-intelligence assistant. "
    "Answer the user's question using ONLY the context passages provided below. "
    "Do not invent, assume, or infer facts that are not explicitly stated in the context. "
    "If the provided context does not contain sufficient information to answer the question, "
    'respond with: "The provided context does not contain sufficient information to answer '
    'this question."'
)

_CONTEXT_HEADER = "=== Context ==="
_CONTEXT_SEPARATOR = "\n---\n"
_NO_CONTEXT_PLACEHOLDER = "[No context provided.]"
_QUERY_HEADER = "=== Question ==="


def build_grounded_messages(
    query: str,
    context_blocks: List[str],
) -> List[dict]:
    """
    Build a structured Ollama /api/chat message list for grounded answer synthesis.

    Parameters
    ----------
    query:
        The user's question.
    context_blocks:
        List of text passages to use as context. May be empty; in that case
        the user message includes a "[No context provided.]" placeholder so
        the model can respond gracefully.

    Returns
    -------
    List[dict]
        Two-element list: system message followed by user message.
        Each dict has keys ``"role"`` and ``"content"``.
    """
    if context_blocks:
        context_section = _CONTEXT_SEPARATOR.join(context_blocks)
    else:
        context_section = _NO_CONTEXT_PLACEHOLDER

    user_content = (
        f"{_CONTEXT_HEADER}\n{context_section}\n\n"
        f"{_QUERY_HEADER}\n{query}"
    )

    return [
        {"role": "system", "content": _SYSTEM_INSTRUCTION},
        {"role": "user",   "content": user_content},
    ]
