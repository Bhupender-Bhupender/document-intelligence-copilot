"""
Prompt adapter for Phase 11 RetrievalEvidence.

The generic grounded prompt builder remains the
base implementation. This module adds evidence
identity, citation IDs, and explicit rules around
parent context.
"""

from __future__ import annotations

from typing import List

from src.generation.prompt_templates import (
    build_grounded_messages,
)

from src.schema.retrieval_service_models import (
    RetrievalEvidence,
)


_EVIDENCE_RESPONSE_RULES = (
    "\n\nEvidence rules:\n"
    "- Each context block has a Citation ID.\n"
    "- Treat the authoritative evidence passage "
    "as the factual support for that Citation ID.\n"
    "- Parent context may help interpretation, "
    "but must not be the sole support for a "
    "factual claim.\n"
    "- Do not invent, alter, or infer Citation "
    "IDs.\n"
    "- Do not claim that a citation supports "
    "information absent from its authoritative "
    "evidence passage."
)


def _format_evidence_block(
    evidence: RetrievalEvidence,
) -> str:
    citation_id = (
        evidence.citation_payload.citation_id
    )

    parts = [
        f"[Citation ID: {citation_id}]",
        "Authoritative evidence:",
        evidence.text,
    ]

    parent_text = (
        evidence.parent_text.strip()
        if evidence.parent_text
        else ""
    )

    child_text = evidence.text.strip()

    if (
        parent_text
        and parent_text != child_text
    ):
        parts.extend(
            [
                "",
                (
                    "Additional parent context "
                    "(background only; do not use "
                    "as sole factual support):"
                ),
                parent_text,
            ]
        )

    return "\n".join(parts)


def build_evidence_grounded_messages(
    query: str,
    evidence: List[RetrievalEvidence],
) -> List[dict]:
    """
    Build provider-neutral grounded messages from
    Phase 11 retrieval evidence.
    """

    context_blocks = [
        _format_evidence_block(item)
        for item in evidence
    ]

    messages = build_grounded_messages(
        query,
        context_blocks,
    )

    if not messages:
        raise RuntimeError(
            "Grounded prompt builder returned "
            "no messages."
        )

    system_message = dict(messages[0])

    system_message["content"] = (
        system_message["content"]
        + _EVIDENCE_RESPONSE_RULES
    )

    return [
        system_message,
        *messages[1:],
    ]
