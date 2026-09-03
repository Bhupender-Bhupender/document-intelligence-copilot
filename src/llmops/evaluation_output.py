from __future__ import annotations

from typing import Any

from src.schema.serving_models import (
    ServingAnswerResponse,
)


def normalize_serving_response(
    response: ServingAnswerResponse,
) -> dict[str, Any]:
    """
    Convert the production serving response into
    the stable deterministic-evaluation output.

    Privacy boundary:
    - preserve answer_text because answer scoring
      requires it;
    - preserve only document identities from
      evidence;
    - do not expose query, evidence text,
      parent text, citation text, filenames,
      page content, or prompts.

    Retrieval document order is preserved and
    duplicate document IDs are intentionally not
    removed. This matches the existing ranked
    retrieval evaluation semantics.
    """
    document_ids: list[str] = []

    for evidence in response.evidence:
        document_id = str(
            evidence.document_id
            or ""
        ).strip()

        if not document_id:
            raise ValueError(
                "Serving evidence contains an "
                "empty document_id."
            )

        document_ids.append(
            document_id
        )

    return {
        "answer_text":
            response.answer_text,

        "retrieved_document_ids":
            document_ids,

        "evidence_count":
            len(
                response.evidence
            ),

        "citation_count":
            len(
                response.sources
            ),
    }