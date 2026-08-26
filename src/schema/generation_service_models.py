from __future__ import annotations

import uuid

from typing import List, Literal, Optional

from pydantic import (
    BaseModel,
    Field,
    field_validator,
)

from src.schema.models import CitationRecord
from src.schema.retrieval_service_models import (
    RetrievalEvidence,
)


GENERATION_CONTRACT_VERSION = (
    "generation_service_v1"
)


class GenerationRequest(BaseModel):
    """
    Provider-independent generation request.

    Retrieval has already completed before this
    contract is created. Generation receives only
    the user's query and Phase 11 evidence.
    """

    query: str

    evidence: List[RetrievalEvidence] = Field(
        default_factory=list
    )

    model: Optional[str] = None

    @field_validator("query")
    @classmethod
    def _validate_query(
        cls,
        value: str,
    ) -> str:
        value = value.strip()

        if not value:
            raise ValueError(
                "query must not be blank"
            )

        return value

    @field_validator("model")
    @classmethod
    def _normalize_model(
        cls,
        value: Optional[str],
    ) -> Optional[str]:
        if value is None:
            return None

        value = value.strip()

        return value or None


class GenerationResponse(BaseModel):
    """
    Provider-independent grounded answer result.
    """

    run_id: str = Field(
        default_factory=lambda: uuid.uuid4().hex
    )

    query: str

    answer_text: str

    model_used: str

    generation_backend: Literal[
        "ollama",
        "databricks",
    ]

    sources: List[CitationRecord] = Field(
        default_factory=list
    )

    evidence: List[RetrievalEvidence] = Field(
        default_factory=list
    )

    latency_ms: float = Field(
        ge=0.0
    )

    generation_contract_version: str = (
        GENERATION_CONTRACT_VERSION
    )
