"""
HTTP serving contracts for the Document Intelligence API.

These models compose the existing Phase 11 retrieval contract and
Phase 12 generation contract. They contain no provider-specific SDK types.
"""

from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from src.schema.models import CitationRecord
from src.schema.retrieval_service_models import (
    RetrievalEvidence,
    RetrievalRequest,
)


class HealthResponse(BaseModel):
    """Minimal liveness response."""

    status: Literal["ok"] = "ok"
    service: Literal["document-intelligence-api"] = (
        "document-intelligence-api"
    )


class ReadinessResponse(BaseModel):
    """
    Safe configuration-readiness response.

    No credentials, URLs, table names, index names, or other configured
    values are exposed. Only backend names and boolean checks are returned.
    """

    status: Literal["ready", "not_ready"]

    runtime_mode: str
    search_backend: str
    generation_backend: str

    checks: Dict[str, bool] = Field(
        default_factory=dict
    )



class ServingAnswerRequest(RetrievalRequest):
    """
    End-to-end answer request.

    Retrieval fields are inherited directly from the Phase 11 contract.
    ``model`` is the only generation-specific request field.
    """

    model: Optional[str] = Field(
        default=None,
        description=(
            "Optional generation model override. "
            "When omitted, the configured backend default is used."
        ),
    )

    @field_validator("model")
    @classmethod
    def validate_model(
        cls,
        value: Optional[str],
    ) -> Optional[str]:
        if value is None:
            return None

        stripped = value.strip()

        if not stripped:
            raise ValueError(
                "model must not be blank"
            )

        return stripped


class ServingAnswerResponse(BaseModel):
    """
    End-to-end HTTP response.

    Generation output remains citation/evidence preserving while retrieval
    metadata is carried alongside it for observability.
    """

    run_id: str
    retrieval_query_id: str

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

    retrieval_latency_ms: float = Field(
        ge=0.0
    )

    generation_latency_ms: float = Field(
        ge=0.0
    )

    total_latency_ms: float = Field(
        ge=0.0
    )

    index_version: Optional[str] = None

    retrieval_config_version: str
    generation_contract_version: str

    applied_filters: List[str] = Field(
        default_factory=list
    )
