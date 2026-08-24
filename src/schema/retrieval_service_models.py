from __future__ import annotations

from datetime import date
from typing import List, Literal, Optional

from pydantic import (
    BaseModel,
    Field,
    model_validator,
)

from src.schema.models import CitationRecord


RETRIEVAL_CONFIG_VERSION = (
    "retrieval_service_v1"
)


class RetrievalDateRange(BaseModel):
    """
    Optional retrieval date constraint.

    The current development corpus does not yet
    expose a date field. The contract exists for
    forward compatibility, but the service must
    reject its use until the backend can enforce it.
    """

    start: Optional[date] = None
    end: Optional[date] = None

    @model_validator(mode="after")
    def validate_range(
        self,
    ) -> "RetrievalDateRange":

        if (
            self.start is not None
            and self.end is not None
            and self.start > self.end
        ):
            raise ValueError(
                "date_range.start cannot "
                "be after date_range.end."
            )

        return self


class RetrievalRequest(BaseModel):
    """
    Stable Phase 11 retrieval request contract.

    Security-sensitive filters are explicit.
    Unsupported filters must never be ignored.
    """

    query: str = Field(
        min_length=1
    )

    tenant_id: Optional[str] = None

    allowed_groups: List[str] = Field(
        default_factory=list
    )

    document_ids: List[str] = Field(
        default_factory=list
    )

    date_range: Optional[
        RetrievalDateRange
    ] = None

    top_k: int = Field(
        default=20,
        ge=1,
        le=200,
    )

    final_k: int = Field(
        default=6,
        ge=1,
        le=50,
    )

    include_parent_context: bool = True

    @model_validator(mode="after")
    def validate_request(
        self,
    ) -> "RetrievalRequest":

        self.query = (
            self.query.strip()
        )

        if not self.query:
            raise ValueError(
                "query cannot be blank."
            )

        if self.final_k > self.top_k:
            raise ValueError(
                "final_k cannot exceed top_k."
            )

        # Stable deduplication while preserving
        # caller order.
        self.document_ids = list(
            dict.fromkeys(
                value.strip()
                for value
                in self.document_ids
                if value
                and value.strip()
            )
        )

        self.allowed_groups = list(
            dict.fromkeys(
                value.strip()
                for value
                in self.allowed_groups
                if value
                and value.strip()
            )
        )

        if self.tenant_id is not None:
            self.tenant_id = (
                self.tenant_id.strip()
                or None
            )

        return self


class RetrievalEvidence(BaseModel):
    """
    Citation-ready evidence returned by
    the retrieval service.

    Page anchors always come from the matched
    child chunk. Parent content is contextual
    only and must never expand citation scope.
    """

    chunk_id: str = Field(
        min_length=1
    )

    document_id: str = Field(
        min_length=1
    )

    page_start: int = Field(
        ge=1
    )

    page_end: int = Field(
        ge=1
    )

    section_path: Optional[str] = None

    text: str = Field(
        min_length=1
    )

    parent_text: Optional[str] = None

    score: Optional[float] = None

    retrieval_method: Literal[
        "vector",
        "bm25",
        "hybrid",
    ]

    citation_payload: CitationRecord

    @model_validator(mode="after")
    def validate_pages(
        self,
    ) -> "RetrievalEvidence":

        if self.page_end < self.page_start:
            raise ValueError(
                "page_end cannot precede "
                "page_start."
            )

        return self


class RetrievalResponse(BaseModel):
    """
    Stable service response consumed later by
    generation and API/App layers.
    """

    query_id: str = Field(
        min_length=1
    )

    results: List[
        RetrievalEvidence
    ] = Field(
        default_factory=list
    )

    latency_ms: float = Field(
        ge=0
    )

    index_version: Optional[str] = None

    retrieval_config_version: str = (
        RETRIEVAL_CONFIG_VERSION
    )

    applied_filters: List[str] = Field(
        default_factory=list
    )
