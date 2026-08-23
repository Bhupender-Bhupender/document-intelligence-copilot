from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class RetrievalEvalExample(BaseModel):
    """In-memory retrieval evaluation input."""

    case_id: str = Field(min_length=1)
    query: str = Field(min_length=1)
    expected_document_id: str = Field(
        min_length=1
    )


class RetrievalEvalCaseResult(BaseModel):
    """
    Privacy-safe per-case retrieval result.

    Query text, target document identity,
    retrieved document identities, filenames,
    and retrieved text are intentionally absent.
    """

    case_id: str = Field(min_length=1)

    result_count: int = Field(ge=0)
    latency_ms: float = Field(ge=0)

    zero_result: bool
    metadata_valid: bool

    hit_at_1: bool
    hit_at_3: bool
    hit_at_5: bool
    hit_at_10: bool

    error_type: Optional[str] = None


class RetrievalEvalReport(BaseModel):
    """Aggregate retrieval-only benchmark report."""

    cases_evaluated: int = Field(ge=0)
    top_k: int = Field(ge=10)

    hit_at_1_count: int = Field(ge=0)
    hit_at_1: float = Field(ge=0, le=1)

    hit_at_3_count: int = Field(ge=0)
    hit_at_3: float = Field(ge=0, le=1)

    hit_at_5_count: int = Field(ge=0)
    hit_at_5: float = Field(ge=0, le=1)

    hit_at_10_count: int = Field(ge=0)
    hit_at_10: float = Field(ge=0, le=1)

    zero_result_count: int = Field(ge=0)
    zero_result_rate: float = Field(
        ge=0,
        le=1,
    )

    retrieval_error_count: int = Field(
        ge=0
    )
    retrieval_error_rate: float = Field(
        ge=0,
        le=1,
    )

    metadata_valid_count: int = Field(
        ge=0
    )
    metadata_valid_rate: float = Field(
        ge=0,
        le=1,
    )

    min_results_returned: int = Field(
        ge=0
    )
    max_results_returned: int = Field(
        ge=0
    )
    mean_results_returned: float = Field(
        ge=0
    )

    mean_latency_ms: float = Field(ge=0)
    median_latency_ms: float = Field(ge=0)
    p95_latency_ms: float = Field(ge=0)

    page_hit_scoring: str = (
        "NOT_AVAILABLE_NO_PAGE_LABELS"
    )

    generation_used: bool = False
    llm_judge_used: bool = False

    operational_retrieval_pass: bool
