"""
Project-native evaluation data contracts.

These models are intentionally separate from the pipeline schema (models.py)
to keep pipeline contracts clean and evaluation contracts independently
versionable.

Public classes
--------------
    EvalExample   — one evaluation query with optional deterministic expectations
    EvalReport    — structured result of one evaluation run
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List

from pydantic import BaseModel, Field


def _new_eval_id() -> str:
    return str(uuid.uuid4())


# --------------------------------------------------------------------------- #
# Evaluation example                                                           #
# --------------------------------------------------------------------------- #


class EvalExample(BaseModel):
    """
    One evaluation query with optional deterministic expectations.

    Fields
    ------
    example_id:
        Auto-generated unique identifier for this example.
    query:
        The natural language question to run through the pipeline.
    expected_source_chunk_ids:
        Chunk IDs that should appear as citation sources in the response.
        Used for source_hit_rate. Empty = not scored.
    expected_file_names:
        File names that should appear in citations.
        Used for file_hit_rate. Empty = not scored.
    expected_page_numbers:
        Page numbers that — combined with expected_file_names — form
        (file_name, page_number) pairs that should appear in citations.
        Used for page_hit_rate. Only scored when expected_file_names is
        also non-empty. Empty = not scored.
    expect_non_empty_answer:
        Whether a non-empty answer_text is expected. Used for
        answer_non_empty_rate.
    expect_citations_valid:
        When True, this example is scored for all-citations-valid:
        every citation in the response must have validation_status="valid".
        Used for citations_all_valid_rate. False = not scored.
    notes:
        Free-text annotation for this example. Not used in metrics.
    """

    example_id: str = Field(default_factory=_new_eval_id)
    query: str
    expected_source_chunk_ids: List[str] = Field(default_factory=list)
    expected_file_names: List[str] = Field(default_factory=list)
    expected_page_numbers: List[int] = Field(default_factory=list)
    expect_non_empty_answer: bool = True
    expect_citations_valid: bool = False
    notes: str = ""


# --------------------------------------------------------------------------- #
# Evaluation report                                                            #
# --------------------------------------------------------------------------- #


class EvalReport(BaseModel):
    """
    Structured result of one evaluation run.

    All rates are float in [0.0, 1.0]. Zero-denominator handling:
    - Total-based rates (answer_non_empty_rate, no_source_rate, etc.) are 0.0
      when total == 0.
    - Restricted-denominator rates (source_hit_rate, file_hit_rate,
      page_hit_rate, citations_all_valid_rate) are 0.0 when no examples
      provide the corresponding expectation type.

    Fields
    ------
    report_id:
        Auto-generated unique identifier for this report.
    total:
        Total number of evaluation examples run.

    --- Total-based metrics ---
    answer_non_empty_count / answer_non_empty_rate:
        Examples where answer_text.strip() != "".
    citation_valid_count / citation_valid_rate:
        Total citations (across all examples) with validation_status="valid",
        over total citations. 0.0 when no citations exist.
    invalid_citation_count / invalid_citation_rate:
        Same denominator — total citations — for invalid citations.
    no_source_count / no_source_rate:
        Examples where response.sources is empty.
    no_supporting_chunk_count / no_supporting_chunk_rate:
        Examples where response.supporting_chunks is empty.

    --- Restricted-denominator metrics ---
    source_hit_count / source_hit_rate:
        Over examples with expected_source_chunk_ids non-empty.
        Hit = at least one expected chunk ID appears in response sources.
    file_hit_count / file_hit_rate:
        Over examples with expected_file_names non-empty.
        Hit = at least one expected file name appears in response sources.
    page_hit_count / page_hit_rate:
        Over examples with both expected_file_names and expected_page_numbers
        non-empty. Hit = at least one (file_name, page_number) pair from the
        cartesian product appears in response sources.
    citations_all_valid_count / citations_all_valid_rate:
        Over examples with expect_citations_valid=True.
        Hit = all citations valid AND at least one citation exists.

    --- Aggregates ---
    flag_frequency:
        Per-flag count aggregated across all responses.
    per_example:
        Lightweight per-example summary dicts. Keys:
        example_id, query, answer_non_empty, source_count,
        supporting_chunk_count, validation_flags.
    """

    report_id: str = Field(default_factory=_new_eval_id)
    total: int

    # Total-based
    answer_non_empty_count: int
    answer_non_empty_rate: float
    citation_valid_count: int
    citation_valid_rate: float
    invalid_citation_count: int
    invalid_citation_rate: float
    no_source_count: int
    no_source_rate: float
    no_supporting_chunk_count: int
    no_supporting_chunk_rate: float

    # Restricted-denominator
    source_hit_count: int
    source_hit_rate: float
    file_hit_count: int
    file_hit_rate: float
    page_hit_count: int
    page_hit_rate: float
    citations_all_valid_count: int
    citations_all_valid_rate: float

    # Aggregates
    flag_frequency: Dict[str, int]
    per_example: List[Dict[str, Any]]
