"""
Rule-based citation and response validator.

Public API
----------
    validate_response(response: AnswerResponse) -> AnswerResponse

Design
------
``validate_response`` is a pure, stateless function. It accepts a finished
``AnswerResponse`` (with ``sources`` and ``supporting_chunks`` already
populated) and returns a new ``AnswerResponse`` with:

    - Each ``CitationRecord.validation_status`` promoted from "unverified"
      to "valid" or "invalid".
    - ``AnswerResponse.validation_flags`` populated with diagnostic strings.

The function is deterministic: same input always produces same output.
No model calls, no I/O.

Citation validation rules
--------------------------
Applied in order; first failure sets ``validation_status = "invalid"`` and
returns immediately (no further checks). All rules pass → "valid".

    1. ``source_chunk_id is None``
    2. ``source_chunk_id`` not found in ``supporting_chunks``
    3. ``citation.doc_id != chunk.doc_id``
    4. ``citation.file_name != chunk.file_name``
    5. ``citation.page_number != chunk.page_number``
    6. Section-title mismatch — **conditional**: only when both
       ``citation.section_title`` and ``chunk.section_title`` are non-None
       and they do not match. Missing optional titles are not an error.
    7. Verbatim span consistency — only when ``citation.is_verbatim=True``:
       a. ``quote_start_char`` and ``quote_end_char`` must both be non-None
          and satisfy ``0 <= start <= end <= len(chunk.text)``.
       b. ``chunk.text[start:end] == citation.quote_text``

Response-level validation flags
---------------------------------
Appended to ``validation_flags`` deterministically:

    "no_supporting_chunks"          — ``supporting_chunks`` is empty
    "no_sources"                    — ``sources`` is empty
    "citation_chunk_count_mismatch" — ``len(sources) != len(supporting_chunks)``
    "missing_source_chunk_id"       — any citation has ``source_chunk_id is None``
    "invalid_citation_present"      — any validated citation is "invalid"
"""
from __future__ import annotations

from typing import Dict, List

from src.schema.models import AnswerResponse, CitationRecord, RetrievedChunk
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #


def validate_response(response: AnswerResponse) -> AnswerResponse:
    """
    Validate citations against supporting chunks and populate validation_flags.

    Parameters
    ----------
    response:
        A finished AnswerResponse whose ``sources`` and ``supporting_chunks``
        are already populated (e.g. after citation construction in Stage 5).

    Returns
    -------
    AnswerResponse
        New instance produced via ``model_copy`` with:
        - ``sources``: each CitationRecord has ``validation_status`` set to
          "valid" or "invalid".
        - ``validation_flags``: response-level diagnostic strings.
    """
    chunk_lookup: Dict[str, RetrievedChunk] = {
        c.chunk_id: c for c in response.supporting_chunks
    }

    validated_citations = [
        _validate_citation(citation, chunk_lookup)
        for citation in response.sources
    ]

    flags = _build_flags(response, validated_citations)

    logger.debug(
        "validation_done",
        total=len(validated_citations),
        valid=sum(1 for c in validated_citations if c.validation_status == "valid"),
        invalid=sum(1 for c in validated_citations if c.validation_status == "invalid"),
        flag_count=len(flags),
    )

    return response.model_copy(
        update={"sources": validated_citations, "validation_flags": flags}
    )


# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #


def _validate_citation(
    citation: CitationRecord,
    chunk_lookup: Dict[str, RetrievedChunk],
) -> CitationRecord:
    """
    Promote a single citation to "valid" or "invalid" deterministically.

    Parameters
    ----------
    citation:
        The CitationRecord to validate (expected status: "unverified").
    chunk_lookup:
        ``{chunk_id: RetrievedChunk}`` built once from supporting_chunks.

    Returns
    -------
    CitationRecord
        New instance with ``validation_status`` set to "valid" or "invalid".
    """
    # Rule 1: missing source_chunk_id
    if citation.source_chunk_id is None:
        return citation.model_copy(update={"validation_status": "invalid"})

    # Rule 2: no matching chunk in supporting evidence
    chunk = chunk_lookup.get(citation.source_chunk_id)
    if chunk is None:
        return citation.model_copy(update={"validation_status": "invalid"})

    # Rule 3: doc_id mismatch
    if citation.doc_id != chunk.doc_id:
        return citation.model_copy(update={"validation_status": "invalid"})

    # Rule 4: file_name mismatch
    if citation.file_name != chunk.file_name:
        return citation.model_copy(update={"validation_status": "invalid"})

    # Rule 5: page_number mismatch
    if citation.page_number != chunk.page_number:
        return citation.model_copy(update={"validation_status": "invalid"})

    # Rule 6: conditional section_title mismatch
    # Only fires when both sides are explicitly present.
    if citation.section_title is not None and chunk.section_title is not None:
        if citation.section_title != chunk.section_title:
            return citation.model_copy(update={"validation_status": "invalid"})

    # Rule 7: verbatim span consistency
    if citation.is_verbatim:
        start = citation.quote_start_char
        end = citation.quote_end_char
        text = chunk.text

        # 7a: span must be non-None and in-bounds
        if start is None or end is None or not (0 <= start <= end <= len(text)):
            return citation.model_copy(update={"validation_status": "invalid"})

        # 7b: exact slice equality
        if text[start:end] != citation.quote_text:
            return citation.model_copy(update={"validation_status": "invalid"})

    return citation.model_copy(update={"validation_status": "valid"})


def _build_flags(
    response: AnswerResponse,
    validated_citations: List[CitationRecord],
) -> List[str]:
    """
    Build response-level validation flags deterministically.

    Parameters
    ----------
    response:
        The original AnswerResponse (pre-update; sources still "unverified").
    validated_citations:
        Citations after ``_validate_citation`` has been applied.

    Returns
    -------
    List[str]
        Ordered diagnostic flag strings. Empty when the response is clean.
    """
    flags: List[str] = []

    if len(response.supporting_chunks) == 0:
        flags.append("no_supporting_chunks")

    if len(response.sources) == 0:
        flags.append("no_sources")

    if len(response.sources) != len(response.supporting_chunks):
        flags.append("citation_chunk_count_mismatch")

    if any(c.source_chunk_id is None for c in response.sources):
        flags.append("missing_source_chunk_id")

    if any(c.validation_status == "invalid" for c in validated_citations):
        flags.append("invalid_citation_present")

    return flags
