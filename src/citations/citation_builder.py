"""
Deterministic citation builder: RetrievedChunk → CitationRecord.

Public API
----------
    build_citations(chunks: List[RetrievedChunk]) -> List[CitationRecord]

Design
------
``build_citations`` is a pure, stateless function. It accepts the list of
retrieved/reranked chunks used as synthesis context and returns a parallel
list of ``CitationRecord`` objects — one per chunk.

The mapping is entirely deterministic: same input always produces the same
output. No random identifiers, no model calls, no I/O.

Citation ID strategy
--------------------
``citation_id`` is computed from stable, immutable source fields:

    key  = "{doc_id}:{chunk_id}:{page_number}"
    hash = sha256(key.encode()).hexdigest()[:32]

This guarantees:
    - Same chunk always gets the same citation_id, regardless of call order.
    - Different chunks (different chunk_id or doc_id or page_number)
      always get different citation_ids (collision probability negligible).
    - No UUID generation at citation time.

Quote semantics
---------------
``quote_text`` is set to the full retrieved chunk passage. This is the text
the language model used as synthesis context. It is a passage-level citation:
honest, deterministic, and directly sourced from the retrieval layer.

This is NOT quote-span extraction from the generated answer text. That
operation belongs to a later step if ever needed. The intent here is to
provide an auditable record of which passages grounded the answer.

``quote_start_char`` and ``quote_end_char`` are the character offsets of the
full passage within itself (0 and len(chunk.text)). They establish the
citation span boundary for Phase 8 validation, which can verify that answer
sentences appear within these bounds.

``is_verbatim=True`` means the cited passage appears verbatim in the source
chunk — which is trivially true since quote_text IS the chunk text.

Validation contract
-------------------
``validation_status`` is always ``"unverified"``. The rule-based validator
in Phase 8 will update this field. Citation construction never assigns
``"valid"`` or ``"invalid"``.

Empty input
-----------
``build_citations([])`` returns ``[]``. No error, no placeholder records.
"""
from __future__ import annotations

import hashlib
from typing import List

from src.schema.models import CitationRecord, RetrievedChunk


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_citation_id(chunk: RetrievedChunk) -> str:
    """
    Derive a deterministic citation identifier from stable chunk fields.

    The key is: "{doc_id}:{chunk_id}:{page_number}"
    The output is the first 32 hex characters of the SHA-256 digest.

    Args:
        chunk: The source chunk. Only doc_id, chunk_id, and page_number are used.

    Returns:
        32-character lowercase hex string.
    """
    key = f"{chunk.doc_id}:{chunk.chunk_id}:{chunk.page_number}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_citations(chunks: List[RetrievedChunk]) -> List[CitationRecord]:
    """
    Build a deterministic citation record for each retrieved chunk.

    One ``CitationRecord`` is produced per input chunk. The output list
    is parallel to the input list (index i of output corresponds to index
    i of input).

    The ``quote_text`` for each citation is the full retrieved passage used
    as synthesis context — not a quote extracted from the generated answer.

    Args:
        chunks: Retrieved (and optionally reranked) child chunks from the
                retrieval and reranking stages. May be empty.

    Returns:
        List of CitationRecord, same length as chunks.
        Empty list when chunks is empty.
    """
    return [
        CitationRecord(
            citation_id=_make_citation_id(chunk),
            doc_id=chunk.doc_id,
            file_name=chunk.file_name,
            page_number=chunk.page_number,
            section_title=chunk.section_title,
            source_chunk_id=chunk.chunk_id,
            quote_text=chunk.text,
            quote_start_char=0,
            quote_end_char=len(chunk.text),
            is_verbatim=True,
            validation_status="unverified",
        )
        for chunk in chunks
    ]
