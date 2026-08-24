from __future__ import annotations

from typing import List, Optional, Sequence

from src.citations.citation_builder import (
    build_citations,
)
from src.schema.models import (
    CitationRecord,
    DocumentChunk,
    RetrievedChunk,
)
from src.schema.retrieval_service_models import (
    RetrievalEvidence,
)


class RetrievalEvidenceContractError(
    RuntimeError
):
    """Evidence could not satisfy its contract."""


def _best_score(
    chunk: RetrievedChunk,
) -> Optional[float]:
    """
    Return the score representing the latest
    ranking stage available on the chunk.
    """

    for value in (
        chunk.rerank_score,
        chunk.fusion_score,
        chunk.vector_score,
        chunk.bm25_score,
    ):
        if value is not None:
            return float(value)

    return None


def _citation_matches_child(
    citation: CitationRecord,
    child: RetrievedChunk,
) -> bool:
    """
    Evidence-level wrong-page guard.

    Citation identity must refer to the exact
    matched child chunk, not its parent context.
    """

    return all([
        citation.source_chunk_id
            == child.chunk_id,

        citation.doc_id
            == child.doc_id,

        citation.file_name
            == child.file_name,

        citation.page_number
            == child.page_number,

        (
            citation.section_title is None
            or child.section_title is None
            or citation.section_title
                == child.section_title
        ),
    ])


def _validate_parent_alignment(
    child: RetrievedChunk,
    parent: Optional[DocumentChunk],
) -> None:

    if parent is None:
        return

    if not child.parent_chunk_id:
        raise RetrievalEvidenceContractError(
            "Parent returned for a child "
            "without parent_chunk_id."
        )

    if (
        parent.chunk_id
        != child.parent_chunk_id
    ):
        raise RetrievalEvidenceContractError(
            "Parent/child chunk identity "
            "misalignment detected."
        )

    if parent.doc_id != child.doc_id:
        raise RetrievalEvidenceContractError(
            "Parent/child document lineage "
            "misalignment detected."
        )


def build_retrieval_evidence(
    retrieved: Sequence[
        RetrievedChunk
    ],
    *,
    parents: Optional[
        Sequence[
            Optional[DocumentChunk]
        ]
    ] = None,
    final_k: int = 6,
    include_parent_context: bool = True,
) -> List[RetrievalEvidence]:
    """
    Convert ranked RetrievedChunks into the
    stable Phase 11 evidence contract.

    Input order is preserved.
    """

    if final_k < 1:
        raise ValueError(
            "final_k must be >= 1."
        )

    selected = list(
        retrieved[:final_k]
    )

    if parents is None:
        aligned_parents: List[
            Optional[DocumentChunk]
        ] = [
            None
            for _ in selected
        ]

    else:
        if len(parents) < len(selected):
            raise RetrievalEvidenceContractError(
                "Parent lookup returned fewer "
                "rows than selected children."
            )

        aligned_parents = list(
            parents[:len(selected)]
        )

    citations = build_citations(
        selected
    )

    if len(citations) != len(selected):
        raise RetrievalEvidenceContractError(
            "Citation builder result count "
            "does not match selected evidence."
        )

    evidence: List[
        RetrievalEvidence
    ] = []

    for (
        child,
        parent,
        citation,
    ) in zip(
        selected,
        aligned_parents,
        citations,
    ):

        _validate_parent_alignment(
            child,
            parent,
        )

        if not _citation_matches_child(
            citation,
            child,
        ):
            raise RetrievalEvidenceContractError(
                "Citation does not match "
                "retrieved child evidence."
            )

        # Parent text is supplemental context.
        # It never changes page_start/page_end.
        parent_text = None

        if (
            include_parent_context
            and parent is not None
        ):
            parent_text = (
                parent.text
                or None
            )

        section_path = (
            child.section_title
            or (
                parent.section_title
                if parent is not None
                else None
            )
        )

        method = (
            child.retrieval_method
            or "hybrid"
        )

        evidence.append(
            RetrievalEvidence(
                chunk_id=child.chunk_id,
                document_id=child.doc_id,

                # Wrong-page guard:
                # citation anchors remain child-level.
                page_start=(
                    child.page_number
                ),
                page_end=(
                    child.page_number
                ),

                section_path=(
                    section_path
                ),

                text=child.text,

                parent_text=(
                    parent_text
                ),

                score=_best_score(
                    child
                ),

                retrieval_method=method,

                citation_payload=(
                    citation
                ),
            )
        )

    return evidence
