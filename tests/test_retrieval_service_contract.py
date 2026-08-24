import pytest

from src.retrieval.evidence_builder import (
    RetrievalEvidenceContractError,
    build_retrieval_evidence,
)
from src.schema.models import (
    DocumentChunk,
    RetrievedChunk,
)
from src.schema.retrieval_service_models import (
    RetrievalRequest,
)


def _child(
    *,
    chunk_id="child-1",
    doc_id="doc-1",
    page_number=4,
    parent_chunk_id="parent-1",
    fusion_score=0.8,
):
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id=f"page-{page_number}",
        file_name="source.pdf",
        page_number=page_number,
        section_title="Risk",
        text="child evidence text",
        word_count=3,
        retrieval_method="hybrid",
        vector_score=None,
        bm25_score=None,
        fusion_score=fusion_score,
        rerank_score=None,
        parent_chunk_id=(
            parent_chunk_id
        ),
        file_type="pdf",
    )


def _parent(
    *,
    chunk_id="parent-1",
    doc_id="doc-1",
    page_number=99,
):
    return DocumentChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id="parent-page",
        file_name="source.pdf",
        page_number=page_number,
        section_title="Parent section",
        text="larger parent context",
        word_count=3,
        chunk_index=0,
        chunk_level="parent",
        parent_chunk_id=None,
        file_type="pdf",
    )


def test_request_requires_final_k_not_exceed_top_k():
    with pytest.raises(
        ValueError,
        match="final_k",
    ):
        RetrievalRequest(
            query="test",
            top_k=5,
            final_k=6,
        )


def test_request_deduplicates_document_allowlist():
    request = RetrievalRequest(
        query="test",
        document_ids=[
            "doc-1",
            "doc-1",
            "doc-2",
        ],
    )

    assert request.document_ids == [
        "doc-1",
        "doc-2",
    ]


def test_evidence_preserves_child_page_anchor():
    child = _child(
        page_number=4
    )

    # Deliberately use a different parent page
    # to prove parent context cannot broaden
    # the citation page.
    parent = _parent(
        page_number=99
    )

    evidence = (
        build_retrieval_evidence(
            [child],
            parents=[parent],
            final_k=1,
            include_parent_context=True,
        )
    )

    result = evidence[0]

    assert result.page_start == 4
    assert result.page_end == 4

    assert (
        result.citation_payload.page_number
        == 4
    )

    assert (
        result.parent_text
        == "larger parent context"
    )


def test_parent_identity_mismatch_is_rejected():
    child = _child(
        parent_chunk_id="parent-1"
    )

    parent = _parent(
        chunk_id="wrong-parent"
    )

    with pytest.raises(
        RetrievalEvidenceContractError,
        match="identity",
    ):
        build_retrieval_evidence(
            [child],
            parents=[parent],
            final_k=1,
        )


def test_parent_document_mismatch_is_rejected():
    child = _child(
        doc_id="doc-1"
    )

    parent = _parent(
        doc_id="doc-2"
    )

    with pytest.raises(
        RetrievalEvidenceContractError,
        match="document lineage",
    ):
        build_retrieval_evidence(
            [child],
            parents=[parent],
            final_k=1,
        )


def test_score_uses_fusion_when_no_rerank():
    evidence = (
        build_retrieval_evidence(
            [_child(
                fusion_score=0.73
            )],
            parents=[_parent()],
            final_k=1,
        )
    )

    assert evidence[0].score == pytest.approx(
        0.73
    )


def test_final_k_preserves_rank_order():
    children = [
        _child(
            chunk_id=f"child-{i}",
            parent_chunk_id=None,
        )
        for i in range(1, 5)
    ]

    evidence = (
        build_retrieval_evidence(
            children,
            final_k=2,
            include_parent_context=False,
        )
    )

    assert [
        result.chunk_id
        for result in evidence
    ] == [
        "child-1",
        "child-2",
    ]


def test_parent_context_can_be_disabled():
    evidence = (
        build_retrieval_evidence(
            [_child()],
            parents=[_parent()],
            final_k=1,
            include_parent_context=False,
        )
    )

    assert (
        evidence[0].parent_text
        is None
    )
