import pytest

from src.retrieval.retrieval_service import (
    RetrievalFilterViolationError,
    UnsupportedRetrievalFilterError,
    run_retrieval_service,
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
    doc_id="doc-allowed",
    page_number=3,
    parent_chunk_id="parent-1",
):
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id=f"page-{page_number}",
        file_name="source.pdf",
        page_number=page_number,
        section_title="Section",
        text="retrieved evidence",
        word_count=2,
        retrieval_method="hybrid",
        vector_score=None,
        bm25_score=None,
        fusion_score=0.91,
        rerank_score=None,
        parent_chunk_id=(
            parent_chunk_id
        ),
        file_type="pdf",
    )


def _parent(
    *,
    chunk_id="parent-1",
    doc_id="doc-allowed",
):
    return DocumentChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id="parent-page",
        page_number=3,
        file_name="source.pdf",
        file_type="pdf",
        section_title="Section",
        text="larger parent context",
        word_count=3,
        chunk_index=0,
        chunk_level="parent",
        parent_chunk_id=None,
    )


def test_document_filter_is_forwarded():
    captured = {}

    def retrieve(
        query,
        top_k,
        filters,
    ):
        captured["query"] = query
        captured["top_k"] = top_k
        captured["filters"] = filters

        return [
            _child()
        ]

    response = run_retrieval_service(
        RetrievalRequest(
            query="test",
            document_ids=[
                "doc-allowed"
            ],
            top_k=10,
            final_k=1,
            include_parent_context=False,
        ),
        _retrieve=retrieve,
    )

    assert captured["top_k"] == 10

    assert captured["filters"] == {
        "document_id": [
            "doc-allowed"
        ]
    }

    assert response.applied_filters == [
        "document_ids"
    ]


def test_unfiltered_request_passes_no_filter():
    captured = {}

    def retrieve(
        query,
        top_k,
        filters,
    ):
        captured["filters"] = filters
        return [_child()]

    run_retrieval_service(
        RetrievalRequest(
            query="test",
            final_k=1,
            include_parent_context=False,
        ),
        _retrieve=retrieve,
    )

    assert (
        captured["filters"]
        is None
    )


def test_backend_cannot_escape_document_allowlist():
    def retrieve(
        query,
        top_k,
        filters,
    ):
        return [
            _child(
                doc_id="doc-not-allowed"
            )
        ]

    with pytest.raises(
        RetrievalFilterViolationError,
        match="allow-list",
    ):
        run_retrieval_service(
            RetrievalRequest(
                query="test",
                document_ids=[
                    "doc-allowed"
                ],
                final_k=1,
                include_parent_context=False,
            ),
            _retrieve=retrieve,
        )


@pytest.mark.parametrize(
    "retrieval_request",
    [
        RetrievalRequest(
            query="test",
            tenant_id="tenant-a",
        ),
        RetrievalRequest(
            query="test",
            allowed_groups=[
                "group-a"
            ],
        ),
    ],
)
def test_unsupported_security_filters_fail_closed(
    retrieval_request,
):
    called = False

    def retrieve(
        query,
        top_k,
        filters,
    ):
        nonlocal called
        called = True
        return []

    with pytest.raises(
        UnsupportedRetrievalFilterError
    ):
        run_retrieval_service(
            retrieval_request,
            _retrieve=retrieve,
        )

    # Critical:
    # search must never happen if an
    # authorization filter cannot be enforced.
    assert called is False


def test_date_filter_fails_closed():
    from datetime import date

    from src.schema.retrieval_service_models import (
        RetrievalDateRange,
    )

    request = RetrievalRequest(
        query="test",
        date_range=RetrievalDateRange(
            start=date(
                2026,
                1,
                1,
            )
        ),
    )

    with pytest.raises(
        UnsupportedRetrievalFilterError
    ):
        run_retrieval_service(
            request,
            _retrieve=(
                lambda q, k, f: []
            ),
        )


def test_parent_context_and_citation_are_aligned():
    child = _child()
    parent = _parent()

    response = run_retrieval_service(
        RetrievalRequest(
            query="test",
            final_k=1,
            include_parent_context=True,
        ),
        _retrieve=(
            lambda q, k, f: [
                child
            ]
        ),
        _parent_lookup=(
            lambda chunks: [
                parent
            ]
        ),
    )

    assert len(
        response.results
    ) == 1

    result = response.results[0]

    assert (
        result.parent_text
        == parent.text
    )

    assert (
        result.page_start
        == child.page_number
    )

    assert (
        result.page_end
        == child.page_number
    )

    assert (
        result
        .citation_payload
        .source_chunk_id
        == child.chunk_id
    )

    assert (
        result
        .citation_payload
        .page_number
        == child.page_number
    )


def test_final_k_limits_parent_lookup():
    children = [
        _child(
            chunk_id=f"child-{i}",
            parent_chunk_id=f"parent-{i}",
        )
        for i in range(1, 5)
    ]

    requested_parent_count = []

    def parent_lookup(
        selected,
    ):
        requested_parent_count.append(
            len(selected)
        )

        return [
            _parent(
                chunk_id=(
                    item.parent_chunk_id
                )
            )
            for item in selected
        ]

    response = run_retrieval_service(
        RetrievalRequest(
            query="test",
            top_k=4,
            final_k=2,
        ),
        _retrieve=(
            lambda q, k, f:
                children
        ),
        _parent_lookup=parent_lookup,
    )

    assert len(
        response.results
    ) == 2

    assert requested_parent_count == [
        2
    ]


def test_parent_lookup_skipped_when_disabled():
    called = False

    def parent_lookup(
        chunks,
    ):
        nonlocal called
        called = True
        return []

    run_retrieval_service(
        RetrievalRequest(
            query="test",
            final_k=1,
            include_parent_context=False,
        ),
        _retrieve=(
            lambda q, k, f: [
                _child()
            ]
        ),
        _parent_lookup=parent_lookup,
    )

    assert called is False
