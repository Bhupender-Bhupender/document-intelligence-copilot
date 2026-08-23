from types import SimpleNamespace

import pytest

from src.evaluation.retrieval_evaluator import (
    run_retrieval_evaluation,
)
from src.schema.retrieval_eval_models import (
    RetrievalEvalExample,
)


def _chunk(
    document_id: str,
    *,
    rank: int,
    parent_chunk_id: str | None = "parent",
):
    return SimpleNamespace(
        chunk_id=f"chunk-{rank}",
        doc_id=document_id,
        page_id=f"page-{rank}",
        file_name="test.pdf",
        page_number=rank,
        section_title=None,
        text="retrieved text",
        word_count=2,
        retrieval_method="hybrid",
        vector_score=None,
        bm25_score=None,
        fusion_score=(
            1.0 / rank
        ),
        rerank_score=None,
        parent_chunk_id=(
            parent_chunk_id
        ),
        file_type="pdf",
    )


def _example():
    return RetrievalEvalExample(
        case_id="case-1",
        query="private query",
        expected_document_id=(
            "doc-target"
        ),
    )


def test_hit_at_k_preserves_chunk_rank_order():
    retrieved = [
        _chunk(
            "wrong-a",
            rank=1,
        ),
        _chunk(
            "wrong-a",
            rank=2,
        ),
        _chunk(
            "wrong-b",
            rank=3,
        ),
        _chunk(
            "doc-target",
            rank=4,
        ),
    ]

    report, cases = (
        run_retrieval_evaluation(
            [_example()],
            _retrieve=(
                lambda query, top_k:
                    retrieved
            ),
        )
    )

    result = cases[0]

    assert result.hit_at_1 is False
    assert result.hit_at_3 is False
    assert result.hit_at_5 is True
    assert result.hit_at_10 is True

    assert report.hit_at_1 == 0.0
    assert report.hit_at_3 == 0.0
    assert report.hit_at_5 == 1.0
    assert report.hit_at_10 == 1.0


def test_error_remains_in_denominator():
    def failing_retrieve(
        query,
        top_k,
    ):
        raise RuntimeError(
            "retrieval failure"
        )

    report, cases = (
        run_retrieval_evaluation(
            [_example()],
            _retrieve=failing_retrieve,
        )
    )

    assert (
        report.retrieval_error_count
        == 1
    )
    assert report.hit_at_10 == 0.0
    assert report.zero_result_count == 1
    assert (
        report.operational_retrieval_pass
        is False
    )

    assert (
        cases[0].error_type
        == "RuntimeError"
    )


def test_invalid_metadata_fails_contract():
    retrieved = [
        _chunk(
            "doc-target",
            rank=1,
            parent_chunk_id=None,
        )
    ]

    report, _ = (
        run_retrieval_evaluation(
            [_example()],
            _retrieve=(
                lambda query, top_k:
                    retrieved
            ),
        )
    )

    assert report.hit_at_1 == 1.0
    assert (
        report.metadata_valid_rate
        == 0.0
    )
    assert (
        report.operational_retrieval_pass
        is False
    )


def test_zero_result_is_operational_failure():
    report, _ = (
        run_retrieval_evaluation(
            [_example()],
            _retrieve=(
                lambda query, top_k: []
            ),
        )
    )

    assert report.zero_result_count == 1
    assert report.hit_at_10 == 0.0
    assert (
        report.operational_retrieval_pass
        is False
    )


def test_top_k_must_support_hit_at_10():
    with pytest.raises(
        ValueError,
        match="at least 10",
    ):
        run_retrieval_evaluation(
            [_example()],
            top_k=5,
            _retrieve=(
                lambda query, top_k: []
            ),
        )


def test_case_artifact_is_privacy_safe():
    _, cases = (
        run_retrieval_evaluation(
            [_example()],
            _retrieve=(
                lambda query, top_k: [
                    _chunk(
                        "doc-target",
                        rank=1,
                    )
                ]
            ),
        )
    )

    payload = (
        cases[0]
        .model_dump(
            mode="json"
        )
    )

    forbidden = {
        "query",
        "expected_document_id",
        "document_id",
        "doc_id",
        "file_name",
        "text",
    }

    assert not (
        forbidden
        & set(payload)
    )
