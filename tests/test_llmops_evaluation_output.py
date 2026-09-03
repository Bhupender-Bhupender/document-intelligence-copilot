from __future__ import annotations

import pytest

from src.llmops.deterministic_scorers import (
    answer_non_empty,
    citation_present,
    evidence_present,
    expected_document_hit,
)
from src.llmops.evaluation_output import (
    normalize_serving_response,
)
from src.schema.retrieval_service_models import (
    RetrievalEvidence,
)
from src.schema.serving_models import (
    ServingAnswerResponse,
)


def _score_value(
    result,
):
    return getattr(
        result,
        "value",
        result,
    )


def _evidence(
    document_id: str,
) -> RetrievalEvidence:
    return RetrievalEvidence.model_construct(
        document_id=document_id,
    )


def _response(
    *,
    answer_text: str = "Grounded answer.",
    document_ids=None,
    citation_count: int = 1,
) -> ServingAnswerResponse:
    if document_ids is None:
        document_ids = [
            "doc-target",
        ]

    return (
        ServingAnswerResponse.model_construct(
            answer_text=answer_text,
            evidence=[
                _evidence(document_id)
                for document_id
                in document_ids
            ],
            sources=[
                object()
                for _ in range(
                    citation_count
                )
            ],
        )
    )


def test_normalizes_real_serving_contract():
    normalized = normalize_serving_response(
        _response(
            document_ids=[
                "doc-1",
                "doc-2",
            ],
            citation_count=2,
        )
    )

    assert normalized == {
        "answer_text":
            "Grounded answer.",

        "retrieved_document_ids":
            [
                "doc-1",
                "doc-2",
            ],

        "evidence_count":
            2,

        "citation_count":
            2,
    }


def test_document_order_is_preserved():
    normalized = normalize_serving_response(
        _response(
            document_ids=[
                "doc-c",
                "doc-a",
                "doc-b",
            ]
        )
    )

    assert (
        normalized[
            "retrieved_document_ids"
        ]
        == [
            "doc-c",
            "doc-a",
            "doc-b",
        ]
    )


def test_duplicate_document_ids_are_preserved():
    normalized = normalize_serving_response(
        _response(
            document_ids=[
                "doc-a",
                "doc-a",
                "doc-b",
            ]
        )
    )

    assert (
        normalized[
            "retrieved_document_ids"
        ]
        == [
            "doc-a",
            "doc-a",
            "doc-b",
        ]
    )


def test_empty_serving_result_is_normalized():
    normalized = normalize_serving_response(
        _response(
            answer_text="",
            document_ids=[],
            citation_count=0,
        )
    )

    assert normalized == {
        "answer_text": "",
        "retrieved_document_ids": [],
        "evidence_count": 0,
        "citation_count": 0,
    }


def test_empty_document_id_is_rejected():
    response = _response(
        document_ids=[
            "doc-valid",
            "   ",
        ]
    )

    with pytest.raises(
        ValueError,
        match="empty document_id",
    ):
        normalize_serving_response(
            response
        )


def test_normalized_contract_is_content_minimal():
    response = _response(
        answer_text="PRIVATE_ANSWER",
        document_ids=[
            "doc-target",
        ],
    )

    response.query = (
        "PRIVATE_QUERY"
    )

    normalized = (
        normalize_serving_response(
            response
        )
    )

    assert set(
        normalized
    ) == {
        "answer_text",
        "retrieved_document_ids",
        "evidence_count",
        "citation_count",
    }

    serialized = repr(
        normalized
    )

    assert (
        "PRIVATE_QUERY"
        not in serialized
    )


def test_expected_document_scorer_uses_evidence_identity():
    normalized = normalize_serving_response(
        _response(
            document_ids=[
                "doc-other",
                "doc-target",
            ]
        )
    )

    result = expected_document_hit(
        outputs=normalized,
        expectations={
            "expected_document_id":
                "doc-target",
        },
    )

    assert (
        _score_value(result)
        is True
    )


def test_all_deterministic_scorers_accept_normalized_output():
    normalized = normalize_serving_response(
        _response(
            answer_text="answer",
            document_ids=[
                "doc-target",
            ],
            citation_count=1,
        )
    )

    expectations = {
        "expected_document_id":
            "doc-target",

        "expect_non_empty_answer":
            True,
    }

    results = [
        answer_non_empty(
            outputs=normalized,
            expectations=expectations,
        ),
        expected_document_hit(
            outputs=normalized,
            expectations=expectations,
        ),
        evidence_present(
            outputs=normalized,
        ),
        citation_present(
            outputs=normalized,
        ),
    ]

    assert all(
        _score_value(result)
        is True
        for result in results
    )