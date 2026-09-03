from __future__ import annotations

import pytest

from src.llmops.deterministic_scorers import (
    DETERMINISTIC_SCORERS,
    answer_non_empty,
    citation_present,
    evidence_present,
    expected_document_hit,
)


def _outputs(
    *,
    answer_text="answer",
    document_ids=None,
    evidence_count=1,
    citation_count=1,
):
    return {
        "answer_text":
            answer_text,

        "retrieved_document_ids":
            (
                document_ids
                if document_ids is not None
                else ["doc-1"]
            ),

        "evidence_count":
            evidence_count,

        "citation_count":
            citation_count,
    }


def _value(
    result,
):
    """
    MLflow Scorer direct invocation returns
    Feedback. Keep tests focused on the actual
    deterministic score value.
    """
    return getattr(
        result,
        "value",
        result,
    )


def test_answer_non_empty_passes():
    result = answer_non_empty(
        outputs=_outputs(),
        expectations={
            "expect_non_empty_answer":
                True,
        },
    )

    assert _value(result) is True


def test_empty_answer_fails_when_expected():
    result = answer_non_empty(
        outputs=_outputs(
            answer_text="   ",
        ),
        expectations={
            "expect_non_empty_answer":
                True,
        },
    )

    assert _value(result) is False


def test_empty_answer_passes_when_expected():
    result = answer_non_empty(
        outputs=_outputs(
            answer_text="",
        ),
        expectations={
            "expect_non_empty_answer":
                False,
        },
    )

    assert _value(result) is True


def test_expected_document_hit_passes():
    result = expected_document_hit(
        outputs=_outputs(
            document_ids=[
                "doc-other",
                "doc-target",
            ],
        ),
        expectations={
            "expected_document_id":
                "doc-target",
        },
    )

    assert _value(result) is True


def test_expected_document_hit_fails():
    result = expected_document_hit(
        outputs=_outputs(
            document_ids=[
                "doc-other",
            ],
        ),
        expectations={
            "expected_document_id":
                "doc-target",
        },
    )

    assert _value(result) is False


def test_expected_document_is_required():
    with pytest.raises(
        ValueError,
        match="expected_document_id",
    ):
        expected_document_hit(
            outputs=_outputs(),
            expectations={},
        )


def test_evidence_present_passes():
    result = evidence_present(
        outputs=_outputs(
            evidence_count=3,
        ),
    )

    assert _value(result) is True


def test_evidence_present_fails():
    result = evidence_present(
        outputs=_outputs(
            evidence_count=0,
        ),
    )

    assert _value(result) is False


def test_citation_present_passes():
    result = citation_present(
        outputs=_outputs(
            citation_count=2,
        ),
    )

    assert _value(result) is True


def test_citation_present_fails():
    result = citation_present(
        outputs=_outputs(
            citation_count=0,
        ),
    )

    assert _value(result) is False


def test_negative_counts_are_rejected():
    with pytest.raises(
        ValueError,
        match="non-negative",
    ):
        evidence_present(
            outputs=_outputs(
                evidence_count=-1,
            ),
        )


def test_document_ids_reject_string():
    with pytest.raises(
        TypeError,
        match="sequence",
    ):
        expected_document_hit(
            outputs=_outputs(
                document_ids="doc-target",
            ),
            expectations={
                "expected_document_id":
                    "doc-target",
            },
        )


def test_registered_scorer_set_is_stable():
    assert len(
        DETERMINISTIC_SCORERS
    ) == 4

    names = {
        scorer.name
        for scorer
        in DETERMINISTIC_SCORERS
    }

    assert names == {
        "answer_non_empty",
        "expected_document_hit",
        "evidence_present",
        "citation_present",
    }