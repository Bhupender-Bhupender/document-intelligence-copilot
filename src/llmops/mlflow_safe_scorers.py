from __future__ import annotations

import re

from typing import Any

from mlflow.genai.scorers import scorer

from src.llmops.deterministic_scorers import (
    citation_present,
    evidence_present,
)


_FINGERPRINT_PATTERN = re.compile(
    r"^[0-9a-f]{64}$"
)


def _require_bool(
    value: Any,
    *,
    field: str,
) -> bool:
    if not isinstance(
        value,
        bool,
    ):
        raise TypeError(
            f"{field} must be a boolean."
        )

    return value


def _require_fingerprint(
    value: Any,
    *,
    field: str,
) -> str:
    if not isinstance(
        value,
        str,
    ):
        raise TypeError(
            f"{field} must be a string."
        )

    if not _FINGERPRINT_PATTERN.fullmatch(
        value
    ):
        raise ValueError(
            f"{field} must be a 64-character "
            "lowercase hexadecimal fingerprint."
        )

    return value


@scorer
def answer_expectation_met(
    outputs: dict[str, Any],
    expectations: dict[str, Any],
) -> bool:
    """
    Compare locally derived answer presence with
    the canonical non-empty-answer expectation.

    Raw answer text never crosses into MLflow.
    """
    actual = _require_bool(
        outputs.get(
            "answer_present"
        ),
        field="answer_present",
    )

    expected = _require_bool(
        expectations.get(
            "expect_non_empty_answer"
        ),
        field="expect_non_empty_answer",
    )

    return actual == expected


@scorer
def expected_document_fingerprint_hit(
    outputs: dict[str, Any],
    expectations: dict[str, Any],
) -> bool:
    """
    Compare privacy-safe document fingerprints.

    Raw document IDs never cross into MLflow.
    """
    expected = _require_fingerprint(
        expectations.get(
            "expected_document_fingerprint"
        ),
        field=(
            "expected_document_fingerprint"
        ),
    )

    fingerprints = outputs.get(
        "retrieved_document_fingerprints"
    )

    if isinstance(
        fingerprints,
        str,
    ):
        raise TypeError(
            "retrieved_document_fingerprints "
            "must be a sequence."
        )

    if fingerprints is None:
        fingerprints = []

    try:
        validated = tuple(
            _require_fingerprint(
                value,
                field=(
                    "retrieved_document_fingerprint"
                ),
            )
            for value in fingerprints
        )

    except TypeError as exc:
        raise TypeError(
            "retrieved_document_fingerprints "
            "must be iterable."
        ) from exc

    return expected in validated


MLFLOW_SAFE_DETERMINISTIC_SCORERS = (
    answer_expectation_met,
    expected_document_fingerprint_hit,
    evidence_present,
    citation_present,
)