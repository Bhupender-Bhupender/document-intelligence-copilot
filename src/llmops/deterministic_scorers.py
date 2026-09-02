from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from mlflow.genai.scorers import scorer


def _as_mapping(
    value: Any,
) -> Mapping[str, Any]:
    if isinstance(
        value,
        Mapping,
    ):
        return value

    model_dump = getattr(
        value,
        "model_dump",
        None,
    )

    if callable(
        model_dump
    ):
        dumped = model_dump()

        if isinstance(
            dumped,
            Mapping,
        ):
            return dumped

    raise TypeError(
        "Evaluation output must be a mapping "
        "or expose model_dump()."
    )


def _non_empty_text(
    value: Any,
) -> bool:
    return (
        isinstance(
            value,
            str,
        )
        and bool(
            value.strip()
        )
    )


def _non_negative_int(
    value: Any,
    *,
    field: str,
) -> int:
    if isinstance(
        value,
        bool,
    ):
        raise TypeError(
            f"{field} must be an integer."
        )

    try:
        result = int(
            value
        )

    except (
        TypeError,
        ValueError,
    ) as exc:
        raise TypeError(
            f"{field} must be an integer."
        ) from exc

    if result < 0:
        raise ValueError(
            f"{field} must be non-negative."
        )

    return result


def _document_ids(
    outputs: Mapping[str, Any],
) -> tuple[str, ...]:
    raw = outputs.get(
        "retrieved_document_ids",
        [],
    )

    if raw is None:
        return ()

    if isinstance(
        raw,
        str,
    ):
        raise TypeError(
            "retrieved_document_ids must "
            "be a sequence, not a string."
        )

    try:
        values = tuple(
            str(value).strip()
            for value in raw
        )

    except TypeError as exc:
        raise TypeError(
            "retrieved_document_ids must "
            "be iterable."
        ) from exc

    if any(
        not value
        for value in values
    ):
        raise ValueError(
            "retrieved_document_ids contains "
            "an empty document ID."
        )

    return values


@scorer
def answer_non_empty(
    outputs: dict[str, Any],
    expectations: dict[str, Any],
) -> bool:
    """
    Score whether answer presence matches the
    deterministic dataset expectation.
    """
    mapped = _as_mapping(
        outputs
    )

    expected = bool(
        expectations.get(
            "expect_non_empty_answer",
            True,
        )
    )

    actual = _non_empty_text(
        mapped.get(
            "answer_text"
        )
    )

    return actual == expected


@scorer
def expected_document_hit(
    outputs: dict[str, Any],
    expectations: dict[str, Any],
) -> bool:
    """
    Score whether the expected document occurs
    in the normalized retrieval evidence.
    """
    expected = str(
        expectations.get(
            "expected_document_id"
        )
        or ""
    ).strip()

    if not expected:
        raise ValueError(
            "expected_document_id is required "
            "for expected_document_hit."
        )

    mapped = _as_mapping(
        outputs
    )

    return expected in _document_ids(
        mapped
    )


@scorer
def evidence_present(
    outputs: dict[str, Any],
) -> bool:
    """
    Score whether at least one evidence item was
    returned to generation.
    """
    mapped = _as_mapping(
        outputs
    )

    count = _non_negative_int(
        mapped.get(
            "evidence_count",
            0,
        ),
        field="evidence_count",
    )

    return count > 0


@scorer
def citation_present(
    outputs: dict[str, Any],
) -> bool:
    """
    Score whether at least one citation/source
    was returned with the answer.
    """
    mapped = _as_mapping(
        outputs
    )

    count = _non_negative_int(
        mapped.get(
            "citation_count",
            0,
        ),
        field="citation_count",
    )

    return count > 0


DETERMINISTIC_SCORERS = (
    answer_non_empty,
    expected_document_hit,
    evidence_present,
    citation_present,
)