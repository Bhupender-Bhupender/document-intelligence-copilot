from __future__ import annotations

import hashlib
import hmac
import secrets

from collections.abc import Mapping
from typing import Any

from src.llmops.evaluation_dataset import (
    EvaluationDatasetBundle,
)


def generate_document_fingerprint_key() -> bytes:
    """
    Generate one ephemeral key for a single
    evaluation run.

    The key must remain local and must not be
    logged to MLflow.
    """
    return secrets.token_bytes(
        32
    )


def fingerprint_document_id(
    document_id: str,
    *,
    key: bytes,
) -> str:
    """
    Convert an internal document ID into an
    evaluation-run-specific HMAC fingerprint.
    """
    if not isinstance(
        key,
        bytes,
    ):
        raise TypeError(
            "Document fingerprint key must "
            "be bytes."
        )

    if len(key) < 16:
        raise ValueError(
            "Document fingerprint key must "
            "contain at least 16 bytes."
        )

    normalized = str(
        document_id
        or ""
    ).strip()

    if not normalized:
        raise ValueError(
            "Cannot fingerprint an empty "
            "document ID."
        )

    return hmac.new(
        key,
        normalized.encode(
            "utf-8"
        ),
        hashlib.sha256,
    ).hexdigest()


def _require_non_negative_int(
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

    if not isinstance(
        value,
        int,
    ):
        raise TypeError(
            f"{field} must be an integer."
        )

    if value < 0:
        raise ValueError(
            f"{field} must be non-negative."
        )

    return value


def _validate_local_output(
    output: Mapping[str, Any],
) -> tuple[
    str,
    tuple[str, ...],
    int,
    int,
]:
    answer_text = output.get(
        "answer_text"
    )

    if not isinstance(
        answer_text,
        str,
    ):
        raise TypeError(
            "answer_text must be a string."
        )

    raw_document_ids = output.get(
        "retrieved_document_ids"
    )

    if isinstance(
        raw_document_ids,
        str,
    ):
        raise TypeError(
            "retrieved_document_ids must "
            "be a sequence."
        )

    if raw_document_ids is None:
        raw_document_ids = []

    try:
        document_ids = tuple(
            str(value or "").strip()
            for value
            in raw_document_ids
        )

    except TypeError as exc:
        raise TypeError(
            "retrieved_document_ids must "
            "be iterable."
        ) from exc

    if any(
        not value
        for value in document_ids
    ):
        raise ValueError(
            "retrieved_document_ids contains "
            "an empty document ID."
        )

    evidence_count = (
        _require_non_negative_int(
            output.get(
                "evidence_count"
            ),
            field="evidence_count",
        )
    )

    citation_count = (
        _require_non_negative_int(
            output.get(
                "citation_count"
            ),
            field="citation_count",
        )
    )

    if (
        evidence_count
        != len(document_ids)
    ):
        raise ValueError(
            "evidence_count does not match "
            "retrieved document count."
        )

    return (
        answer_text,
        document_ids,
        evidence_count,
        citation_count,
    )


def build_mlflow_safe_evaluation_data(
    bundle: EvaluationDatasetBundle,
    outputs_by_case_id: Mapping[
        str,
        Mapping[str, Any],
    ],
    *,
    document_fingerprint_key: bytes | None = None,
) -> list[dict[str, Any]]:
    """
    Convert local deterministic evaluation
    outputs into a representation safe for
    persistence by MLflow GenAI evaluation.

    Raw queries, answers, case IDs, and document
    IDs are deliberately excluded.
    """
    key = (
        document_fingerprint_key
        if document_fingerprint_key
        is not None
        else generate_document_fingerprint_key()
    )

    active_case_ids = tuple(
        case.case_id
        for case
        in bundle.dataset.active_cases
    )

    supplied_case_ids = set(
        outputs_by_case_id
    )

    if (
        supplied_case_ids
        != set(active_case_ids)
    ):
        raise ValueError(
            "Local evaluation outputs do not "
            "match the active canonical cases."
        )

    rows: list[
        dict[str, Any]
    ] = []

    for case_index, (
        case,
        eval_example,
        retrieval_example,
    ) in enumerate(
        zip(
            bundle.dataset.active_cases,
            bundle.eval_examples,
            bundle.retrieval_examples,
            strict=True,
        ),
        start=1,
    ):
        local_output = (
            outputs_by_case_id[
                case.case_id
            ]
        )

        if not isinstance(
            local_output,
            Mapping,
        ):
            raise TypeError(
                "Local evaluation output must "
                "be a mapping."
            )

        (
            answer_text,
            document_ids,
            evidence_count,
            citation_count,
        ) = _validate_local_output(
            local_output
        )

        document_fingerprints = [
            fingerprint_document_id(
                document_id,
                key=key,
            )
            for document_id
            in document_ids
        ]

        expected_fingerprint = (
            fingerprint_document_id(
                retrieval_example
                .expected_document_id,
                key=key,
            )
        )

        rows.append(
            {
                "inputs": {
                    "evaluation_case_index":
                        case_index,
                },

                "outputs": {
                    "answer_present":
                        bool(
                            answer_text.strip()
                        ),

                    "retrieved_document_fingerprints":
                        document_fingerprints,

                    "evidence_count":
                        evidence_count,

                    "citation_count":
                        citation_count,
                },

                "expectations": {
                    "expected_document_fingerprint":
                        expected_fingerprint,

                    "expect_non_empty_answer":
                        eval_example
                        .expect_non_empty_answer,
                },
            }
        )

    return rows