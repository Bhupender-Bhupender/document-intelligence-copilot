from __future__ import annotations

import csv
import json

from pathlib import Path

import pytest

from src.llmops.evaluation_dataset import (
    load_evaluation_dataset_bundle,
)
from src.llmops.mlflow_safe_evaluation import (
    build_mlflow_safe_evaluation_data,
    fingerprint_document_id,
)
from src.llmops.deterministic_scorers import (
    citation_present,
    evidence_present,
)
from src.llmops.mlflow_safe_scorers import (
    MLFLOW_SAFE_DETERMINISTIC_SCORERS,
    answer_expectation_met,
    expected_document_fingerprint_hit,
)


_PRIVATE_CASE_ID = (
    "PRIVATE_CASE_ID"
)

_PRIVATE_QUERY = (
    "PRIVATE_QUERY"
)

_PRIVATE_ANSWER = (
    "PRIVATE_ANSWER"
)

_PRIVATE_DOCUMENT_ID = (
    "doc_private_document"
)


def _value(
    result,
):
    return getattr(
        result,
        "value",
        result,
    )


def _bundle(
    tmp_path: Path,
):
    canonical = (
        tmp_path
        / "evaluation_cases_v1.jsonl"
    )

    manifest = (
        tmp_path
        / "manifest.csv"
    )

    canonical.write_text(
        json.dumps(
            {
                "case_id":
                    _PRIVATE_CASE_ID,
                "dataset_id":
                    "source-private",
                "version":
                    "1.0",
                "query":
                    _PRIVATE_QUERY,
                "target_document_id":
                    "baseline-private",
                "is_active":
                    True,
                "comment":
                    "",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with manifest.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "document_id",
                "sha256",
            ],
        )

        writer.writeheader()

        writer.writerow(
            {
                "document_id":
                    "baseline-private",
                "sha256":
                    "a" * 64,
            }
        )

    return (
        load_evaluation_dataset_bundle(
            canonical,
            manifest,
        )
    )


def _local_outputs(
    bundle,
):
    expected_document_id = (
        bundle
        .retrieval_examples[0]
        .expected_document_id
    )

    return {
        _PRIVATE_CASE_ID: {
            "answer_text":
                _PRIVATE_ANSWER,

            "retrieved_document_ids":
                [
                    expected_document_id,
                ],

            "evidence_count":
                1,

            "citation_count":
                1,
        }
    }


def test_fingerprint_is_deterministic_for_same_key():
    key = b"k" * 32

    first = fingerprint_document_id(
        _PRIVATE_DOCUMENT_ID,
        key=key,
    )

    second = fingerprint_document_id(
        _PRIVATE_DOCUMENT_ID,
        key=key,
    )

    assert first == second
    assert len(first) == 64
    assert _PRIVATE_DOCUMENT_ID not in first


def test_different_keys_produce_different_fingerprints():
    first = fingerprint_document_id(
        _PRIVATE_DOCUMENT_ID,
        key=b"a" * 32,
    )

    second = fingerprint_document_id(
        _PRIVATE_DOCUMENT_ID,
        key=b"b" * 32,
    )

    assert first != second


def test_safe_rows_exclude_private_values(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    rows = (
        build_mlflow_safe_evaluation_data(
            bundle,
            _local_outputs(
                bundle
            ),
            document_fingerprint_key=(
                b"k" * 32
            ),
        )
    )

    serialized = repr(
        rows
    )

    assert _PRIVATE_CASE_ID not in serialized
    assert _PRIVATE_QUERY not in serialized
    assert _PRIVATE_ANSWER not in serialized

    expected_document_id = (
        bundle
        .retrieval_examples[0]
        .expected_document_id
    )

    assert (
        expected_document_id
        not in serialized
    )


def test_safe_row_shape(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    rows = (
        build_mlflow_safe_evaluation_data(
            bundle,
            _local_outputs(
                bundle
            ),
            document_fingerprint_key=(
                b"k" * 32
            ),
        )
    )

    row = rows[0]

    assert row["inputs"] == {
        "evaluation_case_index": 1,
    }

    assert set(
        row["outputs"]
    ) == {
        "answer_present",
        "retrieved_document_fingerprints",
        "evidence_count",
        "citation_count",
    }

    assert set(
        row["expectations"]
    ) == {
        "expected_document_fingerprint",
        "expect_non_empty_answer",
    }


def test_expected_fingerprint_matches_retrieved(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    rows = (
        build_mlflow_safe_evaluation_data(
            bundle,
            _local_outputs(
                bundle
            ),
            document_fingerprint_key=(
                b"k" * 32
            ),
        )
    )

    row = rows[0]

    expected = (
        row["expectations"]
        [
            "expected_document_fingerprint"
        ]
    )

    retrieved = (
        row["outputs"]
        [
            "retrieved_document_fingerprints"
        ]
    )

    assert expected in retrieved


def test_safe_answer_scorer_passes(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    row = (
        build_mlflow_safe_evaluation_data(
            bundle,
            _local_outputs(
                bundle
            ),
            document_fingerprint_key=(
                b"k" * 32
            ),
        )[0]
    )

    result = answer_expectation_met(
        outputs=row["outputs"],
        expectations=(
            row["expectations"]
        ),
    )

    assert _value(result) is True


def test_safe_document_scorer_passes(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    row = (
        build_mlflow_safe_evaluation_data(
            bundle,
            _local_outputs(
                bundle
            ),
            document_fingerprint_key=(
                b"k" * 32
            ),
        )[0]
    )

    result = (
        expected_document_fingerprint_hit(
            outputs=row["outputs"],
            expectations=(
                row["expectations"]
            ),
        )
    )

    assert _value(result) is True


def test_all_safe_scorers_pass(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    row = (
        build_mlflow_safe_evaluation_data(
            bundle,
            _local_outputs(
                bundle
            ),
            document_fingerprint_key=(
                b"k" * 32
            ),
        )[0]
    )

    outputs = row["outputs"]
    expectations = row["expectations"]

    results = [
        answer_expectation_met(
            outputs=outputs,
            expectations=expectations,
        ),
        expected_document_fingerprint_hit(
            outputs=outputs,
            expectations=expectations,
        ),
        evidence_present(
            outputs=outputs,
        ),
        citation_present(
            outputs=outputs,
        ),
    ]

    assert all(
        _value(result)
        is True
        for result in results
    )

    assert {
        scorer.name
        for scorer
        in MLFLOW_SAFE_DETERMINISTIC_SCORERS
    } == {
        "answer_expectation_met",
        "expected_document_fingerprint_hit",
        "evidence_present",
        "citation_present",
    }


def test_missing_case_output_is_rejected(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    with pytest.raises(
        ValueError,
        match="active canonical cases",
    ):
        build_mlflow_safe_evaluation_data(
            bundle,
            {},
            document_fingerprint_key=(
                b"k" * 32
            ),
        )


def test_evidence_count_drift_is_rejected(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    outputs = _local_outputs(
        bundle
    )

    outputs[
        _PRIVATE_CASE_ID
    ][
        "evidence_count"
    ] = 2

    with pytest.raises(
        ValueError,
        match="evidence_count",
    ):
        build_mlflow_safe_evaluation_data(
            bundle,
            outputs,
            document_fingerprint_key=(
                b"k" * 32
            ),
        )