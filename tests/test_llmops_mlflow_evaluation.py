from __future__ import annotations

import csv
import json

from pathlib import Path

import pytest

from src.llmops.evaluation_dataset import (
    load_evaluation_dataset_bundle,
)
from src.llmops.mlflow_evaluation import (
    build_case_query_lookup,
    build_mlflow_evaluation_data,
    make_case_predict_fn,
)


_PRIVATE_QUERY_1 = (
    "PRIVATE_QUERY_CASE_1"
)

_PRIVATE_QUERY_2 = (
    "PRIVATE_QUERY_CASE_2"
)


def _write_fixture(
    tmp_path: Path,
):
    canonical_path = (
        tmp_path
        / "evaluation_cases_v1.jsonl"
    )

    manifest_path = (
        tmp_path
        / "manifest.csv"
    )

    rows = [
        {
            "case_id": "case-1",
            "dataset_id": "source-a",
            "version": "1.0",
            "query": _PRIVATE_QUERY_1,
            "target_document_id":
                "baseline-doc-1",
            "is_active": True,
            "comment": "",
        },
        {
            "case_id": "case-2",
            "dataset_id": "source-b",
            "version": "1.0",
            "query": _PRIVATE_QUERY_2,
            "target_document_id":
                "baseline-doc-2",
            "is_active": True,
            "comment": "",
        },
    ]

    canonical_path.write_text(
        "\n".join(
            json.dumps(row)
            for row in rows
        )
        + "\n",
        encoding="utf-8",
    )

    with manifest_path.open(
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

        writer.writerows(
            [
                {
                    "document_id":
                        "baseline-doc-1",
                    "sha256":
                        "a" * 64,
                },
                {
                    "document_id":
                        "baseline-doc-2",
                    "sha256":
                        "b" * 64,
                },
            ]
        )

    return (
        load_evaluation_dataset_bundle(
            canonical_path,
            manifest_path,
        )
    )


def test_default_mlflow_rows_are_query_private(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    rows = (
        build_mlflow_evaluation_data(
            bundle
        )
    )

    assert len(rows) == 2

    assert rows[0]["inputs"] == {
        "case_id": "case-1",
    }

    serialized = repr(
        rows
    )

    assert (
        _PRIVATE_QUERY_1
        not in serialized
    )

    assert (
        _PRIVATE_QUERY_2
        not in serialized
    )


def test_rows_include_existing_expectations(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    rows = (
        build_mlflow_evaluation_data(
            bundle
        )
    )

    assert (
        rows[0]["expectations"]
        [
            "expected_document_id"
        ]
        == "doc_aaaaaaaaaaaaaaaa"
    )

    assert (
        rows[1]["expectations"]
        [
            "expected_document_id"
        ]
        == "doc_bbbbbbbbbbbbbbbb"
    )

    assert (
        rows[0]["expectations"]
        [
            "expect_non_empty_answer"
        ]
        is True
    )


def test_reference_answer_is_not_synthesized(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    rows = (
        build_mlflow_evaluation_data(
            bundle
        )
    )

    serialized = repr(
        rows
    ).lower()

    assert (
        "reference_answer"
        not in serialized
    )

    assert (
        "expected_answer"
        not in serialized
    )

    assert (
        "expected_response"
        not in serialized
    )


def test_query_inclusion_requires_explicit_opt_in(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    rows = (
        build_mlflow_evaluation_data(
            bundle,
            include_query=True,
        )
    )

    assert (
        rows[0]["inputs"]["query"]
        == _PRIVATE_QUERY_1
    )

    assert (
        rows[1]["inputs"]["query"]
        == _PRIVATE_QUERY_2
    )


def test_case_query_lookup_is_local_mapping(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    lookup = (
        build_case_query_lookup(
            bundle
        )
    )

    assert lookup == {
        "case-1":
            _PRIVATE_QUERY_1,
        "case-2":
            _PRIVATE_QUERY_2,
    }


def test_predict_fn_resolves_case_to_query(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    seen = []

    def predict_query(
        query: str,
    ):
        seen.append(
            query
        )

        return {
            "answer": "result",
        }

    predict_fn = (
        make_case_predict_fn(
            bundle,
            predict_query=(
                predict_query
            ),
        )
    )

    result = predict_fn(
        case_id="case-1"
    )

    assert seen == [
        _PRIVATE_QUERY_1,
    ]

    assert result == {
        "answer": "result",
    }


def test_predict_fn_accepts_matching_opt_in_query(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    predict_fn = (
        make_case_predict_fn(
            bundle,
            predict_query=(
                lambda query: query
            ),
        )
    )

    result = predict_fn(
        case_id="case-1",
        query=_PRIVATE_QUERY_1,
    )

    assert (
        result
        == _PRIVATE_QUERY_1
    )


def test_predict_fn_rejects_query_drift(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    predict_fn = (
        make_case_predict_fn(
            bundle,
            predict_query=(
                lambda query: query
            ),
        )
    )

    with pytest.raises(
        ValueError,
        match=(
            "does not match canonical"
        ),
    ):
        predict_fn(
            case_id="case-1",
            query="WRONG_QUERY",
        )


def test_predict_fn_rejects_unknown_case(
    tmp_path,
):
    bundle = _write_fixture(
        tmp_path
    )

    predict_fn = (
        make_case_predict_fn(
            bundle,
            predict_query=(
                lambda query: query
            ),
        )
    )

    with pytest.raises(
        ValueError,
        match="Unknown evaluation case",
    ):
        predict_fn(
            case_id="missing-case"
        )