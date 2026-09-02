from __future__ import annotations

import json

from pathlib import Path

import pytest

from src.evaluation.canonical_dataset import (
    load_canonical_evaluation_dataset,
)


def _row(
    case_id: str,
    *,
    dataset_id: str = "source-a",
    version: str = "1.0",
    is_active: bool = True,
    **extra,
):
    row = {
        "case_id": case_id,
        "dataset_id": dataset_id,
        "version": version,
        "query": f"query-{case_id}",
        "target_document_id":
            f"document-{case_id}",
        "is_active": is_active,
        "comment":
            f"comment-{case_id}",
    }

    row.update(extra)
    return row


def _write_jsonl(
    path: Path,
    rows,
) -> None:
    path.write_text(
        "\n".join(
            json.dumps(row)
            for row in rows
        )
        + "\n",
        encoding="utf-8",
    )


def test_real_canonical_dataset_contract():
    dataset = load_canonical_evaluation_dataset(
        Path(
            "data/eval/canonical/"
            "evaluation_cases_v1.jsonl"
        )
    )

    assert dataset.total_case_count == 21
    assert dataset.active_case_count == 21
    assert dataset.source_dataset_count == 5

    assert dataset.reference_answer_count == 0
    assert dataset.has_reference_answers is False

    assert len(dataset.content_sha256) == 64

    assert (
        dataset.evaluation_dataset_version
        == "evaluation_cases_v1"
    )

    assert dataset.case_schema_version == "1.0"


def test_eval_projection_uses_stable_case_ids(
    tmp_path,
):
    path = tmp_path / "cases_v1.jsonl"

    _write_jsonl(
        path,
        [
            _row("case-1"),
            _row("case-2"),
        ],
    )

    dataset = load_canonical_evaluation_dataset(path)
    examples = dataset.to_eval_examples()

    assert [
        item.example_id
        for item in examples
    ] == [
        "case-1",
        "case-2",
    ]


def test_inactive_cases_excluded_by_default(
    tmp_path,
):
    path = tmp_path / "cases_v1.jsonl"

    _write_jsonl(
        path,
        [
            _row("active"),
            _row(
                "inactive",
                is_active=False,
            ),
        ],
    )

    dataset = load_canonical_evaluation_dataset(path)

    assert dataset.total_case_count == 2
    assert dataset.active_case_count == 1

    examples = dataset.to_eval_examples()

    assert len(examples) == 1
    assert examples[0].example_id == "active"


def test_multiple_source_dataset_ids_are_allowed(
    tmp_path,
):
    path = tmp_path / "cases_v1.jsonl"

    _write_jsonl(
        path,
        [
            _row(
                "case-1",
                dataset_id="source-a",
            ),
            _row(
                "case-2",
                dataset_id="source-b",
            ),
        ],
    )

    dataset = load_canonical_evaluation_dataset(path)

    assert dataset.source_dataset_count == 2

    assert dataset.source_dataset_ids == (
        "source-a",
        "source-b",
    )


def test_collection_identity_comes_from_file_name(
    tmp_path,
):
    path = (
        tmp_path
        / "evaluation_cases_v7.jsonl"
    )

    _write_jsonl(
        path,
        [_row("case-1")],
    )

    dataset = load_canonical_evaluation_dataset(path)

    assert (
        dataset.evaluation_dataset_version
        == "evaluation_cases_v7"
    )


def test_reference_answer_count_is_detected(
    tmp_path,
):
    path = tmp_path / "cases_v1.jsonl"

    _write_jsonl(
        path,
        [
            _row(
                "case-1",
                reference_answer="expected",
            ),
            _row("case-2"),
        ],
    )

    dataset = load_canonical_evaluation_dataset(path)

    assert dataset.reference_answer_count == 1
    assert dataset.has_reference_answers is True


def test_duplicate_case_ids_are_rejected(
    tmp_path,
):
    path = tmp_path / "cases_v1.jsonl"

    _write_jsonl(
        path,
        [
            _row("same"),
            _row("same"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Duplicate canonical",
    ):
        load_canonical_evaluation_dataset(path)


def test_mixed_case_schema_versions_are_rejected(
    tmp_path,
):
    path = tmp_path / "cases_v1.jsonl"

    _write_jsonl(
        path,
        [
            _row(
                "case-1",
                version="1.0",
            ),
            _row(
                "case-2",
                version="2.0",
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="multiple case schema versions",
    ):
        load_canonical_evaluation_dataset(path)


def test_safe_metadata_contains_no_case_content(
    tmp_path,
):
    path = tmp_path / "cases_v1.jsonl"

    _write_jsonl(
        path,
        [_row("case-1")],
    )

    dataset = load_canonical_evaluation_dataset(path)
    metadata = dataset.safe_metadata()

    serialized = repr(metadata)

    assert "query-case-1" not in serialized
    assert "document-case-1" not in serialized
    assert "source-a" not in serialized

    assert metadata["active_case_count"] == 1
    assert metadata["source_dataset_count"] == 1

    assert (
        metadata["evaluation_dataset_version"]
        == "cases_v1"
    )
