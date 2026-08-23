import json

from pathlib import Path

import pytest

from src.evaluation.canonical_retrieval_dataset import (
    load_databricks_retrieval_examples,
)


def _write_manifest(
    path: Path,
    sha: str,
):
    path.write_text(
        (
            "document_id,sha256\n"
            f"baseline-doc,{sha}\n"
        ),
        encoding="utf-8",
    )


def _write_cases(
    path: Path,
    *,
    case_id: str = "case-1",
):
    row = {
        "case_id": case_id,
        "query": "test query",
        "target_document_id":
            "baseline-doc",
        "is_active": True,
    }

    path.write_text(
        json.dumps(row) + "\n",
        encoding="utf-8",
    )


def test_maps_sha_to_databricks_document_id(
    tmp_path,
):
    canonical = (
        tmp_path / "cases.jsonl"
    )

    manifest = (
        tmp_path / "manifest.csv"
    )

    sha = "a" * 64

    _write_cases(
        canonical
    )

    _write_manifest(
        manifest,
        sha,
    )

    examples = (
        load_databricks_retrieval_examples(
            canonical,
            manifest,
        )
    )

    assert len(examples) == 1
    assert (
        examples[0]
        .expected_document_id
        == "doc_" + ("a" * 16)
    )


def test_invalid_sha_is_rejected(
    tmp_path,
):
    canonical = (
        tmp_path / "cases.jsonl"
    )

    manifest = (
        tmp_path / "manifest.csv"
    )

    _write_cases(
        canonical
    )

    _write_manifest(
        manifest,
        "invalid",
    )

    with pytest.raises(
        ValueError,
        match="invalid SHA-256",
    ):
        load_databricks_retrieval_examples(
            canonical,
            manifest,
        )


def test_duplicate_case_ids_are_rejected(
    tmp_path,
):
    canonical = (
        tmp_path / "cases.jsonl"
    )

    manifest = (
        tmp_path / "manifest.csv"
    )

    row = {
        "case_id": "same-case",
        "query": "test query",
        "target_document_id":
            "baseline-doc",
        "is_active": True,
    }

    canonical.write_text(
        (
            json.dumps(row)
            + "\n"
            + json.dumps(row)
            + "\n"
        ),
        encoding="utf-8",
    )

    _write_manifest(
        manifest,
        "a" * 64,
    )

    with pytest.raises(
        ValueError,
        match="Duplicate active",
    ):
        load_databricks_retrieval_examples(
            canonical,
            manifest,
        )
