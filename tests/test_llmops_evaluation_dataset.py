from __future__ import annotations

import csv
import json

from pathlib import Path

import pytest

import src.llmops.evaluation_dataset as adapter

from src.llmops.evaluation_dataset import (
    build_version_context_for_dataset,
    load_evaluation_dataset_bundle,
)
from src.schema.retrieval_eval_models import (
    RetrievalEvalExample,
)


def _canonical_row(
    case_id: str,
    document_id: str,
    *,
    dataset_id: str,
):
    return {
        "case_id": case_id,
        "dataset_id": dataset_id,
        "version": "1.0",
        "query": f"PRIVATE_QUERY_{case_id}",
        "target_document_id": document_id,
        "is_active": True,
        "comment": "",
    }


def _write_canonical(
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


def _write_manifest(
    path: Path,
    rows,
) -> None:
    with path.open(
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
        writer.writerows(rows)


def _paths(
    tmp_path,
):
    canonical_path = (
        tmp_path
        / "evaluation_cases_v1.jsonl"
    )

    manifest_path = (
        tmp_path
        / "manifest.csv"
    )

    _write_canonical(
        canonical_path,
        [
            _canonical_row(
                "case-1",
                "baseline-doc-1",
                dataset_id="source-a",
            ),
            _canonical_row(
                "case-2",
                "baseline-doc-2",
                dataset_id="source-b",
            ),
        ],
    )

    _write_manifest(
        manifest_path,
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
        ],
    )

    return (
        canonical_path,
        manifest_path,
    )


def test_bundle_projects_same_cases(
    tmp_path,
):
    canonical_path, manifest_path = (
        _paths(tmp_path)
    )

    bundle = (
        load_evaluation_dataset_bundle(
            canonical_path,
            manifest_path,
        )
    )

    assert (
        bundle.evaluation_dataset_version
        == "evaluation_cases_v1"
    )

    assert bundle.active_case_count == 2

    assert [
        item.example_id
        for item in bundle.eval_examples
    ] == [
        "case-1",
        "case-2",
    ]

    assert [
        item.case_id
        for item in bundle.retrieval_examples
    ] == [
        "case-1",
        "case-2",
    ]


def test_source_dataset_groups_do_not_change_identity(
    tmp_path,
):
    canonical_path, manifest_path = (
        _paths(tmp_path)
    )

    bundle = (
        load_evaluation_dataset_bundle(
            canonical_path,
            manifest_path,
        )
    )

    assert (
        bundle.dataset.source_dataset_count
        == 2
    )

    assert (
        bundle.evaluation_dataset_version
        == "evaluation_cases_v1"
    )


def test_retrieval_projection_uses_current_hash_ids(
    tmp_path,
):
    canonical_path, manifest_path = (
        _paths(tmp_path)
    )

    bundle = (
        load_evaluation_dataset_bundle(
            canonical_path,
            manifest_path,
        )
    )

    assert [
        item.expected_document_id
        for item in bundle.retrieval_examples
    ] == [
        "doc_aaaaaaaaaaaaaaaa",
        "doc_bbbbbbbbbbbbbbbb",
    ]


def test_safe_metadata_contains_no_case_content(
    tmp_path,
):
    canonical_path, manifest_path = (
        _paths(tmp_path)
    )

    bundle = (
        load_evaluation_dataset_bundle(
            canonical_path,
            manifest_path,
        )
    )

    metadata = bundle.safe_metadata()
    serialized = repr(metadata)

    assert "PRIVATE_QUERY" not in serialized
    assert "baseline-doc" not in serialized
    assert "doc_aaaaaaaa" not in serialized
    assert "source-a" not in serialized
    assert "source-b" not in serialized

    assert (
        metadata[
            "evaluation_dataset_version"
        ]
        == "evaluation_cases_v1"
    )

    assert (
        metadata[
            "eval_example_count"
        ]
        == 2
    )

    assert (
        metadata[
            "retrieval_example_count"
        ]
        == 2
    )


def test_version_context_uses_bundle_identity(
    tmp_path,
):
    canonical_path, manifest_path = (
        _paths(tmp_path)
    )

    bundle = (
        load_evaluation_dataset_bundle(
            canonical_path,
            manifest_path,
        )
    )

    context = (
        build_version_context_for_dataset(
            bundle,
            generation_model=(
                "generation-model"
            ),
            embedding_model=(
                "embedding-model"
            ),
            index_name="index-name",
            code_revision="abc123",
        )
    )

    assert (
        context.evaluation_dataset_version
        == "evaluation_cases_v1"
    )

    assert (
        context.generation_model
        == "generation-model"
    )

    assert (
        context.embedding_model
        == "embedding-model"
    )

    assert (
        context.index_name
        == "index-name"
    )

    assert (
        context.code_revision
        == "abc123"
    )


def test_retrieval_projection_drift_is_rejected(
    tmp_path,
    monkeypatch,
):
    canonical_path, manifest_path = (
        _paths(tmp_path)
    )

    def wrong_projection(
        canonical_path,
        corpus_manifest_path,
    ):
        return [
            RetrievalEvalExample(
                case_id="different-case",
                query="PRIVATE_QUERY",
                expected_document_id=(
                    "doc_aaaaaaaaaaaaaaaa"
                ),
            )
        ]

    monkeypatch.setattr(
        adapter,
        "load_databricks_retrieval_examples",
        wrong_projection,
    )

    with pytest.raises(
        ValueError,
        match=(
            "Retrieval evaluation projection "
            "is not aligned"
        ),
    ):
        load_evaluation_dataset_bundle(
            canonical_path,
            manifest_path,
        )