from __future__ import annotations

import csv
import json

from pathlib import Path

from src.schema.retrieval_eval_models import (
    RetrievalEvalExample,
)


_HEX = frozenset(
    "0123456789abcdef"
)


def _valid_sha256(value: str) -> bool:
    return (
        len(value) == 64
        and all(
            char in _HEX
            for char in value
        )
    )


def load_databricks_retrieval_examples(
    canonical_path: Path,
    corpus_manifest_path: Path,
) -> list[RetrievalEvalExample]:
    """
    Load active canonical cases and map their
    baseline document identity to the current
    Databricks content-hash document identity.

    No source files or document content are read.
    """

    canonical_rows = [
        json.loads(line)
        for line in canonical_path.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]

    active_rows = [
        row
        for row in canonical_rows
        if row.get(
            "is_active",
            True,
        ) is not False
    ]

    case_ids = [
        str(
            row.get("case_id")
            or ""
        )
        for row in active_rows
    ]

    if (
        len(case_ids)
        != len(set(case_ids))
    ):
        raise ValueError(
            "Duplicate active evaluation case IDs."
        )

    with corpus_manifest_path.open(
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        manifest_rows = list(
            csv.DictReader(handle)
        )

    manifest_by_document: dict[
        str,
        dict[str, str],
    ] = {}

    for row in manifest_rows:

        document_id = str(
            row.get("document_id")
            or ""
        )

        if not document_id:
            raise ValueError(
                "Corpus manifest contains "
                "an empty document_id."
            )

        existing_row = (
            manifest_by_document.get(
                document_id
            )
        )

        if existing_row is not None:

            existing_sha = str(
                existing_row.get("sha256")
                or ""
            ).lower()

            current_sha = str(
                row.get("sha256")
                or ""
            ).lower()

            # Repeated identical identity mappings
            # are harmless and can arise from
            # historical baseline inventory rows.
            #
            # A document_id pointing to two
            # different hashes is a real identity
            # conflict and must never be hidden.
            if existing_sha != current_sha:
                raise ValueError(
                    "Conflicting SHA-256 values "
                    "for duplicate document_id "
                    "in corpus manifest."
                )

            continue

        manifest_by_document[
            document_id
        ] = row

    examples: list[
        RetrievalEvalExample
    ] = []

    baseline_to_databricks: dict[
        str,
        str,
    ] = {}

    for row in active_rows:

        case_id = str(
            row.get("case_id")
            or ""
        )

        query = str(
            row.get("query")
            or ""
        ).strip()

        baseline_document_id = str(
            row.get(
                "target_document_id"
            )
            or ""
        )

        if not case_id:
            raise ValueError(
                "Active case has no case_id."
            )

        if not query:
            raise ValueError(
                f"Case {case_id} has an "
                "empty query."
            )

        if not baseline_document_id:
            raise ValueError(
                f"Case {case_id} has no "
                "target_document_id."
            )

        manifest_row = (
            manifest_by_document.get(
                baseline_document_id
            )
        )

        if manifest_row is None:
            raise ValueError(
                f"Case {case_id} target "
                "is absent from corpus manifest."
            )

        sha256 = str(
            manifest_row.get("sha256")
            or ""
        ).lower()

        if not _valid_sha256(
            sha256
        ):
            raise ValueError(
                f"Case {case_id} maps to "
                "an invalid SHA-256."
            )

        databricks_document_id = (
            f"doc_{sha256[:16]}"
        )

        previous = (
            baseline_to_databricks.get(
                baseline_document_id
            )
        )

        if (
            previous is not None
            and previous
                != databricks_document_id
        ):
            raise ValueError(
                "Inconsistent identity mapping."
            )

        baseline_to_databricks[
            baseline_document_id
        ] = databricks_document_id

        examples.append(
            RetrievalEvalExample(
                case_id=case_id,
                query=query,
                expected_document_id=(
                    databricks_document_id
                ),
            )
        )

    # Separate canonical documents should
    # not silently collapse to the same
    # current document identity.
    if (
        len(
            set(
                baseline_to_databricks.values()
            )
        )
        != len(
            baseline_to_databricks
        )
    ):
        raise ValueError(
            "Derived Databricks document "
            "identity collision detected."
        )

    return examples
