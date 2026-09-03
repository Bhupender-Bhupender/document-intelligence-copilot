from __future__ import annotations

import hashlib
import json

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.schema.eval_models import (
    EvalExample,
)


_REFERENCE_ANSWER_FIELDS = (
    "reference_answer",
    "expected_answer",
    "gold_answer",
    "ground_truth",
)


@dataclass(frozen=True)
class CanonicalEvaluationCase:
    """One case from the canonical evaluation collection."""

    case_id: str
    source_dataset_id: str
    version: str
    query: str
    target_document_id: str
    is_active: bool
    comment: str = ""


@dataclass(frozen=True)
class CanonicalEvaluationDataset:
    """Versioned identity for the canonical evaluation collection."""

    evaluation_dataset_version: str
    case_schema_version: str
    content_sha256: str

    cases: tuple[
        CanonicalEvaluationCase,
        ...,
    ]

    reference_answer_count: int = 0

    @property
    def total_case_count(
        self,
    ) -> int:
        return len(self.cases)

    @property
    def active_cases(
        self,
    ) -> tuple[CanonicalEvaluationCase, ...]:
        return tuple(
            case
            for case in self.cases
            if case.is_active
        )

    @property
    def active_case_count(
        self,
    ) -> int:
        return len(self.active_cases)

    @property
    def source_dataset_ids(
        self,
    ) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    case.source_dataset_id
                    for case in self.cases
                }
            )
        )

    @property
    def source_dataset_count(
        self,
    ) -> int:
        return len(self.source_dataset_ids)

    @property
    def has_reference_answers(
        self,
    ) -> bool:
        return self.reference_answer_count > 0

    def safe_metadata(
        self,
    ) -> dict[str, str | int | bool]:
        """Return content-free experiment metadata."""

        return {
            "evaluation_dataset_version":
                self.evaluation_dataset_version,
            "case_schema_version":
                self.case_schema_version,
            "dataset_sha256":
                self.content_sha256,
            "total_case_count":
                self.total_case_count,
            "active_case_count":
                self.active_case_count,
            "source_dataset_count":
                self.source_dataset_count,
            "reference_answer_count":
                self.reference_answer_count,
            "has_reference_answers":
                self.has_reference_answers,
        }

    def to_eval_examples(
        self,
        *,
        active_only: bool = True,
    ) -> list[EvalExample]:
        """Project into the existing deterministic/semantic contract."""

        cases = (
            self.active_cases
            if active_only
            else self.cases
        )

        return [
            EvalExample(
                example_id=case.case_id,
                query=case.query,
                notes=case.comment,
            )
            for case in cases
        ]


def _required_text(
    row: dict[str, Any],
    field: str,
    *,
    line_number: int,
) -> str:
    value = str(
        row.get(field)
        or ""
    ).strip()

    if not value:
        raise ValueError(
            f"Canonical evaluation line {line_number} "
            f"has no {field}."
        )

    return value


def _reference_answer_present(
    row: dict[str, Any],
) -> bool:
    for field in _REFERENCE_ANSWER_FIELDS:
        if field not in row:
            continue

        value = row.get(field)

        if isinstance(value, str):
            if value.strip():
                return True

        elif value is not None:
            return True

    return False


def load_canonical_evaluation_dataset(
    path: Path,
) -> CanonicalEvaluationDataset:
    """Load and validate one canonical JSONL evaluation collection."""

    raw_bytes = path.read_bytes()

    content_sha256 = hashlib.sha256(
        raw_bytes
    ).hexdigest()

    evaluation_dataset_version = (
        path.stem.strip()
    )

    if not evaluation_dataset_version:
        raise ValueError(
            "Canonical evaluation dataset "
            "has no version identity."
        )

    cases: list[CanonicalEvaluationCase] = []
    reference_answer_count = 0

    for line_number, raw_line in enumerate(
        raw_bytes.decode("utf-8").splitlines(),
        start=1,
    ):
        if not raw_line.strip():
            continue

        try:
            row = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid JSON on canonical evaluation "
                f"line {line_number}."
            ) from exc

        if not isinstance(row, dict):
            raise ValueError(
                f"Canonical evaluation line {line_number} "
                "is not a JSON object."
            )

        is_active = row.get(
            "is_active",
            True,
        )

        if not isinstance(is_active, bool):
            raise ValueError(
                f"Canonical evaluation line {line_number} "
                "has a non-boolean is_active."
            )

        cases.append(
            CanonicalEvaluationCase(
                case_id=_required_text(
                    row,
                    "case_id",
                    line_number=line_number,
                ),
                source_dataset_id=_required_text(
                    row,
                    "dataset_id",
                    line_number=line_number,
                ),
                version=_required_text(
                    row,
                    "version",
                    line_number=line_number,
                ),
                query=_required_text(
                    row,
                    "query",
                    line_number=line_number,
                ),
                target_document_id=_required_text(
                    row,
                    "target_document_id",
                    line_number=line_number,
                ),
                is_active=is_active,
                comment=str(
                    row.get("comment")
                    or ""
                ).strip(),
            )
        )

        if _reference_answer_present(row):
            reference_answer_count += 1

    if not cases:
        raise ValueError(
            "Canonical evaluation dataset contains no cases."
        )

    case_ids = [
        case.case_id
        for case in cases
    ]

    if len(case_ids) != len(set(case_ids)):
        raise ValueError(
            "Duplicate canonical evaluation case IDs."
        )

    versions = {
        case.version
        for case in cases
    }

    if len(versions) != 1:
        raise ValueError(
            "Canonical evaluation dataset "
            "contains multiple case schema versions."
        )

    case_schema_version = next(
        iter(versions)
    )

    return CanonicalEvaluationDataset(
        evaluation_dataset_version=(
            evaluation_dataset_version
        ),
        case_schema_version=(
            case_schema_version
        ),
        content_sha256=content_sha256,
        cases=tuple(cases),
        reference_answer_count=(
            reference_answer_count
        ),
    )
