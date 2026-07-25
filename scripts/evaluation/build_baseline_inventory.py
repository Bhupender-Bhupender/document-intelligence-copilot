from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASELINE_DIR = PROJECT_ROOT / "docs" / "baseline"

DOCUMENT_ROOTS = {
    "raw": PROJECT_ROOT / "data" / "raw",
    "sample": PROJECT_ROOT / "docs" / "sample_docs",
    "smoke": PROJECT_ROOT / "data" / "eval" / "smoke" / "pdfs",
}

QUESTION_ROOTS = [
    PROJECT_ROOT / "data" / "eval" / "kpi_queries",
    PROJECT_ROOT / "data" / "eval" / "smoke" / "kpi_queries",
]

SUPPORTED_EXTENSIONS = {
    ".pdf",
    ".txt",
    ".md",
    ".docx",
    ".xlsx",
    ".csv",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)

    return digest.hexdigest()


def count_cases(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)

    if isinstance(payload, dict):
        queries = payload.get("queries")

        if isinstance(queries, list):
            return len(queries)

        if isinstance(queries, dict):
            return len(queries)

        for key in (
            "questions",
            "items",
            "cases",
            "kpi_queries",
            "evaluation_cases",
        ):
            value = payload.get(key)

            if isinstance(value, list):
                return len(value)

            if isinstance(value, dict):
                return len(value)

        return 1

    return 0


def build_document_inventory() -> tuple[int, int]:
    public_rows: list[dict[str, object]] = []
    private_rows: list[dict[str, str]] = []

    for source_group, root in DOCUMENT_ROOTS.items():
        if not root.exists():
            continue

        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue

            extension = path.suffix.lower()

            if extension not in SUPPORTED_EXTENSIONS:
                continue

            file_hash = sha256_file(path)
            document_id = f"doc_{file_hash[:12]}"

            public_rows.append(
                {
                    "document_id": document_id,
                    "source_group": source_group,
                    "file_type": extension.removeprefix("."),
                    "size_bytes": path.stat().st_size,
                    "sha256": file_hash,
                }
            )

            private_rows.append(
                {
                    "document_id": document_id,
                    "original_path": str(path),
                }
            )

    public_path = BASELINE_DIR / "corpus_manifest.csv"
    private_path = BASELINE_DIR / "corpus_local_mapping.csv"

    with public_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "document_id",
                "source_group",
                "file_type",
                "size_bytes",
                "sha256",
            ],
        )
        writer.writeheader()
        writer.writerows(public_rows)

    with private_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["document_id", "original_path"],
        )
        writer.writeheader()
        writer.writerows(private_rows)

    return len(public_rows), sum(int(row["size_bytes"]) for row in public_rows)


def build_question_inventory() -> tuple[int, int]:
    public_rows: list[dict[str, object]] = []
    private_rows: list[dict[str, str]] = []

    total_questions = 0

    for root in QUESTION_ROOTS:
        if not root.exists():
            continue

        for path in sorted(root.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                status = "valid"
                question_count = count_cases(payload)
            except (OSError, json.JSONDecodeError):
                status = "invalid"
                question_count = 0

            file_hash = sha256_file(path)
            dataset_id = f"eval_{file_hash[:12]}"

            public_rows.append(
                {
                    "dataset_id": dataset_id,
                    "format": "json",
                    "question_count": question_count,
                    "validation_status": status,
                    "sha256": file_hash,
                }
            )

            private_rows.append(
                {
                    "dataset_id": dataset_id,
                    "original_path": str(path),
                }
            )

            total_questions += question_count

    public_path = BASELINE_DIR / "evaluation_inventory.csv"
    private_path = BASELINE_DIR / "evaluation_local_mapping.csv"

    with public_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "dataset_id",
                "format",
                "question_count",
                "validation_status",
                "sha256",
            ],
        )
        writer.writeheader()
        writer.writerows(public_rows)

    with private_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["dataset_id", "original_path"],
        )
        writer.writeheader()
        writer.writerows(private_rows)

    return len(public_rows), total_questions


def main() -> None:
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)

    document_count, total_bytes = build_document_inventory()
    dataset_count, question_count = build_question_inventory()

    print(f"Documents inventoried: {document_count}")
    print(f"Corpus size bytes: {total_bytes}")
    print(f"Evaluation datasets: {dataset_count}")
    print(f"Evaluation questions: {question_count}")


if __name__ == "__main__":
    main()
