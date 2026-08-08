from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASELINE_DIR = PROJECT_ROOT / "docs" / "baseline"
OUTPUT_DIR = PROJECT_ROOT / "data" / "eval" / "canonical"

QUESTION_ROOTS = [
    ("main", PROJECT_ROOT / "data" / "eval" / "kpi_queries"),
    ("smoke", PROJECT_ROOT / "data" / "eval" / "smoke" / "kpi_queries"),
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)

    return digest.hexdigest()


def normalize_name(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[_\-\s]+queries?$", "", value)
    return re.sub(r"[^a-z0-9]", "", value)


def extract_query_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()

    if isinstance(value, dict):
        for key in ("query", "question", "prompt", "text", "input"):
            text = value.get(key)

            if isinstance(text, str):
                return text.strip()

    return ""


def keyed_value(payload: dict[str, Any], field: str, key: str) -> Any:
    value = payload.get(field)

    if isinstance(value, dict):
        return value.get(key)

    return None


def load_document_candidates() -> list[dict[str, str]]:
    public_path = BASELINE_DIR / "corpus_manifest.csv"
    private_path = BASELINE_DIR / "corpus_local_mapping.csv"

    if not public_path.exists() or not private_path.exists():
        raise FileNotFoundError(
            "Run build_baseline_inventory.py before creating the canonical dataset."
        )

    public_rows = {
        row["document_id"]: row
        for row in csv.DictReader(public_path.open(encoding="utf-8"))
    }

    candidates: list[dict[str, str]] = []

    with private_path.open(encoding="utf-8") as file:
        for row in csv.DictReader(file):
            document_id = row["document_id"]
            public = public_rows.get(document_id, {})
            original_path = Path(row["original_path"])

            candidates.append(
                {
                    "document_id": document_id,
                    "source_group": public.get("source_group", ""),
                    "normalized_stem": normalize_name(original_path.stem),
                }
            )

    return candidates


def match_document_id(
    query_file: Path,
    question_group: str,
    candidates: list[dict[str, str]],
) -> str | None:
    query_stem = normalize_name(query_file.stem)

    matches = [
        candidate
        for candidate in candidates
        if candidate["normalized_stem"] == query_stem
    ]

    preferred_group = "smoke" if question_group == "smoke" else None

    if preferred_group:
        preferred = [
            candidate
            for candidate in matches
            if candidate["source_group"] == preferred_group
        ]

        if preferred:
            matches = preferred

    unique_ids = sorted({candidate["document_id"] for candidate in matches})

    if len(unique_ids) == 1:
        return unique_ids[0]

    return None


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    candidates = load_document_candidates()
    cases: list[dict[str, Any]] = []

    for question_group, root in QUESTION_ROOTS:
        if not root.exists():
            continue

        for path in sorted(root.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            queries = payload.get("queries")

            if not isinstance(queries, dict):
                raise ValueError(f"{path} does not contain a queries dictionary")

            dataset_hash = sha256_file(path)
            dataset_id = f"eval_{dataset_hash[:12]}"
            target_document_id = match_document_id(
                path,
                question_group,
                candidates,
            )

            for query_key, query_value in queries.items():
                query_key = str(query_key)
                query_text = extract_query_text(query_value)

                template_id = keyed_value(
                    payload,
                    "templates_mapping",
                    query_key,
                )

                templates = payload.get("templates", {})
                template = (
                    templates.get(str(template_id))
                    if isinstance(templates, dict)
                    and template_id is not None
                    else None
                )

                case_id_source = (
                    f"{dataset_id}|{query_key}|{query_text}"
                )
                case_hash = hashlib.sha256(
                    case_id_source.encode("utf-8")
                ).hexdigest()

                cases.append(
                    {
                        "case_id": f"case_{case_hash[:16]}",
                        "dataset_id": dataset_id,
                        "query_key": query_key,
                        "query": query_text,
                        "target_document_id": target_document_id,
                        "kpi": keyed_value(payload, "KPIs", query_key),
                        "template_id": template_id,
                        "template": template,
                        "comment": keyed_value(
                            payload,
                            "comments",
                            query_key,
                        ),
                        "version": keyed_value(
                            payload,
                            "version",
                            query_key,
                        ),
                        "is_active": keyed_value(
                            payload,
                            "isActive",
                            query_key,
                        ),
                        "excel_table": payload.get("excel_table"),
                        "excel_sheet": payload.get("excel_sheet"),
                        "start_row": payload.get("start_row"),
                        "start_col": payload.get("start_col"),
                    }
                )

    case_ids = [case["case_id"] for case in cases]
    empty_queries = [
        case["case_id"]
        for case in cases
        if not case["query"]
    ]
    mapped_count = sum(
        1 for case in cases if case["target_document_id"]
    )

    if len(case_ids) != len(set(case_ids)):
        raise ValueError("Duplicate case IDs detected")

    if empty_queries:
        raise ValueError(
            f"Empty queries detected: {len(empty_queries)}"
        )

    output_path = OUTPUT_DIR / "evaluation_cases_v1.jsonl"

    with output_path.open("w", encoding="utf-8") as file:
        for case in cases:
            file.write(
                json.dumps(case, ensure_ascii=False) + "\n"
            )

    summary = {
        "dataset_version": "evaluation_cases_v1",
        "total_cases": len(cases),
        "unique_case_ids": len(set(case_ids)),
        "datasets": len({case["dataset_id"] for case in cases}),
        "non_empty_queries": len(cases) - len(empty_queries),
        "mapped_document_ids": mapped_count,
        "unmapped_document_ids": len(cases) - mapped_count,
    }

    summary_path = BASELINE_DIR / "canonical_evaluation_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
