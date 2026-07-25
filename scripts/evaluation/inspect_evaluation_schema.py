from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]

QUESTION_ROOTS = [
    PROJECT_ROOT / "data" / "eval" / "kpi_queries",
    PROJECT_ROOT / "data" / "eval" / "smoke" / "kpi_queries",
]

QUESTION_KEYS = {
    "question",
    "query",
    "prompt",
    "user_query",
    "input",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest


def count_question_objects(value: Any) -> int:
    if isinstance(value, dict):
        current = 1 if QUESTION_KEYS.intersection(value.keys()) else 0
        return current + sum(count_question_objects(v) for v in value.values())

    if isinstance(value, list):
        return sum(count_question_objects(item) for item in value)

    return 0


def describe_child(key: str, value: Any) -> str:
    if isinstance(value, list):
        return f"{key}:list[{len(value)}]"

    if isinstance(value, dict):
        return f"{key}:dict[{len(value)}]"

    return f"{key}:{type(value).__name__}"


def main() -> None:
    print(
        "dataset_id | top_type | top_length | question_objects | structure"
    )

    for root in QUESTION_ROOTS:
        if not root.exists():
            continue

        for path in sorted(root.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            dataset_id = f"eval_{sha256_file(path)[:12]}"

            if isinstance(payload, dict):
                top_length = len(payload)
                structure = ", ".join(
                    describe_child(key, value)
                    for key, value in payload.items()
                )
            elif isinstance(payload, list):
                top_length = len(payload)
                structure = "list_items"
            else:
                top_length = 1
                structure = type(payload).__name__

            question_objects = count_question_objects(payload)

            print(
                f"{dataset_id} | "
                f"{type(payload).__name__} | "
                f"{top_length} | "
                f"{question_objects} | "
                f"{structure}"
            )


if __name__ == "__main__":
    main()
