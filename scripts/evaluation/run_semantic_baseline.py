from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.semantic_evaluator import run_semantic_evaluation
from src.schema.eval_models import EvalExample
from src.schema.models import AnswerResponse


CANONICAL_DATASET = (
    PROJECT_ROOT
    / "data"
    / "eval"
    / "canonical"
    / "evaluation_cases_v1.jsonl"
)

DETERMINISTIC_RESULTS = (
    PROJECT_ROOT
    / "data"
    / "eval"
    / "baseline_runtime"
    / "deterministic_results.jsonl"
)

PRIVATE_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "eval"
    / "baseline_runtime"
    / "semantic_results.json"
)

SUMMARY_OUTPUT = (
    PROJECT_ROOT
    / "docs"
    / "baseline"
    / "semantic_baseline_summary.json"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--judge-model",
        default="qwen3:8b",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.7,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
    )

    args = parser.parse_args()

    cases = {
        row["case_id"]: row
        for row in load_jsonl(CANONICAL_DATASET)
    }

    result_rows = load_jsonl(DETERMINISTIC_RESULTS)

    if args.limit is not None:
        result_rows = result_rows[: args.limit]

    examples: list[EvalExample] = []
    responses: list[AnswerResponse] = []

    for row in result_rows:
        case_id = row["case_id"]
        case = cases[case_id]

        examples.append(
            EvalExample(
                example_id=case_id,
                query=case["query"],
                expected_source_chunk_ids=[],
                expected_file_names=[],
                expected_page_numbers=[],
                expect_non_empty_answer=True,
                expect_citations_valid=True,
                notes=f"dataset_id={case['dataset_id']}",
            )
        )

        responses.append(
            AnswerResponse.model_validate(row["response"])
        )

    started_at = utc_now()
    timer = time.perf_counter()

    report = run_semantic_evaluation(
        examples,
        responses,
        _judge=None,
        judge_model=args.judge_model,
        threshold=args.threshold,
    )

    full_report = report.model_dump(mode="json")

    PRIVATE_OUTPUT.write_text(
        json.dumps(full_report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    safe_metrics = dict(full_report)
    safe_metrics.pop("per_example", None)

    summary = {
        "baseline_name": "local-baseline-v1",
        "dataset_version": "evaluation_cases_v1",
        "started_at": started_at,
        "completed_at": utc_now(),
        "judge_model": args.judge_model,
        "threshold": args.threshold,
        "cases_evaluated": len(examples),
        "wall_time_seconds": round(
            time.perf_counter() - timer,
            3,
        ),
        "semantic_metrics": safe_metrics,
        "private_detailed_results": str(
            PRIVATE_OUTPUT.relative_to(PROJECT_ROOT)
        ),
    }

    SUMMARY_OUTPUT.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    print("\nSEMANTIC BASELINE SUMMARY")
    print(f"Cases evaluated: {len(examples)}")
    print(f"Judge model: {args.judge_model}")

    for key, value in safe_metrics.items():
        if key != "report_id":
            print(f"{key}: {value}")


if __name__ == "__main__":
    main()
