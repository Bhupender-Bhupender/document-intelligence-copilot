from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from app.service import answer_query, index_document
from src.evaluation.evaluator import _compute_metrics
from src.schema.eval_models import EvalExample
from src.schema.models import AnswerResponse


CANONICAL_DATASET = (
    PROJECT_ROOT
    / "data"
    / "eval"
    / "canonical"
    / "evaluation_cases_v1.jsonl"
)

LOCAL_MAPPING = (
    PROJECT_ROOT
    / "docs"
    / "baseline"
    / "corpus_local_mapping.csv"
)

RUNTIME_DIR = (
    PROJECT_ROOT
    / "data"
    / "eval"
    / "baseline_runtime"
)

SUMMARY_PATH = (
    PROJECT_ROOT
    / "docs"
    / "baseline"
    / "deterministic_baseline_summary.json"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_cases() -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in CANONICAL_DATASET.read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]


def load_document_mapping() -> dict[str, Path]:
    mapping: dict[str, Path] = {}

    with LOCAL_MAPPING.open(encoding="utf-8") as file:
        for row in csv.DictReader(file):
            mapping[row["document_id"]] = Path(
                row["original_path"]
            )

    return mapping


def ensure_index(
    document_id: str,
    document_path: Path,
    *,
    force_reindex: bool,
) -> Path:
    index_dir = RUNTIME_DIR / "indexes" / document_id
    sentinel = index_dir / "baseline_index_complete.json"

    if sentinel.exists() and not force_reindex:
        print(f"Reusing index: {document_id}")
        return index_dir

    index_dir.mkdir(parents=True, exist_ok=True)

    print(f"Indexing: {document_id}")
    started = time.perf_counter()

    manifest = index_document(
        document_path,
        index_dir=index_dir,
        embed_model=None,
        _indexing_pipeline=None,
    )

    sentinel.write_text(
        json.dumps(
            {
                "document_id": document_id,
                "indexed_at": utc_now(),
                "duration_seconds": round(
                    time.perf_counter() - started,
                    3,
                ),
                "manifest": manifest.model_dump(mode="json"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return index_dir


def error_response(
    query: str,
    model: str,
    error: Exception,
) -> AnswerResponse:
    return AnswerResponse(
        query=query,
        answer_text="",
        model_used=model,
        sources=[],
        supporting_chunks=[],
        validation_flags=[
            f"baseline_runner_error:{type(error).__name__}"
        ],
        latency_ms=None,
    )


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--retrieval-top-k",
        type=int,
        default=10,
    )
    parser.add_argument(
        "--rerank-top-k",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--model",
        default="qwen3:8b",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--force-reindex",
        action="store_true",
    )

    args = parser.parse_args()

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    cases = load_cases()

    if args.limit is not None:
        cases = cases[: args.limit]

    document_mapping = load_document_mapping()

    examples: list[EvalExample] = []
    responses: list[AnswerResponse] = []
    detailed_rows: list[dict[str, Any]] = []

    indexed_documents: set[str] = set()
    errors: list[dict[str, str]] = []

    run_started = utc_now()
    wall_start = time.perf_counter()

    for position, case in enumerate(cases, start=1):
        case_id = case["case_id"]
        query = case["query"]
        document_id = case["target_document_id"]

        print(
            f"[{position}/{len(cases)}] "
            f"Running {case_id}"
        )

        document_path = document_mapping.get(document_id)

        if document_path is None:
            raise KeyError(
                f"No local mapping for {document_id}"
            )

        if not document_path.exists():
            raise FileNotFoundError(
                f"Mapped document does not exist: "
                f"{document_id}"
            )

        expected_file_name = document_path.name

        example = EvalExample(
            example_id=case_id,
            query=query,
            expected_source_chunk_ids=[],
            expected_file_names=[expected_file_name],
            expected_page_numbers=[],
            expect_non_empty_answer=True,
            expect_citations_valid=True,
            notes=f"dataset_id={case['dataset_id']}",
        )

        examples.append(example)

        try:
            index_dir = ensure_index(
                document_id,
                document_path,
                force_reindex=args.force_reindex,
            )

            indexed_documents.add(document_id)

            response = answer_query(
                query,
                index_dir=index_dir,
                retrieval_top_k=args.retrieval_top_k,
                rerank_top_k=args.rerank_top_k,
                model=args.model,
                _answer_pipeline=None,
            )

        except Exception as error:
            errors.append(
                {
                    "case_id": case_id,
                    "error_type": type(error).__name__,
                    "message": str(error),
                }
            )

            response = error_response(
                query,
                args.model,
                error,
            )

        responses.append(response)

        detailed_rows.append(
            {
                "case_id": case_id,
                "dataset_id": case["dataset_id"],
                "target_document_id": document_id,
                "response": response.model_dump(mode="json"),
            }
        )

    report = _compute_metrics(examples, responses)
    report_data = report.model_dump(mode="json")

    detailed_path = (
        RUNTIME_DIR / "deterministic_results.jsonl"
    )

    with detailed_path.open("w", encoding="utf-8") as file:
        for row in detailed_rows:
            file.write(
                json.dumps(row, ensure_ascii=False) + "\n"
            )

    error_path = RUNTIME_DIR / "errors.json"
    error_path.write_text(
        json.dumps(errors, indent=2),
        encoding="utf-8",
    )

    safe_report = dict(report_data)
    safe_report.pop("per_example", None)

    summary = {
        "baseline_name": "local-baseline-v1",
        "dataset_version": "evaluation_cases_v1",
        "run_started_at": run_started,
        "run_completed_at": utc_now(),
        "configuration": {
            "retrieval_top_k": args.retrieval_top_k,
            "rerank_top_k": args.rerank_top_k,
            "model": args.model,
        },
        "documents_indexed": len(indexed_documents),
        "cases_attempted": len(cases),
        "error_count": len(errors),
        "wall_time_seconds": round(
            time.perf_counter() - wall_start,
            3,
        ),
        "deterministic_metrics": safe_report,
        "private_detailed_results": str(
            detailed_path.relative_to(PROJECT_ROOT)
        ),
    }

    SUMMARY_PATH.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    print("\nBASELINE SUMMARY")
    print(f"Cases attempted: {len(cases)}")
    print(f"Documents indexed: {len(indexed_documents)}")
    print(f"Errors: {len(errors)}")

    for key, value in safe_report.items():
        if key != "flag_frequency":
            print(f"{key}: {value}")


if __name__ == "__main__":
    main()
