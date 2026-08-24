from __future__ import annotations

import json

from collections import Counter
from pathlib import Path

from src.core.config import config
from src.evaluation.canonical_retrieval_dataset import (
    load_databricks_retrieval_examples,
)
from src.evaluation.retrieval_evaluator import (
    run_retrieval_evaluation,
)


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

CANONICAL_PATH = (
    PROJECT_ROOT
    / "data"
    / "eval"
    / "canonical"
    / "evaluation_cases_v1.jsonl"
)

CORPUS_MANIFEST_PATH = (
    PROJECT_ROOT
    / "docs"
    / "baseline"
    / "corpus_manifest.csv"
)

RUNTIME_DIR = (
    PROJECT_ROOT
    / "data"
    / "eval"
    / "runtime"
)

AGGREGATE_PATH = (
    RUNTIME_DIR
    / "databricks_retrieval_baseline_latest.json"
)

DETAIL_PATH = (
    RUNTIME_DIR
    / "databricks_retrieval_cases_latest.jsonl"
)


def run():
    if config.search_backend != "databricks":
        raise RuntimeError(
            "Retrieval benchmark requires "
            "search_backend='databricks'."
        )

    examples = (
        load_databricks_retrieval_examples(
            CANONICAL_PATH,
            CORPUS_MANIFEST_PATH,
        )
    )

    report, cases = (
        run_retrieval_evaluation(
            examples,
            top_k=10,
        )
    )

    RUNTIME_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    aggregate = {
        "benchmark_version":
            "phase10_databricks_retrieval_v1",

        "backend":
            "databricks_ai_search_hybrid",

        "report":
            report.model_dump(
                mode="json"
            ),
    }

    AGGREGATE_PATH.write_text(
        json.dumps(
            aggregate,
            indent=2,
        ),
        encoding="utf-8",
    )

    with DETAIL_PATH.open(
        "w",
        encoding="utf-8",
    ) as handle:
        for result in cases:
            handle.write(
                result.model_dump_json()
            )
            handle.write("\n")

    error_types = Counter(
        result.error_type
        for result in cases
        if result.error_type
    )

    print(
        "RETRIEVAL_EVALUATOR_COMPLETED:",
        True,
    )
    print(
        "CASES_EVALUATED:",
        report.cases_evaluated,
    )

    print(
        "DOCUMENT_HIT_AT_1_COUNT:",
        report.hit_at_1_count,
    )
    print(
        "DOCUMENT_HIT_AT_1:",
        round(
            report.hit_at_1,
            4,
        ),
    )

    print(
        "DOCUMENT_HIT_AT_3_COUNT:",
        report.hit_at_3_count,
    )
    print(
        "DOCUMENT_HIT_AT_3:",
        round(
            report.hit_at_3,
            4,
        ),
    )

    print(
        "DOCUMENT_HIT_AT_5_COUNT:",
        report.hit_at_5_count,
    )
    print(
        "DOCUMENT_HIT_AT_5:",
        round(
            report.hit_at_5,
            4,
        ),
    )

    print(
        "DOCUMENT_HIT_AT_10_COUNT:",
        report.hit_at_10_count,
    )
    print(
        "DOCUMENT_HIT_AT_10:",
        round(
            report.hit_at_10,
            4,
        ),
    )

    print(
        "ZERO_RESULT_COUNT:",
        report.zero_result_count,
    )

    print(
        "RETRIEVAL_ERROR_COUNT:",
        report.retrieval_error_count,
    )

    print(
        "RETRIEVAL_ERROR_TYPES:",
        dict(error_types),
    )

    print(
        "METADATA_VALID_RATE:",
        round(
            report.metadata_valid_rate,
            4,
        ),
    )

    print(
        "MEAN_LATENCY_MS:",
        round(
            report.mean_latency_ms,
            2,
        ),
    )

    print(
        "MEDIAN_LATENCY_MS:",
        round(
            report.median_latency_ms,
            2,
        ),
    )

    print(
        "P95_LATENCY_MS:",
        round(
            report.p95_latency_ms,
            2,
        ),
    )

    print(
        "PAGE_HIT_SCORING:",
        report.page_hit_scoring,
    )

    print(
        "GENERATION_USED:",
        report.generation_used,
    )

    print(
        "LLM_JUDGE_USED:",
        report.llm_judge_used,
    )

    print(
        "OPERATIONAL_RETRIEVAL_PASS:",
        report.operational_retrieval_pass,
    )

    print(
        "AGGREGATE_ARTIFACT_WRITTEN:",
        AGGREGATE_PATH.exists(),
    )

    print(
        "CASE_ARTIFACT_WRITTEN:",
        DETAIL_PATH.exists(),
    )

    return report


if __name__ == "__main__":
    run()
