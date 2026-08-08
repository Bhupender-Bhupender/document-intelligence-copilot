from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASELINE_DIR = PROJECT_ROOT / "docs" / "baseline"

DETERMINISTIC_PATH = (
    BASELINE_DIR / "deterministic_baseline_summary.json"
)
SEMANTIC_PATH = (
    BASELINE_DIR / "semantic_baseline_summary.json"
)
CANONICAL_PATH = (
    BASELINE_DIR / "canonical_evaluation_summary.json"
)
OUTPUT_PATH = (
    BASELINE_DIR / "local_baseline_report_v1.md"
)


def percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def main() -> None:
    deterministic = json.loads(
        DETERMINISTIC_PATH.read_text(encoding="utf-8")
    )
    semantic = json.loads(
        SEMANTIC_PATH.read_text(encoding="utf-8")
    )
    canonical = json.loads(
        CANONICAL_PATH.read_text(encoding="utf-8")
    )

    d_metrics = deterministic["deterministic_metrics"]
    s_metrics = semantic["semantic_metrics"]

    cases = deterministic["cases_attempted"]
    wall_time = deterministic["wall_time_seconds"]
    average_latency = wall_time / cases if cases else 0.0

    report = f"""# Local Document Intelligence Baseline Report

## Baseline identity

- Baseline name: `{deterministic["baseline_name"]}`
- Evaluation dataset: `{deterministic["dataset_version"]}`
- Generator model: `{deterministic["configuration"]["model"]}`
- Retrieval top-k: {deterministic["configuration"]["retrieval_top_k"]}
- Rerank top-k: {deterministic["configuration"]["rerank_top_k"]}
- Runtime mode: Local CPU generation
- Documents evaluated: {deterministic["documents_indexed"]}
- Evaluation cases: {cases}

## Test and dataset validation

- Canonical cases: {canonical["total_cases"]}
- Unique case IDs: {canonical["unique_case_ids"]}
- Source datasets: {canonical["datasets"]}
- Cases mapped to documents: {canonical["mapped_document_ids"]}
- Unmapped cases: {canonical["unmapped_document_ids"]}
- Execution errors: {deterministic["error_count"]}

## Deterministic evaluation

| Metric | Result |
|---|---:|
| Non-empty answer rate | {percent(d_metrics["answer_non_empty_rate"])} |
| Correct-file retrieval rate | {percent(d_metrics["file_hit_rate"])} |
| Citation validity rate | {percent(d_metrics["citation_valid_rate"])} |
| All citations valid per answer | {percent(d_metrics["citations_all_valid_rate"])} |
| No-source rate | {percent(d_metrics["no_source_rate"])} |
| Invalid citation rate | {percent(d_metrics["invalid_citation_rate"])} |
| Total wall time | {wall_time:.1f} seconds |
| Average wall time per case | {average_latency:.1f} seconds |

## Semantic evaluation

| Metric | Result |
|---|---:|
| Mean groundedness | {percent(s_metrics["mean_groundedness"])} |
| Mean answer relevance | {percent(s_metrics["mean_answer_relevance"])} |
| Mean context relevance | {percent(s_metrics["mean_context_relevance"])} |
| Mean completeness | {percent(s_metrics["mean_completeness"])} |
| Cases above combined threshold | {s_metrics["above_threshold_count"]}/{s_metrics["total"]} |
| Above-threshold rate | {percent(s_metrics["above_threshold_rate"])} |
| Judge parse failures | {s_metrics["parse_failure_count"]} |

## Baseline interpretation

The local system completed all evaluation queries without execution errors.
It returned non-empty answers, retrieved the expected document, and produced
valid citations for every evaluated case.

Answer relevance and context relevance are the strongest semantic dimensions.
Completeness is the main improvement area, indicating that some answers may
be correct and grounded but do not include every relevant requested detail.

## Known limitations

- Expected page numbers have not yet been labelled, so page-hit rate is not
  currently measurable.
- Expected chunk IDs have not yet been labelled, so source-chunk hit rate is
  not currently measurable.
- The same Qwen3-8B model was used for generation and semantic judging, so the
  semantic scores are directional rather than fully independent.
- Generation ran in CPU-only mode because the local Ollama CUDA runner was
  unstable.
- Average latency includes retrieval, reranking, generation, validation, and
  local CPU limitations.

## Databricks comparison targets

The Azure Databricks implementation should aim to:

1. Maintain zero execution errors.
2. Maintain 100% valid citations.
3. Maintain or improve correct-file retrieval.
4. Improve completeness beyond the local baseline.
5. Add labelled page-level and chunk-level retrieval metrics.
6. Reduce end-to-end latency.
7. Add incremental ingestion, idempotency, monitoring, and governance.
"""

    OUTPUT_PATH.write_text(report, encoding="utf-8")
    print(f"Created: {OUTPUT_PATH}")
    print(f"Average seconds per case: {average_latency:.1f}")


if __name__ == "__main__":
    main()
