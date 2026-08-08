# Local Document Intelligence Baseline Report

## Baseline identity

- Baseline name: `local-baseline-v1`
- Evaluation dataset: `evaluation_cases_v1`
- Generator model: `qwen3:8b`
- Retrieval top-k: 4
- Rerank top-k: 2
- Runtime mode: Local CPU generation
- Documents evaluated: 4
- Evaluation cases: 21

## Test and dataset validation

- Canonical cases: 21
- Unique case IDs: 21
- Source datasets: 5
- Cases mapped to documents: 21
- Unmapped cases: 0
- Execution errors: 0

## Deterministic evaluation

| Metric | Result |
|---|---:|
| Non-empty answer rate | 100.0% |
| Correct-file retrieval rate | 100.0% |
| Citation validity rate | 100.0% |
| All citations valid per answer | 100.0% |
| No-source rate | 0.0% |
| Invalid citation rate | 0.0% |
| Total wall time | 2636.1 seconds |
| Average wall time per case | 125.5 seconds |

## Semantic evaluation

| Metric | Result |
|---|---:|
| Mean groundedness | 76.7% |
| Mean answer relevance | 93.3% |
| Mean context relevance | 89.0% |
| Mean completeness | 74.3% |
| Cases above combined threshold | 12/21 |
| Above-threshold rate | 57.1% |
| Judge parse failures | 0 |

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
