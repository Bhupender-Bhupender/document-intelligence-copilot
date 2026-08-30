# Phase 14 — Lakeflow Jobs and Reliability

## Status

**Complete**

Phase 14 converts the existing Document Intelligence Bronze → Silver → Gold → AI Search pipeline into a source-controlled, serverless, repairable Databricks Lakeflow Job.

## Production DAG

```text
ingest
  ↓
extract_silver
  ↓
quality_gate
  ↓
publish_gold
  ↓
search_sync_validate
```

The workflow contains five tasks and uses `max_concurrent_runs = 1`.

## Source-controlled orchestration

The Job is managed through:

- `databricks.yml`
- `resources/document_intelligence_job.yml`
- `databricks/jobs/_runtime.py`
- `databricks/jobs/01_ingest.py`
- `databricks/jobs/02_extract_silver.py`
- `databricks/jobs/03_quality_gate.py`
- `databricks/jobs/04_publish_gold.py`
- `databricks/jobs/05_search_sync_validate.py`

The runtime derives Unity Catalog object names from one catalog parameter.

## Runtime boundaries

### Bronze ingestion

Reuses `run_incremental_ingestion` with explicit landing, manifest, and run-table contracts.

### Silver processing

Reuses document routing, native extraction, and managed OCR recovery.

For Databricks PDFs, the cloud path is:

```text
pypdf/native extraction
  ↓
requires_ocr state
  ↓
Databricks managed recovery
  ↓
ai_parse_document
```

Local development preserves the existing Docling/RapidOCR path.

### Silver quality gate

Reuses `run_silver_quality_checks` and `enforce_quality_gate`.

The quality task has `max_retries = 0`, so critical deterministic data-quality failures fail closed and block downstream publishing.

### Gold publishing

Reuses `run_gold_chunking` and preserves the hierarchical chunking and lineage contracts.

### AI Search synchronization

Uses the existing triggered Delta Sync index. Before synchronization, the task validates:

- non-empty parent and child Gold tables
- unique parent chunk IDs
- unique child chunk IDs
- no orphan child chunks
- no duplicate current chunking-manifest rows

It then requires indexed-row count to match Gold child-row count.

## Live validation

Independent task validation passed for all five tasks:

```text
PHASE14_TASK_INGEST_PASS: True
PHASE14_TASK_EXTRACT_SILVER_PASS: True
PHASE14_TASK_QUALITY_GATE_PASS: True
PHASE14_TASK_PUBLISH_GOLD_PASS: True
PHASE14_TASK_SEARCH_SYNC_PASS: True
```

The first complete five-task DAG also passed:

```text
PHASE14_FULL_PIPELINE_PASS: True
```

## Idempotency evidence

A second full run completed successfully without changing the input dataset.

```text
Gold parent chunks: 134
Gold child chunks: 329
duplicate parent chunks: 0
duplicate child chunks: 0
orphan child chunks: 0
duplicate current manifest entries: 0
AI Search source rows: 329
AI Search indexed rows: 329
```

Results:

```text
PHASE14_GOLD_CONTRACT_PASS: True
PHASE14_IDEMPOTENT_RERUN_PASS: True
```

## Retry and repair evidence

A controlled failure was introduced only at the Search boundary using a nonexistent index name.

Observed automatic retry attempts:

```text
attempt 0 -> FAILED
attempt 1 -> FAILED
attempt 2 -> FAILED
```

The valid bundle configuration was then restored and redeployed. A Lakeflow repair reran the failed Search boundary with the corrected configuration:

```text
attempt 3 -> SUCCESS
```

The repaired run finished:

```text
TERMINATED
SUCCESS
PHASE14_SEARCH_TASK_AFTER_REPAIR_SUCCESS: True
PHASE14_REPAIR_RECOVERY_PASS: True
```

No Bronze, Silver, or Gold data was intentionally corrupted during the controlled-failure test.

## Reliability controls

Phase 14 now provides:

- explicit task dependencies
- serverless execution
- bounded retries and retry intervals
- task and job timeouts
- `max_concurrent_runs = 1`
- incremental ingestion
- fail-closed Silver quality checks
- idempotent Gold publishing
- Gold structural postconditions
- AI Search convergence checks
- controlled Lakeflow repair/recovery

## Production boundary

Phase 14 owns orchestration and reliability for Bronze ingestion, Silver routing/extraction, managed OCR recovery, Silver quality validation, Gold publishing, and AI Search synchronization.

The Phase 13 Databricks App remains the online query-serving boundary.

## Result

Phase 14 provides a repeatable, source-controlled and repairable Lakeflow workflow with validated retries, idempotent reruns, structural integrity checks, Search convergence, and successful repair of a failed task.
