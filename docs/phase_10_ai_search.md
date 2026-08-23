# Phase 10 — Databricks AI Search

## Status

Phase 10 implements the managed Databricks retrieval index over the
productionized Gold child-chunk corpus.

Acceptance checkpoint:

> Hybrid index synchronizes updates and meets the retrieval baseline.

## Implemented architecture

Current development path:

Gold `child_chunks`
→ Delta Change Data Feed
→ Triggered Databricks AI Search Delta Sync
→ Databricks-managed Qwen3 embeddings
→ HYBRID retrieval
→ project-native `RetrievedChunk`
→ optional Gold parent-chunk expansion

The local retrieval implementation remains available through the
retrieval gateway. Databricks AI Search is a backend implementation,
not a replacement for the project retrieval abstraction.

## Current index state

Observed Phase 10 final state:

- Gold child rows: 329
- AI Search indexed rows: 329
- Row reconciliation: passed
- Index ready: true
- Detailed state: `ONLINE_NO_PENDING_UPDATE`
- Pipeline type: `TRIGGERED`
- Primary key: `chunk_id`
- Embedding source: `text`
- Managed embedding model: Qwen3 0.6B Databricks endpoint
- HYBRID smoke query: passed

The development index intentionally uses one existing AI Search
endpoint.

## Metadata

The retrieval index preserves the metadata required by the current
retrieval adapter, including document/page lineage and parent chunk
identity.

`section_title` is not currently synchronized into the child search
index. This is not a Phase 10 blocker because parent expansion can
recover parent context from Gold. Revisit child-level section metadata
if section filtering becomes a retrieval requirement.

## Retrieval-only benchmark

Dataset:

- 21 active canonical cases
- 4 canonical target documents
- all 21 targets represented in Gold
- one shared 329-child-chunk search index

Evaluation method:

- one HYBRID top-10 retrieval per query
- returned chunk order preserved
- no document deduplication before Hit@K
- no answer generation
- no LLM judge
- retrieval errors remain in the denominator
- page Hit@K is not scored because canonical page labels are absent

Observed results:

| Metric | Result |
| --- | ---: |
| Document Hit@1 | 21/21 — 100% |
| Document Hit@3 | 21/21 — 100% |
| Document Hit@5 | 21/21 — 100% |
| Document Hit@10 | 21/21 — 100% |
| Zero-result rate | 0% |
| Retrieval error rate | 0% |
| Metadata-valid cases | 21/21 — 100% |
| Mean latency | 308.39 ms |
| Median latency | 241.03 ms |
| P95 latency | 695.32 ms |

These numbers apply to the current versioned canonical evaluation set.
They must not be described as universal retrieval accuracy.

## Delta Sync incident

During the corpus update, Gold increased from 256 to 329 child chunks
but the existing AI Search index remained at 256 rows.

Diagnosis showed:

- Gold source contract valid
- Change Data Feed enabled
- all 73 inserted rows visible through CDF
- managed embedding probe succeeded
- index pipeline failed

The pipeline error reported that the Delta version required by the old
sync checkpoint was outside the deleted-file retention window.

The table was using a seven-day deleted-file retention period.

Resolution:

1. Increase `delta.deletedFileRetentionDuration` to 30 days.
2. Increase `delta.logRetentionDuration` to 30 days.
3. Preserve Change Data Feed.
4. Delete only the failed AI Search index.
5. Keep the existing endpoint.
6. Recreate the same Triggered Delta Sync index from the current Gold
   snapshot.
7. Validate exact 329/329 row reconciliation.
8. Run a HYBRID smoke query.

Final state:

`ONLINE_NO_PENDING_UPDATE`

The 30-day setting reduces recurrence risk for this development
workflow. It does not restore Delta history that has already expired.

## Retrieval evaluator

Phase 10 adds a retrieval-only evaluation path separate from the
answer-generation evaluator.

It records aggregate retrieval metrics plus privacy-safe per-case
results containing only:

- case ID
- Hit@K flags
- result count
- latency
- metadata-contract status
- error type

It does not persist:

- query text
- expected document IDs
- retrieved document IDs
- filenames
- document text
- chunk text

## Limitations

The current benchmark is intentionally small and deterministic.

The canonical set currently contains 21 active cases across four target
documents. Future evaluation work should expand document diversity and
add expected page labels so page-level retrieval quality can be scored.

Security/ACL retrieval testing belongs to the later governance phase.
Load and concurrency testing belongs to the performance phase.

## Phase 10 decision

Phase 10 can be marked complete when:

- retrieval evaluator unit tests pass
- complete regression suite passes
- the reusable evaluator reproduces the Databricks benchmark
- Phase 10 branch is merged to `main`

Phase 11 should build on the existing retrieval gateway rather than
introducing another retrieval architecture.
