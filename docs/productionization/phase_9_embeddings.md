# Phase 9 — Embeddings and Vectorization

## Status

Complete

## Objective

Validate the embedding strategy for the Databricks deployment and prepare
the Gold child-chunk corpus for managed vectorization by Databricks AI Search.

## Retrieval Unit

Child chunks are the primary retrieval units.

Parent chunks remain available in Gold for parent-context expansion after
retrieval.

## Embedding Strategy

Local runtime:
- Qwen3 embedding model through the existing local embedding abstraction.

Databricks runtime:
- Databricks-managed Qwen3 embedding endpoint.
- Embeddings are computed by AI Search from `gold.child_chunks.text`.
- Embedding vectors are not duplicated manually into another Delta table.

## Source Table

`docintel_dev.gold.child_chunks`

The table contains the retrieval text together with document, page,
section, parent-chunk, and chunk lineage.

## Validation

The managed embedding capability was validated successfully against Gold
child chunks.

A three-row embedding test produced:
- embeddings created: 3
- unique chunks: 3

The source Delta table has Change Data Feed enabled for incremental
Delta Sync processing.

## Design Decision

Phase 9 prepares the embedding contract and source data.

Actual lifecycle management of embeddings is delegated to the Databricks
AI Search Delta Sync index in Phase 10. This prevents duplicate embedding
storage and removes custom synchronization logic.

## Next

Phase 10 — Databricks AI Search and Hybrid Retrieval.