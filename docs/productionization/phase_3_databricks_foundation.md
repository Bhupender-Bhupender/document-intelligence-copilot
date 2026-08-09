# Phase 3 — Databricks Free Edition Foundation

## Status

- Phase: 3
- Status: In Progress
- Runtime: Databricks Free Edition
- Compute: Serverless
- Governance: Unity Catalog

## Architecture Decision

The implementation uses Databricks Free Edition and Databricks-managed
default storage.

ADLS Gen2 and Azure-managed identity remain part of the future enterprise
architecture but are not runtime dependencies of this free implementation.

## Unity Catalog Layout

Development catalog:
- TO_RECORD

Schemas:
- bronze
- silver
- gold
- evaluation
- monitoring

## Validation

- Free Edition workspace accessible: TO_VERIFY
- Unity Catalog enabled: TO_VERIFY
- Serverless notebook executed: TO_VERIFY
- Project catalog created or workspace catalog selected: TO_VERIFY
- Project schemas created: TO_VERIFY
- Delta table created successfully: TO_VERIFY
- Delta history verified: TO_VERIFY
- Temporary table removed: TO_VERIFY

## Next

Create managed Unity Catalog volumes for the raw document corpus and project
artifacts, then define the first persistent Bronze Delta tables.

## Part B — Bronze Foundation

Completed:

- Created Unity Catalog managed volume `bronze.document_landing`.
- Created `incoming`, `archive`, and `quarantine` landing directories.
- Validated read/write access through `/Volumes`.
- Created persistent Delta table `bronze.document_manifest`.
- Created persistent Delta table `bronze.ingestion_runs`.
- Validated Delta writes and transaction history.
- Removed temporary validation data.
- Added reproducible Bronze DDL to the repository.

## Bronze Design

Unstructured document bytes are stored in a Unity Catalog managed volume.

Delta tables store control-plane metadata:

- `document_manifest` tracks individual files and document versions.
- `ingestion_runs` tracks pipeline executions and operational counts.

This separation provides the basis for deduplication, idempotency,
incremental ingestion, provenance, quarantine handling, and monitoring.

## Status

Phase 3: Complete

Next:
Phase 4 — Manifest-driven incremental document ingestion.
