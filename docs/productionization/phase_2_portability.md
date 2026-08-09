# Phase 2 — Portable Application Refactor

## Status

- Phase: 2
- Status: In Progress

## Deployment Decision

The implementation target is Databricks Free Edition.

The working application must remain portable between:

1. Local development
2. Databricks Free Edition
3. Future enterprise Azure Databricks deployment

Azure ADLS Gen2 integration remains part of the documented enterprise
architecture but is not a runtime dependency of the free implementation.

## Portability Goals

- Preserve the current working local application.
- Keep parsing, chunking, retrieval, generation, and evaluation logic reusable.
- Separate local filesystem assumptions from application logic.
- Separate retrieval backend implementation from retrieval orchestration.
- Separate model-provider implementation from answer-generation logic.
- Centralize runtime configuration.
- Avoid hard-coded local paths.
- Keep cloud-specific code behind adapters.
- Make the package importable from notebooks, jobs, Apps, and tests.

## Target Runtime Modes

### Local

- Local files
- LlamaIndex SimpleVectorStore
- BM25
- Local Qwen embeddings
- Local Qwen reranker
- Ollama generation
- Local Gradio

### Databricks Free Edition

- Databricks-managed storage
- Delta tables
- Unity Catalog
- Databricks AI Search
- Databricks Jobs / Lakeflow
- MLflow
- Databricks App
- Managed or supported model endpoint where available

### Future Azure Enterprise

- ADLS Gen2
- Azure Databricks
- Unity Catalog external locations
- Managed identity
- Enterprise networking and governance

## Portability Audit Result

The existing application already contained strong portability boundaries:

- project-native service contracts
- indexing gateway
- retrieval gateway
- OCR router
- storage abstraction
- centralized configuration
- dependency injection for testing

The audit confirmed that most filesystem findings are expected parser and
local-runtime behavior rather than architectural coupling.

## Changes Completed

- Added explicit local/databricks runtime mode.
- Added provider-neutral generation backend configuration.
- Added a generation gateway between answer orchestration and Ollama.
- Preserved the local Ollama implementation as an adapter.
- Prepared a Databricks generation adapter boundary for a later phase.
- Removed obsolete prototype modules when confirmed unused.
- Kept parsing APIs Path-based because file parsing remains an appropriate
  boundary for local temporary files and Databricks Volumes.
- Re-ran the complete regression suite.

## Status

Phase 2: Complete

Next:
Phase 3 — Databricks Free Edition foundation and Unity Catalog setup.
