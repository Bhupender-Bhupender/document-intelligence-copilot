# Azure Architecture Mapping

Document Intelligence Copilot — mapping from local-first implementation to Azure deployment.

> **Scope**: This document covers architecture mapping and deployment decisions only.
> No IaC, no Azure SDK code, no deployment YAML is produced here.
> Implementation of adapters, containers, and cloud wiring happens in the next step.

---

## Guiding principle

The local pipeline is the source of truth.
Azure services are substituted **behind explicit adapter boundaries** — they never
reach past their insertion point.
**Project-native schemas** (`ParsedPage`, `RetrievedChunk`, `AnswerResponse`, etc.)
are the only currency that crosses module boundaries, regardless of which backend is active.
`app/service.py` and `app/ui.py` are **not touched** by any Azure work.

---

## Component-to-Azure mapping

| Local component | Current implementation | Azure target | Stance | Rationale |
|---|---|---|---|---|
| Raw document storage | Local disk (`data/raw/`, `docs/sample_docs/`) | Azure Blob Storage — `documents-raw` container | **Replace** | Managed, durable, geo-redundant; enables async indexing triggers; no self-managed FS |
| Processed artifact storage | JSONL files in `data/processed/` | Azure Blob Storage — `documents-processed` container | **Replace** | Same storage account; one service covers all artifact I/O |
| Evaluation artifact storage | In-memory / stdout | Azure Blob Storage — `eval-reports` container | **Defer** | Not on deployment critical path; target defined here, wired in next step |
| Document parsing (layout) | Docling (`src/parsing/`) | Kept in container | **Keep** | Docling table/structure awareness has no direct Azure equivalent; runs in any container |
| Document parsing (scanned OCR) | PaddleOCR (`src/ocr/`) | Azure AI Document Intelligence — OCR prebuilt | **Replace** | Azure DI is managed, no model loading, no Windows C++ Redistributable requirement; superior for image-heavy PDFs |
| Chunking | Custom hierarchical chunker (`src/chunking/`) | Kept in container | **Keep** | Pure Python business logic; no Azure equivalent; no change needed |
| Embedding generation | Qwen3-Embedding-0.6B via LlamaIndex (`src/indexing/`) | Kept in container (CPU) | **Keep (initially)** | LlamaIndex `HuggingFaceEmbedding` → `OpenAIEmbedding` swap is one-line when needed; defer until re-indexing is justified |
| Vector store | ChromaDB via LlamaIndex (`data/chroma_db/`) | Azure AI Search — vector index | **Replace** | Managed HNSW vector search; eliminates ChromaDB file persistence in cloud |
| BM25 retrieval | rank-bm25 (`src/retrieval/bm25_retriever.py`) | Azure AI Search — built-in BM25 | **Replace** | Covered by Azure AI Search hybrid search; no separate BM25 index to maintain |
| RRF fusion | Custom `src/retrieval/hybrid_retriever.py` | Azure AI Search — hybrid RRF | **Replace** | Azure AI Search RRF fusion is built-in; custom fusion code removed from hot path |
| Reranking | Qwen3-Reranker-0.6B (`src/reranking/`) | Kept in container | **Keep (initially)** | Quality differentiator; applied as post-processing on Azure AI Search results; Azure AI Search semantic ranker is a future swap option |
| Answer generation | Qwen3-8B via Ollama (`src/generation/`) | See two-track stance below | **Track A (MVP)** | See generation section |
| Service layer | `app/service.py` | Azure Container Apps — hosting only | **Wrap** | No code change; ACA provides HTTP ingress, scaling, and secret injection |
| UI | Gradio Blocks (`app/ui.py`) | Same ACA container as service (combined) | **Wrap** | Gradio is the HTTP frontend; co-location with service is simplest for MVP |
| Logging | structlog → stdout | ACA stdout → Azure Monitor / Log Analytics | **Wrap** | ACA streams stdout to Log Analytics automatically; switch structlog renderer to JSON in cloud config |
| Secrets / config | pydantic-settings + `.env` | Azure Key Vault + ACA secret refs + Managed Identity | **Replace** | No credentials in image or plain env vars; pydantic-settings already reads env vars — no code change |
| Evaluation harness | In-process (`src/evaluation/`) | Kept in container; Blob for output | **Keep / Defer** | Eval logic unchanged; output persistence to Blob defined here, wired in next step |

---

## Substitution boundaries (Phase 11B insertion points)

This section defines exactly **where** Azure adapters are inserted so that Phase 11B
does not become a cross-cutting rewrite.

### Rule: project-native types do not leak Azure types

Every adapter below **receives** Azure API responses and **emits** the existing
project-native schema type. Nothing downstream changes.

---

### Boundary 1 — OCR backend

```
src/ocr/
  ├── paddle_ocr.py          ← current implementation (kept for local dev)
  └── azure_di_ocr.py        ← NEW adapter (Phase 11B)
```

**Insertion point**: the OCR routing logic that currently dispatches to PaddleOCR
(triggered by `extraction_status == "empty"` from `pdf_reader.py`).
In the Azure path, this dispatch calls `azure_di_ocr.py` instead.

**Stable contract**:
- Input: raw page bytes / file path
- Output: `ParsedPage` (text, extraction_status, ocr_confidence, ocr_engine)
- `src/parsing/` is untouched; `src/ingestion/router.py` is untouched

---

### Boundary 2 — Indexing and retrieval backend

```
src/indexing/
  ├── index_builder.py       ← current implementation (ChromaDB/LlamaIndex)
  └── azure_search_indexer.py ← NEW adapter (Phase 11B)

src/retrieval/
  ├── vector_retriever.py    ← current implementation (kept for local dev)
  ├── bm25_retriever.py      ← current implementation (kept for local dev)
  ├── hybrid_retriever.py    ← current implementation (kept for local dev)
  └── azure_search_retriever.py ← NEW adapter (Phase 11B)
```

**Insertion point**: `app/service.py → index_document()` dispatches to one of the two
indexing paths via config; `run_pipeline()` dispatches to one of the two retrieval
paths via config.

**Stable contract**:
- Indexing output: `IndexManifest` — unchanged
- Retrieval output: `List[RetrievedChunk]` — unchanged
- Reranking, synthesis, citations, validation, evaluation: all unchanged
- `app/service.py`, `app/ui.py`: untouched

---

### Boundary 3 — Artifact storage backend

```
src/storage/
  ├── artifact_writer.py     ← current implementation (local JSONL)
  └── blob_artifact_writer.py ← NEW adapter (Phase 11B)
```

**Insertion point**: `start_run` / `complete_run` / `save_manifest` in
`src/storage/run_manifest.py`; `write_jsonl` / `read_jsonl_raw` in
`src/storage/artifact_writer.py`. Config switch selects local or Blob backend.

**Stable contract**:
- `RunManifest` structure: unchanged
- JSONL artifact schema: unchanged
- Callers (`run_pipeline`, eval harness): untouched

---

### Boundary 4 — Generation provider

```
src/generation/
  ├── ollama_llm.py          ← current implementation (Track A)
  └── azure_llm.py           ← NEW adapter (Phase 11B, Track B only)
```

**Insertion point**: `answer_engine.py → synthesise()` calls the LLM wrapper.
The wrapper is selected by `GENERATION_BACKEND` environment variable.
`answer_pipeline.py` is untouched.

**Stable contract**:
- Input: `List[dict]` messages
- Output: `str` answer text
- `AnswerResponse` assembly: unchanged
- `app/service.py`, `app/ui.py`: untouched

---

### Unchanged consumers (hard invariant)

`app/service.py` and `app/ui.py` must remain **exactly as written** throughout
all of Phase 11. They consume only `IndexManifest`, `AnswerResponse`, and
`ServiceError`. No Azure type, no adapter reference, no config branch crosses
this boundary.

---

## Answer-generation: two-track stance

### Track A — Local-parity / Ollama-in-container (preferred MVP)

- Qwen3-8B runs in the ACA container via an Ollama daemon
- Ollama is started as a background process by the container entrypoint script
- `src/generation/ollama_llm.py` is **unchanged** — it already talks to Ollama over localhost HTTP
- Model weights are downloaded at container startup from Azure Blob Storage
  (avoids baking a multi-GB model into the image)
- No Azure LLM dependency; no per-token cost; behaviour is identical to local development
- `GENERATION_BACKEND=ollama` (or unset) activates this path

**This is the recommended MVP deployment path.**
Reasons: cost-predictable, no Azure LLM dependency to provision, full local/cloud
parity, battle-tested Qwen3-8B behaviour.

### Track B — Managed-cloud generation (deferred, parity-preserving alternative)

- Qwen3-8B (or a comparable model) served via Azure AI Foundry model endpoint,
  Azure OpenAI, or Azure Machine Learning Online Endpoint
- Integration point: `src/generation/azure_llm.py` — a new adapter behind the
  same generation boundary as Track A
- Activated by `GENERATION_BACKEND=azure`; no changes to `answer_pipeline.py`,
  `service.py`, or `ui.py`
- Deferred until GPU-in-container becomes too expensive or managed SLAs are required

**This is the parity-preserving alternative.**
The generation boundary is designed so Track B is a drop-in substitution.
No cost is incurred until Track B is explicitly enabled.

---

## Decision record

### A — Document and artifact storage: Azure Blob Storage

**Decision**: Azure Blob Storage, Standard LRS, one account, three containers.

| Container | Contents |
|---|---|
| `documents-raw` | Original uploaded files (.txt, .md, .pdf, .docx) |
| `documents-processed` | JSONL chunks, manifests, index metadata |
| `eval-reports` | Serialized EvalReport / SemanticEvalReport (deferred) |

**Rejected alternative**: Azure Data Lake Storage Gen2 — ADLS adds hierarchical namespace
and Spark/Databricks integration neither of which is needed here. Standard Blob is sufficient.

---

### B — Parsing / OCR: hybrid strategy

**Decision**: Docling stays in-container for all structured document types.
Azure AI Document Intelligence (OCR prebuilt or Layout model) replaces PaddleOCR for
scanned / image-heavy PDFs.

**Routing**: The existing `extraction_status == "empty"` signal from `pdf_reader.py`
already determines which OCR path is invoked. The same conditional is kept;
only the OCR backend implementation changes (Boundary 1 above).

**Why not replace Docling with Azure DI entirely**:
Docling provides table structure extraction and reading-order reconstruction that Azure
DI's Layout model does not match for complex .docx and structured .pdf documents.
The hybrid approach uses the right tool per document type.

**Why replace PaddleOCR**:
PaddleOCR requires a specific Python version and C++ Redistributable on Windows, making
it fragile in containers. Azure DI is fully managed, requires no GPU, charges per page
(S0: 1,500 free pages/month), and handles image-heavy PDFs reliably.

---

### C — Retrieval backend: Azure AI Search

**Decision**: Azure AI Search (Basic tier) replaces ChromaDB, rank-bm25, and the custom
RRF fusion layer for the cloud path.

Azure AI Search provides:
- HNSW vector search (replaces ChromaDB)
- Built-in BM25 keyword search (replaces rank-bm25)
- Hybrid RRF scoring (replaces `hybrid_retriever.py`)
- Optional semantic ranker (future Qwen3-Reranker replacement)

**What stays app-managed**:
- Qwen3-Reranker-0.6B post-processing — applied as a step on top of Azure AI Search results
  using the existing `src/reranking/` code; not replaced until a quality comparison is done
- `RetrievedChunk` schema and all downstream pipeline stages

**Local dev path**: ChromaDB + rank-bm25 + custom RRF remain active; config switch selects backend.

**LlamaIndex abstraction note**: `llama-index-vector-stores-azureaisearch` provides the
LlamaIndex vector store adapter for Azure AI Search, keeping the LlamaIndex orchestration
layer intact.

---

### D — Hosting: Azure Container Apps

**Decision**: Azure Container Apps (ACA), consumption plan, combined service + UI container for MVP.

**Why ACA over App Service**: ACA natively supports containerized multi-process applications,
consumption-based scaling (including scale-to-zero for portfolio cost control), and managed
secret injection from Key Vault. App Service is less suitable for containers that bundle
background processes (Ollama daemon).

**Why ACA over AKS**: No Kubernetes overhead; ACA is the managed container runtime abstraction.

**MVP deployment unit**: One ACA app serving Gradio on HTTP, backed by the Ollama sidecar
and the full local pipeline stack. Azure Container Registry (Basic) stores the image.

**Future split**: If the service layer is promoted to a REST API for multiple frontends,
split into a dedicated backend ACA app and a frontend ACA app.

---

### E — Secrets and configuration: Azure Key Vault + Managed Identity

**Decision**: Azure Key Vault (Standard tier) for all secrets; ACA secret references pull
from Key Vault at runtime via system-assigned Managed Identity; no credentials in the image.

| Secret | Stored in Key Vault |
|---|---|
| Blob Storage connection string | Yes |
| Azure AI Search API key | Yes |
| Azure AI Document Intelligence key | Yes |
| Azure OpenAI key (Track B, if activated) | Yes |

Non-secret app config (chunking parameters, top-k defaults, log level, `GENERATION_BACKEND`)
stays as ACA environment variables — readable, not sensitive.

pydantic-settings already reads from environment variables; no code change is needed.
Key Vault secrets are injected as environment variables by ACA at startup.

---

### F — Observability: Azure Monitor + Log Analytics

**Decision**: Structured log passthrough for MVP; Application Insights as a future enhancement.

ACA streams container stdout/stderr to a Log Analytics Workspace automatically.
structlog is switched from `ConsoleRenderer` to `JSONRenderer` in the cloud config
(controlled by `LOG_FORMAT=json` environment variable; one-line change in
`src/utils/logging_utils.py` — this is the only file that may need a config branch).

**Application Insights** (OpenTelemetry exporter) is documented as the next observability
layer but is not required for MVP. Deferred to Phase 12 portfolio hardening.

---

## Migration stance summary

| Stance | Components |
|---|---|
| **Keep as-is** | Schema models, chunking, embedding (Qwen3), reranking (Qwen3), Docling parser, evaluation harness, `app/service.py`, `app/ui.py` |
| **Wrap** (hosting / config / renderer only) | Service layer (ACA hosting), UI (ACA hosting), structlog (JSON renderer via env var), pydantic-settings (reads Key Vault-injected env vars already) |
| **Replace** in cloud path | PaddleOCR → Azure AI Document Intelligence; ChromaDB → Azure AI Search; rank-bm25 → Azure AI Search; RRF fusion → Azure AI Search; local disk storage → Azure Blob Storage; `.env` secrets → Azure Key Vault |
| **Defer** | Embedding model switch (Qwen3 → Azure OpenAI embeddings), LLM switch (Track A → Track B), Azure AI Search semantic ranker, eval artifact persistence, Application Insights |

---

## Deployment units

Seven discrete Azure resources constitute the full deployment.

| # | Unit | Azure resource | Tier (MVP) | Notes |
|---|---|---|---|---|
| 1 | App container | Azure Container Apps | Consumption | Service + UI + Ollama in one container; HTTP ingress; scale-to-zero |
| 2 | Container registry | Azure Container Registry | Basic | Stores app image; ACA pulls from here |
| 3 | Document and artifact storage | Azure Blob Storage | Standard LRS | Three containers: raw, processed, eval-reports |
| 4 | Retrieval service | Azure AI Search | Free (dev) → Basic (demo) | One index; vector + hybrid + optional semantic ranker |
| 5 | OCR / layout service | Azure AI Document Intelligence | S0 | 1,500 free pages/month; pay-per-page above that |
| 6 | Secrets | Azure Key Vault | Standard | All connection strings and API keys |
| 7 | Logs | Azure Monitor / Log Analytics Workspace | Pay-as-you-go | ACA stdout stream; no agent needed |

**Excluded from MVP**: Azure Front Door, Application Gateway, Azure OpenAI, AML endpoints,
Azure Functions — these are either Phase 12 concerns or Track B activation items.

---

## MVP vs fuller production

### Minimum viable Azure deployment

- Units 1–7 above, exactly as described
- Track A generation (Qwen3-8B / Ollama in container)
- Azure AI Search Free tier during development; Basic for demo runs
- Single ACA container (combined service + UI)
- Manual document upload to Blob via Azure Storage Explorer or CLI
- structlog JSON renderer → Log Analytics for basic observability
- **Estimated monthly cost (active demo)**: ~$75–100 (dominated by Azure AI Search Basic)
- **Estimated monthly cost (idle / scale-to-zero)**: ~$5–15 (storage + Key Vault ops + Log Analytics ingestion)

### Fuller production setup

| Addition | Purpose | When to add |
|---|---|---|
| Split ACA apps (backend + frontend) | Independent scaling of service vs UI | When multiple frontends are needed |
| GPU-enabled compute (ACA GPU profile or AML) | Faster embedding / reranking inference | When latency SLA requires it |
| Track B generation (Azure AI Foundry / Azure OpenAI) | Managed LLM, no Ollama daemon | When Ollama-in-container becomes operationally complex |
| Azure AI Search semantic ranker | Replace custom Qwen3-Reranker | After quality comparison with custom reranker |
| Embedding model switch (Azure OpenAI text-embedding-3-small) | Managed embeddings, no GPU in container | After re-indexing cost/benefit is justified |
| Application Insights (OpenTelemetry) | Distributed tracing, custom metrics | Phase 12 portfolio hardening |
| Azure Front Door | CDN + WAF for UI | If public-facing production |
| Azure Pipelines / GitHub Actions CI | Automated build + push to ACR | Phase 12 |

---

## Risks and tradeoffs

| Risk | Detail | Mitigation |
|---|---|---|
| Azure AI Search Basic cost | ~$75/month — significant for portfolio use | Use Free tier (1 index, 50 MB, no SLA) during development; switch to Basic only for demo runs |
| Container image size | Qwen3-8B (Ollama), Qwen3-Embedding-0.6B, Qwen3-Reranker-0.6B together produce a multi-GB image | Download model weights at container startup from Blob Storage; use multi-stage build to keep base image lean |
| Ollama daemon in ACA container | ACA supports multi-process containers but the entrypoint must start Ollama as a background process before the app | Standard pattern: shell entrypoint starts `ollama serve &` then launches the Python app; well-documented |
| Azure DI page cost | S0 charges $1.50/1,000 pages above 1,500 free pages/month | Add an upload guard in the indexing path to warn on large documents; acceptable for portfolio |
| LlamaIndex + Azure AI Search adapter | `llama-index-vector-stores-azureaisearch` must be validated against the current LlamaIndex version in use | Pin and test in Phase 11B before committing; local ChromaDB path remains active as fallback |
| No GPU on ACA consumption plan | Qwen3-Embedding (0.6B) and Qwen3-Reranker (0.6B) run on CPU — indexing latency is higher | Acceptable for portfolio demo; CPU inference is functional; GPU path deferred |
| Local vs cloud config parity | Two active backends (local / Azure) must be maintained without diverging the pipeline contract | `DEPLOYMENT_TARGET` env var gates adapter selection; project-native schemas enforce the contract at every boundary |
| PaddleOCR local retention | PaddleOCR is kept for local Windows dev even after Azure DI replaces it in cloud | Boundary 1 isolates the choice; both paths emit `ParsedPage`; no cross-cutting impact |

---

## What this document does not cover

- Bicep, Terraform, ARM, or deployment YAML
- Azure SDK integration code
- Docker or container entrypoint scripts
- CI/CD pipeline definitions
- Azure role assignment specifics

These are all Phase 11B (next execution step) deliverables.
