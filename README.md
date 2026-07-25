# Document Intelligence Copilot

A production-quality, local-first Retrieval-Augmented Generation (RAG) system for enterprise document analysis. Built with a **Hybrid-Hierarchical** retrieval architecture and optional Agentic Retrieval. Phase-by-phase engineering from raw document ingestion to verified, cited answer synthesis.

---

## Architecture

```
Raw Documents
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│  Ingestion Layer                                        │
│  text_reader / pdf_reader  ──►  router.py               │
│  ParsedPage (text + extraction_status OCR signal)       │
└────────────────────────┬────────────────────────────────┘
                         │
         ┌───────────────▼──────────────┐
         │  Parsing Lane (Phase 2+)     │
         │  Docling    │  RapidOCR     │
         │  (layout)   │  (OCR recovery)│
         └───────────────┬──────────────┘
                         │
         ┌───────────────▼──────────────┐
         │  Chunking Layer              │
         │  word_chunker (flat)         │
         │  hierarchical (Phase 3)      │
         └───────────────┬──────────────┘
                         │
         ┌───────────────▼──────────────┐
         │  Indexing (Phase 4)          │
         │  LlamaIndex SimpleVectorStore│
         │  Qwen3-Embedding-0.6B        │
         └───────────────┬──────────────┘
                         │
         ┌───────────────▼──────────────┐
         │  Hybrid Retrieval (Phase 5)  │
         │  Vector + BM25 + RRF fusion  │
         │  Qwen3-Reranker-0.6B         │
         └───────────────┬──────────────┘
                         │
         ┌───────────────▼──────────────┐
         │  Answer Synthesis (Phase 6)  │
         │  Qwen3-8B via Ollama         │
         └───────────────┬──────────────┘
                         │
         ┌───────────────▼──────────────┐
         │  Citations + Validation      │
         │  (Phases 7–8)                │
         └──────────────────────────────┘
```

---

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Schema / Config | Pydantic v2, pydantic-settings | Single source of truth models |
| Logging | structlog | Stdlib bridge, ConsoleRenderer |
| Document Parsing | Docling | Layout-aware; Phase 2 |
| OCR | Docling / RapidOCR (local); Azure DI (cloud) | Active. Local OCR recovery routes through Docling's embedded RapidOCR. PaddleOCR is a deferred future lane — not active. |
| Chunking | Custom sliding-window | Word-level, Phase 1; hierarchical, Phase 3 |
| Embedding | Qwen3-Embedding-0.6B | HuggingFace Hub; Phase 4 |
| Vector Store | LlamaIndex SimpleVectorStore (on-disk JSON, `data/index/`) | Active. ChromaDB is installed but **not** the active store in the current pipeline. |
| BM25 Index | rank-bm25 | Phase 5 |
| Reranker | Qwen3-Reranker-0.6B | HuggingFace Hub; Phase 5 |
| LLM | Qwen3-8B | Local Ollama daemon; Phase 6 |
| Framework | LlamaIndex | Core orchestration; Phase 4+ |
| Evaluation | RAGAS | Phase 9 |
| Service Layer | Gradio | Phase 10 |
| Cloud Mapping | Azure AI Document Intelligence, Azure Search | Phase 11 |

---

## Phase Plan

| # | Phase | Status |
|---|---|---|
| 0 | Audit and planning baseline | Complete |
| 1 | Foundation — schema, ingestion, chunking, storage | Complete |
| 2 | Parsing and OCR — Docling + RapidOCR | Complete — Docling layout parser + RapidOCR recovery active; Azure DI adapter wired |
| 3 | Hierarchical chunking — parent / child structure | Complete |
| 4 | Indexing and storage — LlamaIndex SimpleVectorStore | Complete |
| 5 | Hybrid retrieval and reranking | Complete |
| 6 | Answer synthesis — Qwen3-8B via Ollama | Complete |
| 7 | Citation builder and query routing | Complete |
| 8 | Rule-based validation | Complete |
| 9 | Evaluation — deterministic + semantic (LLM-judge) harnesses | Complete |
| 10 | Service layer — Gradio UI | Complete |
| 11 | Azure integration and deployment mapping | In Progress — adapters built; not production-deployed |
| 12 | Portfolio hardening — docs, benchmarks, demo | Planned |

---

## Repository Structure

```
document-intelligence-copilot/
├── src/
│   ├── schema/          # Canonical Pydantic v2 data models
│   ├── core/            # AppConfig (pydantic-settings)
│   ├── utils/           # text_utils, logging_utils
│   ├── ingestion/       # text/PDF readers, format router
│   │   └── readers/
│   ├── chunking/        # Sliding-window word chunker
│   ├── storage/         # JSONL artifact writer, run manifest
│   ├── parsing/         # Docling parser — parse_with_docling() (.docx active, .pdf next)
│   ├── ocr/             # Phase 2: OCR recovery routing (Docling/RapidOCR active; Azure DI adapter wired; PaddleOCR deferred)
│   ├── indexing/        # Phase 4: LlamaIndex stores
│   ├── retrieval/       # Phase 5: hybrid retriever
│   ├── reranking/       # Phase 5: Qwen3-Reranker postprocessor
│   ├── generation/      # Phase 6: Ollama LLM
│   ├── citations/       # Phase 7: citation builder
│   ├── validation/      # Phase 8: rule-based validators
│   └── evaluation/      # Phase 9: eval dataset runners
├── app/                 # Phase 10: service entry points and UI
├── docs/
│   ├── sample_docs/     # company_policy.txt, quarterly_summary.md, etc.
│   ├── diagrams/
│   └── notebooks/
├── tests/               # pytest smoke tests, one file per layer
├── data/
│   ├── processed/       # Pipeline output artifacts (JSONL, manifests)
│   └── chroma_db/       # Legacy ChromaDB (read-only reference)
├── requirements-core.txt        # Phase 1 dependencies only
├── requirements-full.txt        # Complete annotated dependency list
├── requirements-container.txt   # Container-safe subset (no PaddleOCR)
├── Dockerfile                   # Linux container image
├── run.py                       # Import-safe entrypoint (main() + __main__ guard)
├── .env.example                 # Runtime env-var contract
└── README.md
```

---

## Setup

### Prerequisites

- Python 3.12 (3.10/3.11 also supported; 3.12 required for the container image)
- [Ollama](https://ollama.com/download) installed and running (required for answer synthesis)

### Install core dependencies

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements-core.txt
```

### Run tests

```bash
pytest tests/ -v
```

### Build the local index (required before the "Ask" tab works)

Retrieval reads a persisted index from `data/index/`. On a fresh checkout that
directory does not exist yet, so a question asked before any document is indexed
will fail. Create the default index once with the bootstrap script:

```bash
# Index the default sample document (docs/sample_docs/company_policy.txt)
python scripts/bootstrap_index.py

# Or index a document of your choice
python scripts/bootstrap_index.py --file docs/sample_docs/company_policy.txt
python scripts/bootstrap_index.py --file data/raw/test_pdfs/adani.pdf
```

The first run downloads the embedding model, then writes `data/index/child_index/`
and `data/index/parent_store/`. Re-running the script rebuilds the index safely.

> **Vector store**: the active local vector store is LlamaIndex's
> `SimpleVectorStore`, persisted to disk under `data/index/`. **ChromaDB is not
> the active vector store** in the current pipeline (the `chromadb`-based scripts
> under `src/` are legacy prototypes only).

### Pull the generation model (Phase 6 onward)

```bash
ollama pull qwen3:8b
```

### Install full stack (when reaching each phase)

See `requirements-full.txt` for phase-annotated dependency groups. Install incrementally as each phase begins; do **not** install the full file at once.

---

## Running in a Container

The included `Dockerfile` builds a `python:3.12-slim` image with the container-safe dependency set.

```bash
# Build
docker build -t document-intelligence-copilot .

# Run — local defaults (Ollama on host)
docker run -p 7860:7860 \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  document-intelligence-copilot

# Run — Azure backends
docker run -p 7860:7860 \
  -e STORAGE_BACKEND=azure_blob \
  -e AZURE_STORAGE_ACCOUNT_URL=https://<account>.blob.core.windows.net \
  -e OCR_BACKEND=azure_di \
  -e AZURE_DI_ENDPOINT=https://<resource>.cognitiveservices.azure.com/ \
  -e SEARCH_BACKEND=azure_search \
  -e AZURE_SEARCH_ENDPOINT=https://<service>.search.windows.net \
  document-intelligence-copilot
```

The Gradio UI is available at `http://localhost:7860`.

Copy `.env.example` to `.env` and pass it to the container with `--env-file .env` for a cleaner invocation. See `.env.example` for the full runtime env-var contract.

> **Ollama topology**: Ollama is not bundled in the image. Point `OLLAMA_BASE_URL` at any running Ollama instance — local daemon, Docker network sidecar, or remote endpoint.

---

## Data Contract

All pipeline data flows through the canonical models defined in `src/schema/models.py`:

- `RawDocument` — file metadata and provenance (no text content)
- `ParsedPage` — per-page text content + extraction quality signal
- `ParsedBlock` — sub-page layout blocks (populated in Phase 2)
- `DocumentChunk` — sliding-window or hierarchical text chunks
- `RetrievedChunk` — chunks returned by the retriever with scores
- `CitationRecord` — verified source attribution records
- `AnswerResponse` — final LLM answer with citations and supporting chunks
- `RunManifest` — pipeline run provenance and artifact registry

---

## Legacy Scripts

The following scripts in `src/` are the original prototype and are **not** part of the new pipeline. They are preserved for reference:

| File | Notes |
|---|---|
| `src/ingest.py` | Flat JSONL ingestor, no schema — **deprecated** |
| `src/chunk.py` | Word sliding window, no schema — **deprecated** |
| `src/embed.py` | MiniLM + direct ChromaDB client — **deprecated** |
| `src/retrieve.py` | Vector-only CLI retriever — **deprecated** |
