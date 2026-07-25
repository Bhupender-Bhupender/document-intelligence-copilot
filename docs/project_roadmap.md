# Project Roadmap

Document Intelligence Copilot — phase-by-phase engineering plan.

> **Note**: Phase numbers appear ONLY in this file and in `docs/build_log.md`.
> No phase identifiers exist in source code, file names, folder names, or symbols.

---

## Phase Overview

| # | Name | Status | Key Deliverables |
|---|---|---|---|
| 0 | Audit and Planning Baseline | **Complete** | Dependency audit, architecture decision log, phase plan |
| 1 | Foundation | **Complete** | Schema models, ingestion readers, router, word chunker, storage, tests |
| 2 | Parsing and OCR | Planned | Docling lane, PaddleOCR lane, ParsedBlock population, extraction routing |
| 3 | Hierarchical Chunking | Planned | Parent/child chunk structure, DocumentChunk.chunk_level != "flat" |
| 4 | Indexing and Storage | Planned | LlamaIndex + ChromaDB abstraction, Qwen3-Embedding-0.6B, BM25 index |
| 5 | Hybrid Retrieval and Reranking | Planned | Vector + BM25, RRF fusion, Qwen3-Reranker-0.6B postprocessor |
| 6 | Answer Synthesis | Planned | Qwen3-8B via Ollama, prompt templates, AnswerResponse assembly |
| 7 | Citations and Query Routing | Planned | CitationRecord builder, agentic routing, query classification |
| 8 | Rule-Based Validation | **Complete** | Citation validation (7 rules), validation_flags, Stage 6 wired |
| 9 | Evaluation | **Complete** | `EvalExample`, `EvalReport`, `SemanticScore`, `SemanticEvalReport`, deterministic + semantic harnesses, judge prompts, 9A+9B combined: 94 tests |
| 10 | Service Layer | **In Progress** | 10A: `app/service.py`, `ServiceError`, `index_document()`, `answer_query()`, 32 tests (complete); 10B: Gradio UI (planned) |
| 11 | Azure Integration | **In Progress** | Architecture mapping complete (`docs/azure_architecture.md`); adapter implementation pending |
| 12 | Portfolio Hardening | Planned | Benchmarks, architecture diagrams, demo video, public documentation |

---

## Phase Details

### Phase 0 — Audit and Planning Baseline
**Status**: Complete

- Audited existing prototype scripts (`src/ingest.py`, `src/chunk.py`, `src/embed.py`, `src/retrieve.py`)
- Selected final technology stack (Qwen3 family, LlamaIndex, ChromaDB, Ollama, Docling, PaddleOCR)
- Established canonical data contract using Pydantic v2 models
- Resolved architecture decisions: `RawDocument` as metadata-only carrier, `ParsedPage` as content unit, `extraction_status` as OCR routing signal
- Confirmed Python 3.11 target; Windows OS requirements noted for PaddleOCR
- Defined 13-phase roadmap (0–12)

---

### Phase 1 — Foundation
**Status**: Complete

**Goal**: Establish the typed, tested skeleton that all future phases build on.

**Deliverables**:
- `src/schema/models.py` — 8 Pydantic v2 models (canonical data contract)
- `src/core/config.py` — AppConfig via pydantic-settings, module-level singleton
- `src/utils/text_utils.py` — `clean_text`, `normalize_text`, `classify_extraction_status`
- `src/utils/logging_utils.py` — structlog setup with stdlib bridge
- `src/ingestion/readers/text_reader.py` — `.txt`/`.md` reader
- `src/ingestion/readers/pdf_reader.py` — pypdf baseline, OCR-signal-compatible
- `src/ingestion/router.py` — format detection, `UnsupportedFormatError`
- `src/chunking/word_chunker.py` — flat sliding-window, `chunk_page`/`chunk_pages`
- `src/storage/artifact_writer.py` — `write_jsonl`, `read_jsonl_raw`
- `src/storage/run_manifest.py` — `start_run`, `complete_run`, `save_manifest`
- `tests/conftest.py`, `tests/test_schema.py`, `tests/test_ingestion.py`, `tests/test_chunking.py`
- `requirements-core.txt`, `requirements-full.txt`
- Deprecation notices added to legacy `src/embed.py` and `src/retrieve.py`
- Package stub `__init__.py` files for all future phase directories

**Key decisions**:
- Pydantic models are mutable (no `frozen=True`) to allow progressive enrichment
- `RawDocument.total_pages` set post-open (requires mutability)
- Empty pages (`extraction_status="empty"`) return `[]` from chunker — Phase 2 trigger
- Weak pages chunked as-is (may produce one small chunk)
- Legacy prototype scripts preserved untouched

---

### Phase 2 — Parsing and OCR
**Status**: Planned

**Prerequisites**:
- Validate PaddleOCR on Windows Python 3.11 with C++ Redistributable
- `pip install docling paddlepaddle paddleocr`

**Planned deliverables**:
- `src/parsing/docling_parser.py` — layout-aware parser for PDF, DOCX, HTML
  - Populates `ParsedBlock.bounding_box`, `ParsedBlock.block_type`, `ParsedBlock.reading_order`
  - Populates `ParsedPage.section_title` from heading detection
  - Sets `parse_method="docling"` on ParsedPage
- `src/ocr/paddle_ocr.py` — OCR engine for scanned/image PDFs
  - Routes pages where `extraction_status == "empty"` from pypdf reader
  - Sets `parse_method="paddleocr"`, `ocr_confidence`, `ocr_engine` on ParsedPage
- Update `src/ingestion/router.py` to route `.docx`, `.doc`, `.pptx`, `.xlsx`, `.html` through Docling
- Update `src/ingestion/readers/pdf_reader.py` to trigger PaddleOCR for empty pages
- Expand test suite: `tests/test_parsing.py`, `tests/test_ocr.py`

---

### Phase 3 — Hierarchical Chunking
**Status**: Planned

**Planned deliverables**:
- `src/chunking/hierarchical_chunker.py`
  - Parent chunks: section-level (large context window)
  - Child chunks: sentence-level (precision retrieval targets)
  - Sets `chunk_level="parent"` or `"child"`, `parent_chunk_id` linkage
- Integration with `ParsedBlock.section_title` for section-boundary detection
- Expand tests for parent/child linkage correctness

---

### Phase 4 — Indexing and Storage
**Status**: Planned

**Planned deliverables**:
- `src/indexing/` — LlamaIndex node constructors, ChromaDB vector store wrapper
- Qwen3-Embedding-0.6B as LlamaIndex `HuggingFaceEmbedding`
- BM25 index construction from chunks
- `DocumentChunk.is_indexed = True` and `embedding_model` set on indexed chunks
- `data/index/` as persistent index directory

---

### Phase 5 — Hybrid Retrieval and Reranking
**Status**: Complete (325 passed, 14 skipped)

**Completed deliverables**:
- `src/retrieval/vector_retriever.py` — dense child retrieval + parent lookup (Phase 5A)
- `src/retrieval/bm25_retriever.py` — BM25Plus sparse retrieval (Phase 5B)
- `src/retrieval/hybrid_retriever.py` — RRF fusion + deduplication (Phase 5C-1)
- `src/reranking/qwen_reranker.py` — Qwen3-Reranker-0.6B via `CrossEncoder.predict`; lazy loading, `_model` injection (Phase 5C-2)
- `RetrievedChunk` all scores populated: `vector_score`, `bm25_score`, `fusion_score`, `rerank_score`

**Deferred** (non-blocking for Phase 6):
- Metadata filtering by `file_name`, `page_number`, `section_title`
- Retrieval caching / warm-loading of index
- Real production index build at `data/index/`

---

### Phase 6 — Answer Synthesis
**Status**: Complete (371 passed, 16 skipped)

**Completed deliverables** (answer synthesis core):
- `src/generation/ollama_llm.py` — Ollama `/api/chat` wrapper via `httpx`; lazy client; `_client` injection; `generate(messages, model)` API
- `src/generation/prompt_templates.py` — `build_grounded_messages(query, context_blocks)` → `List[dict]` with system + user turns
- `src/generation/answer_engine.py` — `synthesise()` with parent-context enrichment and three-condition child fallback
- `src/generation/__init__.py` — exports `build_grounded_messages`, `generate`, `run_pipeline`, `synthesise`
- `tests/test_answer_engine.py` — 28 unit tests + 1 gated integration test (`OLLAMA_INTEGRATION_TESTS=1`)

**Completed deliverables** (pipeline wiring):
- `src/generation/answer_pipeline.py` — `run_pipeline()` coordinator: `retrieve_hybrid → rerank → lookup_parents → synthesise`
- `tests/test_answer_pipeline.py` — 18 unit tests + 1 gated integration test (`PIPELINE_INTEGRATION_TESTS=1`)
- Full suite: **371 passed, 16 skipped** (prior baseline: 353/15)

**Deferred to Phase 7 (citations)**:
- `CitationRecord` population — `sources` always `[]` until Phase 7
- Quote extraction from retrieved text

**Deferred to Phase 8 (validation)**:
- `validation_flags` population — always `[]` until Phase 8

**Deferred (non-blocking)**:
- Production index build at `data/index/` (real retrieval path blocked until index exists)
- Query routing / agentic behavior — later phase

---

### Phase 6 — Answer Synthesis
**Status**: Complete (371 passed, 16 skipped)

**Completed deliverables** (answer synthesis core):
- `src/generation/ollama_llm.py` — Ollama `/api/chat` wrapper via `httpx`; lazy client; `_client` injection; `generate(messages, model)` API
- `src/generation/prompt_templates.py` — `build_grounded_messages(query, context_blocks)` → `List[dict]` with system + user turns
- `src/generation/answer_engine.py` — `synthesise()` with parent-context enrichment and three-condition child fallback
- `src/generation/__init__.py` — exports `build_grounded_messages`, `generate`, `run_pipeline`, `synthesise`
- `tests/test_answer_engine.py` — 28 unit tests + 1 gated integration test (`OLLAMA_INTEGRATION_TESTS=1`)

**Completed deliverables** (pipeline wiring):
- `src/generation/answer_pipeline.py` — `run_pipeline()` coordinator: `retrieve_hybrid → rerank → lookup_parents → synthesise`
- `tests/test_answer_pipeline.py` — 18 unit tests + 1 gated integration test (`PIPELINE_INTEGRATION_TESTS=1`)
- Full suite: **371 passed, 16 skipped** (prior baseline: 353/15)

---

### Phase 7 — Citations & Query Routing
**Status**: Complete (422 passed, 16 skipped)

**Completed deliverables** (Phase 7A — citation construction):
- `src/citations/citation_builder.py` — pure `build_citations(chunks) -> List[CitationRecord]`; deterministic `citation_id` via sha256
- `src/citations/__init__.py` — exports `build_citations`
- `src/generation/answer_pipeline.py` — Stage 5 added; `AnswerResponse.sources` populated from reranked chunks
- `tests/test_citation_builder.py` — 21 unit tests across 6 classes

**Completed deliverables** (Phase 7B — query routing):
- `src/schema/models.py` — `RoutingPlan` model added (`query_type`, `retrieval_top_k`, `rerank_top_k`, `emphasize_parent_context`, `notes`)
- `src/retrieval/query_router.py` — deterministic `route_query(query) -> RoutingPlan`; five query types; heuristic-only (no LLM)
- `src/retrieval/__init__.py` — exports `route_query`
- `src/generation/answer_pipeline.py` — `routing_plan` param added; three active routing effects (top_k overrides + parent-context gate)
- `tests/test_query_router.py` — 30 unit tests across 7 classes
- Full suite: **422 passed, 16 skipped** (prior baseline: 392/16)

**Deferred to Phase 8 (validation)**:
- `validation_status` promotion from "unverified" → "valid"/"invalid"
- `validation_flags` population — always `[]` until Phase 8

**Deferred (non-blocking)**:
- Agentic multi-step retrieval loops (optional Agentic Retrieval path)
- Production index build at `data/index/`
- Citation quote extraction from generated answer text

---

### Phase 8A — Rule-Based Citation Validation (Complete)

**Deliverables completed**:
- `src/validation/validators.py` — `validate_response`, `_validate_citation` (7 rules), `_build_flags` (5 flags)
- `src/validation/__init__.py` — `validate_response` exported
- `src/generation/answer_pipeline.py` — Stage 6 wired; `_validator` injection param added
- `tests/test_validators.py` — 31 tests across 6 classes
- Full suite: **453 passed, 16 skipped** (prior baseline: 422/16)

**Validation rules implemented**:
1. `source_chunk_id is None` → invalid
2. `source_chunk_id` not in `supporting_chunks` → invalid
3. `doc_id` mismatch → invalid
4. `file_name` mismatch → invalid
5. `page_number` mismatch → invalid
6. Conditional `section_title` mismatch (when both sides present) → invalid
7. Verbatim span: bounds check + exact slice equality → invalid

**Response-level flags**: `no_supporting_chunks`, `no_sources`,
`citation_chunk_count_mismatch`, `missing_source_chunk_id`,
`invalid_citation_present`

**Deferred to Phase 9**:
- Evaluation metrics and answer quality scoring
- LLM-based grading

---

### Phase 9A — Deterministic Evaluation Harness (Complete)

**Deliverables completed**:
- `src/schema/eval_models.py` — `EvalExample`, `EvalReport` Pydantic models
- `src/evaluation/evaluator.py` — `run_evaluation`, `_compute_metrics`, `_rate`
- `src/evaluation/__init__.py` — `run_evaluation` exported
- `tests/test_evaluator.py` — 38 tests across 6 classes
- Full suite: **491 passed, 16 skipped** (prior baseline: 453/16)

**Metrics implemented** (9 deterministic):
`answer_non_empty_rate`, `citation_valid_rate`, `invalid_citation_rate`,
`no_source_rate`, `no_supporting_chunk_rate`, `source_hit_rate`,
`file_hit_rate`, `page_hit_rate`, `citations_all_valid_rate`, `flag_frequency`

**Zero-denominator rule**: `count / denom if denom > 0 else 0.0` — no fuzzy fallback anywhere.

**Deferred to Phase 9B**:
- RAGAS-style semantic metrics (faithfulness, context precision/recall)
- LLM-based answer quality grading
- Eval dataset building and storage
- Answer relevance scoring

---

### Phase 9B — Semantic Evaluation Harness (Complete)

**Deliverables completed**:
- `src/schema/semantic_eval_models.py` — `SemanticScore`, `SemanticEvalReport` Pydantic models
- `src/evaluation/judge_prompts.py` — `build_judge_messages` judge prompt template
- `src/evaluation/semantic_evaluator.py` — `run_semantic_evaluation`, `_parse_scores`, `_aggregate_scores`
- `src/evaluation/__init__.py` — `run_semantic_evaluation` exported
- `tests/test_semantic_evaluator.py` — 56 tests + 1 gated integration test across 7 classes
- Full suite: **547 passed, 17 skipped** (prior baseline: 491/16)

**Semantic metrics**: `mean_groundedness`, `mean_answer_relevance`, `mean_context_relevance`, `mean_completeness`, `above_threshold_rate`, `parse_failure_count`

**Contract enforced**:
- Length guard: `ValueError` if `len(examples) != len(responses)`
- Parse failures: 0.0 scores, included in means, counted in `parse_failure_count`
- Normalization: whitespace strip + single `` ```json ``` `` fence removal only
- Score clamping: `[0.0, 1.0]` on all four dimensions
- Judge injection: `_judge` param for test isolation; gated integration test for live Ollama

---

### Phases 10–12

---

### Phase 10A — Service Layer (Complete)

**Completed deliverables**:
- `app/service.py` — `ServiceError`, `index_document()`, `answer_query()`; lazy pipeline imports; full kwarg forwarding; `raise ServiceError(...) from exc` chaining
- `app/__init__.py` — re-exports `index_document`, `answer_query`, `ServiceError`
- `tests/test_service.py` — 32 unit tests across 4 classes

---

### Phase 10B — Local Gradio Blocks UI (Complete)

**Completed deliverables**:
- `app/ui.py` — `build_ui()` returns a `gr.Blocks` app; two-tab layout (Index Document / Ask a Question); format helpers (`_format_index_result`, `_format_citations`, `_format_flags`); event handlers (`_handle_index`, `_handle_answer`)
- `app/__init__.py` — updated to also export `build_ui`
- `tests/test_ui.py` — 41 tests across 6 classes: `TestFormatIndexResult`, `TestFormatCitations`, `TestFormatFlags`, `TestHandleIndex`, `TestHandleAnswer`, `TestBuildUi`

**Key properties**:
- `gr.File(..., type="filepath")` — explicit contract; handler receives `Optional[str]`; no duck-typing
- Import-light hard rule: `from __future__ import annotations`; `TYPE_CHECKING` guards; `import gradio as gr` lazy inside `build_ui()` only
- `ServiceError` surfaced with clean prefix; unexpected exceptions shown with separate generic prefix; no raw tracebacks
- Three-output answer handler: `(answer_text, citations, flags)`
- UI calls only `app.service` — never reaches into `src/` directly

**Test evidence**:
- `tests/test_ui.py` alone: **40 passed, 1 skipped** (Gradio not installed; `pytest.importorskip` guard)
- Fast-suite regression check: **256 passed, 3 skipped** (prior 216 + 40 new; zero regressions)

---

### Phase 11 — Azure Integration

**Status**: Architecture mapping complete. Adapter implementation is the next step.

**Completed (architecture mapping)**:
- `docs/azure_architecture.md` — full component-to-Azure mapping, substitution boundaries, two-track generation stance, deployment unit inventory, MVP vs production comparison, risks

**Architecture decisions locked**:
- **Document storage**: Azure Blob Storage (3 containers: raw, processed, eval-reports)
- **OCR backend**: Azure AI Document Intelligence replaces PaddleOCR in cloud; Docling retained for layout/structured docs
- **Retrieval backend**: Azure AI Search replaces ChromaDB + rank-bm25 + RRF fusion; Qwen3-Reranker kept in container
- **Hosting**: Azure Container Apps (consumption plan); combined service + UI container for MVP
- **Generation Track A (MVP)**: Qwen3-8B / Ollama in container — preferred path; no Azure LLM cost
- **Generation Track B (deferred)**: managed endpoint via `azure_llm.py` adapter; activated by `GENERATION_BACKEND=azure`
- **Secrets**: Azure Key Vault + Managed Identity + ACA secret references
- **Observability**: ACA stdout → Azure Monitor / Log Analytics; structlog JSON renderer via `LOG_FORMAT=json`

**Substitution boundaries (Phase 11B insertion points)**:
- Boundary 1 — OCR: `src/ocr/azure_di_ocr.py` adapter; emits `ParsedPage`; upstream unchanged
- Boundary 2 — Retrieval: `src/indexing/azure_search_indexer.py` + `src/retrieval/azure_search_retriever.py`; emit `IndexManifest` / `List[RetrievedChunk]`; downstream unchanged
- Boundary 3 — Storage: `src/storage/blob_artifact_writer.py`; emits same `RunManifest` / JSONL contracts
- Boundary 4 — Generation: `src/generation/azure_llm.py` (Track B only); `answer_pipeline.py` unchanged
- Hard invariant: `app/service.py` and `app/ui.py` are **not touched** by any Azure work

**Deployment units (7)**:
1. Azure Container Apps — app container (service + UI + Ollama)
2. Azure Container Registry — image store
3. Azure Blob Storage — documents-raw, documents-processed, eval-reports
4. Azure AI Search — vector + hybrid retrieval index
5. Azure AI Document Intelligence — OCR / layout (S0)
6. Azure Key Vault — all secrets and connection strings
7. Azure Monitor / Log Analytics Workspace — structured log stream

**Completed (first execution chunk)**:
- `src/storage/blob_artifact_writer.py` — Blob Storage artifact adapter (active)
- `src/storage/artifact_store.py` — config-switch gateway; `artifact_writer.write_jsonl` and `run_manifest.save_manifest` delegate through it
- `src/core/config.py` — `storage_backend`, `azure_storage_account_url`, `azure_storage_container_artifacts`, `azure_storage_container_manifests` added
- `requirements-full.txt` — `azure-storage-blob>=12.0,<13` and `azure-identity>=1.15,<2` uncommented
- `tests/test_blob_artifact_writer.py` — 15 tests; 271 passed, 3 skipped (fast-suite)

**Completed (second execution chunk)**:
- `src/ocr/azure_di_ocr.py` — Azure DI OCR adapter; `src/ocr/ocr_router.py` made import-light
- `src/core/config.py` — `ocr_backend`, `azure_di_endpoint` added
- `requirements-full.txt` — `azure-ai-documentintelligence>=1.0,<2` uncommented
- `tests/test_azure_di_ocr.py` — 17 tests; 288 passed, 3 skipped (fast-suite)

**Completed (third execution chunk)**:
- `src/indexing/azure_search_indexer.py` — Azure AI Search indexer adapter
- `src/indexing/index_gateway.py` — `route_index()` config-switch gateway
- `src/retrieval/azure_search_retriever.py` — Azure AI Search retriever (BM25 + parent lookup)
- `src/retrieval/retrieval_gateway.py` — `route_retrieve()` and `route_lookup_parents()` gateways
- `src/core/config.py` — `search_backend`, `azure_search_endpoint`, `azure_search_index_name` added
- `requirements-full.txt` — `azure-search-documents>=11.0,<12` uncommented
- `tests/test_azure_search_adapters.py` — 27 tests; 315 passed, 13 skipped (full tests/ suite)

**Completed (fourth execution chunk)**:
- `requirements-container.txt` — container-safe dependency subset (excludes PaddleOCR/PaddlePaddle)
- `Dockerfile` — `python:3.12-slim` single-stage image; `libgomp1`, port 7860, `CMD ["python", "run.py"]`
- `run.py` — import-safe entrypoint; `main()` + `__main__` guard; `GRADIO_SERVER_NAME`/`GRADIO_SERVER_PORT` env-var contract
- `.env.example` — full runtime env-var contract (bind, logging, generation, embedding, all backend switches)
- `tests/test_container_runtime.py` — 11 tests (`TestEntrypoint`, `TestEnvVarContract`); all pass
- `tests/test_ocr.py` — all 18 stale `parse_with_docling` mock targets fixed → `_run_local_ocr`
- Full suite: **689 passed, 18 skipped**

**Pending for next execution chunks**:
- `src/generation/azure_llm.py` — managed generation adapter (Track B, deferred)
- ACA deployment manifests (deferred)

---

### Phase 12 — Portfolio Hardening
See phase overview table above. Details will be added as this phase approaches.
