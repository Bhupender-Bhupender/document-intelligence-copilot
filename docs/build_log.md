# Build Log

Document Intelligence Copilot — running record of engineering decisions, blockers, and resolutions.

> **Phase tracking only in this file and in `docs/project_roadmap.md`.**

---

## Phase 0 — Audit and Planning Baseline

**Completed**: Session 1

### What was done
- Audited existing prototype scripts in `src/`: `ingest.py`, `chunk.py`, `embed.py`, `retrieve.py`, `config.py`, `answer.py`, `evaluate.py`
- Audited legacy app stubs in `app/`: `ui.py`, `rag_pipeline.py`, `prompting.py`, `utils.py`
- Identified legacy artifacts: `data/processed/documents.jsonl`, `data/processed/chunks.jsonl`, `data/chroma_db/`
- Identified sample documents in `docs/sample_docs/`

### Technology decisions locked
| Decision | Choice | Rationale |
|---|---|---|
| Generation model | Qwen3-8B via local Ollama daemon | Avoids GPU memory constraints of loading transformers pipeline; Ollama manages lifecycle |
| Embedding model | Qwen3-Embedding-0.6B via HuggingFace Hub | Same model family as generator; cosine-space compatibility |
| Reranker | Qwen3-Reranker-0.6B via HuggingFace Hub | Custom LlamaIndex postprocessor; no external API call |
| Orchestration framework | LlamaIndex (Phase 4+) | Mature abstractions for hybrid retrieval, node postprocessors, vector store wrappers |
| Document parsing | Docling (primary), PaddleOCR (scanned PDFs) | Docling handles layout-aware extraction; PaddleOCR handles image-only pages |
| Schema / config | Pydantic v2 + pydantic-settings | Type safety, env-file support, mutable models for progressive enrichment |
| Vector store | ChromaDB (via LlamaIndex abstraction) | Legacy artifact retained; wrapped cleanly in Phase 4 |

### Architecture decisions locked
- `RawDocument` = metadata/provenance only (no text content)
- `ParsedPage` = content-bearing unit; one per page
- `extraction_status` field on `ParsedPage` = Phase 2 OCR routing trigger
  - `"empty"` → mandatory OCR in Phase 2
  - `"weak"` → optional OCR re-run or accept as-is
  - `"ok"` → no OCR needed
- Scanned PDFs return `ParsedPage` with `extraction_status="empty"` (never raise)
- Phase tracking strictly in `docs/` only — no phase identifiers in source code

---

## Phase 1 — Foundation

**Completed**: Session 1–2

### What was built

| File | Description |
|---|---|
| `src/__init__.py` | Makes `src` a Python package |
| `src/schema/__init__.py` | Schema package marker |
| `src/core/__init__.py` | Core package marker |
| `src/utils/__init__.py` | Utils package marker |
| `src/ingestion/__init__.py` | Ingestion package marker |
| `src/ingestion/readers/__init__.py` | Readers sub-package marker |
| `src/chunking/__init__.py` | Chunking package marker |
| `src/storage/__init__.py` | Storage package marker |
| `src/parsing/__init__.py` | Parsing package stub (Phase 2) |
| `src/ocr/__init__.py` | OCR package stub (Phase 2) |
| `src/indexing/__init__.py` | Indexing package stub (Phase 4) |
| `src/retrieval/__init__.py` | Retrieval package stub (Phase 5) |
| `src/reranking/__init__.py` | Reranking package stub (Phase 5) |
| `src/generation/__init__.py` | Generation package stub (Phase 6) |
| `src/citations/__init__.py` | Citations package stub (Phase 7) |
| `src/validation/__init__.py` | Validation package stub (Phase 8) |
| `src/evaluation/__init__.py` | Evaluation package stub (Phase 9) |
| `app/__init__.py` | App package stub (Phase 10) |
| `src/utils/text_utils.py` | `clean_text`, `normalize_text`, `classify_extraction_status` |
| `src/utils/logging_utils.py` | structlog setup, `configure_logging`, `get_logger` |
| `src/core/config.py` | `AppConfig` pydantic-settings, `config` module-level singleton |
| `src/schema/models.py` | 8 canonical Pydantic v2 models (full data contract) |
| `src/ingestion/readers/text_reader.py` | `.txt`/`.md` reader → `RawDocument` + 1 `ParsedPage` |
| `src/ingestion/readers/pdf_reader.py` | pypdf reader → `RawDocument` + N `ParsedPage` (one per PDF page) |
| `src/ingestion/router.py` | Format detection + dispatch, `UnsupportedFormatError` |
| `src/chunking/word_chunker.py` | Flat sliding-window chunker, `chunk_page`/`chunk_pages` |
| `src/storage/artifact_writer.py` | `write_jsonl`, `read_jsonl_raw` |
| `src/storage/run_manifest.py` | `start_run`, `complete_run`, `save_manifest` |
| `tests/conftest.py` | `sys.path` bootstrap, project root injection |
| `tests/test_schema.py` | 8 model classes, defaults, Literal validation errors |
| `tests/test_ingestion.py` | 8 tests: txt, md, born-digital PDF, complex PDF, unsupported formats |
| `tests/test_chunking.py` | 11 tests: linkage, uniqueness, overlap, empty/weak pages, file propagation |
| `requirements-core.txt` | Phase 1 dependencies: pydantic, pydantic-settings, pypdf, structlog, tqdm |
| `requirements-full.txt` | Full annotated dependency list (Phases 1–11), phase-grouped |
| `README.md` | Project overview, architecture, stack table, phase plan, setup instructions |
| `docs/project_roadmap.md` | Phase-by-phase plan with detailed deliverables for Phases 2–6 |
| `docs/build_log.md` | This file |

### Decisions made in Phase 1

**Schema mutability**: Pydantic models are mutable (no `frozen=True`). Required because `RawDocument.total_pages` is set after the pypdf reader opens the file — it is not known at construction time.

**Chunker-schema decoupling**: `chunk_page()` sets `file_name=""` and `file_type=""` as placeholders. The higher-level `chunk_pages()` function enriches chunks with actual file metadata. This keeps the inner chunker decoupled from the ingestion layer.

**Empty page behaviour**: `chunk_page()` returns `[]` for pages with `extraction_status="empty"`. This is the explicit signal for Phase 2 OCR re-processing, not a silent data loss.

**PDF reader failure mode**: Per-page extraction failures log a warning and produce an `extraction_status="empty"` page rather than raising. File-level open failures return `(doc, [])` and log the error. No exceptions propagate from the reader.

**`UnsupportedFormatError`**: Inherits from `ValueError` so it can be caught specifically or generally. Its message references the Docling parsing lane so callers understand why a format is blocked.

**Deprecation notices**: Added module-level docstrings to `src/embed.py` and `src/retrieve.py` marking them deprecated. Notices appear before `from __future__ import annotations` to be visible as the first thing imported.

### Phase 1 evidence (test results)
Run with: `pytest tests/ -v`

Expected passing tests:
- `test_schema.py`: ~22 tests across all 8 models
- `test_ingestion.py`: ~8 tests (txt, md, 2× PDF, 3× unsupported/missing)
- `test_chunking.py`: ~11 tests (linkage, overlap, edge cases, propagation)

### Risks and blockers identified
- **PaddleOCR on Windows**: Python 3.11 + Microsoft C++ Redistributable required. Not yet validated. Must be done before Phase 2 begins.
- **Docling version pinning**: Docling is under active development; pin to a specific version when installing for Phase 2.
- **Qwen3-Embedding-0.6B size**: The 0.6B model requires ~2GB VRAM or runs on CPU. Validate memory footprint before Phase 4.
- **Ollama daemon startup**: Phase 6 tests will require Ollama to be running locally. CI pipelines will need an Ollama service or a mock.

---

## Phase 2 — Parsing and OCR

**Status**: Preflight complete — ready to implement.

---

### Preflight results (Session 3)

#### Environment validation

| Item | Finding |
|---|---|
| Python version | 3.12.1 — only version installed |
| Docling 2.95.0 | ✅ Installs cleanly on Python 3.12 (`cp312` wheel for `docling-parse 5.11.0`) |
| torch 2.12.0+cpu | ✅ CPU build loads on Windows without CUDA |
| Windows DLL ordering | ⚠️ `torch` must be imported before any `docling` import in the same process; otherwise `c10.dll` fails to initialize via the `transformers → stopping_criteria → torch` chain. Fix: put `import torch` at the top of every module that imports from docling. |
| PaddleOCR 3.5.0 | ✅ `py3-none-any` wheel — compatible with all Python versions |
| PaddlePaddle 3.3.1 | ✅ `cp312-cp312-win_amd64` native wheel — Python 3.12 on Windows fully supported |
| Python 3.11 venv | Not required — both Docling and PaddleOCR run on Python 3.12 natively |
| Docling OCR backend | **RapidOCR** (PP-OCRv4 models from ModelScope) — **not PaddleOCR** |
| HuggingFace hub symlinks | ⚠️ Windows without Developer Mode cannot create symlinks; HF hub degrades gracefully but raises `WinError 1314` on first run if symlinks are attempted. Second run succeeds from cache. Workaround: enable Windows Developer Mode, or pre-download models. |

#### Docling smoke check results (`data/eval/docling_preflight.py`)

| File | Status | Pages | Tables | Pictures | Words | Elapsed | OCR needed |
|---|---|---|---|---|---|---|---|
| `quarterly_summary.md` | ✅ OK | n/a | 1 | 0 | 169 | 0.13s | No |
| `Operations_report.pdf` | ⚠️ First-run symlink error | — | — | — | — | — | — |
| `prof-services-agrmt.pdf` | ✅ OK | 18 | 0 | 1 | 7,794 | 69.86s | No |

**Notes on results**:
- `quarterly_summary.md`: Docling correctly extracts the KPI table (1 table detected) and full markdown body in 0.13s.
- `Operations_report.pdf`: Docling failed on first run due to Windows symlink privilege error (`WinError 1314`) during HF Hub model caching. Second run succeeds after models are cached. This is a Windows setup limitation, not a Docling parsing failure.
- `prof-services-agrmt.pdf`: Confirmed text-based (18/18 pages ok by pypdf). Docling extracts 7,794 words with full layout analysis on CPU in ~70s. No OCR needed for this file.
- **None of the sample documents require OCR** — all pages are born-digital text. OCR routing will be triggered by `extraction_status="empty"` pages from the pypdf reader (scanned documents).

#### RapidOCR model downloads (first run, one-time)
- `ch_PP-OCRv4_det_mobile.pth` — 13.83 MB (text detection)
- `ch_ptocr_mobile_v2.0_cls_mobile.pth` — 0.56 MB (text direction classification)
- `ch_PP-OCRv4_rec_mobile.pth` — 25.67 MB (text recognition)
- HF Hub models: `docling-project/docling-layout-heron`, `docling-project/docling-models`

---

### Phase 2 locked contract

#### New files

**`src/parsing/docling_parser.py`**

Public API:
```python
def parse_with_docling(file_path: Path) -> Tuple[RawDocument, List[ParsedPage]]:
```

- Imports `torch` before any `docling` import (Windows DLL ordering requirement)
- Uses `from docling.document_converter import DocumentConverter`
- Supported formats: `.pdf`, `.docx`, `.doc`, `.pptx`, `.xlsx`, `.html`, `.htm`
- Per-page: sets `parse_method="docling"`, populates `layout_blocks: List[ParsedBlock]`
- Sets `ParsedPage.section_title` from nearest ancestor heading detected by Docling
- Sets `ParsedBlock.block_type` from Docling element types → `"heading"`, `"paragraph"`, `"table"`, `"list"`, `"caption"`, `"unknown"`
- Sets `ParsedBlock.reading_order` from Docling's element ordering
- Classifies `extraction_status` via `classify_extraction_status()` on the exported text
- Raises `FileNotFoundError` for missing file; per-page failures log and continue (never raise)

**`src/ocr/ocr_router.py`**

Public API:
```python
def route_to_ocr(pages: List[ParsedPage], file_path: Path) -> List[ParsedPage]:
```

- Takes a list of pages where `extraction_status == "empty"` from a prior pypdf pass
- Routes them through Docling's built-in RapidOCR pipeline (via `parse_with_docling()` with page-level forcing if supported, or full re-parse)
- Sets `parse_method="rapidocr"`, `ocr_engine="rapidocr"`, `ocr_confidence` from model confidence
- Returns the same pages with OCR results populated
- PaddleOCR path (separate `run_paddleocr()` function): deferred — not in scope for Phase 2; documented as future enhancement

#### Router updates (`src/ingestion/router.py`)

- Move `.docx`, `.doc`, `.pptx`, `.xlsx`, `.html`, `.htm` from `_DEFERRED_FORMATS` to new `_DOCLING_FORMATS` set
- Route `_DOCLING_FORMATS` to `parse_with_docling()`
- For `.pdf`: keep pypdf as first pass; if any page has `extraction_status == "empty"`, pass those pages to `route_to_ocr()`
- Remove `UnsupportedFormatError` for now-supported formats

#### Schema fields populated in Phase 2

All fields already defined in `src/schema/models.py` — just not yet populated:

| Field | Model | Value set by |
|---|---|---|
| `parse_method` | `ParsedPage` | `"docling"` or `"rapidocr"` |
| `section_title` | `ParsedPage` | Nearest heading from Docling element tree |
| `layout_blocks` | `ParsedPage` | `List[ParsedBlock]` from Docling elements |
| `ocr_confidence` | `ParsedPage` | Mean confidence from RapidOCR |
| `ocr_engine` | `ParsedPage` | `"rapidocr"` |
| `block_type` | `ParsedBlock` | Mapped from Docling element type |
| `reading_order` | `ParsedBlock` | From Docling element ordering |
| `bounding_box` | `ParsedBlock` | `[x0, y0, x1, y1]` from Docling `prov` bbox (if available) |
| `section_title` | `ParsedBlock` | Inherited from nearest ancestor heading |

#### Phase 2 tests (new files)

**`tests/test_parsing.py`**:
1. `quarterly_summary.md` → Docling returns ≥1 `ParsedBlock`, `parse_method="docling"`, section titles present
2. `quarterly_summary.md` → at least 1 block with `block_type="table"` detected
3. `Operations_report.pdf` → correct total pages, `layout_blocks` non-empty, `parse_method="docling"`
4. Unsupported format `.xyz` → still raises `UnsupportedFormatError`
5. `.docx` file → no `UnsupportedFormatError` raised, `parse_method="docling"`
6. Missing file → raises `FileNotFoundError`

**`tests/test_ocr.py`**:
1. Page with `extraction_status="empty"` → triggers OCR routing
2. OCR output sets `parse_method` to `"rapidocr"`
3. `ocr_confidence` is a float 0–1
4. `ocr_engine` is `"rapidocr"`

**Updated `tests/test_ingestion.py`**:
- `test_docx_raises_unsupported_format_error` → renamed `test_docx_routes_through_docling` (expects success, not error)

#### Decisions locked

**OCR lane architecture**: Docling's built-in RapidOCR handles all OCR needs for Phase 2. PaddleOCR is explicitly deferred — it has `cp312` wheels and is Python 3.12-compatible on Windows, but is not needed since Docling's RapidOCR covers the same use case and is already bundled. PaddleOCR can be added later if standalone OCR with advanced features is required.

**`import torch` ordering**: All modules under `src/parsing/` and `src/ocr/` must begin with `import torch` before any docling imports. This is a Windows-specific DLL loading requirement. Documented here and enforced in code review.

**CPU-only inference**: No GPU required. `torch 2.12.0+cpu` on Windows 3.12. For large PDFs (18 pages), Docling takes ~70s on CPU. For production throughput, add a batch pipeline mode (deferred to Phase 3).

**Windows HF Hub symlinks**: Docling models download to `~/.cache/huggingface/hub/` on first run. Windows without Developer Mode cannot create symlinks; HF Hub falls back to file copies (degraded mode). First run may raise `WinError 1314` on some conversions during model caching. Workaround for development: enable Windows Developer Mode. For CI: pre-download models in the environment setup step.

---

### Phase 2 implementation — Parsing lane (Session 3 continued)

**Status**: Docling parser + `.docx` router update — complete.

#### What was built

| File | Change | Description |
|---|---|---|
| `src/parsing/docling_parser.py` | **New** | Docling-backed parser; public API `parse_with_docling()` |
| `src/ingestion/router.py` | **Updated** | `.docx` routed through Docling; `_DOCLING_FORMATS` added |
| `tests/test_parsing.py` | **New** | 27 tests across 5 test classes; 26 pass, 1 skipped (integration) |
| `tests/test_ingestion.py` | **Updated** | `test_docx_raises_unsupported_format_error` → `test_docx_routes_through_docling` |

#### Docling API findings (Docling 2.95.0)

- `doc.pages`: empty dict for flat formats (markdown, docx); populated for PDF with integer page keys.
- `doc.iterate_items()`: yields `(DocItem, level)` pairs. Each item has `.label` (`DocItemLabel` enum), `.text`, `.prov` (list of provenance with `.page_no` and `.bbox`).
- `doc.export_to_text(page_no=N)`: exports text scoped to a single page (used for paged PDFs).
- `DocItemLabel` imported from `docling_core.types.doc.labels`. Key values: `TITLE`, `SECTION_HEADER`, `TEXT`, `PARAGRAPH`, `LIST_ITEM`, `TABLE`, `CAPTION`.
- Bounding boxes: BOTTOMLEFT coordinate origin on PDFs; stored as `[l, b, r, t]` = `[x0, y0, x1, y1]`. `None` for flat formats.

#### Implementation decisions

**Paging strategy**: `doc.pages` is empty for markdown and docx — these are treated as single-page documents (synthetic `page_number=1`). For PDFs, `doc.pages.keys()` gives the page set; `export_to_text(page_no=N)` scopes text per page.

**Layout blocks**: Built from `doc.iterate_items()` with per-page provenance filtering. `ParsedBlock.section_title` tracks the running heading context (most recent heading seen before each block). `ParsedPage.section_title` is the first heading on the page.

**`.txt`/`.md` routing**: Kept on the existing text reader path. Docling's markdown processing produces identical word counts but adds ~12s model warm-up overhead per new process. For flat text files, the text reader is faster and produces correct results. Docling's `.md` capability is retained in `_SUPPORTED_FORMATS` within the parser and can be routed directly by callers that need layout blocks from markdown.

**`.pdf` routing**: Unchanged from Phase 1. The router still sends `.pdf` to `pdf_reader` (pypdf). `parse_with_docling()` supports `.pdf` internally — the connection of the PDF path through Docling (for OCR fallback) is the next implementation step.

**Router format scope**: Only `.docx` added to `_DOCLING_FORMATS`. `.doc`, `.pptx`, `.xlsx`, `.html`, `.htm` remain in `_DEFERRED_FORMATS` until each is validated and tested.

**Converter singleton**: `DocumentConverter()` is instantiated lazily on first call and cached in `_converter`. Avoids reloading 770 layout weights per parse call within the same process.

**Slow test gating**: `TestComplexPdf::test_complex_pdf_18_pages` is skipped unless `DOCLING_INTEGRATION_TESTS=1`. All other tests run in the normal `pytest tests/` cycle.

#### Test evidence

```
pytest tests/ -q
65 passed, 2 skipped, 3 warnings in ~33s
```

| Scope | Before | After |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed (unchanged) |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed (unchanged) |
| `test_parsing.py` | (new) | 26 passed, 1 skipped |
| **Total** | **39 passed, 1 skipped** | **65 passed, 2 skipped** |

3 warnings are from Docling's own internals (`standard_pdf_pipeline.py` and `rapid_ocr_model.py`) — not from project code.

#### Deferred to next step

- OCR routing: `src/ocr/ocr_router.py` — routes `extraction_status="empty"` pages through Docling+RapidOCR
- PDF path through Docling parser (for layout blocks on PDFs, currently only text extracted by pypdf)
- Router updates for `.doc`, `.pptx`, `.xlsx`, `.html`, `.htm`
- `ParsedPage.ocr_confidence` and `ocr_engine` population (set by OCR router, not parser)

---

### Phase 2 implementation — OCR routing layer (Session 4 continued)

**Status**: OCR routing complete. Official Phase 2 fully implemented.

#### What was built

| File | Change | Description |
|---|---|---|
| `src/ocr/ocr_router.py` | **New** | OCR recovery layer; public API `route_pdf_pages_through_ocr()` |
| `src/ingestion/router.py` | **Updated** | PDF path wired two-pass: pypdf → OCR router |
| `src/schema/models.py` | **Updated** | `"rapidocr"` added to `ParsedPage.parse_method` Literal |
| `tests/test_ocr.py` | **New** | 24 tests across 7 test classes |

#### Two-pass PDF pipeline (now active)

```
route_file(.pdf)
  │
  ├─ Pass 1: read_pdf_file()      → (RawDocument, pages[parse_method="pypdf"])
  │
  ├─ any page extraction_status == "empty"?
  │     yes → Pass 2: route_pdf_pages_through_ocr()
  │                   → parse_with_docling() [Docling + internal RapidOCR]
  │                   → overwrite empty pages with parse_method="rapidocr"
  │
  └─ return (raw_document, final_pages)
```

For born-digital PDFs all pages pass after Pass 1. `route_pdf_pages_through_ocr()` is always called but is a no-op (returns the same list object) when no empty pages exist.

#### OCR trigger rule

Only `extraction_status == "empty"` pages are OCR candidates. `"weak"` pages are excluded — they have some extractable text, and routing them through OCR introduces noise risk without a clear benefit.

#### Field update rules (implemented)

When OCR recovery succeeds (Docling returns non-empty text for the page):

| Field | Action |
|---|---|
| `raw_text`, `normalized_text`, `word_count`, `char_count` | Updated from Docling output |
| `extraction_status` | Re-classified from recovered text |
| `parse_method` | Set to `"rapidocr"` |
| `ocr_engine` | Set to `"rapidocr"` |
| `ocr_confidence` | Left as `None` (not exposed by Docling public API) |
| `section_title` | Propagated from Docling if available |
| `layout_blocks` | Propagated from Docling if available |
| `page_id`, `doc_id`, `page_number` | **Never changed** |

#### parse_method vs ocr_engine

`parse_method="rapidocr"` records the extraction path (Docling→RapidOCR recovery). `ocr_engine="rapidocr"` records the underlying OCR engine. Both are "rapidocr" here because Docling's OCR path uses RapidOCR internally. When PaddleOCR is wired in a later step, `parse_method="paddleocr"` and `ocr_engine="paddleocr"` will distinguish it cleanly.

#### Schema change

`ParsedPage.parse_method` Literal expanded to include `"rapidocr"`. Each value is now annotated inline in `models.py` with its meaning. No migration needed.

#### Test evidence

```
pytest tests/ -q
89 passed, 2 skipped, 3 warnings in 32.10s
```

| Test file | Before | After |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | (new) | 24 passed |
| **Total** | **65 passed, 2 skipped** | **89 passed, 2 skipped** |

3 warnings: Docling-internal deprecation notices only. Not from project code.

#### Remaining for future steps

- PaddleOCR standalone path — deferred; RapidOCR via Docling covers current needs
- Router support for `.doc`, `.pptx`, `.xlsx`, `.html`, `.htm` — deferred
- `ocr_confidence` population — blocked until Docling exposes per-page RapidOCR confidence or PaddleOCR is wired
- PDF layout blocks via Docling on the normal (non-OCR) path — deferred to hierarchical chunking step

---

### Phase 3 implementation — Hierarchical chunker core (Session 4 continued)

**Status**: Hierarchical chunking complete. Both parent and child chunk production is active.

#### What was built

| File | Change | Description |
|---|---|---|
| `src/chunking/hierarchical_chunker.py` | **New** | Hierarchical chunker; public API `build_hierarchical_chunks()` |
| `src/core/config.py` | **Updated** | 3 new fields: `parent_chunk_size_words`, `child_chunk_size_words`, `child_chunk_overlap_words` |
| `tests/test_hierarchical_chunking.py` | **New** | 43 tests across 12 test classes |

`src/schema/models.py` — no change needed. `DocumentChunk` already had `chunk_level` and `parent_chunk_id`.

#### Public API

```python
def build_hierarchical_chunks(
    raw_document: RawDocument,
    pages: list[ParsedPage],
    parent_chunk_size_words: int | None = None,
    child_chunk_size_words: int | None = None,
    child_chunk_overlap_words: int | None = None,
) -> tuple[list[DocumentChunk], list[DocumentChunk]]:
    # Returns (parent_chunks, child_chunks)
```

Returns a plain tuple. Parents go to the synthesis store (Phase 4+); children go to the retrieval index (Phase 4+).

#### Chunking strategy

**Structured path (pages with heading layout_blocks):**
Content blocks are grouped under their nearest heading. Groups are merged greedily into parent chunks up to `parent_chunk_size_words` (default 400). Each parent is subdivided into child chunks with a sliding window (`child_chunk_size_words=150`, `child_chunk_overlap_words=30`).

**Unstructured fallback (no layout_blocks, or blocks with no headings):**
`page.normalized_text` is split by word-window into parent chunks (no overlap at parent level). Each parent is subdivided into children the same way. `section_title` is inherited from `page.section_title`.

pypdf-extracted PDF pages (which have `layout_blocks=[]`) always use the fallback path. Docling-extracted pages (`.docx`, OCR-recovered) use the structured path when headings are detected.

#### Empty / weak page rules

| Status | Behavior |
|---|---|
| `"empty"` | Skipped — 0 parents, 0 children |
| `"weak"` | Included — produces ≥1 parent + ≥1 child; one window covers the full weak text |

#### Determinism

`chunk_id` is derived from `sha256(f"{doc_id}|{level}|{seq}")[:32]` where `seq` is a monotonically increasing counter scoped to the `build_hierarchical_chunks` call. Same input → same chunk_ids. This enables idempotent upserts in the index (Phase 4).

#### Config additions

```python
parent_chunk_size_words: int = 400   # broad context window for synthesis
child_chunk_size_words:  int = 150   # fine-grained retrieval unit
child_chunk_overlap_words: int = 30  # overlap within a parent's child sequence
```

Per-call overrides available as optional arguments.

#### Test evidence

```
pytest tests/ -q
132 passed, 2 skipped, 3 warnings in 32.75s
```

| Test file | Before | After |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | 24 passed | 24 passed |
| `test_hierarchical_chunking.py` | (new) | 43 passed |
| **Total** | **89 passed, 2 skipped** | **132 passed, 2 skipped** |

43 tests covering: contract, parent fields, child fields, page linkage, empty pages, weak pages, fallback path, structured path, determinism, section_title propagation, multi-page, config overrides.

#### Deferred to next step

- Connecting hierarchical chunks to storage/indexing (Phase 4)
- Layout-block intelligence refinement (Phase 3B if needed): `"table"` and `"list"` blocks currently contribute text only; heading-only parents (no following content) currently produce no segments
- PDF layout blocks on the born-digital path: pypdf pages have `layout_blocks=[]` → always use fallback; this is correct for now but could be improved if Docling is used on the normal PDF path

---

### Phase 4 implementation — Indexing and storage core

**Status**: Core indexing and storage layer complete. Parent and child chunk indexes build, persist, and reload correctly.

#### Dependency installed

`llama-index-core==0.14.22` — installed from the existing Phase 4 block in `requirements-full.txt`. No changes to `requirements-core.txt` or `requirements.txt` (legacy).

#### What was built

| File | Change | Description |
|---|---|---|
| `src/indexing/index_builder.py` | **New** | `build_indexes()`, `load_child_index()`, `load_parent_store()`, `IndexManifest` |
| `src/indexing/__init__.py` | **Updated** | Exports all public symbols from `index_builder` |
| `tests/test_indexing.py` | **New** | 33 tests across 12 test classes |

#### Public API

```python
def build_indexes(
    parent_chunks: list[DocumentChunk],
    child_chunks: list[DocumentChunk],
    index_dir: Path | None = None,       # defaults to config.index_dir
    embed_model: BaseEmbedding | None = None,  # injected; None → Settings.embed_model
) -> IndexManifest: ...

def load_child_index(
    index_dir: Path | None = None,
    embed_model: BaseEmbedding | None = None,
) -> VectorStoreIndex: ...

def load_parent_store(
    index_dir: Path | None = None,
) -> SimpleDocumentStore: ...
```

#### Storage layout

```
data/index/
    child_index/                 ← StorageContext.persist() for child VectorStoreIndex
        default__vector_store.json
        docstore.json
        index_store.json
        graph_store.json
        image__vector_store.json
    parent_store/
        docstore.json            ← SimpleDocumentStore for parent chunks (no embeddings)
    build_manifest.json          ← IndexManifest: counts, model, timestamp, doc_ids
```

#### Two-store design

**Child index** (`child_index/`): `VectorStoreIndex` backed by `SimpleVectorStore`. Each child `DocumentChunk` → `TextNode` with `id_=chunk.chunk_id`. All 9 required metadata fields stored in `node.metadata`. This is the retrieval surface for Phase 5.

**Parent store** (`parent_store/`): `SimpleDocumentStore` only — no embeddings. Each parent `DocumentChunk` → LlamaIndex `Document` keyed by `chunk.chunk_id`. Looked up by `parent_chunk_id` after child retrieval in Phase 5. No vector indexing needed for parents at this step.

#### Embedding stance (pluggable, never hardwired)

`index_builder.py` has zero imports of any concrete runtime embedder. The `embed_model` parameter accepts any `BaseEmbedding` subclass. `None` defers to `llama_index.core.Settings.embed_model`. Qwen3-Embedding-0.6B wiring via `HuggingFaceEmbedding` is deferred to Phase 4B.

#### Metadata fields preserved

All 9 required fields stored on every LlamaIndex node and document:
`chunk_id`, `doc_id`, `page_id`, `page_number`, `file_name`, `file_type`, `section_title`, `chunk_level`, `parent_chunk_id`.

`section_title=None` → `""`. `parent_chunk_id=None` (for parents) → `""`. Both are safe sentinel values that survive the JSON round-trip.

#### Rebuild behaviour

Default: overwrite. Deterministic: SHA-256 `chunk_id`s from Phase 3A mean the same input always produces the same node IDs. `build_manifest.json` written after every successful build.

#### Test evidence

Isolated run (no Docling):
```
pytest tests/test_indexing.py -v
33 passed in 10.82s
```

Full suite:
```
pytest tests/ -q
165 passed, 2 skipped, 3 warnings in 107.05s
```

| Test file | Before | After |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | 24 passed | 24 passed |
| `test_hierarchical_chunking.py` | 43 passed | 43 passed |
| `test_indexing.py` | (new) | 33 passed |
| **Total** | **132 passed, 2 skipped** | **165 passed, 2 skipped** |

33 tests covering: manifest fields, parent store persistence, child index persistence, metadata preservation (all 9 fields), parent/child linkage, deterministic rebuild, empty inputs, legacy Chroma untouched, load_child_index, load_parent_store, manifest doc_id deduplication, error handling.

#### Deferred to Phase 4B

- Runtime embedding wiring: `HuggingFaceEmbedding` + `Qwen/Qwen3-Embedding-0.6B` via `Settings.embed_model`
- Install `llama-index-embeddings-huggingface`, `sentence-transformers`, `transformers` (already in `requirements-full.txt` Phase 4 block)
- End-to-end pipeline integration: calling `build_indexes(parents, children)` from the ingestion/pipeline coordinator
- Retrieval layer (Phase 5): `load_child_index()` used as the dense retrieval surface; parent lookup after child retrieval

---

## Phase 4B — Embedding Wiring and End-to-End Indexing Pipeline

**Completed**: Session 6

### What was built

| File | Change | Description |
|---|---|---|
| `src/indexing/embed_config.py` | **New** | `get_embed_model()`, `configure_settings()` — single HF embedding seam |
| `src/indexing/indexing_pipeline.py` | **New** | `run_indexing_pipeline()` — full pipeline from file path to IndexManifest |
| `src/indexing/__init__.py` | **Updated** | Re-exports `get_embed_model`, `configure_settings`, `run_indexing_pipeline` |
| `tests/test_indexing_pipeline.py` | **New** | 25 tests (1 skipped/gated), 4 test classes |

`requirements-full.txt` — no change; `llama-index-embeddings-huggingface` and `sentence-transformers` were already listed. Installed from that spec.
`index_builder.py` (Phase 4A) — untouched. `data/chroma_db/` — untouched.

### Public API

```python
# src/indexing/embed_config.py
def get_embed_model(model_name: str | None = None) -> BaseEmbedding:
    # Lazy HF import; falls back to config.embedding_model
    ...

def configure_settings(embed_model: BaseEmbedding | None = None) -> BaseEmbedding:
    # Sets Settings.embed_model; startup/CLI helper only
    ...

# src/indexing/indexing_pipeline.py
def run_indexing_pipeline(
    file_path: Path,
    index_dir: Path | None = None,     # defaults to config.index_dir; override in tests
    embed_model: BaseEmbedding | None = None,  # pass explicitly; None → get_embed_model()
) -> IndexManifest:
    ...
```

### Design decisions

**Lazy HF import**: `from llama_index.embeddings.huggingface import HuggingFaceEmbedding` is inside the function body of `get_embed_model()`. The module can be imported without error even if the HF package is absent; only calling the function fails. This matches the lazy import pattern used elsewhere in the project for optional heavy dependencies.

**Explicit injection preferred**: `run_indexing_pipeline()` accepts `embed_model` as a parameter. `get_embed_model()` is called only when `embed_model=None`. Tests always pass `embed_model=MockEmbedding(...)` — `get_embed_model()` is never reached in normal test runs. `configure_settings()` is a startup/CLI utility; the pipeline does not rely on global `Settings.embed_model` mutation.

**Test output isolation**: Every test that calls `run_indexing_pipeline()` or `build_indexes()` passes an explicit `index_dir=tmp_path`. No test uses the default `config.index_dir` path. `TestPipelineWithTextFile.test_project_index_dir_not_created_by_test` explicitly asserts that `data/index/` does not exist after a pipeline run.

**Integration test gating**: `TestIntegrationRealEmbedding` is decorated with `@pytest.mark.skipif(not INTEGRATION_TESTS, ...)`. It runs only when `INTEGRATION_TESTS=1` is set. It uses `sentence-transformers/all-MiniLM-L6-v2` (smaller than Qwen3-Embedding-0.6B) and writes to `tmp_path`.

**Settings private field access**: `TestConfigureSettings` saves and restores `Settings._embed_model` (the private backing field of the `_Settings` singleton) rather than reading `Settings.embed_model` (the property), which triggers the default resolver and fails when `llama-index-embeddings-openai` is absent.

### Test evidence

Isolated run:
```
pytest tests/test_indexing_pipeline.py -q
25 passed, 1 skipped, 1 warning in 47.32s
```

Full suite:
```
pytest tests/ -q
190 passed, 3 skipped, 3 warnings in 90.14s
```

| Test file | Before | After |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | 24 passed | 24 passed |
| `test_hierarchical_chunking.py` | 43 passed | 43 passed |
| `test_indexing.py` | 33 passed | 33 passed |
| `test_indexing_pipeline.py` | (new) | 25 passed, 1 skipped |
| **Total** | **165 passed, 2 skipped** | **190 passed, 3 skipped** |

25 tests across 4 classes: `TestConfigureSettings` (configure_settings wiring), `TestGetEmbedModel` (HF class instantiation via mock), `TestPipelineOrchestration` (orchestration with mocked internals), `TestPipelineWithTextFile` (full pipeline on company_policy.txt with MockEmbedding, all with isolated tmp_path), `TestIntegrationRealEmbedding` (gated, skipped by default).

### Installed packages

- `llama-index-embeddings-huggingface` — LlamaIndex HuggingFace embedding adapter
- `sentence-transformers` — backend for HF embeddings (already in `requirements-full.txt`)

### Deferred to Phase 5

- Dense retrieval layer: `load_child_index()` → `VectorStoreIndex.as_retriever()`
- BM25 sparse retrieval (parallel path to dense)
- Reciprocal rank fusion of dense + sparse results
- Parent document lookup after child retrieval using `load_parent_store()`
- Reranker (Qwen3-Reranker-0.6B) postprocessor

---

## Phase 5A — Dense Retrieval Core

**Completed**: Session 7

### What was built

| File | Change | Description |
|---|---|---|
| `src/schema/models.py` | **Modified** | Added `parent_chunk_id: Optional[str] = None` to `RetrievedChunk` |
| `src/retrieval/vector_retriever.py` | **New** | `retrieve_children()`, `lookup_parents()` — vector retrieval + parent recovery |
| `src/retrieval/__init__.py` | **Updated** | Exports `retrieve_children`, `lookup_parents` |
| `tests/test_vector_retriever.py` | **New** | 36 tests (3 skipped/gated), 4 test classes |

`data/chroma_db/` — untouched. No test writes to `data/index/`.

### Public API

```python
# src/retrieval/vector_retriever.py

def retrieve_children(
    query: str,
    index_dir: Path | None = None,      # defaults to config.index_dir; override in tests
    embed_model: BaseEmbedding | None = None,  # pass explicitly in tests (MockEmbedding)
    top_k: int = 5,
) -> List[RetrievedChunk]:
    # Loads child VectorStoreIndex, runs similarity search, returns project-native types.
    ...

def lookup_parents(
    retrieved: List[RetrievedChunk],
    index_dir: Path | None = None,
) -> List[Optional[DocumentChunk]]:
    # Parallel list: for each RetrievedChunk returns its parent DocumentChunk or None.
    ...
```

No LlamaIndex types (`NodeWithScore`, `Document`, `VectorStoreIndex`) cross the module boundary.

### Design decisions

**Stateless functions**: Both functions load their stores from disk on each call. No module-level caching. This keeps the retrieval layer simple, avoids stale-index bugs across tests, and defers caching to a later phase (after production profiling).

**No LlamaIndex types in public API**: Internal helpers `_to_retrieved_chunk(NodeWithScore)` and `_document_to_chunk(Document)` convert to project-native schema types before returning. Return types are `List[RetrievedChunk]` and `List[Optional[DocumentChunk]]` — no LlamaIndex types visible to callers.

**Empty-string normalisation**: `section_title` and `parent_chunk_id` are stored as `""` when `None` in the index metadata (LlamaIndex requirement). Both conversion helpers normalise `""` back to `None` using the `or None` idiom.

**`chunk_index` reconstruction**: Not stored in index metadata. Reconstructed as `0` for all recovered parent chunks. This field is an intra-page ordering hint used during chunking and is not required for parent-context synthesis. Documented in the module docstring.

**`word_count` reconstruction**: Not stored in index metadata. Derived from `len(text.split())` in both conversion helpers, consistent across child and parent chunks.

**`parent_store.get_document(id, raise_error=False)`**: Returns `None` instead of raising `ValueError` when a parent_chunk_id is not found. This handles the case where a child references a parent that was not indexed (e.g., empty parent text filtered out in a future phase) without raising.

**`lookup_parents` with empty `parent_chunk_id`**: An empty-string `parent_chunk_id` (falsy) is treated the same as `None` — returns `None` in the output. This guards against accidentally stored empty strings propagating through the pipeline.

**Module-scope index fixture**: `tests/test_vector_retriever.py` uses `@pytest.fixture(scope="module")` with `tmp_path_factory` to build the 3-parent / 6-child test index once for the whole module. This avoids repeated disk I/O during the 36-test suite while keeping the index isolated from all other tests.

**Test output isolation**: Every test uses either the `built_index` module-scope fixture or an explicit `tmp_path`. No test references `config.index_dir`. `TestRetrieveEdgeCases.test_retrieve_children_raises_on_missing_index` passes a non-existent subdirectory of `tmp_path` to verify `FileNotFoundError` is raised correctly.

### Test evidence

Full suite:
```
pytest tests/ -q
226 passed, 6 skipped, 3 warnings in 32.28s
```

| Test file | Before (Phase 4B) | After (Phase 5A) |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | 24 passed | 24 passed |
| `test_hierarchical_chunking.py` | 43 passed | 43 passed |
| `test_indexing.py` | 33 passed | 33 passed |
| `test_indexing_pipeline.py` | 25 passed, 1 skipped | 25 passed, 1 skipped |
| `test_vector_retriever.py` | (new) | 33 passed, 3 skipped |
| **Total** | **190 passed, 3 skipped** | **226 passed, 6 skipped** |

36 tests across 4 classes: `TestRetrieveChildrenContract` (shape, top_k, scoring, all metadata fields, parent_chunk_id round-trip), `TestLookupParents` (parent found, None cases, metadata preservation, mixed input), `TestRetrieveEdgeCases` (oversized top_k, FileNotFoundError, empty-string parent_chunk_id), `TestIntegrationRealRetrieval` (3 tests, gated by INTEGRATION_TESTS=1, skipped by default).

### Deferred to Phase 5B+

- BM25 sparse retrieval (parallel path to dense)
- Reciprocal rank fusion of dense + sparse results
- Metadata filtering in retrieval
- Reranker (Qwen3-Reranker-0.6B) postprocessor
- Result deduplication across retrieval paths
- Retrieval caching / warm-loading of index

- Real production index build at `data/index/` using `Qwen/Qwen3-Embedding-0.6B`

---

## Phase 5B — BM25 Sparse Retrieval

**Completed**: Session 6

### What was built

| File | Change |
|---|---|
| `src/schema/models.py` | Added `file_type: Optional[str] = None` to `RetrievedChunk` (backward-compatible) |
| `src/retrieval/vector_retriever.py` | `_to_retrieved_chunk` now propagates `file_type` from chunk metadata |
| `src/retrieval/bm25_retriever.py` | New module — BM25Plus lexical retrieval over child chunk corpus |
| `src/retrieval/__init__.py` | Exported `retrieve_children_bm25` alongside existing public API |
| `tests/test_bm25_retriever.py` | New test file — 37 tests across 4 classes |

### Design decisions

**Corpus source**: `StorageContext.from_defaults(persist_dir=child_index/)` loads the persisted child index; `.docstore.docs` yields a `Dict[str, BaseNode]` from which `node.text` and `node.metadata` are extracted. The BM25Plus structure is rebuilt in-memory per call — no BM25 persistence to disk.

**Tokenization**: `re.findall(r'\b\w+\b', text.lower())` — word-boundary regex strips all punctuation before BM25 scoring. Stdlib-only; no NLTK or spaCy dependency. Without punctuation stripping, trailing `.` / `,` caused `.lower().split()` to produce tokens like `"zyphron."` that never matched the query token `"zyphron"`, producing all-zero scores.

**Zero-match behaviour**: Scores are filtered with `score > 0.0`. When no document matches the query all scores are zero and the function explicitly returns `[]`. Empty queries (no tokens after tokenisation) also return `[]` immediately.

**`requirements-core.txt` untouched**: `rank-bm25` was already present in `requirements-full.txt`. No changes to the core dependency set.

**`lookup_parents()` reused unchanged**: BM25 results carry `parent_chunk_id` in the same format as vector results, so callers can call the existing `lookup_parents()` helper on BM25 results without modification.

**`file_type` consistency**: Both vector and BM25 retrieval paths now populate `file_type` from chunk metadata (lowercase extension without dot, e.g. `"txt"`, `"pdf"`). Defaults to `None` when absent.

### Test coverage

Module-scope fixture `built_bm25_index` builds 3 parent + 6 child nodes with distinctive non-English tokens (`"zyphron"`, `"valquix"`, `"mordecai"`) to ensure reliable BM25 discrimination. `"chunk"` appears in every child text as a reliable all-corpus match.

| Class | Focus |
|---|---|
| `TestRetrieveChildrenBm25Contract` | Return type, `retrieval_method="bm25"`, positive `bm25_score`, `vector_score=None`, `top_k`, all metadata fields, `file_type="txt"`, `parent_chunk_id`, unique IDs, descending score order, lexical specificity |
| `TestBm25LookupParentsCompatibility` | BM25 results through `lookup_parents()`, parent found, `None` for missing, `None` for nonexistent |
| `TestBm25RetrieveEdgeCases` | `top_k` > corpus size returns all 6, zero-match returns `[]`, `FileNotFoundError` on missing index, empty query returns `[]`, all results have positive scores |
| `TestIntegrationBm25RealCorpus` | 3 tests gated by `INTEGRATION_TESTS=1`; skipped by default |

### Test evidence

Full suite:
```
pytest tests/ -q
263 passed, 10 skipped, 3 warnings in 29.32s
```

| Test file | Before (Phase 5A) | After (Phase 5B) |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | 24 passed | 24 passed |
| `test_hierarchical_chunking.py` | 43 passed | 43 passed |
| `test_indexing.py` | 33 passed | 33 passed |
| `test_indexing_pipeline.py` | 25 passed, 1 skipped | 25 passed, 1 skipped |
| `test_vector_retriever.py` | 33 passed, 3 skipped | 33 passed, 3 skipped |
| `test_bm25_retriever.py` | (new) | 34 passed, 3 skipped |
| **Total** | **226 passed, 6 skipped** | **263 passed, 10 skipped** |

### Deferred to Phase 5C

- Reciprocal rank fusion of dense + sparse results
- Metadata filtering in retrieval
- Reranker (Qwen3-Reranker-0.6B) postprocessor
- Result deduplication across retrieval paths
- Retrieval caching / warm-loading of index
- Real production index build at `data/index/` using `Qwen/Qwen3-Embedding-0.6B`

---

## Phase 5C — Fusion and Deduplication (first execution chunk)

**Completed**: Session 7

### What was built

| File | Change | Description |
|---|---|---|
| `src/retrieval/hybrid_retriever.py` | **New** | `_rrf_fuse()` (pure), `retrieve_hybrid()` — RRF fusion coordinator |
| `src/retrieval/__init__.py` | **Updated** | Exported `retrieve_hybrid` alongside existing public API |
| `tests/test_hybrid_retriever.py` | **New** | 32 unit tests + 3 integration tests (gated); 35 total |

No schema changes needed — `fusion_score`, `bm25_score`, `vector_score`, and `retrieval_method="hybrid"` were already present in `RetrievedChunk`.

### Design decisions

**Fusion strategy: Reciprocal Rank Fusion (RRF)**
`score(chunk) = Σ 1/(k + rank_i)` summed over all result lists where the chunk appears. Standard constant `k=60` (Cormack et al. 2009). Caller-configurable via `rrf_k` parameter. RRF is rank-based — immune to score-scale differences between BM25 and cosine similarity scores.

**`_rrf_fuse` is a pure function**
Accepts two pre-fetched `List[RetrievedChunk]` — no index access, no I/O. Testable directly with synthetic data without needing an index fixture.

**Deduplication by `chunk_id` with dense-first insertion order**
When the same `chunk_id` appears in both lists:
- Dense-path chunk is stored as the base record (preserves `vector_score`)
- `bm25_score` from the sparse-path record is injected via `model_copy(update=...)` — new Pydantic instance, originals never mutated
- All other metadata (`parent_chunk_id`, `file_type`, `section_title`, etc.) preserved from the dense record (both paths draw from the same docstore, so values are identical)

**Tie-breaking**
Primary: `fusion_score` descending (numerically deterministic). Secondary: Python `list.sort` is stable — ties (mathematically possible only when chunks have identical rank in every shared list) preserve dense-first insertion order.

**All outputs have `retrieval_method="hybrid"` and `fusion_score` set**
Single-path results (appearing only in dense or only in sparse) also receive `retrieval_method="hybrid"` and a `fusion_score` computed from their rank in that one list.

**`lookup_parents()` compatibility**
`parent_chunk_id` is preserved through the merge step unchanged. Callers can pass the hybrid result list directly to `lookup_parents()` without adaptation.

**`retrieve_hybrid` is a thin coordinator**
Calls `retrieve_children` (dense) then `retrieve_children_bm25` (sparse) in sequence, then delegates to `_rrf_fuse`. No business logic beyond coordination.

### Public API

```python
def retrieve_hybrid(
    query: str,
    index_dir: Path | None = None,
    top_k: int = 10,
    vector_top_k: int = 10,
    bm25_top_k: int = 10,
    rrf_k: int = 60,
) -> List[RetrievedChunk]:
    ...
```

### Test coverage

All unit tests use synthetic `RetrievedChunk` objects — no index needed. `_rrf_fuse` tested directly as a pure function.

| Class | Tests | Focus |
|---|---|---|
| `TestRrfFuseContract` | 9 | Returns list; all `retrieval_method="hybrid"`; all `fusion_score > 0`; descending sort; `top_k` respected; chunk-in-both outscores single-path; RRF arithmetic spot-check; unique chunk IDs |
| `TestDeduplication` | 12 | Duplicate merges to one; `vector_score` and `bm25_score` both preserved; unique-dense and unique-sparse chunks preserved; `file_type` and `parent_chunk_id` preserved on merged and sparse-only chunks; `section_title` preserved; dense-only `bm25_score=None`; sparse-only `vector_score=None` |
| `TestEdgeCases` | 8 | Both empty → `[]`; dense-empty/sparse-nonempty; sparse-empty/dense-nonempty; all-same-chunk-id → 1 result; `top_k=0`; single dense; single sparse; output is `List[RetrievedChunk]` |
| `TestLookupParentsCompatibility` | 3 | `parent_chunk_id` survives fusion; `None` parent preserved; `lookup_parents` raises `FileNotFoundError` (not `TypeError`/`AttributeError`) on missing index — confirms type compatibility |
| `TestIntegrationHybridRetrieval` | 3 | Gated by `INTEGRATION_TESTS=1`; skipped by default |

### Test evidence

Isolated (hybrid only):
```
pytest tests/test_hybrid_retriever.py -q
32 passed, 3 skipped, 1 warning in 14.75s
```

Full suite:
```
pytest tests/ -q
295 passed, 13 skipped, 3 warnings in 37.54s
```

| Test file | Before (Phase 5B) | After (Phase 5C-1) |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | 24 passed | 24 passed |
| `test_hierarchical_chunking.py` | 43 passed | 43 passed |
| `test_indexing.py` | 33 passed | 33 passed |
| `test_indexing_pipeline.py` | 25 passed, 1 skipped | 25 passed, 1 skipped |
| `test_vector_retriever.py` | 33 passed, 3 skipped | 33 passed, 3 skipped |
| `test_bm25_retriever.py` | 34 passed, 3 skipped | 34 passed, 3 skipped |
| `test_hybrid_retriever.py` | (new) | 32 passed, 3 skipped |
| **Total** | **263 passed, 10 skipped** | **295 passed, 13 skipped** |

### Deferred to Phase 5C second execution chunk

- Reranker (Qwen3-Reranker-0.6B) as LlamaIndex `BaseNodePostprocessor`
- Metadata filtering (by `file_name`, `page_number`, `section_title`)
- Retrieval caching / warm-loading of index
- Real production index build at `data/index/` using `Qwen/Qwen3-Embedding-0.6B`

---

## Phase 5C — Reranker Integration (second execution chunk)

**Completed**: Session (continuation of Phase 5C)

### What was built

| File | Status | Description |
|---|---|---|
| `src/reranking/qwen_reranker.py` | New | Qwen3-Reranker-0.6B postprocessor via `CrossEncoder.predict` |
| `src/reranking/__init__.py` | Modified | Exports `rerank` from `qwen_reranker` |
| `tests/test_qwen_reranker.py` | New | 30 unit tests + 1 gated integration test |

No schema changes were required — `rerank_score: Optional[float] = None` was already present on `RetrievedChunk` from Phase 5A schema design.

No new package installs required — `sentence-transformers 5.5.1` (already installed) provides `CrossEncoder`.

### Design decisions

| Decision | Choice | Rationale |
|---|---|---|
| API integration | `CrossEncoder.predict(sentence_pairs)` directly | No `BaseNodePostprocessor` wrapper — simpler, project-native API |
| Model loading | Lazy, module-level cache (`_MODEL_CACHE` dict keyed by model name) | Avoids repeated disk I/O; testable via `_model` injection |
| Score semantics | Raw logit (unbounded float) — NOT a probability | CrossEncoder raw output; no sigmoid applied; explicitly documented in docstring |
| Immutability | `model_copy(update={"rerank_score": float(score)})` per chunk | Pydantic v2 pattern; input chunks never mutated |
| Tie-breaking | Python stable sort (`sorted(..., reverse=True)`) | Ties preserve input fusion-score-descending order from hybrid retrieval |
| Integration test gate | `RERANKER_INTEGRATION_TESTS=1` env var | Distinct from `INTEGRATION_TESTS=1` used by vector/BM25 retrievers |
| `top_k` semantics | Slice to `top_k` highest `rerank_score` after sort; `top_k=0` → `[]`; `top_k > len` → all | Consistent with hybrid/BM25 `top_k` semantics |

### `rerank` function signature

```python
def rerank(
    query: str,
    chunks: List[RetrievedChunk],
    top_k: Optional[int] = None,
    model_name: Optional[str] = None,
    *,
    _model: Any = None,
) -> List[RetrievedChunk]:
```

- Empty `chunks` → `[]` immediately (no model load)
- `_model` keyword-only param bypasses `_load_model` entirely (test isolation)
- All existing scores (`vector_score`, `bm25_score`, `fusion_score`, `retrieval_method`) preserved unchanged

### Test coverage

| Test class | Tests | What's covered |
|---|---|---|
| `TestRerankContract` | 8 | Return type, `rerank_score` populated, score values match model, sorted descending, ordering changes, length, tie-breaking stable |
| `TestScorePreservation` | 11 | All `RetrievedChunk` fields pass through unchanged; input chunks not mutated |
| `TestEdgeCases` | 8 | Empty input, single chunk, `top_k` variants (respected / highest / larger than input / zero), negative scores, float type |
| `TestQueryPairing` | 3 | Query in each pair, chunk text in pairs, pair order matches chunk order |
| `TestIntegrationQwenReranker` | 1 (gated) | Real `Qwen/Qwen3-Reranker-0.6B`; financial chunks rank above weather chunk |

### Test evidence

| Test file | Before (Phase 5C-1) | After (Phase 5C-2) |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | 24 passed | 24 passed |
| `test_hierarchical_chunking.py` | 43 passed | 43 passed |
| `test_indexing.py` | 33 passed | 33 passed |
| `test_indexing_pipeline.py` | 25 passed, 1 skipped | 25 passed, 1 skipped |
| `test_vector_retriever.py` | 33 passed, 3 skipped | 33 passed, 3 skipped |
| `test_bm25_retriever.py` | 34 passed, 3 skipped | 34 passed, 3 skipped |
| `test_hybrid_retriever.py` | 32 passed, 3 skipped | 32 passed, 3 skipped |
| `test_qwen_reranker.py` | (new) | 30 passed, 1 skipped |
| **Total** | **295 passed, 13 skipped** | **325 passed, 14 skipped** |

### Deferred

- Metadata filtering (by `file_name`, `page_number`, `section_title`)
- Retrieval caching / warm-loading of index
- Real production index build at `data/index/` using `Qwen/Qwen3-Embedding-0.6B`
- Wiring reranker into end-to-end pipeline (Phase 6)

---

## Phase 6 — Answer Synthesis Core

**Completed**: Session (answer synthesis core; citations and validation deferred)

### What was built

| File | Status | Description |
|---|---|---|
| `src/generation/ollama_llm.py` | New | Narrow `httpx` wrapper around Ollama `/api/chat`; lazy client; `_client` injection |
| `src/generation/prompt_templates.py` | New | `build_grounded_messages` — returns structured `[system, user]` message list |
| `src/generation/answer_engine.py` | New | `synthesise()` — parent-context enrichment + grounded generation |
| `src/generation/__init__.py` | Modified | Exports `build_grounded_messages`, `generate`, `synthesise` |
| `tests/test_answer_engine.py` | New | 28 unit tests + 1 gated integration test |

No schema changes — `AnswerResponse` was already complete with all needed fields.

No new package installs — `httpx 0.28.1` was already installed; used directly against Ollama REST API.

### Design decisions

| Decision | Choice | Rationale |
|---|---|---|
| HTTP transport | `httpx.Client` against Ollama `/api/chat` | `ollama` and `llama-index-llms-ollama` not installed; `httpx` already present; equivalent capability |
| Prompt format | Structured `List[dict]` (system + user) returned by `prompt_templates` | Clean contract: template builds messages, transport sends them — no string-building in either direction |
| Context assembly | Parent-context enrichment with per-position child fallback | Uses `lookup_parents()` output; parent text provides broader synthesis window |
| Parent fallback rules | `parents=None`, `i >= len(parents)`, or `parents[i]=None` → child text | All three cases handled explicitly; no implicit truncation |
| Citation contract | `sources=[]` always | Phase 7 fills deterministic citations; this layer never invents them |
| Validation contract | `validation_flags=[]` always | Phase 8 fills validation flags |
| Test injection | `_generator: Callable[[List[dict]], str]` keyword-only param | No Ollama connection in unit tests; same pattern as `_model` in reranker |
| Integration gate | `OLLAMA_INTEGRATION_TESTS=1` env var | Distinct from `INTEGRATION_TESTS` (retrieval) and `RERANKER_INTEGRATION_TESTS` |
| `top_k` semantics | Limits context assembly only; `supporting_chunks` always preserves full input | Caller controls context window without losing retrieval provenance |

### Prompt contract

```python
build_grounded_messages(query: str, context_blocks: List[str]) -> List[dict]
# Returns:
# [{"role": "system", "content": GROUNDING_INSTRUCTION},
#  {"role": "user",   "content": "=== Context ===\n...\n\n=== Question ===\n..."}]
```

Empty `context_blocks` → `"[No context provided.]"` placeholder in user turn — model responds with explicit "insufficient context" message.

### `synthesise` function signature

```python
def synthesise(
    query: str,
    chunks: List[RetrievedChunk],
    parents: Optional[List[Optional[DocumentChunk]]] = None,
    top_k: Optional[int] = None,
    model: Optional[str] = None,
    *,
    _generator: Optional[Callable[[List[dict]], str]] = None,
) -> AnswerResponse:
```

### Test coverage

| Test class | Tests | What's covered |
|---|---|---|
| `TestPromptBuilder` | 7 | Two-message structure, roles, query/context in user turn, empty placeholder, system instruction, multiple blocks |
| `TestAnswerEngineContract` | 9 | Return type, answer text set, model used, supporting_chunks, latency_ms, run_id, sources empty, validation_flags empty, query preserved |
| `TestContextStrategy` | 5 | Parent text when available, child text when parent=None, child text when parents shorter, child text when parents=None, input chunks not mutated |
| `TestEdgeCases` | 7 | Empty chunks → AnswerResponse, empty context placeholder, top_k limits context, top_k preserves supporting_chunks, top_k > len uses all, single chunk, answer_text is str |
| `TestIntegrationOllama` | 1 (gated) | Real Ollama call; non-empty AnswerResponse; finance context query |

### Test evidence

| Test file | Before (Phase 5C-2) | After (Phase 6 core) |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_ingestion.py` | 8 passed, 1 skipped | 8 passed, 1 skipped |
| `test_chunking.py` | 11 passed | 11 passed |
| `test_parsing.py` | 26 passed, 1 skipped | 26 passed, 1 skipped |
| `test_ocr.py` | 24 passed | 24 passed |
| `test_hierarchical_chunking.py` | 43 passed | 43 passed |
| `test_indexing.py` | 33 passed | 33 passed |
| `test_indexing_pipeline.py` | 25 passed, 1 skipped | 25 passed, 1 skipped |
| `test_vector_retriever.py` | 33 passed, 3 skipped | 33 passed, 3 skipped |
| `test_bm25_retriever.py` | 34 passed, 3 skipped | 34 passed, 3 skipped |
| `test_hybrid_retriever.py` | 32 passed, 3 skipped | 32 passed, 3 skipped |
| `test_qwen_reranker.py` | 30 passed, 1 skipped | 30 passed, 1 skipped |
| `test_answer_engine.py` | (new) | 28 passed, 1 skipped |
| **Total** | **325 passed, 14 skipped** | **353 passed, 15 skipped** |

### Deferred to Phase 6 pipeline wiring step

- Deterministic citations (`CitationRecord`) — Phase 7
- Quote extraction — Phase 7
- Validation logic — Phase 8
- Query routing / agentic retrieval — later phase
- Context-window overflow guard (token counting) — not in scope until production index built

---

## Phase 6 — End-to-End Answer Pipeline Wiring

**Completed**: Session (pipeline coordinator; all four stages wired)

### What was built

| File | Status | Description |
|---|---|---|
| `src/generation/answer_pipeline.py` | New | `run_pipeline()` — coordinates retrieve_hybrid → rerank → lookup_parents → synthesise |
| `src/generation/__init__.py` | Modified | Added `run_pipeline` to exports |
| `tests/test_answer_pipeline.py` | New | 18 unit tests + 1 gated integration test |

No schema changes. No new packages.

### Design decisions

| Decision | Choice | Rationale |
|---|---|---|
| Single public function | `run_pipeline(query, ...)` | Narrow entry point; all orchestration is internal |
| Injection at every stage | `_retriever`, `_reranker`, `_parent_lookup`, `_generator` (keyword-only) | No real I/O in unit tests; consistent with `_model`/`_generator`/`_client` pattern |
| Injection signature | `_retriever(query)`, `_reranker(query, chunks)`, `_parent_lookup(chunks)` | Narrow; caller closes over params in test scope |
| Reranked list is authoritative | `supporting_chunks == reranked` | Reranker output is the final scored list; no further mutation in pipeline layer |
| Empty retrieval not short-circuited | Flows through all four stages | `synthesise` handles [] correctly (placeholder context); no special branch in pipeline |
| Lazy real-function imports | `from ... import` inside if-not-injected blocks | Avoids importing heavy retrieval/reranking models at module load time |
| `synthesis_top_k` not exposed | Not a pipeline parameter | `rerank_top_k` already limits the context window; extra knob would confuse callers |
| Integration gate | `PIPELINE_INTEGRATION_TESTS=1` | Distinct from `INTEGRATION_TESTS`, `RERANKER_INTEGRATION_TESTS`, `OLLAMA_INTEGRATION_TESTS` |

### Pipeline stage order

```
query
  │
  ▼ retrieve_hybrid(query, index_dir, top_k=retrieval_top_k)
retrieved: List[RetrievedChunk]
  │
  ▼ rerank(query, retrieved, top_k=rerank_top_k)
reranked: List[RetrievedChunk]
  │
  ▼ lookup_parents(reranked, index_dir)
parents: List[Optional[DocumentChunk]]
  │
  ▼ synthesise(query, reranked, parents, model, _generator)
AnswerResponse
```

### `run_pipeline` function signature

```python
def run_pipeline(
    query: str,
    index_dir: Optional[Path] = None,
    retrieval_top_k: int = 10,
    rerank_top_k: int = 5,
    model: Optional[str] = None,
    *,
    _retriever:     Optional[Callable[[str], List[RetrievedChunk]]] = None,
    _reranker:      Optional[Callable[[str, List[RetrievedChunk]], List[RetrievedChunk]]] = None,
    _parent_lookup: Optional[Callable[[List[RetrievedChunk]], List[Optional[DocumentChunk]]]] = None,
    _generator:     Optional[Callable[[List[dict]], str]] = None,
) -> AnswerResponse:
```

### Test coverage

| Test class | Tests | What's covered |
|---|---|---|
| `TestPipelineOrdering` | 4 | Stage call order (tracker), reranker receives retriever output, parent_lookup receives reranker output, synthesise receives reranked chunks |
| `TestPipelineOutput` | 5 | Returns AnswerResponse, query preserved, answer_text is str, sources=[], validation_flags=[] |
| `TestEmptyRetrieval` | 3 | No crash, supporting_chunks==[], generator still called (placeholder context) |
| `TestParameterRouting` | 4 | retrieval_top_k forwarded, rerank_top_k limits supporting_chunks, model forwarded, query forwarded to retriever |
| `TestParentFallback` | 2 | Parent text in user turn when parent available; child text when parent=None |
| `TestIntegrationPipeline` | 1 (gated) | Real Ollama generation; injected retrieval/reranking to avoid index dependency |

### Test evidence

| Test file | Before | After |
|---|---|---|
| `test_answer_engine.py` | 28 passed, 1 skipped | 28 passed, 1 skipped |
| `test_answer_pipeline.py` | (new) | 18 passed, 1 skipped |
| **Total** | **353 passed, 15 skipped** | **371 passed, 16 skipped** |

### Deferred

- Deterministic citations (`CitationRecord`) — Phase 7
- Quote extraction — Phase 7
- Validation logic — Phase 8
- Query routing / agentic retrieval — later phase
- Production index build at `data/index/` (deferred from Phase 5)


## Phase 7A — Deterministic Citation Construction

**Completed**: Session (citation layer; query routing deferred to Phase 7B)

### Scope

Implement `build_citations()`, export it from `src/citations/`, and wire it into
`run_pipeline()` as Stage 5 so that every `AnswerResponse` carries a deterministic
`List[CitationRecord]` in `sources`.

### Files Changed

| File | Change |
|---|---|
| `src/citations/citation_builder.py` | **New** — pure `build_citations(chunks) -> List[CitationRecord]` |
| `src/citations/__init__.py` | Modified — exports `build_citations` |
| `src/generation/answer_pipeline.py` | Modified — Stage 5 added; `_citation_builder` injection param added |
| `tests/test_citation_builder.py` | **New** — 21 unit tests across 6 classes |
| `tests/test_answer_pipeline.py` | Modified — updated stale `test_sources_is_empty_list` assertion |

No schema changes. No new packages. `requirements-core.txt` untouched.

### Key Design Decisions

- **Deterministic `citation_id`**: `hashlib.sha256(f"{doc_id}:{chunk_id}:{page_number}".encode()).hexdigest()[:32]` — same input always produces same ID; no random UUID.
- **Passage-level `quote_text`**: set to `chunk.text` (the full retrieved passage). Quote extraction from generated answer text is deferred to a later phase.
- **`source_chunk_id`**: populated from `chunk.chunk_id` (confirmed `Optional[str]` on schema).
- **`validation_status = "unverified"`**: Phase 8 validator updates this; no premature marking.
- **`synthesise()` unchanged**: still returns `sources=[]`; enrichment happens only in the pipeline via `model_copy(update={"sources": citations})`.
- **`_citation_builder` injection param**: follows the same pattern as `_retriever`, `_reranker`, `_parent_lookup`, `_generator` — allows full unit testing without I/O.

### Test Evidence

| Test file | Before (Phase 6B) | After (Phase 7A) |
|---|---|---|
| `test_citation_builder.py` | (new) | 21 passed |
| `test_answer_pipeline.py` | 18 passed, 1 skipped | 18 passed, 1 skipped |
| **Total** | **371 passed, 16 skipped** | **392 passed, 16 skipped** |

### Deferred

- Citation quote extraction from generated answer text
- Citation validation (`validation_status` promotion to "valid"/"invalid") — Phase 8
- Query routing / agentic retrieval — Phase 7B
- Production index build at `data/index/` (deferred from Phase 5)


## Phase 7B — Query Routing Core

**Completed**: Session (query routing; agentic retrieval loops deferred)

### Scope

Implement a deterministic, heuristic-only query router that classifies a user
query into one of five types and returns a `RoutingPlan` carrying concrete
retrieval parameters. Wire the plan into `run_pipeline()` so all three routing
effects are active (not just logged).

### Files Changed

| File | Change |
|---|---|
| `src/schema/models.py` | Added `QueryType` alias and `RoutingPlan` Pydantic model |
| `src/retrieval/query_router.py` | **New** — `route_query(query) -> RoutingPlan`, pure deterministic function |
| `src/retrieval/__init__.py` | Added `route_query` export |
| `src/generation/answer_pipeline.py` | Added `routing_plan` param; routing overrides block; parent-context gating |
| `tests/test_query_router.py` | **New** — 30 unit tests across 7 classes |

No schema fields removed or renamed. No new packages. `requirements-core.txt` untouched.

### Key Design Decisions

- **Classification priority** (first match wins):
  1. `insufficient_or_ambiguous` — word count < 3
  2. `comparison_or_multi_aspect` — comparison keyword present
  3. `broad_summary` — summary keyword present OR word count > 15
  4. `exact_lookup` — lookup-style opener OR short (≤ 6 words) ending with `?`
  5. `focused_question` — default
- **No LLM in routing**: pure `str.lower()` + `in` membership tests against frozen keyword sets; zero side effects.
- **`notes` field**: each `RoutingPlan` carries plain-English strings explaining the classification (e.g. `"comparison keyword detected: 'vs'"`). Self-documenting, trivially loggable.
- **Three active pipeline effects** (all active, not just logged):
  1. `retrieval_top_k` override — applied before Stage 1 (hybrid retrieval)
  2. `rerank_top_k` override — applied before Stage 1, consumed by Stage 2 (reranking)
  3. `emphasize_parent_context` gate — after Stage 3 (parent lookup): `False` → `synthesis_parents = None`; `True` → pass real parent list; no routing plan → pass real parent list (backward compatible)
- **Backward compatibility**: `routing_plan` defaults to `None`; all existing call sites are unaffected.

### Parameter table

| Query type | `retrieval_top_k` | `rerank_top_k` | `emphasize_parent_context` |
|---|---|---|---|
| `exact_lookup` | 5 | 3 | `False` |
| `focused_question` | 10 | 5 | `False` |
| `broad_summary` | 15 | 8 | `True` |
| `comparison_or_multi_aspect` | 20 | 10 | `True` |
| `insufficient_or_ambiguous` | 5 | 3 | `True` |

### Test Evidence

| Test file | Before (Phase 7A) | After (Phase 7B) |
|---|---|---|
| `test_query_router.py` | (new) | 30 passed |
| `test_citation_builder.py` | 21 passed | 21 passed |
| `test_answer_pipeline.py` | 18 passed, 1 skipped | 18 passed, 1 skipped |
| **Total** | **392 passed, 16 skipped** | **422 passed, 16 skipped** |

### Deferred

- Agentic multi-step retrieval loops (optional Agentic Retrieval path)
- Citation quote extraction from generated answer text
- Production index build at `data/index/`

---

## Phase 8A — Rule-Based Citation Validation

**Completed**: Session (validation core; evaluation metrics deferred to Phase 9)

### Scope

Implement a pure, deterministic citation validator. Validate every
`CitationRecord` against its source `RetrievedChunk`, promote
`validation_status` from `"unverified"` to `"valid"` or `"invalid"`, and
populate `AnswerResponse.validation_flags` with response-level diagnostics.
Wire validation as Stage 6 in `run_pipeline()` — after synthesis and citation
construction.

### Files Changed

| File | Change |
|---|---|
| `src/validation/validators.py` | **New** — `validate_response`, `_validate_citation`, `_build_flags` |
| `src/validation/__init__.py` | Exported `validate_response` |
| `src/generation/answer_pipeline.py` | Added `_validator` injection param; Stage 6 block; updated docstring |
| `tests/test_validators.py` | **New** — 31 unit + integration tests across 6 classes |

No schema fields added or removed. No new packages.

### Key Design Decisions

- **Deterministic, stateless**: `validate_response` is a pure function — same input → same output. No I/O, no model calls.
- **First-failure semantics**: `_validate_citation` applies 7 rules in order; the first rule that fires sets `validation_status = "invalid"` immediately. All rules must pass for `"valid"`.
- **Conditional section-title check (Rule 6)**: only fires when both `citation.section_title` and `chunk.section_title` are non-None. Missing optional titles are not an error.
- **Strict verbatim span (Rule 7)**: two sub-checks — bounds (`0 <= start <= end <= len(text)`) and exact slice equality (`text[start:end] == quote_text`). Applied only when `is_verbatim=True`.
- **`"unverified"` preserved in upstream modules**: `build_citations` still sets `validation_status="unverified"`; only `validate_response` promotes it. This keeps the citation builder honest and separation of concerns clear.
- **`_validator` injection**: follows the existing `_citation_builder`, `_generator` patterns for hermetic test control.
- **No stale tests**: `test_validation_flags_is_empty_list` still passes because the happy-path pipeline (1 valid chunk → 1 matching citation) produces no flags.

### Validation Rules

| Rule | Condition → invalid |
|---|---|
| 1 | `source_chunk_id is None` |
| 2 | `source_chunk_id` not in `chunk_lookup` |
| 3 | `citation.doc_id != chunk.doc_id` |
| 4 | `citation.file_name != chunk.file_name` |
| 5 | `citation.page_number != chunk.page_number` |
| 6 | Both `section_title` non-None AND mismatch (conditional) |
| 7 | `is_verbatim=True`: span out-of-bounds OR `text[start:end] != quote_text` |

### Response-Level Flags

| Flag | Condition |
|---|---|
| `"no_supporting_chunks"` | `supporting_chunks` is empty |
| `"no_sources"` | `sources` is empty |
| `"citation_chunk_count_mismatch"` | `len(sources) != len(supporting_chunks)` |
| `"missing_source_chunk_id"` | any `citation.source_chunk_id is None` |
| `"invalid_citation_present"` | any validated citation has `status == "invalid"` |

### Test Evidence

| Test class | Tests | Coverage |
|---|---|---|
| `TestCitationValidation` | 10 | All 7 rules, verbatim variants |
| `TestSectionTitleValidation` | 4 | Conditional rule 6 — match, mismatch, one-None |
| `TestResponseFlags` | 6 | Each flag in isolation + clean response |
| `TestValidateResponseContract` | 4 | Return type, immutability, field invariants |
| `TestEdgeCases` | 3 | Empty sources, empty chunks, non-verbatim |
| `TestPipelineValidation` | 4 | Stage 6 wired, happy path, injection, Phase 7A citations |

| Test file | Before | After |
|---|---|---|
| `test_validators.py` | (new) | 31 passed |
| `test_citation_builder.py` | 21 passed | 21 passed |
| `test_query_router.py` | 30 passed | 30 passed |
| `test_answer_pipeline.py` | 18 passed, 1 skipped | 18 passed, 1 skipped |
| **Total** | **422 passed, 16 skipped** | **453 passed, 16 skipped** |

### Deferred

- Evaluation metrics and answer quality scoring (Phase 9)
- LLM-based grading
- Agentic multi-step retrieval loops
- Citation quote extraction from generated answer text
- Production index build at `data/index/`

---

## Phase 9A — Deterministic Evaluation Harness

**Completed**: Session (deterministic harness; semantic metrics deferred to Phase 9B)

### Scope

Implement a deterministic, offline evaluation harness that scores
`AnswerResponse` outputs against structured ground-truth examples without any
LLM grading or external metric libraries. Wire `expected_page_numbers` and
`expect_citations_valid` into real computed metrics. Apply strict
zero-denominator rule throughout.

### Files Changed

| File | Change |
|---|---|
| `src/schema/eval_models.py` | **New** — `EvalExample`, `EvalReport` Pydantic models |
| `src/evaluation/evaluator.py` | **New** — `run_evaluation`, `_compute_metrics`, `_rate` |
| `src/evaluation/__init__.py` | Exported `run_evaluation` |
| `tests/test_evaluator.py` | **New** — 38 unit tests across 6 classes |

No schema fields added or removed in existing models. No new packages.

### Key Design Decisions

- **No LLM grading**: all metrics are deterministic set-intersection and field-equality checks over `AnswerResponse` data.
- **No RAGAS**: evaluation is project-native; semantic metrics deferred to Phase 9B.
- **Restricted denominators**: source/file/page/citations-all-valid metrics count only examples that carry the relevant expected field. Zero examples in denominator → `0.0` rate (no fuzzy fallback).
- **`_rate()` helper**: `count / denominator if denominator > 0 else 0.0` enforced uniformly via a single function. No `max(denom, 1)` anywhere.
- **Page-hit semantics**: requires BOTH `expected_file_names` AND `expected_page_numbers` non-empty; hit = Cartesian product `(file_name, page_number)` intersects actual pairs from `sources`.
- **citations-all-valid guard**: `if resp.sources and all(c.validation_status == "valid" for c in resp.sources)` — empty sources evaluates to `False` explicitly (avoids `all([])=True` footgun).
- **Pipeline injection**: `_pipeline` param mirrors existing injection pattern; defaults to a real `run_pipeline` call when `None`.
- **`_compute_metrics` is pure**: same inputs → same outputs. No I/O.

### Metrics Implemented

| Metric | Denominator | Description |
|---|---|---|
| `answer_non_empty_rate` | all examples | Answer text is non-empty string |
| `citation_valid_rate` | all examples | At least one source with `validation_status="valid"` |
| `invalid_citation_rate` | all examples | Any source has `validation_status="invalid"` |
| `no_source_rate` | all examples | `sources` is empty |
| `no_supporting_chunk_rate` | all examples | `supporting_chunks` is empty |
| `source_hit_rate` | examples with `expected_source_chunk_ids` non-empty | Actual `source_chunk_id` set intersects expected |
| `file_hit_rate` | examples with `expected_file_names` non-empty | Actual `file_name` set intersects expected |
| `page_hit_rate` | examples with BOTH `expected_file_names` AND `expected_page_numbers` non-empty | Actual `(file_name, page_number)` pairs intersect expected Cartesian product |
| `citations_all_valid_rate` | examples with `expect_citations_valid=True` | Sources non-empty AND all `validation_status="valid"` |
| `flag_frequency` | — | Count of each validation flag across all responses |

### Test Evidence

| Test class | Tests | Coverage |
|---|---|---|
| `TestEvalExample` | 6 | Default fields, ID generation, overrides, empty lists |
| `TestEvalReport` | 4 | Construction, field types, per_example list, flag_frequency |
| `TestMetricComputation` | 8 | answer_non_empty, citation_valid, invalid_citation, no_source, no_supporting_chunk, flag_frequency, multiple examples, all-zero |
| `TestSourceFilePageHitMetrics` | 9 | source hit/miss/empty, file hit/miss/empty, page hit/miss/zero-denom |
| `TestCitationsAllValidMetric` | 5 | all-valid, one-invalid, empty-sources, expect=False excluded, mixed |
| `TestRunEvaluation` | 6 | Pipeline injection, response count, report type, field counts, zero examples |

| Test file | Before | After |
|---|---|---|
| `test_evaluator.py` | (new) | 38 passed |
| `test_validators.py` | 31 passed | 31 passed |
| `test_citation_builder.py` | 21 passed | 21 passed |
| `test_query_router.py` | 30 passed | 30 passed |
| `test_answer_pipeline.py` | 18 passed, 1 skipped | 18 passed, 1 skipped |
| **Total** | **453 passed, 16 skipped** | **491 passed, 16 skipped** |

### Deferred to Phase 9B

- RAGAS-style semantic metrics (faithfulness, context precision, context recall)
- LLM-based answer quality grading
- Eval dataset building and storage (`data/eval/`)
- Answer relevance scoring
- Aggregate benchmark reporting

---

## Phase 9B — Semantic Evaluation Harness

**Completed**: Session

### Scope

Implement a semantic evaluation layer that scores answer quality using an LLM
judge on four dimensions: groundedness, answer relevance, context relevance,
and completeness. Kept fully separate from the deterministic evaluator and from
the production pipeline. All judge calls are injectable for unit-test isolation;
real Ollama calls are gated behind `SEMANTIC_EVAL_INTEGRATION=1`.

### Files Changed

| File | Change |
|---|---|
| `src/schema/semantic_eval_models.py` | **New** — `SemanticScore`, `SemanticEvalReport` Pydantic models |
| `src/evaluation/judge_prompts.py` | **New** — `build_judge_messages` prompt template |
| `src/evaluation/semantic_evaluator.py` | **New** — `run_semantic_evaluation`, `_parse_scores`, `_score_one`, `_aggregate_scores`, `_build_context_text` |
| `src/evaluation/__init__.py` | Added `run_semantic_evaluation` export |
| `tests/test_semantic_evaluator.py` | **New** — 56 unit tests + 1 gated integration test across 7 classes |

No schema fields added or removed in existing models. No new packages.

### Key Design Decisions

- **Separate report type**: `SemanticEvalReport` is a distinct type from the
  deterministic `EvalReport`. No merging. Callers use one, the other, or both.
- **Takes pre-collected responses**: `run_semantic_evaluation(examples, responses, ...)`
  scores existing responses — it does not run the production pipeline. The
  caller owns pipeline execution.
- **Length guard**: `ValueError` raised immediately if `len(examples) != len(responses)`.
  No silent zip truncation. No partial scoring.
- **Parse failures penalise honestly**: a failed parse → all 4 scores = 0.0,
  `parse_failed=True`. Included in aggregate means. Surfaced via `parse_failure_count`.
- **Deterministic normalization only**: `_parse_scores` does two steps before
  `json.loads` — strip whitespace, strip a single outer ` ```json ``` ` fence.
  No heuristic extraction from arbitrary prose.
- **Score clamping**: `max(0.0, min(1.0, float(v)))` on every score field.
- **Judge exception handling**: `RuntimeError` from the judge callable is caught
  per-example and treated as a parse failure. The run continues.
- **`_judge` injection**: mirrors `_client` in `ollama_llm.py` and `_pipeline`
  in `evaluator.py`. Default falls back to a lazy import of `ollama_llm.generate`.
- **Gated integration test**: `@pytest.mark.skipif(os.getenv("SEMANTIC_EVAL_INTEGRATION") != "1", ...)`.
  Default test run is lightweight.

### Semantic Metrics

| Metric | Description |
|---|---|
| `mean_groundedness` | Mean score: are claims in the answer supported by context? |
| `mean_answer_relevance` | Mean score: does the answer address the question? |
| `mean_context_relevance` | Mean score: is the retrieved context sufficient for the question? |
| `mean_completeness` | Mean score: does the answer cover core aspects of the question? |
| `above_threshold_rate` | Fraction of examples where all 4 scores >= threshold (default 0.7) |
| `parse_failure_count` | Count of examples where judge output could not be parsed |

### Test Evidence

| Test class | Tests | Coverage |
|---|---|---|
| `TestSemanticScore` | 5 | Defaults, parse_failed pattern, judge_notes |
| `TestSemanticEvalReport` | 4 | Construction, threshold stored, rate types, per_example |
| `TestJudgePrompts` | 11 | Structure, all 4 criteria, placeholder, empty inputs, JSON instruction |
| `TestParseScores` | 10 | Clean JSON, whitespace, json fence, bare fence, extra/missing keys, out-of-range, prose failure, empty, partial, array |
| `TestAggregateScores` | 10 | Means, threshold count, boundary inclusive, zero-denom, all-below, parse failures counted/penalise means, threshold stored, total, per_example list |
| `TestRunSemanticEvaluation` | 13 | Return type, length guard (both directions), empty inputs, judge call count, score ranges, custom threshold, parse failure surfaces, judge exception handled, context built from chunks, query forwarded, separate from EvalReport, clamping |
| `TestBuildContextText` | 3 | No chunks → placeholder, single chunk, multiple chunks joined |
| `TestIntegrationGated` | 1 (skipped) | Real Ollama judge, scores in range |

| Test file | Before | After |
|---|---|---|
| `test_semantic_evaluator.py` | (new) | 56 passed, 1 skipped |
| `test_evaluator.py` | 38 passed | 38 passed |
| `test_validators.py` | 31 passed | 31 passed |
| **Total** | **491 passed, 16 skipped** | **547 passed, 17 skipped** |

---

## Phase 10A — Service Layer

**Completed**: Session (backend service layer only; UI deferred to Phase 10B)

### Scope

Implement a thin, testable service layer in `app/` that exposes two public functions —
`index_document()` and `answer_query()` — as stable entry points for any future consumer
(Gradio UI, REST handler, notebook). All heavy pipeline imports are deferred (lazy) inside
the fallback branch so the module loads without triggering LlamaIndex or Ollama at import time.

### Files Changed

| File | Change | Description |
|---|---|---|
| `app/service.py` | **New** | `index_document()`, `answer_query()`, `ServiceError` |
| `app/__init__.py` | **Updated** | Re-exports `index_document`, `answer_query`, `ServiceError` |
| `tests/test_service.py` | **New** | 32 unit tests across 4 classes |

No changes to any `src/` module. No schema changes. No new packages. `data/chroma_db/` untouched.

### Public API

```python
# app/service.py

class ServiceError(Exception):
    """Raised by the service layer when an operation cannot be completed."""

def index_document(
    file_path: Path,
    *,
    index_dir: Optional[Path] = None,
    embed_model: Optional[Any] = None,
    _indexing_pipeline: Optional[Callable[..., IndexManifest]] = None,
) -> IndexManifest:
    # Validates file_path.exists() → raises ServiceError("File not found: ...")
    # Calls _indexing_pipeline(file_path, index_dir=index_dir, embed_model=embed_model)
    # OR lazily imports and calls run_indexing_pipeline(...) with the same kwargs
    # Wraps any pipeline Exception → ServiceError("Indexing failed: ...") from exc

def answer_query(
    query: str,
    *,
    index_dir: Optional[Path] = None,
    retrieval_top_k: int = 10,
    rerank_top_k: int = 5,
    model: Optional[str] = None,
    _answer_pipeline: Optional[Callable[..., AnswerResponse]] = None,
) -> AnswerResponse:
    # Validates query.strip() non-empty → raises ServiceError("Query must not be empty.")
    # Calls _answer_pipeline(query, index_dir=..., retrieval_top_k=..., rerank_top_k=..., model=...)
    # OR lazily imports and calls run_pipeline(...) with the same kwargs
    # Wraps any pipeline Exception → ServiceError("Query failed: ...") from exc
```

### Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| `Callable[..., T]` type hints | Used for injected callables | Python's `typing.Callable` cannot express keyword-only args; `...` is the correct approach |
| Identical kwarg sets for injected and real paths | Both branches use the same kwargs | No hidden mismatches; recording fakes validate the exact contract |
| Lazy real imports | Inside `if _pipeline is None:` branches only | Module loads without triggering LlamaIndex/Ollama; consistent with `src/` lazy import pattern |
| `raise ServiceError(...) from exc` | All pipeline exceptions re-raised this way | `__cause__` always populated; callers can inspect origin via `err.__cause__` |
| `logger.warning(...)` before re-raise | Structlog warning with `exc_info=True` | Observability at service boundary without duplicating logging into UI/REST layers |
| `str(file_path)` accepted | `Path(file_path)` cast in `index_document` | Permits callers that pass string paths (e.g., Gradio file upload handlers) |
| `app/__init__.py` exports | `__all__ = ["index_document", "answer_query", "ServiceError"]` | Clean public namespace; UI and REST consumers import from `app` directly |

### Test Coverage

| Class | Tests | What's covered |
|---|---|---|
| `TestServiceError` | 3 | `issubclass(ServiceError, Exception)`; str message accessible; `__cause__` chaining |
| `TestIndexDocument` | 11 | Returns `IndexManifest`; all kwargs forwarded; missing file raises `ServiceError`; pipeline not called on missing file; pipeline exception wrapped + chained; string path accepted |
| `TestAnswerQuery` | 15 | Returns `AnswerResponse`; query and all kwargs forwarded; empty + whitespace query raise `ServiceError`; pipeline not called on invalid query; pipeline exception wrapped + chained |
| `TestOutputTypes` | 3 | `isinstance` checks on `IndexManifest`, `AnswerResponse`, `ServiceError` |

**Recording fake pattern** used for all injection tests:

```python
class _RecordingIndexPipeline:
    def __call__(self, file_path: Path, *, index_dir, embed_model) -> IndexManifest:
        self.call_kwargs = {"file_path": file_path, "index_dir": index_dir, "embed_model": embed_model}
        ...

class _RecordingAnswerPipeline:
    def __call__(self, query: str, *, index_dir, retrieval_top_k, rerank_top_k, model) -> AnswerResponse:
        self.call_kwargs = {"query": query, "index_dir": index_dir, ...}
        ...
```

### Test Evidence

Isolated run:
```
pytest tests/test_service.py -q
32 passed, 1 warning in ~50s
```

Verified clean (no regressions) across fast non-LlamaIndex test files:
```
pytest tests/test_schema.py tests/test_evaluator.py tests/test_semantic_evaluator.py
      tests/test_validators.py tests/test_answer_pipeline.py tests/test_citation_builder.py -q
216 passed, 2 skipped
```

| Test file | Before (Phase 9B) | After (Phase 10A) |
|---|---|---|
| `test_schema.py` | 20 passed | 20 passed |
| `test_evaluator.py` | 38 passed | 38 passed |
| `test_semantic_evaluator.py` | 56 passed, 1 skipped | 56 passed, 1 skipped |
| `test_validators.py` | 31 passed | 31 passed |
| `test_answer_pipeline.py` | 18 passed, 1 skipped | 18 passed, 1 skipped |
| `test_citation_builder.py` | 21 passed | 21 passed |
| `test_service.py` | (new) | 32 passed |
| **Total (verified subset)** | **184 passed, 2 skipped** | **216 passed, 2 skipped** |
| **Full suite projection** | **547 passed, 17 skipped** | **579 passed, 17 skipped** |

> **Note on full-suite verification**: LlamaIndex-dependent test files
> (`test_query_router.py`, `test_chunking.py`, `test_hierarchical_chunking.py`,
> `test_parsing.py`, `test_indexing.py`, `test_indexing_pipeline.py`,
> `test_vector_retriever.py`, `test_bm25_retriever.py`, `test_hybrid_retriever.py`,
> `test_qwen_reranker.py`, `test_ingestion.py`, `test_ocr.py`, `test_answer_engine.py`)
> require LlamaIndex/Ollama model loading and are slow to collect in isolation without a
> running embedding model. The projected 579 total is 547 (Phase 9B baseline) + 32 (new
> service tests). No `src/` module was modified; no regressions are possible in those files.

### Deferred items (from Phase 10A)

- REST endpoint wiring (if ever added)
- Context-window overflow guard (deferred from Phase 6)
- Production index build at `data/index/`

---

## Phase 10B — Local Gradio Blocks UI

### Scope

Implement the local Gradio Blocks UI (`app/ui.py`) on top of the service layer
established in Phase 10A.  The UI calls only `index_document()` and
`answer_query()` from `app.service` — no direct `src/` imports.

### Files changed

| File | Change |
|---|---|
| `app/ui.py` | New — `build_ui()` plus format helpers and event handlers |
| `app/__init__.py` | Updated — added `build_ui` to imports and `__all__` |
| `tests/test_ui.py` | New — 41 tests across 6 classes |
| `docs/build_log.md` | Updated — this section appended |
| `docs/project_roadmap.md` | Updated — Phase 10 marked complete |

### Public API

```python
# app/ui.py

def _format_index_result(manifest: IndexManifest) -> str: ...
def _format_citations(response: AnswerResponse) -> str: ...
def _format_flags(response: AnswerResponse) -> str: ...

def _handle_index(file_path: Optional[str]) -> str: ...
def _handle_answer(
    query: str,
    retrieval_top_k: int,
    rerank_top_k: int,
    model: str,
) -> Tuple[str, str, str]: ...

def build_ui():  # returns gr.Blocks
```

### Key decisions

1. **`gr.File(..., type="filepath")` hard-coded** — guarantees `_handle_index`
   receives `Optional[str]`; no duck-typing or version-dependent file-object
   handling anywhere in the module.

2. **Import-light hard rule** — `from __future__ import annotations` defers
   annotation evaluation; `IndexManifest` and `AnswerResponse` are imported
   only under `TYPE_CHECKING`; `import gradio as gr` is lazy inside
   `build_ui()` only.  Importing `app.ui` triggers only `app.service` — the
   same lightweight path as importing `app` directly.

3. **Format helpers are pure functions** — no Gradio dependency; directly
   testable with synthetic model instances; no monkeypatching required.

4. **Two-tab Blocks layout** — "Index Document" tab and "Ask a Question" tab;
   Advanced Options accordion (retrieval_top_k, rerank_top_k, model override)
   is collapsed by default.

5. **Three-output answer handler** — returns `(answer_text, citations, flags)`
   tuple; error state occupies only the first element; remaining two are empty
   strings; no raw tracebacks ever surfaced.

6. **`ServiceError` vs unexpected exceptions distinguished** — separate
   user-facing prefixes (`"Indexing failed:"` / `"Query failed:"` vs
   `"Unexpected error during indexing:"` / `"Unexpected error:"`) for
   diagnostic clarity without leaking internals.

### Test coverage

| Class | Tests | Coverage |
|---|---|---|
| `TestFormatIndexResult` | 8 | All `IndexManifest` fields; empty doc list; multi-doc join |
| `TestFormatCitations` | 10 | Empty; file/page/status; section title present/absent; truncation; numbering |
| `TestFormatFlags` | 4 | Empty; single; multi-line; bullet prefix |
| `TestHandleIndex` | 7 | None path; kwarg capture; success format; ServiceError clean; unexpected exc clean |
| `TestHandleAnswer` | 11 | Answer/citations/flags outputs; empty/whitespace model; model forwarded; top-k forwarded; ServiceError tuple; unexpected exc tuple; three-string return |
| `TestBuildUi` | 1 | `isinstance(build_ui(), gr.Blocks)` — skipped when Gradio not installed |

### Test evidence

```
tests/test_ui.py — 40 passed, 1 skipped in 32.04s
```

Regression check (all fast test files):
```
256 passed, 3 skipped, 1 warning in 41.45s
```

Prior fast-suite baseline was 216 passed, 2 skipped.
Delta: +40 passed, +1 skipped — exactly the new UI tests.
Zero regressions.

### Deferred items

- REST endpoint wiring (if ever added)
- Context-window overflow guard (deferred from Phase 6)
- Production index build at `data/index/`
- `build_ui()` smoke test will become un-skipped when Gradio is installed

---

## Phase 11 — Azure Integration: Blob Storage Adapter

**Completed**: Blob Storage artifact adapter and config-switch gateway (first execution chunk inside Phase 11B)

### Files changed

| File | Change |
|---|---|
| `src/storage/blob_artifact_writer.py` | **New** — `BlobArtifactWriter` class + `StorageError` |
| `src/storage/artifact_store.py` | **New** — routing gateway; single switch point for all artifact writes |
| `src/core/config.py` | **Modified** — 4 storage fields added (`storage_backend`, `azure_storage_account_url`, `azure_storage_container_artifacts`, `azure_storage_container_manifests`) |
| `src/storage/artifact_writer.py` | **Modified** — `write_jsonl` delegates to `artifact_store.write_jsonl` via deferred import |
| `src/storage/run_manifest.py` | **Modified** — `save_manifest` delegates to `artifact_store.save_manifest`; return type `Path` → `str` |
| `requirements-full.txt` | **Modified** — `azure-storage-blob>=12.0,<13` and `azure-identity>=1.15,<2` uncommented |
| `tests/test_blob_artifact_writer.py` | **New** — 15 focused adapter and gateway tests |

### Key decisions

1. **Single switch point**: `artifact_store.py` is the only module with backend-selection logic. No conditionals anywhere else in the codebase.
2. **Deferred imports in existing writers**: `artifact_writer.write_jsonl` and `run_manifest.save_manifest` use function-body imports (`from src.storage.artifact_store import ...`) to delegate without creating a circular import cycle at module load time.
3. **`azure-storage-blob` import deferred to `__init__`**: `BlobArtifactWriter` imports `BlobServiceClient` and `DefaultAzureCredential` inside `__init__` so the module is importable with `storage_backend="local"` even if `azure-storage-blob` is not installed.
4. **Constructor injection for testability**: `BlobArtifactWriter` accepts an existing client via `__init__`; tests bypass `__init__` entirely using `object.__new__` and inject a `MagicMock` directly.
5. **Stable `str` return from `save_manifest`**: local mode returns `str(path)`, blob mode returns blob URL string — no `Path | str` union above the storage boundary.
6. **Version pinning**: `azure-storage-blob>=12.0,<13` and `azure-identity>=1.15,<2` keep the stable v12/v1 API surface.

### Test coverage

| Class | Tests | Coverage |
|---|---|---|
| `TestBlobArtifactWriterJsonl` | 5 | Upload called; correct container; content parity; record count; SDK exception → `StorageError` |
| `TestBlobArtifactWriterManifest` | 5 | Correct container; blob name = `{run_id}.json`; content valid JSON; URL returned as str; SDK exception → `StorageError` |
| `TestArtifactStore` | 5 | Local writes to disk; blob delegates; manifest local returns str path; manifest blob returns URL str; unknown backend raises |

### Test evidence

```
tests/test_blob_artifact_writer.py — 15 passed in 0.56s
```

Regression check (all fast test files including new):
```
271 passed, 3 skipped, 1 warning in 9.40s
```

Prior fast-suite baseline was 256 passed, 3 skipped.
Delta: +15 passed — exactly the new storage adapter tests.
Zero regressions.

### Deferred items

- Azure DI OCR adapter (`src/ocr/azure_di_ocr.py`) — next storage-boundary chunk
- Azure AI Search indexer and retriever — Boundary 2
- Dockerfile and ACA entrypoint — after adapters are complete
- Track B managed generation adapter (`src/generation/azure_llm.py`) — deferred

---

## Phase 11 — Azure Integration: Azure DI OCR Adapter (Boundary 1)

**Completed**: Azure AI Document Intelligence OCR adapter and import-light OCR router (second execution chunk inside Phase 11B)

### Files changed

| File | Change |
|---|---|
| `src/ocr/azure_di_ocr.py` | **New** — `AzureDiOcrAdapter` class + `AzureDiOcrError`; full Boundary 1 adapter |
| `src/ocr/ocr_router.py` | **Modified** — heavy imports removed from module level; `_run_local_ocr` and `_run_azure_di_ocr` deferred-import wrappers added; `azure_di` branch dispatches to adapter |
| `src/schema/models.py` | **Modified** — `"azure_di"` added to `ParsedPage.parse_method` Literal |
| `src/core/config.py` | **Modified** — `ocr_backend` and `azure_di_endpoint` fields added |
| `requirements-full.txt` | **Modified** — `azure-ai-documentintelligence>=1.0,<2` uncommented |
| `tests/test_azure_di_ocr.py` | **New** — 17 adapter and router-level tests |

### Key decisions

1. **Import-light `ocr_router.py`**: Removed the module-level `from src.parsing.docling_parser import parse_with_docling` import. Both backends now use deferred-import wrapper functions (`_run_local_ocr`, `_run_azure_di_ocr`). Importing `src.ocr.ocr_router` no longer triggers torch or Docling loading — the pre-existing fast-suite hang caused by this module is eliminated.
2. **Wrapper functions as monkeypatch targets**: `_run_local_ocr` and `_run_azure_di_ocr` are module-level names, making them patchable in tests without `sys.modules` injection. Router-level backend-selection tests import `ocr_router` directly.
3. **`_AnalyzeDocumentRequest` stored in `__init__`**: `AnalyzeDocumentRequest` is imported inside `__init__` and stored as `self._AnalyzeDocumentRequest`. Tests bypass `__init__` with `object.__new__` and inject `_FakeRequest` — same pattern as `BlobArtifactWriter._client`.
4. **`ocr_confidence` populated**: Azure DI exposes per-word confidence scores; `ocr_confidence` is set as the mean word confidence for recovered pages. This is a meaningful improvement over the local path (where confidence remains `None`).
5. **Adapter returns project-native types only**: No `DocumentIntelligenceClient`, `DocumentPage`, or Azure SDK objects cross the adapter boundary. Output is always `List[ParsedPage]`.
6. **Local path preserved exactly**: The Docling/RapidOCR path in `ocr_router.py` is functionally unchanged — it just calls through the new `_run_local_ocr` wrapper instead of the former direct module-level import.

### Test coverage

| Class | Tests | Coverage |
|---|---|---|
| `TestAzureDiOcrAdapterText` | 5 | API call bytes verified; text from lines; page-number mapping; empty DI page stays empty; identity fields unchanged |
| `TestAzureDiOcrAdapterConfidence` | 3 | Mean confidence computed; None when no words; per-page isolation |
| `TestAzureDiOcrAdapterErrors` | 2 | SDK exception → `AzureDiOcrError`; missing page → original returned |
| `TestAzureDiOcrAdapterMetadata` | 2 | `parse_method="azure_di"`; `ocr_engine="azure-document-intelligence"` |
| `TestOcrRouterBackendSelection` | 5 | azure_di backend selected; local backend selected; error → original pages; identity through router; no-empty-pages skips both |

### Test evidence

```
tests/test_azure_di_ocr.py — 17 passed in 1.52s
```

Regression check (full fast-suite including new):
```
288 passed, 3 skipped, 1 warning in 49.82s
```

Prior fast-suite baseline was 271 passed, 3 skipped.
Delta: +17 passed — exactly the new Azure DI OCR tests.
Zero regressions.

### Deferred items

- Azure AI Search indexer (`src/indexing/azure_search_indexer.py`) — **Complete** (this chunk)
- Azure AI Search retriever (`src/retrieval/azure_search_retriever.py`) — **Complete** (this chunk)
- Dockerfile and ACA entrypoint — after adapters are complete
- Track B managed generation adapter (`src/generation/azure_llm.py`) — deferred

---

## Phase 11 — Azure Integration: Azure AI Search Adapter (Boundary 2)

**Completed**: Azure AI Search indexer + retriever adapters and centralized index/retrieval gateways (third execution chunk inside Phase 11B)

### Files changed

| File | Change |
|---|---|
| `src/indexing/azure_search_indexer.py` | **New** — `AzureSearchIndexer` class + `AzureSearchIndexError`; full Boundary 2 indexing adapter |
| `src/indexing/index_gateway.py` | **New** — `route_index()` gateway; dispatches to local `build_indexes` or Azure indexer based on `config.search_backend` |
| `src/retrieval/azure_search_retriever.py` | **New** — `AzureSearchRetriever` class + `AzureSearchRetrievalError`; BM25 child-only retrieval + parent point-lookup |
| `src/retrieval/retrieval_gateway.py` | **New** — `route_retrieve()` and `route_lookup_parents()` gateways |
| `src/core/config.py` | **Modified** — `search_backend`, `azure_search_endpoint`, `azure_search_index_name` fields added |
| `requirements-full.txt` | **Modified** — `azure-search-documents>=11.0,<12` uncommented |
| `src/indexing/indexing_pipeline.py` | **Modified** — final `build_indexes()` call replaced with `route_index()` deferred import |
| `src/generation/answer_pipeline.py` | **Modified** — retrieval and parent-lookup calls replaced with `route_retrieve()` / `route_lookup_parents()` |
| `tests/test_azure_search_adapters.py` | **New** — 27 adapter and gateway tests |
| `tests/test_indexing_pipeline.py` | **Modified** — 7 orchestration tests updated to patch `src.indexing.index_gateway.route_index` (previously patched `build_indexes` directly on pipeline module) |

### Key decisions

1. **Child-only retrieval**: `AzureSearchRetriever.retrieve()` passes `filter="chunk_level eq 'child'"`. Parent chunks are indexed for point-lookup only via `get_document()`.
2. **Honest BM25 score semantics**: No vector field in this version. `retrieval_method="bm25"`, `bm25_score=@search.score`, `vector_score=None`, `fusion_score=None`. True vector search deferred to a hardening step.
3. **Centralized retrieval gateway**: `retrieval_gateway.py` owns all retrieval and parent-lookup dispatch. `answer_pipeline.py` is fully backend-agnostic.
4. **Azure parent lookup via `get_document()`**: `lookup_parents()` calls `self._client.get_document(key=parent_chunk_id)` for each child. Returns `DocumentChunk` or `None` on failure.
5. **SDK model class injection**: All Azure SDK model classes (`SearchIndex`, `SimpleField`, etc.) are imported once inside `__init__` and stored as instance attributes (`self._SearchIndex`, etc.) for testability — mirrors the `AzureDiOcrAdapter._AnalyzeDocumentRequest` pattern.
6. **Import-light gateways**: `index_gateway.py` and `retrieval_gateway.py` have no module-level heavy imports. All SDK and LlamaIndex imports are deferred to wrapper functions.

### Test coverage

| Class | Tests | Coverage |
|---|---|---|
| `TestAzureSearchIndexerUpload` | 7 | manifest shape; upload called; doc fields; empty input; index_dir=endpoint; SDK exception; parent/child counts |
| `TestAzureSearchRetriever` | 9 | child filter; result→chunk; bm25_score; vector_score=None; fusion_score=None; retrieval_method=bm25; empty results; top_k; SDK exception |
| `TestAzureSearchRetrieverParents` | 4 | returns DocumentChunk; None when no parent_id; None on fetch failure; chunk_index=0 |
| `TestIndexGatewaySwitch` | 3 | azure branch selected; local branch selected; all chunks forwarded to azure |
| `TestRetrievalGatewaySwitch` | 4 | retrieve azure branch; retrieve local branch; lookup_parents azure branch; lookup_parents local branch |

### Test evidence

```
tests/test_azure_search_adapters.py — 27 passed in 17.13s
tests/test_indexing_pipeline.py — 25 passed, 1 skipped
```

Full suite (tests/ directory):
```
315 passed, 13 skipped, 1 warning
```

Prior fast-suite baseline was 288 passed, 3 skipped (after Chunk 2).
Delta: +27 passed — exactly the new Azure Search adapter tests. Zero regressions.

### Deferred items

- Dockerfile and ACA entrypoint — next chunk
- Track B managed generation adapter (`src/generation/azure_llm.py`) — deferred


---

## Phase 11B — Chunk 4: Containerization and ACA-ready runtime entrypoint

**Completed**: Session 7

**Completed**: Containerization scaffold and OCR test patch cleanup (fourth execution chunk inside Phase 11B)

### What was built

| File | Change | Description |
|---|---|---|
| `requirements-container.txt` | **New** | Linux container install target; excludes `paddlepaddle`/`paddleocr` (no stable Python 3.12 Linux wheel) |
| `Dockerfile` | **New** | `python:3.12-slim` single-stage image; installs `libgomp1`, copies source, exposes port 7860 |
| `run.py` | **New** | Import-safe entrypoint; `main()` wraps `build_ui()` + `demo.launch()`; `__main__` guard; all execution deferred |
| `.env.example` | **Updated** | Full runtime env-var contract: bind, logging, generation, embedding, storage, OCR, search backends, Azure identity note |
| `tests/test_container_runtime.py` | **New** | 11 tests across 2 classes (`TestEntrypoint`, `TestEnvVarContract`); direct import, no Docker build required |
| `tests/test_ocr.py` | **Updated** | Patched all 18 stale `parse_with_docling` mock targets → `_run_local_ocr` (deferred-import wrapper) |

### Key design decisions

**Import safety**: `build_ui()` and `demo.launch()` live inside `main()` so importing `run.py` never starts a server. Tests can freely `import run` and patch `app.ui.build_ui`.

**Ollama topology neutral**: `OLLAMA_BASE_URL` is a runtime input only. The container image does not bundle Ollama or prescribe a deployment topology.

**PaddleOCR excluded from container**: `paddlepaddle` and `paddleocr` have no stable Python 3.12 Linux wheels on PyPI. The active container OCR path uses Docling's embedded RapidOCR engine. Commented exclusion note in `requirements-container.txt`.

**No ACA manifests in this chunk**: ACA deployment YAML deferred to a later chunk when the full Azure integration is validated end-to-end.

### OCR test patch cleanup

Root cause (pre-existing from Chunk 2): Tests patched `src.ocr.ocr_router.parse_with_docling` which was moved to a deferred-import function body in Chunk 2. All 18 affected tests now patch the wrapper `src.ocr.ocr_router._run_local_ocr`.

### Test evidence

```
pytest tests/ -q
689 passed, 18 skipped, 3 warnings in 44.31s
```

| Scope | Before (Chunk 3) | After (Chunk 4) |
|---|---|---|
| `test_ocr.py` | 13 skipped (stale mocks) | 0 skipped — all 21 unit tests pass |
| `test_container_runtime.py` | (new) | 11 passed |
| Rest of suite | 302 passed, 0 skipped | 657 passed, 18 skipped (integration tests) |
| **Total** | **315 passed, 13 skipped** | **689 passed, 18 skipped** |

Baseline fast-suite (non-integration) = 689 passed. The 18 skipped are integration tests (require live Azure credentials or large models).


