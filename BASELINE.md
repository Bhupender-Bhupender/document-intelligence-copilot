# Local Document Intelligence Baseline

## Baseline identity

- Baseline name: local-baseline-v1
- Baseline phase: Phase 0
- Runtime mode: Local
- Status: Working baseline
- Date frozen: 2026-07-25

## Current capabilities

- Gradio document upload and question-answer interface
- Native PDF and text-document processing
- OCR recovery for scanned documents
- Layout-aware document parsing
- Canonical document, page, block, chunk and answer schemas
- Hierarchical parent-child chunking
- Dense semantic retrieval
- BM25 keyword retrieval
- Reciprocal Rank Fusion
- Qwen reranking
- LLM-based answer generation
- Page-level citations
- Citation and evidence validation
- Evaluation and result-export functionality
- Docker-based local deployment

## Current active technologies

- Python
- Gradio
- LlamaIndex
- Docling
- RapidOCR
- Qwen3-8B through Ollama
- Qwen3-Embedding-0.6B
- Qwen3-Reranker-0.6B
- LlamaIndex SimpleVectorStore
- BM25
- Docker

## Current storage and retrieval

- Local document and artifact storage
- Persisted indexes under data/index/
- LlamaIndex SimpleVectorStore as the active vector store
- ChromaDB retained only as a legacy reference
- Local hybrid dense and BM25 retrieval

## Current observed performance

The application currently produces relevant answers approximately 80–90%
of the time on manually reviewed questions.

This is an observed estimate and is not yet a formal benchmark. It will be
replaced with reproducible retrieval, answer, citation and latency metrics
during Phase 0.

## Current cloud readiness

Azure storage, OCR and search adapters have been prepared or mapped, but the
application has not yet been productionized on Azure Databricks.

## Baseline purpose

This version is the known-good local implementation against which the Azure
Databricks productionized platform will be evaluated.

The baseline will be used to identify regressions or improvements in:

- Document extraction
- Chunking
- Retrieval accuracy
- Correct-page retrieval
- Citation validity
- Answer quality
- Ingestion performance
- Query latency
- Reliability and failure handling

## Automated test baseline

- Test date: 2026-07-25
- Test command: `pytest tests/ -q`
- Passed: 726
- Skipped: 25
- Failed: 0
- Warnings: 3 deprecation warnings
- Runtime: 69.94 seconds
- Baseline status: Passed

The warnings relate to dependency deprecations in dateutil, Docling, and
RapidOCR. They do not currently block the application but should be monitored
during future dependency upgrades.

## Evaluation corpus baseline

- Documents inventoried: 9
- Total corpus size: 2,513,105 bytes
- Evaluation datasets: 5
- Evaluation queries: 21
- Dataset validation status: Valid
- Document identity method: SHA-256-based stable IDs
- Raw documents and local path mappings: Excluded from Git
