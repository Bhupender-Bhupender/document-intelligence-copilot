"""
Phase 12 hybrid-mode smoke validation.

Databricks AI Search
    -> Phase 11 RetrievalResponse
    -> RetrievalEvidence
    -> local Ollama/Qwen
    -> Phase 12 GenerationResponse

The script intentionally prints only contract/status
information, not query text or document contents.
"""

from src.core.config import config

from scripts.evaluation.run_databricks_retrieval_baseline import (
    CANONICAL_PATH,
    CORPUS_MANIFEST_PATH,
)

from src.evaluation.canonical_retrieval_dataset import (
    load_databricks_retrieval_examples,
)

from src.retrieval.retrieval_service import (
    run_retrieval_service,
)

from src.schema.retrieval_service_models import (
    RetrievalRequest,
)

from src.generation.evidence_answer_engine import (
    generate_from_evidence,
)

from src.schema.generation_service_models import (
    GenerationRequest,
)


# ------------------------------------------------------------
# Hybrid runtime:
# retrieval = Databricks
# generation = local Ollama
# ------------------------------------------------------------

if hasattr(config, "search_backend"):
    config.search_backend = "databricks"

if hasattr(config, "runtime_mode"):
    config.runtime_mode = "databricks"

config.generation_backend = "ollama"


# ------------------------------------------------------------
# Reuse one existing canonical evaluation case.
# Do not print its query/document metadata.
# ------------------------------------------------------------

examples = load_databricks_retrieval_examples(
    canonical_path=CANONICAL_PATH,
    corpus_manifest_path=CORPUS_MANIFEST_PATH,
)

if not examples:
    raise RuntimeError(
        "No canonical retrieval examples available."
    )

example = examples[0]


# ------------------------------------------------------------
# Phase 11 retrieval
# ------------------------------------------------------------

retrieval_response = run_retrieval_service(
    RetrievalRequest(
        query=example.query,
        document_ids=[
            example.expected_document_id
        ],
        top_k=10,
        final_k=3,
        include_parent_context=True,
    )
)

if not retrieval_response.results:
    raise RuntimeError(
        "Phase 11 retrieval returned no evidence."
    )


retrieval_document_only = all(
    item.document_id
    == example.expected_document_id
    for item in retrieval_response.results
)

retrieval_citations_ready = all(
    item.citation_payload.source_chunk_id
    == item.chunk_id
    for item in retrieval_response.results
)


# ------------------------------------------------------------
# Phase 12 local generation
# ------------------------------------------------------------

generation_response = generate_from_evidence(
    GenerationRequest(
        query=example.query,
        evidence=retrieval_response.results,
        model="qwen3:8b",
    )
)


# ------------------------------------------------------------
# Cross-service contract validation
# ------------------------------------------------------------

same_evidence = (
    [
        item.chunk_id
        for item in generation_response.evidence
    ]
    ==
    [
        item.chunk_id
        for item in retrieval_response.results
    ]
)

same_citations = (
    [
        item.citation_id
        for item in generation_response.sources
    ]
    ==
    [
        item.citation_payload.citation_id
        for item in retrieval_response.results
    ]
)

citation_chunk_alignment = all(
    source.source_chunk_id == evidence.chunk_id
    for source, evidence in zip(
        generation_response.sources,
        generation_response.evidence,
    )
)

parent_context_available = any(
    bool(item.parent_text)
    for item in retrieval_response.results
)

filter_recorded = (
    "document_ids"
    in retrieval_response.applied_filters
)

answer_nonempty = bool(
    generation_response.answer_text.strip()
)

source_count_matches = (
    len(generation_response.sources)
    == len(retrieval_response.results)
)

backend_correct = (
    generation_response.generation_backend
    == "ollama"
)


checks = [
    len(retrieval_response.results) > 0,
    retrieval_document_only,
    retrieval_citations_ready,
    filter_recorded,
    answer_nonempty,
    same_evidence,
    same_citations,
    citation_chunk_alignment,
    source_count_matches,
    backend_correct,
]


print(
    "PHASE12C2_RETRIEVAL_RESULT_COUNT:",
    len(retrieval_response.results),
)

print(
    "PHASE12C2_DATABRICKS_DOCUMENT_FILTER:",
    retrieval_document_only,
)

print(
    "PHASE12C2_RETRIEVAL_CITATIONS_READY:",
    retrieval_citations_ready,
)

print(
    "PHASE12C2_FILTER_RECORDED:",
    filter_recorded,
)

print(
    "PHASE12C2_PARENT_CONTEXT_AVAILABLE:",
    parent_context_available,
)

print(
    "PHASE12C2_GENERATION_BACKEND:",
    generation_response.generation_backend,
)

print(
    "PHASE12C2_GENERATION_MODEL:",
    generation_response.model_used,
)

print(
    "PHASE12C2_ANSWER_NONEMPTY:",
    answer_nonempty,
)

print(
    "PHASE12C2_SAME_EVIDENCE:",
    same_evidence,
)

print(
    "PHASE12C2_SAME_CITATIONS:",
    same_citations,
)

print(
    "PHASE12C2_CITATION_CHUNK_ALIGNMENT:",
    citation_chunk_alignment,
)

print(
    "PHASE12C2_SOURCE_COUNT_MATCHES:",
    source_count_matches,
)

print(
    "PHASE12C2_RETRIEVAL_LATENCY_VALID:",
    retrieval_response.latency_ms >= 0,
)

print(
    "PHASE12C2_GENERATION_LATENCY_VALID:",
    generation_response.latency_ms >= 0,
)

print(
    "PHASE12C2_HYBRID_PASS:",
    all(checks),
)

