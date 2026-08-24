from src.core.config import config

from src.generation.evidence_answer_engine import (
    generate_from_evidence,
)

from src.schema.generation_service_models import (
    GenerationRequest,
)

from src.schema.models import CitationRecord

from src.schema.retrieval_service_models import (
    RetrievalEvidence,
)


config.generation_backend = "ollama"


evidence = RetrievalEvidence(
    chunk_id="phase12-smoke-child",
    document_id="phase12-smoke-document",
    page_start=1,
    page_end=1,
    section_path="Policy",
    text=(
        "The annual compliance review must "
        "be completed by 31 March."
    ),
    parent_text=(
        "The compliance programme describes "
        "annual governance and review duties. "
        "The annual compliance review must "
        "be completed by 31 March."
    ),
    score=1.0,
    retrieval_method="hybrid",
    citation_payload=CitationRecord(
        citation_id="phase12-smoke-citation",
        doc_id="phase12-smoke-document",
        file_name="synthetic.pdf",
        page_number=1,
        section_title="Policy",
        quote_text=(
            "The annual compliance review must "
            "be completed by 31 March."
        ),
        source_chunk_id="phase12-smoke-child",
        is_verbatim=True,
        validation_status="valid",
    ),
)


response = generate_from_evidence(
    GenerationRequest(
        query=(
            "When must the annual compliance "
            "review be completed?"
        ),
        evidence=[evidence],
        model="qwen3:8b",
    )
)


print(
    "PHASE12C_LOCAL_BACKEND:",
    response.generation_backend,
)

print(
    "PHASE12C_LOCAL_MODEL:",
    response.model_used,
)

print(
    "PHASE12C_ANSWER_NONEMPTY:",
    bool(response.answer_text.strip()),
)

print(
    "PHASE12C_SOURCE_COUNT:",
    len(response.sources),
)

print(
    "PHASE12C_EVIDENCE_COUNT:",
    len(response.evidence),
)

print(
    "PHASE12C_CITATION_PRESERVED:",
    (
        len(response.sources) == 1
        and response.sources[0].citation_id
        == evidence.citation_payload.citation_id
        and response.sources[0].source_chunk_id
        == evidence.chunk_id
    ),
)

print(
    "PHASE12C_EVIDENCE_PRESERVED:",
    (
        len(response.evidence) == 1
        and response.evidence[0].chunk_id
        == evidence.chunk_id
    ),
)

print(
    "PHASE12C_LATENCY_VALID:",
    response.latency_ms >= 0,
)

print(
    "PHASE12C_LOCAL_PASS:",
    all(
        [
            response.generation_backend
            == "ollama",
            bool(
                response.answer_text.strip()
            ),
            len(response.sources) == 1,
            len(response.evidence) == 1,
            response.sources[0].citation_id
            == evidence.citation_payload.citation_id,
            response.sources[0].source_chunk_id
            == evidence.chunk_id,
        ]
    ),
)
