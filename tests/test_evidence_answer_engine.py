from __future__ import annotations

import pytest

from src.core.config import config

from src.generation.evidence_answer_engine import (
    EvidenceGenerationError,
    generate_from_evidence,
)

from src.generation.evidence_prompt import (
    build_evidence_grounded_messages,
)

from src.schema.generation_service_models import (
    GENERATION_CONTRACT_VERSION,
    GenerationRequest,
)

from src.schema.models import CitationRecord

from src.schema.retrieval_service_models import (
    RetrievalEvidence,
)


def _evidence(
    *,
    chunk_id: str = "child-1",
    citation_id: str = "citation-1",
    child_text: str = (
        "The policy requires annual review."
    ),
    parent_text: str | None = (
        "The governance framework requires "
        "annual review and assigns ownership."
    ),
) -> RetrievalEvidence:
    citation = CitationRecord(
        citation_id=citation_id,
        doc_id="doc-1",
        file_name="example.pdf",
        page_number=3,
        section_title="Governance",
        quote_text=child_text,
        source_chunk_id=chunk_id,
        is_verbatim=True,
        validation_status="valid",
    )

    return RetrievalEvidence(
        chunk_id=chunk_id,
        document_id="doc-1",
        page_start=3,
        page_end=3,
        section_path="Governance",
        text=child_text,
        parent_text=parent_text,
        score=0.91,
        retrieval_method="hybrid",
        citation_payload=citation,
    )


def test_generation_request_normalizes_query():
    request = GenerationRequest(
        query="  What is required?  ",
        evidence=[],
    )

    assert request.query == (
        "What is required?"
    )


def test_generation_request_rejects_blank_query():
    with pytest.raises(
        ValueError,
        match="query must not be blank",
    ):
        GenerationRequest(
            query="   ",
            evidence=[],
        )


def test_evidence_prompt_contains_citation_and_child():
    evidence = _evidence()

    messages = (
        build_evidence_grounded_messages(
            "What is required?",
            [evidence],
        )
    )

    combined = "\n".join(
        item["content"]
        for item in messages
    )

    assert (
        "[Citation ID: citation-1]"
        in combined
    )

    assert (
        evidence.text
        in combined
    )

    assert (
        "Authoritative evidence:"
        in combined
    )


def test_parent_context_is_explicitly_secondary():
    evidence = _evidence()

    messages = (
        build_evidence_grounded_messages(
            "What is required?",
            [evidence],
        )
    )

    combined = "\n".join(
        item["content"]
        for item in messages
    )

    assert evidence.parent_text in combined

    assert (
        "background only"
        in combined
    )

    assert (
        "must not be the sole support"
        in combined
    )


def test_empty_evidence_uses_existing_no_context_behavior():
    messages = (
        build_evidence_grounded_messages(
            "What is required?",
            [],
        )
    )

    combined = "\n".join(
        item["content"]
        for item in messages
    )

    assert (
        "[No context provided.]"
        in combined
    )


def test_generation_preserves_phase11_evidence_and_citations(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    evidence = _evidence()

    captured = {}

    def fake_generator(messages):
        captured["messages"] = messages
        return "Annual review is required."

    response = generate_from_evidence(
        GenerationRequest(
            query="What is required?",
            evidence=[evidence],
            model="test-model",
        ),
        _generator=fake_generator,
        _clock=iter(
            [10.0, 10.025]
        ).__next__,
    )

    assert response.answer_text == (
        "Annual review is required."
    )

    assert response.model_used == (
        "test-model"
    )

    assert response.generation_backend == (
        "ollama"
    )

    assert response.evidence == [
        evidence
    ]

    assert response.sources == [
        evidence.citation_payload
    ]

    assert response.latency_ms == (
        pytest.approx(25.0)
    )

    assert (
        response.generation_contract_version
        == GENERATION_CONTRACT_VERSION
    )

    assert captured["messages"]


def test_generation_uses_existing_gateway(
    monkeypatch,
):
    monkeypatch.setattr(
        config,
        "generation_backend",
        "ollama",
    )

    captured = {}

    def fake_gateway(
        messages,
        model=None,
    ):
        captured["messages"] = messages
        captured["model"] = model
        return "Grounded answer"

    monkeypatch.setattr(
        "src.generation."
        "evidence_answer_engine.generate",
        fake_gateway,
    )

    response = generate_from_evidence(
        GenerationRequest(
            query="Question?",
            evidence=[_evidence()],
            model="gateway-model",
        )
    )

    assert response.answer_text == (
        "Grounded answer"
    )

    assert captured["model"] == (
        "gateway-model"
    )


def test_generation_rejects_empty_backend_response():
    with pytest.raises(
        EvidenceGenerationError,
        match="empty response",
    ):
        generate_from_evidence(
            GenerationRequest(
                query="Question?",
                evidence=[_evidence()],
            ),
            _generator=lambda messages: "   ",
        )


def test_no_parent_context_still_generates_prompt():
    evidence = _evidence(
        parent_text=None
    )

    messages = (
        build_evidence_grounded_messages(
            "Question?",
            [evidence],
        )
    )

    combined = "\n".join(
        message["content"]
        for message in messages
    )

    assert evidence.text in combined

    assert (
        "Additional parent context"
        not in combined
    )
