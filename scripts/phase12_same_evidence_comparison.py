"""
Phase 12E-1 acceptance smoke.

Retrieves evidence exactly once from the Phase 11 retrieval service,
then sends that same evidence contract to:

1. Local Ollama / Qwen3-8B
2. Databricks managed generation

No query, answer, document ID, citation ID, or evidence text is printed.
"""

from __future__ import annotations

from typing import Any, List

from scripts.evaluation.run_databricks_retrieval_baseline import (
    CANONICAL_PATH,
    CORPUS_MANIFEST_PATH,
)
from src.core.config import config
from src.evaluation.canonical_retrieval_dataset import (
    load_databricks_retrieval_examples,
)
from src.generation.databricks_llm import (
    DatabricksGenerationResult,
    generate_with_metadata,
)
from src.generation.evidence_answer_engine import (
    generate_from_evidence,
)
from src.retrieval.retrieval_service import (
    run_retrieval_service,
)
from src.schema.generation_service_models import (
    GenerationRequest,
)
from src.schema.retrieval_service_models import (
    RetrievalRequest,
)


LOCAL_MODEL = "qwen3:8b"


def _expected_document_ids(example: Any) -> List[str]:
    """
    Support the canonical dataset's singular or plural document-ID
    representation without exposing identifiers.
    """
    plural = getattr(
        example,
        "expected_document_ids",
        None,
    )

    if plural:
        return list(plural)

    singular = getattr(
        example,
        "expected_document_id",
        None,
    )

    if singular:
        return [singular]

    raise RuntimeError(
        "Canonical retrieval example has no expected document ID."
    )


def _dump_models(items: list[Any]) -> list[dict[str, Any]]:
    return [
        item.model_dump(mode="json")
        for item in items
    ]


def main() -> None:
    if hasattr(config, "search_backend"):
        config.search_backend = "databricks"

    if hasattr(config, "runtime_mode"):
        config.runtime_mode = "databricks"

    managed_model = (
        config.databricks_generation_model.strip()
    )

    if not managed_model:
        raise RuntimeError(
            "Databricks generation model is not configured."
        )

    examples = load_databricks_retrieval_examples(
        CANONICAL_PATH,
        CORPUS_MANIFEST_PATH,
    )

    if not examples:
        raise RuntimeError(
            "No canonical retrieval examples are available."
        )

    example = examples[0]

    retrieval_call_count = 0

    retrieval_request = RetrievalRequest(
        query=example.query,
        document_ids=_expected_document_ids(example),
        top_k=10,
        final_k=3,
        include_parent_context=True,
    )

    retrieval_call_count += 1
    retrieval_response = run_retrieval_service(
        retrieval_request
    )

    evidence = retrieval_response.results

    if not evidence:
        raise RuntimeError(
            "Phase 12E retrieval returned no evidence."
        )

    original_backend = config.generation_backend

    managed_capture: dict[
        str,
        DatabricksGenerationResult,
    ] = {}

    try:
        # ---------------------------------------------------------
        # Local generation
        # ---------------------------------------------------------
        config.generation_backend = "ollama"

        local_request = GenerationRequest(
            query=example.query,
            evidence=evidence,
            model=LOCAL_MODEL,
        )

        local_response = generate_from_evidence(
            local_request
        )

        # ---------------------------------------------------------
        # Managed generation
        # ---------------------------------------------------------
        config.generation_backend = "databricks"

        managed_request = GenerationRequest(
            query=example.query,
            evidence=evidence,
            model=managed_model,
        )

        def managed_generator(
            messages,
            model=None,
        ) -> str:
            result = generate_with_metadata(
                messages,
                model=model,
            )

            managed_capture["result"] = result
            return result.text

        managed_response = generate_from_evidence(
            managed_request,
            _generator=managed_generator,
        )

    finally:
        config.generation_backend = original_backend

    managed_metadata = managed_capture.get("result")

    if managed_metadata is None:
        raise RuntimeError(
            "Managed generation metadata was not captured."
        )

    original_evidence = _dump_models(evidence)

    local_evidence = _dump_models(
        local_response.evidence
    )

    managed_evidence = _dump_models(
        managed_response.evidence
    )

    local_sources = _dump_models(
        local_response.sources
    )

    managed_sources = _dump_models(
        managed_response.sources
    )

    original_sources = _dump_models(
        [
            item.citation_payload
            for item in evidence
        ]
    )

    same_evidence = (
        original_evidence
        == local_evidence
        == managed_evidence
    )

    same_citations = (
        original_sources
        == local_sources
        == managed_sources
    )

    parent_context_available = any(
        bool(
            item.parent_text
            and item.parent_text.strip()
        )
        for item in evidence
    )

    managed_tokens_available = all(
        value is not None
        for value in (
            managed_metadata.prompt_tokens,
            managed_metadata.completion_tokens,
            managed_metadata.total_tokens,
        )
    )

    print(
        "PHASE12E_RETRIEVAL_CALL_COUNT:",
        retrieval_call_count,
    )
    print(
        "PHASE12E_RETRIEVAL_RESULT_COUNT:",
        len(evidence),
    )
    print(
        "PHASE12E_PARENT_CONTEXT_AVAILABLE:",
        parent_context_available,
    )
    print(
        "PHASE12E_LOCAL_BACKEND:",
        local_response.generation_backend,
    )
    print(
        "PHASE12E_MANAGED_BACKEND:",
        managed_response.generation_backend,
    )
    print(
        "PHASE12E_LOCAL_ANSWER_NONEMPTY:",
        bool(local_response.answer_text.strip()),
    )
    print(
        "PHASE12E_MANAGED_ANSWER_NONEMPTY:",
        bool(managed_response.answer_text.strip()),
    )
    print(
        "PHASE12E_EXACT_SAME_EVIDENCE:",
        same_evidence,
    )
    print(
        "PHASE12E_EXACT_SAME_CITATIONS:",
        same_citations,
    )
    print(
        "PHASE12E_SOURCE_COUNT_EQUAL:",
        len(local_response.sources)
        == len(managed_response.sources)
        == len(evidence),
    )
    print(
        "PHASE12E_LOCAL_LATENCY_VALID:",
        local_response.latency_ms >= 0,
    )
    print(
        "PHASE12E_MANAGED_LATENCY_VALID:",
        managed_response.latency_ms >= 0,
    )
    print(
        "PHASE12E_MANAGED_TOKENS_AVAILABLE:",
        managed_tokens_available,
    )

    passed = all(
        [
            retrieval_call_count == 1,
            bool(evidence),
            local_response.generation_backend
            == "ollama",
            managed_response.generation_backend
            == "databricks",
            bool(local_response.answer_text.strip()),
            bool(managed_response.answer_text.strip()),
            same_evidence,
            same_citations,
            len(local_response.sources)
            == len(managed_response.sources)
            == len(evidence),
            local_response.latency_ms >= 0,
            managed_response.latency_ms >= 0,
            managed_tokens_available,
        ]
    )

    print(
        "PHASE12E_SAME_EVIDENCE_PASS:",
        passed,
    )

    if not passed:
        raise RuntimeError(
            "Phase 12E same-evidence acceptance failed."
        )


if __name__ == "__main__":
    main()
