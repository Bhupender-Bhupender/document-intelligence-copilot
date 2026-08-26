"""
Phase 12E-2 controlled generation-provider comparison.

For three canonical retrieval examples:

    retrieval ONCE
        |
        +--> local Qwen3-8B
        |
        +--> Databricks managed model

The existing local semantic evaluator then evaluates the already-generated
responses. Retrieval is never repeated for the judge.

No question text, answer text, evidence text, document identifiers,
citation identifiers, filenames, workspace identifiers, or credentials
are printed or written to the comparison artifact.
"""

from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any, List

from scripts.evaluation.run_databricks_retrieval_baseline import (
    CANONICAL_PATH,
    CORPUS_MANIFEST_PATH,
)
from src.core.config import config
from src.evaluation.canonical_retrieval_dataset import (
    load_databricks_retrieval_examples,
)
from src.evaluation.semantic_evaluator import (
    run_semantic_evaluation,
)
from src.generation.databricks_llm import (
    DatabricksGenerationResult,
    generate_with_metadata,
)
from src.generation.evidence_answer_engine import (
    generate_from_evidence,
)
from src.schema.eval_models import EvalExample
from src.schema.generation_service_models import (
    GenerationRequest,
)
from src.schema.models import (
    AnswerResponse,
    RetrievedChunk,
)
from src.schema.retrieval_service_models import (
    RetrievalRequest,
)
from src.retrieval.retrieval_service import (
    run_retrieval_service,
)


LOCAL_MODEL = "qwen3:8b"
CASE_COUNT = 3

REPORT_PATH = Path(
    "data/eval/results/"
    "phase12_generation_comparison.json"
)


def _expected_document_ids(example: Any) -> List[str]:
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
        "Canonical example has no expected document identifier."
    )


def _dump_models(items: list[Any]) -> list[dict[str, Any]]:
    return [
        item.model_dump(mode="json")
        for item in items
    ]


def _evidence_to_chunks(
    evidence: list[Any],
) -> list[RetrievedChunk]:
    """
    Reconstruct the minimum project-native RetrievedChunk representation
    required by the existing semantic evaluator.

    Child text remains the authoritative support text.
    """
    chunks: list[RetrievedChunk] = []

    for index, item in enumerate(evidence):
        citation = item.citation_payload

        chunks.append(
            RetrievedChunk(
                chunk_id=item.chunk_id,
                doc_id=item.document_id,
                page_id=f"phase12e-page-{index}",
                file_name=citation.file_name,
                page_number=item.page_start,
                section_title=citation.section_title,
                text=item.text,
                word_count=len(item.text.split()),
                retrieval_method=item.retrieval_method,
            )
        )

    return chunks


def _to_answer_response(
    generated: Any,
) -> AnswerResponse:
    return AnswerResponse(
        run_id=generated.run_id,
        query=generated.query,
        answer_text=generated.answer_text,
        model_used=generated.model_used,
        sources=generated.sources,
        supporting_chunks=_evidence_to_chunks(
            generated.evidence
        ),
        validation_flags=[],
        latency_ms=generated.latency_ms,
    )


def _sources_match_evidence(
    generated: Any,
) -> bool:
    actual = _dump_models(
        generated.sources
    )

    expected = _dump_models(
        [
            item.citation_payload
            for item in generated.evidence
        ]
    )

    return actual == expected


def _has_allowed_inline_citation(
    generated: Any,
) -> bool:
    """
    Diagnostic only.

    This checks whether the answer text contains at least one citation ID
    supplied with the evidence. It does not affect the hard Phase 12
    architectural pass because citation presentation can be changed later.
    """
    answer = generated.answer_text

    allowed_ids = [
        item.citation_payload.citation_id
        for item in generated.evidence
        if getattr(
            item.citation_payload,
            "citation_id",
            None,
        )
    ]

    if not allowed_ids:
        return False

    return any(
        citation_id in answer
        for citation_id in allowed_ids
    )


def _mean(values: list[float]) -> float:
    return round(mean(values), 2) if values else 0.0


def _rate(values: list[bool]) -> float:
    if not values:
        return 0.0

    return round(
        sum(values) / len(values),
        4,
    )


def main() -> None:
    managed_model = (
        config.databricks_generation_model.strip()
    )

    if not managed_model:
        raise RuntimeError(
            "Databricks generation model is not configured."
        )

    if hasattr(config, "search_backend"):
        config.search_backend = "databricks"

    if hasattr(config, "runtime_mode"):
        config.runtime_mode = "databricks"

    canonical = load_databricks_retrieval_examples(
        CANONICAL_PATH,
        CORPUS_MANIFEST_PATH,
    )

    if len(canonical) < CASE_COUNT:
        raise RuntimeError(
            f"Need at least {CASE_COUNT} canonical examples."
        )

    selected = canonical[:CASE_COUNT]

    eval_examples: list[EvalExample] = []

    local_answers: list[AnswerResponse] = []
    managed_answers: list[AnswerResponse] = []

    local_generation_latencies: list[float] = []
    managed_generation_latencies: list[float] = []
    retrieval_latencies: list[float] = []

    local_source_consistency: list[bool] = []
    managed_source_consistency: list[bool] = []

    local_inline_citations: list[bool] = []
    managed_inline_citations: list[bool] = []

    same_evidence_checks: list[bool] = []
    same_citation_checks: list[bool] = []

    managed_prompt_tokens: list[int] = []
    managed_completion_tokens: list[int] = []
    managed_total_tokens: list[int] = []

    retrieval_call_count = 0
    local_generation_count = 0
    managed_generation_count = 0

    original_backend = config.generation_backend

    try:
        for example in selected:
            # ---------------------------------------------------------
            # Retrieval exactly once for this comparison case
            # ---------------------------------------------------------
            retrieval_request = RetrievalRequest(
                query=example.query,
                document_ids=_expected_document_ids(
                    example
                ),
                top_k=10,
                final_k=3,
                include_parent_context=True,
            )

            retrieval_call_count += 1

            retrieval_response = (
                run_retrieval_service(
                    retrieval_request
                )
            )

            evidence = retrieval_response.results

            if not evidence:
                raise RuntimeError(
                    "Controlled comparison retrieval "
                    "returned no evidence."
                )

            retrieval_latencies.append(
                float(
                    retrieval_response.latency_ms
                )
            )

            evidence_snapshot = _dump_models(
                evidence
            )

            citation_snapshot = _dump_models(
                [
                    item.citation_payload
                    for item in evidence
                ]
            )

            # ---------------------------------------------------------
            # Local generation
            # ---------------------------------------------------------
            config.generation_backend = "ollama"

            local_request = GenerationRequest(
                query=example.query,
                evidence=evidence,
                model=LOCAL_MODEL,
            )

            local_generation_count += 1

            local_result = generate_from_evidence(
                local_request
            )

            # ---------------------------------------------------------
            # Managed generation
            # ---------------------------------------------------------
            config.generation_backend = "databricks"

            managed_capture: dict[
                str,
                DatabricksGenerationResult,
            ] = {}

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

            managed_request = GenerationRequest(
                query=example.query,
                evidence=evidence,
                model=managed_model,
            )

            managed_generation_count += 1

            managed_result = generate_from_evidence(
                managed_request,
                _generator=managed_generator,
            )

            metadata = managed_capture.get(
                "result"
            )

            if metadata is None:
                raise RuntimeError(
                    "Managed token metadata was "
                    "not captured."
                )

            if any(
                value is None
                for value in (
                    metadata.prompt_tokens,
                    metadata.completion_tokens,
                    metadata.total_tokens,
                )
            ):
                raise RuntimeError(
                    "Managed token usage metadata "
                    "is incomplete."
                )

            managed_prompt_tokens.append(
                int(metadata.prompt_tokens)
            )

            managed_completion_tokens.append(
                int(metadata.completion_tokens)
            )

            managed_total_tokens.append(
                int(metadata.total_tokens)
            )

            # ---------------------------------------------------------
            # Contract invariants
            # ---------------------------------------------------------
            same_evidence_checks.append(
                evidence_snapshot
                == _dump_models(
                    local_result.evidence
                )
                == _dump_models(
                    managed_result.evidence
                )
            )

            same_citation_checks.append(
                citation_snapshot
                == _dump_models(
                    local_result.sources
                )
                == _dump_models(
                    managed_result.sources
                )
            )

            local_source_consistency.append(
                _sources_match_evidence(
                    local_result
                )
            )

            managed_source_consistency.append(
                _sources_match_evidence(
                    managed_result
                )
            )

            local_inline_citations.append(
                _has_allowed_inline_citation(
                    local_result
                )
            )

            managed_inline_citations.append(
                _has_allowed_inline_citation(
                    managed_result
                )
            )

            local_generation_latencies.append(
                float(local_result.latency_ms)
            )

            managed_generation_latencies.append(
                float(
                    managed_result.latency_ms
                )
            )

            # Existing semantic evaluator needs AnswerResponse.
            local_answers.append(
                _to_answer_response(
                    local_result
                )
            )

            managed_answers.append(
                _to_answer_response(
                    managed_result
                )
            )

            # No gold answer exists in the current EvalExample schema.
            # This object is therefore used for semantic judging only.
            eval_examples.append(
                EvalExample(
                    query=example.query,
                    expect_non_empty_answer=True,
                    expect_citations_valid=False,
                    notes=(
                        "Phase 12E provider "
                        "comparison."
                    ),
                )
            )

    finally:
        config.generation_backend = (
            original_backend
        )

    # -------------------------------------------------------------
    # Quality comparison
    #
    # Use exactly the same judge model for both providers.
    # The judge receives already-created answers, so retrieval and
    # generation are NOT rerun here.
    # -------------------------------------------------------------
    local_semantic = (
        run_semantic_evaluation(
            eval_examples,
            local_answers,
            judge_model=LOCAL_MODEL,
            threshold=0.7,
        )
    )

    managed_semantic = (
        run_semantic_evaluation(
            eval_examples,
            managed_answers,
            judge_model=LOCAL_MODEL,
            threshold=0.7,
        )
    )

    mechanical_pass = all(
        [
            retrieval_call_count
            == CASE_COUNT,
            local_generation_count
            == CASE_COUNT,
            managed_generation_count
            == CASE_COUNT,
            all(same_evidence_checks),
            all(same_citation_checks),
            all(local_source_consistency),
            all(managed_source_consistency),
            all(
                bool(
                    answer.answer_text.strip()
                )
                for answer in local_answers
            ),
            all(
                bool(
                    answer.answer_text.strip()
                )
                for answer in managed_answers
            ),
            len(managed_total_tokens)
            == CASE_COUNT,
        ]
    )

    semantic_reports_valid = (
        local_semantic.total
        == CASE_COUNT
        and managed_semantic.total
        == CASE_COUNT
        and local_semantic.parse_failure_count
        == 0
        and managed_semantic.parse_failure_count
        == 0
    )

    comparison_pass = (
        mechanical_pass
        and semantic_reports_valid
    )

    report = {
        "phase": "12E-2",
        "case_count": CASE_COUNT,

        "architecture": {
            "retrieval_call_count":
                retrieval_call_count,
            "local_generation_count":
                local_generation_count,
            "managed_generation_count":
                managed_generation_count,
            "same_evidence_rate":
                _rate(
                    same_evidence_checks
                ),
            "same_citation_payload_rate":
                _rate(
                    same_citation_checks
                ),
        },

        "local": {
            "model": LOCAL_MODEL,

            "mean_generation_latency_ms":
                _mean(
                    local_generation_latencies
                ),

            "source_payload_consistency_rate":
                _rate(
                    local_source_consistency
                ),

            "inline_allowed_citation_rate":
                _rate(
                    local_inline_citations
                ),

            "mean_groundedness":
                round(
                    local_semantic.mean_groundedness,
                    4,
                ),

            "mean_answer_relevance":
                round(
                    local_semantic.mean_answer_relevance,
                    4,
                ),

            "mean_context_relevance":
                round(
                    local_semantic.mean_context_relevance,
                    4,
                ),

            "mean_completeness":
                round(
                    local_semantic.mean_completeness,
                    4,
                ),

            "semantic_parse_failures":
                local_semantic.parse_failure_count,

            "token_usage_available": False,

            "currency_cost_per_question":
                None,

            "cost_basis":
                "Local hardware cost not metered.",
        },

        "managed": {
            "model": managed_model,

            "mean_generation_latency_ms":
                _mean(
                    managed_generation_latencies
                ),

            "source_payload_consistency_rate":
                _rate(
                    managed_source_consistency
                ),

            "inline_allowed_citation_rate":
                _rate(
                    managed_inline_citations
                ),

            "mean_groundedness":
                round(
                    managed_semantic.mean_groundedness,
                    4,
                ),

            "mean_answer_relevance":
                round(
                    managed_semantic.mean_answer_relevance,
                    4,
                ),

            "mean_context_relevance":
                round(
                    managed_semantic.mean_context_relevance,
                    4,
                ),

            "mean_completeness":
                round(
                    managed_semantic.mean_completeness,
                    4,
                ),

            "semantic_parse_failures":
                managed_semantic.parse_failure_count,

            "mean_prompt_tokens":
                _mean(
                    managed_prompt_tokens
                ),

            "mean_completion_tokens":
                _mean(
                    managed_completion_tokens
                ),

            "mean_total_tokens":
                _mean(
                    managed_total_tokens
                ),

            "total_tokens":
                sum(
                    managed_total_tokens
                ),

            "currency_cost_per_question":
                None,

            "cost_basis": (
                "Free Edition development run: "
                "retain token counts; do not "
                "invent currency cost."
            ),
        },

        "retrieval": {
            "mean_latency_ms":
                _mean(
                    retrieval_latencies
                ),
        },

        "evaluation": {
            "reference_answer_scoring":
                False,

            "quality_interpretation": (
                "Answer relevance and completeness "
                "are semantic quality proxies, not "
                "gold-answer correctness."
            ),

            "judge_model":
                LOCAL_MODEL,

            "judge_limitation": (
                "Local Qwen3-8B judges both "
                "providers. This keeps the judge "
                "constant but may introduce "
                "model-family/self-evaluation bias."
            ),
        },

        "mechanical_pass":
            mechanical_pass,

        "semantic_reports_valid":
            semantic_reports_valid,

        "comparison_pass":
            comparison_pass,
    }

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_PATH.write_text(
        json.dumps(
            report,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    # -------------------------------------------------------------
    # Privacy-safe console evidence
    # -------------------------------------------------------------
    print(
        "PHASE12E2_CASE_COUNT:",
        CASE_COUNT,
    )

    print(
        "PHASE12E2_RETRIEVAL_CALL_COUNT:",
        retrieval_call_count,
    )

    print(
        "PHASE12E2_LOCAL_GENERATION_COUNT:",
        local_generation_count,
    )

    print(
        "PHASE12E2_MANAGED_GENERATION_COUNT:",
        managed_generation_count,
    )

    print(
        "PHASE12E2_SAME_EVIDENCE_RATE:",
        _rate(same_evidence_checks),
    )

    print(
        "PHASE12E2_SAME_CITATION_RATE:",
        _rate(same_citation_checks),
    )

    print(
        "PHASE12E2_LOCAL_SOURCE_CONSISTENCY_RATE:",
        _rate(local_source_consistency),
    )

    print(
        "PHASE12E2_MANAGED_SOURCE_CONSISTENCY_RATE:",
        _rate(managed_source_consistency),
    )

    print(
        "PHASE12E2_LOCAL_INLINE_CITATION_RATE:",
        _rate(local_inline_citations),
    )

    print(
        "PHASE12E2_MANAGED_INLINE_CITATION_RATE:",
        _rate(managed_inline_citations),
    )

    print(
        "PHASE12E2_RETRIEVAL_MEAN_LATENCY_MS:",
        _mean(retrieval_latencies),
    )

    print(
        "PHASE12E2_LOCAL_MEAN_LATENCY_MS:",
        _mean(local_generation_latencies),
    )

    print(
        "PHASE12E2_MANAGED_MEAN_LATENCY_MS:",
        _mean(
            managed_generation_latencies
        ),
    )

    print(
        "PHASE12E2_LOCAL_GROUNDEDNESS:",
        round(
            local_semantic.mean_groundedness,
            4,
        ),
    )

    print(
        "PHASE12E2_MANAGED_GROUNDEDNESS:",
        round(
            managed_semantic.mean_groundedness,
            4,
        ),
    )

    print(
        "PHASE12E2_LOCAL_ANSWER_RELEVANCE:",
        round(
            local_semantic.mean_answer_relevance,
            4,
        ),
    )

    print(
        "PHASE12E2_MANAGED_ANSWER_RELEVANCE:",
        round(
            managed_semantic.mean_answer_relevance,
            4,
        ),
    )

    print(
        "PHASE12E2_LOCAL_COMPLETENESS:",
        round(
            local_semantic.mean_completeness,
            4,
        ),
    )

    print(
        "PHASE12E2_MANAGED_COMPLETENESS:",
        round(
            managed_semantic.mean_completeness,
            4,
        ),
    )

    print(
        "PHASE12E2_LOCAL_JUDGE_PARSE_FAILURES:",
        local_semantic.parse_failure_count,
    )

    print(
        "PHASE12E2_MANAGED_JUDGE_PARSE_FAILURES:",
        managed_semantic.parse_failure_count,
    )

    print(
        "PHASE12E2_LOCAL_TOKEN_USAGE_AVAILABLE:",
        False,
    )

    print(
        "PHASE12E2_MANAGED_TOKEN_USAGE_AVAILABLE:",
        len(managed_total_tokens)
        == CASE_COUNT,
    )

    print(
        "PHASE12E2_MANAGED_MEAN_TOTAL_TOKENS:",
        _mean(managed_total_tokens),
    )

    print(
        "PHASE12E2_MANAGED_TOTAL_TOKENS:",
        sum(managed_total_tokens),
    )

    print(
        "PHASE12E2_REFERENCE_ANSWER_SCORING_AVAILABLE:",
        False,
    )

    print(
        "PHASE12E2_REPORT_WRITTEN:",
        REPORT_PATH.exists(),
    )

    print(
        "PHASE12E2_MECHANICAL_PASS:",
        mechanical_pass,
    )

    print(
        "PHASE12E2_SEMANTIC_REPORTS_VALID:",
        semantic_reports_valid,
    )

    print(
        "PHASE12E2_COMPARISON_PASS:",
        comparison_pass,
    )

    if not comparison_pass:
        raise RuntimeError(
            "Phase 12E-2 controlled comparison "
            "did not pass."
        )


if __name__ == "__main__":
    main()
