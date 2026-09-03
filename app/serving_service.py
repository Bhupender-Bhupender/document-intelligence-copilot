"""
Shared application orchestration for Phase 13 serving.

Both FastAPI and Gradio call this module directly.

This layer composes:
    Phase 11 retrieval
        ->
    Phase 12 evidence generation

It contains no AI Search SDK calls, SQL queries, prompt construction,
citation construction, or provider HTTP logic.
"""

from __future__ import annotations

from time import perf_counter
from typing import Callable, Optional

from src.core.config import config
from src.llmops.tracing import (
    GENERATION_SPAN,
    GENERATION_SPAN_TYPE,
    RAG_REQUEST_SPAN,
    RAG_REQUEST_SPAN_TYPE,
    RETRIEVAL_SPAN,
    RETRIEVAL_SPAN_TYPE,
    set_safe_span_attributes,
    start_safe_span,
)
from src.generation.evidence_answer_engine import (
    generate_from_evidence,
)
from src.retrieval.retrieval_service import (
    run_retrieval_service,
)
from src.schema.generation_service_models import (
    GenerationRequest,
    GenerationResponse,
)
from src.schema.retrieval_service_models import (
    RetrievalRequest,
    RetrievalResponse,
)
from src.schema.serving_models import (
    ReadinessResponse,
    ServingAnswerRequest,
    ServingAnswerResponse,
)
from src.utils.logging_utils import get_logger


logger = get_logger(__name__)


RetrievalRunner = Callable[
    [RetrievalRequest],
    RetrievalResponse,
]

GenerationRunner = Callable[
    [GenerationRequest],
    GenerationResponse,
]


class ServingServiceError(Exception):
    """Safe application-level serving failure."""


def _build_retrieval_request(
    request: ServingAnswerRequest,
) -> RetrievalRequest:
    """Recover the canonical Phase 11 request."""
    return RetrievalRequest.model_validate(
        request.model_dump(
            exclude={"model"}
        )
    )


def _resolve_generation_model(
    requested_model: Optional[str],
) -> str:
    """
    Resolve the correct backend-specific model.

    Important:
    Databricks must not accidentally receive the local
    qwen3:8b model tag when no explicit model was supplied.
    """
    if requested_model:
        return requested_model.strip()

    if config.generation_backend == "databricks":
        model = (
            config.databricks_generation_model
            .strip()
        )

        if not model:
            raise ServingServiceError(
                "Managed generation model is not configured."
            )

        return model

    model = config.generation_model.strip()

    if not model:
        raise ServingServiceError(
            "Local generation model is not configured."
        )

    return model


def get_readiness() -> ReadinessResponse:
    """
    Return configuration readiness without making external calls.

    This is intentionally different from liveness:
    - /health proves the process is running.
    - /ready proves required configuration is present.

    External Databricks/Ollama connectivity is validated separately by
    controlled integration tests rather than causing model/search calls
    from the readiness endpoint.
    """

    runtime_mode = str(
        config.runtime_mode
    ).strip()

    search_backend = str(
        config.search_backend
    ).strip()

    generation_backend = str(
        config.generation_backend
    ).strip()

    checks: dict[str, bool] = {
        "runtime_mode_configured":
            bool(runtime_mode),

        "search_backend_configured":
            bool(search_backend),

        "generation_backend_configured":
            bool(generation_backend),
    }

    if search_backend == "databricks":
        checks.update(
            {
                "search_endpoint_configured":
                    bool(
                        config
                        .databricks_ai_search_endpoint_name
                        .strip()
                    ),

                "search_index_configured":
                    bool(
                        config
                        .databricks_ai_search_index_name
                        .strip()
                    ),

                "parent_table_configured":
                    bool(
                        config
                        .databricks_parent_chunks_table
                        .strip()
                    ),

                "sql_warehouse_configured":
                    bool(
                        config
                        .databricks_sql_warehouse_id
                        .strip()
                    ),
            }
        )

    elif search_backend == "azure_search":
        checks.update(
            {
                "azure_search_endpoint_configured":
                    bool(
                        config
                        .azure_search_endpoint
                        .strip()
                    ),

                "azure_search_index_configured":
                    bool(
                        config
                        .azure_search_index_name
                        .strip()
                    ),
            }
        )

    elif search_backend == "local":
        checks[
            "local_search_configured"
        ] = True

    else:
        checks[
            "search_backend_supported"
        ] = False

    if generation_backend == "databricks":
        checks[
            "managed_model_configured"
        ] = bool(
            config
            .databricks_generation_model
            .strip()
        )

    elif generation_backend == "ollama":
        checks.update(
            {
                "local_model_configured":
                    bool(
                        config
                        .generation_model
                        .strip()
                    ),

                "ollama_base_url_configured":
                    bool(
                        config
                        .ollama_base_url
                        .strip()
                    ),
            }
        )

    else:
        checks[
            "generation_backend_supported"
        ] = False

    ready = all(
        checks.values()
    )

    return ReadinessResponse(
        status=(
            "ready"
            if ready
            else "not_ready"
        ),
        runtime_mode=runtime_mode,
        search_backend=search_backend,
        generation_backend=generation_backend,
        checks=checks,
    )



def retrieve_evidence(
    request: RetrievalRequest,
    *,
    _retrieval_runner: Optional[
        RetrievalRunner
    ] = None,
) -> RetrievalResponse:
    """Execute the Phase 11 retrieval service."""
    runner = (
        _retrieval_runner
        or run_retrieval_service
    )

    try:
        return runner(request)

    except ServingServiceError:
        raise

    except Exception as exc:
        logger.warning(
            "serving_retrieval_failed",
            error_type=type(exc).__name__,
        )

        raise ServingServiceError(
            "Retrieval service unavailable."
        ) from exc


def _answer_with_evidence_core(
    request: ServingAnswerRequest,
    *,
    _retrieval_runner: Optional[
        RetrievalRunner
    ] = None,
    _generation_runner: Optional[
        GenerationRunner
    ] = None,
    _tracing_enabled: bool = False,
    _clock: Callable[[], float] = perf_counter,
) -> ServingAnswerResponse:
    """
    Run one end-to-end evidence-grounded answer request.

    Retrieval happens exactly once. Its RetrievalEvidence objects
    are supplied directly to the Phase 12 generator.
    """
    retrieval_runner = (
        _retrieval_runner
        or run_retrieval_service
    )

    generation_runner = (
        _generation_runner
        or generate_from_evidence
    )

    started = _clock()

    try:
        resolved_model = (
            _resolve_generation_model(
                request.model
            )
        )

        retrieval_request = (
            _build_retrieval_request(
                request
            )
        )

        with start_safe_span(
            name=RETRIEVAL_SPAN,
            span_type=RETRIEVAL_SPAN_TYPE,
            attributes={
                "top_k":
                    retrieval_request.top_k,
                "final_k":
                    retrieval_request.final_k,
            },
            enabled=_tracing_enabled,
        ) as retrieval_span:
            retrieval_response = (
                retrieval_runner(
                    retrieval_request
                )
            )

            set_safe_span_attributes(
                retrieval_span,
                {
                    "result_count": len(
                        retrieval_response.results
                    ),
                    "latency_ms":
                        retrieval_response.latency_ms,
                },
            )

        generation_request = (
            GenerationRequest(
                query=request.query,
                evidence=(
                    retrieval_response.results
                ),
                model=resolved_model,
            )
        )

        with start_safe_span(
            name=GENERATION_SPAN,
            span_type=GENERATION_SPAN_TYPE,
            attributes={
                "model": resolved_model,
                "generation_backend":
                    config.generation_backend,
                "evidence_count": len(
                    retrieval_response.results
                ),
            },
            enabled=_tracing_enabled,
        ) as generation_span:
            generation_response = (
                generation_runner(
                    generation_request
                )
            )

            set_safe_span_attributes(
                generation_span,
                {
                    "source_count": len(
                        generation_response.sources
                    ),
                    "evidence_count": len(
                        generation_response.evidence
                    ),
                    "latency_ms":
                        generation_response.latency_ms,
                    "generation_contract_version":
                        generation_response
                        .generation_contract_version,
                },
            )

        total_latency_ms = (
            _clock() - started
        ) * 1000.0

        return ServingAnswerResponse(
            run_id=(
                generation_response.run_id
            ),
            retrieval_query_id=(
                retrieval_response.query_id
            ),
            query=(
                generation_response.query
            ),
            answer_text=(
                generation_response.answer_text
            ),
            model_used=(
                generation_response.model_used
            ),
            generation_backend=(
                generation_response
                .generation_backend
            ),
            sources=(
                generation_response.sources
            ),
            evidence=(
                generation_response.evidence
            ),
            retrieval_latency_ms=(
                retrieval_response.latency_ms
            ),
            generation_latency_ms=(
                generation_response.latency_ms
            ),
            total_latency_ms=(
                total_latency_ms
            ),
            index_version=(
                retrieval_response.index_version
            ),
            retrieval_config_version=(
                retrieval_response
                .retrieval_config_version
            ),
            generation_contract_version=(
                generation_response
                .generation_contract_version
            ),
            applied_filters=(
                retrieval_response
                .applied_filters
            ),
        )

    except ServingServiceError:
        raise

    except Exception as exc:
        logger.warning(
            "serving_answer_failed",
            error_type=type(exc).__name__,
        )

        raise ServingServiceError(
            "Answer service unavailable."
        ) from exc


def answer_with_evidence(
    request: ServingAnswerRequest,
    *,
    _retrieval_runner: Optional[
        RetrievalRunner
    ] = None,
    _generation_runner: Optional[
        GenerationRunner
    ] = None,
    _tracing_enabled: Optional[
        bool
    ] = None,
    _clock: Callable[[], float] = perf_counter,
) -> ServingAnswerResponse:
    """
    Run one evidence-grounded request with optional
    metadata-only MLflow tracing.

    Raw query, answer, evidence, prompt, and document
    content are not intentionally recorded.
    """
    tracing_enabled = (
        config.llmops_tracing_enabled
        if _tracing_enabled is None
        else _tracing_enabled
    )

    with start_safe_span(
        name=RAG_REQUEST_SPAN,
        span_type=RAG_REQUEST_SPAN_TYPE,
        attributes={
            "runtime_mode":
                config.runtime_mode,
            "generation_backend":
                config.generation_backend,
        },
        enabled=tracing_enabled,
    ) as root_span:

        response = (
            _answer_with_evidence_core(
                request,
                _retrieval_runner=(
                    _retrieval_runner
                ),
                _generation_runner=(
                    _generation_runner
                ),
                _tracing_enabled=(
                    tracing_enabled
                ),
                _clock=_clock,
            )
        )

        set_safe_span_attributes(
            root_span,
            {
                "evidence_count": len(
                    response.evidence
                ),
                "citation_count": len(
                    response.sources
                ),
                "retrieval_latency_ms":
                    response.retrieval_latency_ms,
                "generation_latency_ms":
                    response.generation_latency_ms,
                "total_latency_ms":
                    response.total_latency_ms,
                "retrieval_config_version":
                    response
                    .retrieval_config_version,
                "generation_contract_version":
                    response
                    .generation_contract_version,
            },
        )

        return response
