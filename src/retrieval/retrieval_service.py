from __future__ import annotations

from src.observability.emitter import (
    emit_operational_event_safely,
)

import time
import uuid

from collections.abc import Callable
from typing import List, Optional

from src.retrieval.evidence_builder import (
    build_retrieval_evidence,
)

from src.core.config import config
from src.llmops.tracing import (
    EVIDENCE_BUILD_SPAN,
    EVIDENCE_BUILD_SPAN_TYPE,
    set_safe_span_attributes,
    start_safe_span,
)
from src.schema.models import (
    DocumentChunk,
    RetrievedChunk,
)
from src.schema.retrieval_service_models import (
    RetrievalRequest,
    RetrievalResponse,
)


class UnsupportedRetrievalFilterError(
    ValueError
):
    """
    Requested filter cannot currently be
    enforced by the indexed corpus.
    """


class RetrievalFilterViolationError(
    RuntimeError
):
    """
    Backend returned evidence outside an
    enforced document allow-list.
    """


RetrieveFn = Callable[
    [
        str,
        int,
        Optional[dict],
    ],
    List[RetrievedChunk],
]

ParentLookupFn = Callable[
    [List[RetrievedChunk]],
    List[Optional[DocumentChunk]],
]


def _default_retrieve(
    query: str,
    top_k: int,
    filters: Optional[dict],
) -> List[RetrievedChunk]:

    from src.retrieval.retrieval_gateway import (
        route_retrieve,
    )

    return route_retrieve(
        query=query,
        top_k=top_k,
        filters=filters,
    )


def _default_parent_lookup(
    retrieved: List[RetrievedChunk],
) -> List[Optional[DocumentChunk]]:

    from src.retrieval.retrieval_gateway import (
        route_lookup_parents,
    )

    return route_lookup_parents(
        retrieved
    )


def _build_backend_filters(
    request: RetrievalRequest,
) -> tuple[
    Optional[dict],
    List[str],
]:
    """
    Translate stable service filters into
    backend filters.

    Unsupported security-sensitive filters
    fail closed. They are never ignored.
    """

    unsupported = []

    if request.tenant_id is not None:
        unsupported.append(
            "tenant_id"
        )

    if request.allowed_groups:
        unsupported.append(
            "allowed_groups"
        )

    if request.date_range is not None:
        unsupported.append(
            "date_range"
        )

    if unsupported:
        raise UnsupportedRetrievalFilterError(
            "Unsupported retrieval filters: "
            + ", ".join(
                unsupported
            )
        )

    filters: dict = {}
    applied_filters: List[str] = []

    if request.document_ids:

        # Standard Databricks AI Search
        # filter syntax:
        #
        # {"document_id": ["id1", "id2"]}
        #
        # matches any listed exact value.
        filters[
            "document_id"
        ] = list(
            request.document_ids
        )

        applied_filters.append(
            "document_ids"
        )

    return (
        filters or None,
        applied_filters,
    )


def _enforce_document_allowlist(
    retrieved: List[RetrievedChunk],
    document_ids: List[str],
) -> None:
    """
    Defense-in-depth check.

    Even though document filters are pushed
    into AI Search, never allow a backend bug
    or configuration problem to leak evidence
    outside the requested allow-list.
    """

    if not document_ids:
        return

    allowed = set(
        document_ids
    )

    if any(
        chunk.doc_id not in allowed
        for chunk in retrieved
    ):
        raise RetrievalFilterViolationError(
            "Retrieved evidence violated "
            "the document allow-list."
        )


def _run_retrieval_service_core(
    request: RetrievalRequest,
    *,
    _retrieve: Optional[
        RetrieveFn
    ] = None,
    _parent_lookup: Optional[
        ParentLookupFn
    ] = None,
    _clock: Callable[[], float] = (
        time.perf_counter
    ),
) -> RetrievalResponse:
    """
    Execute Phase 11 retrieval.

    Flow:
      request validation
      -> backend filter construction
      -> ranked child retrieval
      -> allow-list enforcement
      -> final_k selection
      -> optional parent lookup
      -> citation-ready evidence
      -> stable response
    """

    retrieve = (
        _retrieve
        or _default_retrieve
    )

    parent_lookup = (
        _parent_lookup
        or _default_parent_lookup
    )

    backend_filters, applied_filters = (
        _build_backend_filters(
            request
        )
    )

    started = _clock()

    retrieved = retrieve(
        request.query,
        request.top_k,
        backend_filters,
    )

    retrieved = list(
        retrieved
    )

    _enforce_document_allowlist(
        retrieved,
        request.document_ids,
    )

    selected = retrieved[
        :request.final_k
    ]

    parents = None

    if (
        request.include_parent_context
        and selected
    ):
        parents = parent_lookup(
            selected
        )

        if len(parents) != len(selected):
            raise RuntimeError(
                "Parent lookup result count "
                "does not match selected "
                "retrieval results."
            )

    with start_safe_span(
        name=EVIDENCE_BUILD_SPAN,
        span_type=EVIDENCE_BUILD_SPAN_TYPE,
        attributes={
            "selected_count": len(selected),
            "final_k": request.final_k,
            "include_parent_context":
                request.include_parent_context,
        },
        enabled=(
            config.llmops_tracing_enabled
        ),
    ) as evidence_span:
        evidence = (
            build_retrieval_evidence(
                selected,
                parents=parents,
                final_k=request.final_k,
                include_parent_context=(
                    request
                    .include_parent_context
                ),
            )
        )

        set_safe_span_attributes(
            evidence_span,
            {
                "evidence_count": len(
                    evidence
                ),
                "citation_count": len(
                    evidence
                ),
                "parent_context_count": sum(
                    1
                    for item in evidence
                    if item.parent_text
                ),
            },
        )

    latency_ms = max(
        0.0,
        (
            _clock()
            - started
        )
        * 1000,
    )

    return RetrievalResponse(
        query_id=uuid.uuid4().hex,
        results=evidence,
        latency_ms=latency_ms,

        # We do not currently maintain an
        # explicit semantic index version.
        # Do not invent one.
        index_version=None,

        applied_filters=(
            applied_filters
        ),
    )


def run_retrieval_service(
    request: RetrievalRequest,
    *,
    _retrieve: Optional[
        RetrieveFn
    ] = None,
    _parent_lookup: Optional[
        ParentLookupFn
    ] = None,
    _clock: Callable[[], float] = (
        time.perf_counter
    ),
    _event_emitter=None,
    _event_clock: Callable[[], float] = (
        time.perf_counter
    ),
) -> RetrievalResponse:
    """
    Execute retrieval with privacy-safe operational
    telemetry around the existing Phase 11 core.
    """
    event_started = (
        _event_clock()
    )

    try:
        response = (
            _run_retrieval_service_core(
                request,
                _retrieve=_retrieve,
                _parent_lookup=(
                    _parent_lookup
                ),
                _clock=_clock,
            )
        )

    except Exception as exc:
        latency_ms = max(
            0.0,
            (
                _event_clock()
                - event_started
            )
            * 1000.0,
        )

        emit_operational_event_safely(
            {
                "event_name":
                    "retrieval.request.failed",

                "component":
                    "retrieval",

                "operation":
                    "run_retrieval_service",

                "status":
                    "error",

                "runtime_mode":
                    config.runtime_mode,

                "backend":
                    config.search_backend,

                "latency_ms":
                    latency_ms,

                "error_type":
                    type(exc).__name__,
            },
            _emitter=_event_emitter,
        )

        raise


    parent_context_count = sum(
        1
        for item
        in response.results
        if getattr(
            item,
            "parent_text",
            None,
        )
    )


    emit_operational_event_safely(
        {
            "event_name":
                "retrieval.request.completed",

            "component":
                "retrieval",

            "operation":
                "run_retrieval_service",

            "status":
                "success",

            "runtime_mode":
                config.runtime_mode,

            "backend":
                config.search_backend,

            "latency_ms":
                response.latency_ms,

            "result_count":
                len(
                    response.results
                ),

            "parent_context_count":
                parent_context_count,

            "retrieval_config_version":
                response
                .retrieval_config_version,
        },
        _emitter=_event_emitter,
    )


    return response
