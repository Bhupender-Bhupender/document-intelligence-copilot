"""
Thin FastAPI boundary for the Document Intelligence Copilot.

HTTP responsibilities only:
- request validation
- response serialization
- HTTP status mapping
- liveness/readiness endpoints

Business orchestration lives in app.serving_service.
"""

from __future__ import annotations

from typing import Callable, Optional

from fastapi import (
    FastAPI,
    HTTPException,
    Response,
)

from app.serving_service import (
    ServingServiceError,
    answer_with_evidence,
    get_readiness,
    retrieve_evidence,
)
from src.schema.retrieval_service_models import (
    RetrievalRequest,
    RetrievalResponse,
)
from src.schema.serving_models import (
    HealthResponse,
    ReadinessResponse,
    ServingAnswerRequest,
    ServingAnswerResponse,
)
from src.utils.logging_utils import get_logger


logger = get_logger(__name__)


RetrievalService = Callable[
    [RetrievalRequest],
    RetrievalResponse,
]

AnswerService = Callable[
    [ServingAnswerRequest],
    ServingAnswerResponse,
]

ReadinessService = Callable[
    [],
    ReadinessResponse,
]


def create_app(
    *,
    _retrieval_service: Optional[
        RetrievalService
    ] = None,
    _answer_service: Optional[
        AnswerService
    ] = None,
    _readiness_service: Optional[
        ReadinessService
    ] = None,
) -> FastAPI:
    """Build the HTTP application."""

    retrieval_service = (
        _retrieval_service
        or retrieve_evidence
    )

    answer_service = (
        _answer_service
        or answer_with_evidence
    )

    readiness_service = (
        _readiness_service
        or get_readiness
    )

    api = FastAPI(
        title="Document Intelligence API",
        version="1.0.0",
    )

    @api.get(
        "/api/v1/health",
        response_model=HealthResponse,
        tags=["system"],
    )
    def health() -> HealthResponse:
        """
        Process liveness.

        A 200 response means only that the application process is alive.
        """
        return HealthResponse()

    @api.get(
        "/api/v1/ready",
        response_model=ReadinessResponse,
        tags=["system"],
    )
    def ready(
        response: Response,
    ) -> ReadinessResponse:
        """
        Configuration readiness.

        No provider or model calls are made.
        """
        try:
            result = readiness_service()

            if result.status != "ready":
                response.status_code = 503

            return result

        except Exception as exc:
            logger.warning(
                "api_readiness_failed",
                error_type=type(exc).__name__,
            )

            raise HTTPException(
                status_code=503,
                detail=(
                    "Readiness check unavailable."
                ),
            ) from exc

    @api.post(
        "/api/v1/retrieve",
        response_model=RetrievalResponse,
        tags=["retrieval"],
    )
    def retrieve(
        request: RetrievalRequest,
    ) -> RetrievalResponse:
        try:
            return retrieval_service(
                request
            )

        except ServingServiceError as exc:
            raise HTTPException(
                status_code=503,
                detail=str(exc),
            ) from exc

        except Exception as exc:
            logger.warning(
                "api_retrieve_failed",
                error_type=type(exc).__name__,
            )

            raise HTTPException(
                status_code=503,
                detail=(
                    "Retrieval service unavailable."
                ),
            ) from exc

    @api.post(
        "/api/v1/answer",
        response_model=ServingAnswerResponse,
        tags=["generation"],
    )
    def answer(
        request: ServingAnswerRequest,
    ) -> ServingAnswerResponse:
        try:
            return answer_service(
                request
            )

        except ServingServiceError as exc:
            raise HTTPException(
                status_code=503,
                detail=str(exc),
            ) from exc

        except Exception as exc:
            logger.warning(
                "api_answer_failed",
                error_type=type(exc).__name__,
            )

            raise HTTPException(
                status_code=503,
                detail=(
                    "Answer service unavailable."
                ),
            ) from exc

    return api


app = create_app()
