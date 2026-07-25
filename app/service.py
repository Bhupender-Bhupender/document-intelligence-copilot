"""
Service layer for the Document Intelligence Copilot.

This module provides the application boundary over the indexing and answer
pipelines. All public outputs are project-native types: IndexManifest and
AnswerResponse. No LlamaIndex, httpx, or internal storage types cross the
service boundary.

Public API
----------
    index_document(
        file_path: Path,
        *,
        index_dir: Optional[Path] = None,
        embed_model: Optional[Any] = None,
        _indexing_pipeline: Optional[Callable[..., IndexManifest]] = None,
    ) -> IndexManifest

    answer_query(
        query: str,
        *,
        index_dir: Optional[Path] = None,
        retrieval_top_k: int = 10,
        rerank_top_k: int = 5,
        model: Optional[str] = None,
        _answer_pipeline: Optional[Callable[..., AnswerResponse]] = None,
    ) -> AnswerResponse

Callable contracts
------------------
Both _indexing_pipeline and _answer_pipeline receive the same keyword arguments
that would be forwarded to the real pipeline functions. Fake and real paths are
symmetric — the argument list is written once and used for both branches.

    _indexing_pipeline(file_path, index_dir=..., embed_model=...) -> IndexManifest
    _answer_pipeline(query, index_dir=..., retrieval_top_k=..., rerank_top_k=..., model=...) -> AnswerResponse

Error handling
--------------
ServiceError is raised for:
- Validation failures (file not found, empty query) — clear service-level message.
- Pipeline failures (any Exception from the underlying pipeline) — chained via
  `raise ServiceError(...) from exc` so __cause__ is always available.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional

from src.indexing.index_builder import IndexManifest
from src.schema.models import AnswerResponse
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class ServiceError(Exception):
    """Raised by the service layer when an operation cannot be completed."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def index_document(
    file_path: Path,
    *,
    index_dir: Optional[Path] = None,
    embed_model: Optional[Any] = None,
    _indexing_pipeline: Optional[Callable[..., IndexManifest]] = None,
) -> IndexManifest:
    """
    Index a single document file and return build statistics.

    Validates that the file exists, then delegates to run_indexing_pipeline
    (or the injected callable when provided for testing).

    Parameters
    ----------
    file_path:
        Path to the source document (.txt, .md, .pdf, .docx).
    index_dir:
        Directory to persist index files. Defaults to config.index_dir
        when None. Always override in tests by passing a tmp_path-based
        directory to keep test output isolated.
    embed_model:
        LlamaIndex BaseEmbedding instance for child chunk embedding.
        If None, the underlying pipeline calls get_embed_model(), which
        loads the HuggingFace model from config.embedding_model.
        Always pass an explicit mock in tests to avoid model downloads.
    _indexing_pipeline:
        Test injection. When provided, replaces run_indexing_pipeline.
        Callable contract (both fake and real paths receive the same args):
            _indexing_pipeline(file_path, index_dir=..., embed_model=...) -> IndexManifest

    Returns
    -------
    IndexManifest
        Build statistics and provenance metadata from the indexing run.

    Raises
    ------
    ServiceError
        If the file does not exist, or if the indexing pipeline raises.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise ServiceError(f"File not found: {file_path}")

    callable_ = _indexing_pipeline
    if callable_ is None:
        from src.indexing.indexing_pipeline import run_indexing_pipeline
        callable_ = run_indexing_pipeline

    try:
        return callable_(file_path, index_dir=index_dir, embed_model=embed_model)
    except ServiceError:
        raise
    except Exception as exc:
        logger.warning("index_document failed", file_path=str(file_path), error=str(exc))
        raise ServiceError(f"Indexing failed: {exc}") from exc


def answer_query(
    query: str,
    *,
    index_dir: Optional[Path] = None,
    retrieval_top_k: int = 10,
    rerank_top_k: int = 5,
    model: Optional[str] = None,
    _answer_pipeline: Optional[Callable[..., AnswerResponse]] = None,
) -> AnswerResponse:
    """
    Answer a natural language query and return a grounded response.

    Validates that the query is non-empty, then delegates to run_pipeline
    (or the injected callable when provided for testing).

    Parameters
    ----------
    query:
        The user's natural language question. Must be non-empty after strip().
    index_dir:
        Root index directory for retrieval and parent lookup.
        Defaults to config.index_dir when None.
    retrieval_top_k:
        Maximum candidates returned by hybrid retrieval before reranking.
    rerank_top_k:
        Maximum results returned by the reranker.
    model:
        Ollama model tag override for generation. Defaults to
        config.generation_model when None.
    _answer_pipeline:
        Test injection. When provided, replaces run_pipeline.
        Callable contract (both fake and real paths receive the same args):
            _answer_pipeline(
                query,
                index_dir=...,
                retrieval_top_k=...,
                rerank_top_k=...,
                model=...,
            ) -> AnswerResponse

    Returns
    -------
    AnswerResponse
        Grounded answer with supporting chunks, citations, and validation flags.

    Raises
    ------
    ServiceError
        If the query is empty or whitespace-only, or if the answer pipeline raises.
    """
    if not query.strip():
        raise ServiceError("Query must not be empty.")

    callable_ = _answer_pipeline
    if callable_ is None:
        from src.generation.answer_pipeline import run_pipeline
        callable_ = run_pipeline

    try:
        return callable_(
            query,
            index_dir=index_dir,
            retrieval_top_k=retrieval_top_k,
            rerank_top_k=rerank_top_k,
            model=model,
        )
    except ServiceError:
        raise
    except Exception as exc:
        logger.warning("answer_query failed", query=query[:80], error=str(exc))
        raise ServiceError(f"Query failed: {exc}") from exc
