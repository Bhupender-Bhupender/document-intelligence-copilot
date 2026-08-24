"""
Application configuration for the Document Intelligence Copilot.

All pipeline parameters are centralised here. Values are loaded from
environment variables or a .env file at the project root if present.
Safe defaults are provided so the system runs without any .env file
during development.

Usage:
    from src.core.config import config

    print(config.chunk_size_words)
    print(config.embedding_model)
"""
from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root resolved relative to this file's location:
#   src/core/config.py â†’ parent â†’ src/core â†’ parent â†’ src â†’ parent â†’ project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class AppConfig(BaseSettings):
    """
    Central typed configuration for the Document Intelligence Copilot.

    Override any field via an environment variable (e.g. CHUNK_SIZE_WORDS=100)
    or by placing a .env file at the project root.
    """

    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ------------------------------------------------------------------ #
    # Runtime environment
    # ------------------------------------------------------------------ #

    runtime_mode: Literal["local", "databricks"] = Field(
        default="local",
        description="Execution environment for the application.",
    )

    # ------------------------------------------------------------------ #
    # Path settings                                                        #
    # ------------------------------------------------------------------ #

    raw_input_dir: Path = Field(
        default=_PROJECT_ROOT / "docs" / "sample_docs",
        description="Default directory for source documents.",
    )
    processed_dir: Path = Field(
        default=_PROJECT_ROOT / "data" / "processed",
        description="Output directory for intermediate pipeline artifacts.",
    )
    index_dir: Path = Field(
        default=_PROJECT_ROOT / "data" / "index",
        description="Output directory for LlamaIndex stores (Phase 4+).",
    )
    manifest_dir: Path = Field(
        default=_PROJECT_ROOT / "data" / "processed" / "manifests",
        description="Output directory for pipeline run manifests.",
    )
    # Legacy ChromaDB path â€” kept for non-destructive reference only.
    # The new pipeline does not write to this location.
    legacy_chroma_dir: Path = Field(
        default=_PROJECT_ROOT / "data" / "chroma_db",
        description="Legacy ChromaDB location. Not used by the new pipeline.",
    )

    # ------------------------------------------------------------------ #
    # Model name settings                                                  #
    # ------------------------------------------------------------------ #
    databricks_embedding_model: str = Field(
    default="databricks-qwen3-embedding-0-6b",
    description=(
        "Databricks hosted embedding model used "
        "for AI Search and managed vectorization."
    ),
    )
    embedding_model: str = Field(
        default="Qwen/Qwen3-Embedding-0.6B",
        description="HuggingFace model ID for embeddings (Phase 4+).",
    )
    reranker_model: str = Field(
        default="Qwen/Qwen3-Reranker-0.6B",
        description="HuggingFace model ID for reranking (Phase 5+).",
    )
    generation_backend: Literal["ollama", "databricks"] = Field(
        default="ollama",
        description=(
            "Generation provider. 'ollama' is used locally; "
            "'databricks' is implemented during the serving phase."
        ),
    )

    generation_model: str = Field(
        default="qwen3:8b",
        description="Ollama model tag for answer generation (Phase 6+).",
    )
    ollama_base_url: str = Field(
        default="http://localhost:11434",
        description="Base URL for the local Ollama daemon (Phase 6+).",
    )

    # ------------------------------------------------------------------ #
    # Chunking parameters                                                  #
    # ------------------------------------------------------------------ #

    chunk_size_words: int = Field(
        default=80,
        description="Target word count per flat chunk.",
    )
    chunk_overlap_words: int = Field(
        default=20,
        description="Word overlap between consecutive flat chunks.",
    )

    # Hierarchical chunking â€” parent/child windows for hybrid RAG.
    # Parents provide broad synthesis context; children are the retrieval units.
    parent_chunk_size_words: int = Field(
        default=400,
        description="Target word count per parent chunk.",
    )
    child_chunk_size_words: int = Field(
        default=150,
        description="Target word count per child chunk (retrieval unit).",
    )
    child_chunk_overlap_words: int = Field(
        default=30,
        description="Word overlap between consecutive child chunks within a parent.",
    )

    # ------------------------------------------------------------------ #
    # Extraction quality thresholds                                        #
    # ------------------------------------------------------------------ #

    extraction_empty_threshold: int = Field(
        default=0,
        description=(
            "Pages with word_count at or below this value are classified "
            "'empty' and skipped by the chunker. Triggers OCR in Phase 2."
        ),
    )
    extraction_weak_threshold: int = Field(
        default=20,
        description=(
            "Pages with word_count below this value are classified 'weak'. "
            "They are chunked as-is but flagged for OCR review in Phase 2."
        ),
    )

    # ------------------------------------------------------------------ #
    # Retrieval parameters                                                 #
    # ------------------------------------------------------------------ #

    retrieval_top_k: int = Field(
        default=5,
        description="Default number of chunks to retrieve per query.",
    )

    # ------------------------------------------------------------------ #
    # Logging                                                              #
    # ------------------------------------------------------------------ #

    log_level: str = Field(
        default="INFO",
        description="Logging level: DEBUG, INFO, WARNING, ERROR.",
    )

    # ------------------------------------------------------------------ #
    # Storage backend                                                      #
    # ------------------------------------------------------------------ #

    storage_backend: Literal["local", "azure_blob"] = Field(
        default="local",
        description=(
            "Storage backend for artifact and manifest writes. "
            "'local' writes to disk; 'azure_blob' writes to Azure Blob Storage."
        ),
    )
    azure_storage_account_url: str = Field(
        default="",
        description=(
            "Azure Storage account URL "
            "(https://<account>.blob.core.windows.net). "
            "Required when storage_backend='azure_blob'."
        ),
    )
    azure_storage_container_artifacts: str = Field(
        default="documents-processed",
        description="Blob container name for JSONL artifacts.",
    )
    azure_storage_container_manifests: str = Field(
        default="documents-processed",
        description="Blob container name for run manifests.",
    )

    # ------------------------------------------------------------------ #
    # OCR backend                                                          #
    # ------------------------------------------------------------------ #

    ocr_backend: Literal["local", "azure_di"] = Field(
        default="local",
        description=(
            "OCR backend for empty-page recovery. "
            "'local' uses Docling/RapidOCR; "
            "'azure_di' uses Azure AI Document Intelligence prebuilt-read."
        ),
    )
    azure_di_endpoint: str = Field(
        default="",
        description=(
            "Azure AI Document Intelligence resource endpoint URL "
            "(https://<resource>.cognitiveservices.azure.com/). "
            "Required when ocr_backend='azure_di'."
        ),
    )

    # ------------------------------------------------------------------ #
    # Search backend                                                        #
    # ------------------------------------------------------------------ #

    search_backend: Literal["local", "azure_search", "databricks"] = Field(
        default="local",
        description=(
            "Search backend for indexing/retrieval. "
            "'local' uses LlamaIndex/BM25 hybrid; "
            "'azure_search' uses Azure AI Search; "
            "'databricks' uses Databricks AI Search for retrieval "
            "with Delta Sync managing the search index."
        ),
    )
    azure_search_endpoint: str = Field(
        default="",
        description=(
            "Azure AI Search service endpoint URL "
            "(https://<service>.search.windows.net). "
            "Required when search_backend='azure_search'."
        ),
    )
    azure_search_index_name: str = Field(
        default="document-intelligence",
        description=(
            "Azure AI Search index name. "
            "Used when search_backend='azure_search'."
        ),
    )

    databricks_ai_search_endpoint_name: str = Field(
        default="",
        description=(
            "Databricks AI Search serving endpoint name. "
            "Used when search_backend='databricks'."
        ),
    )

    databricks_ai_search_index_name: str = Field(
        default="",
        description=(
            "Fully qualified Unity Catalog Databricks AI Search index name. "
            "Used when search_backend='databricks'."
        ),
    )

    databricks_parent_chunks_table: str = Field(
    default="",
    description=(
        "Fully qualified Gold parent-chunks Delta table. "
        "Used for deterministic parent-context lookup when "
        "search_backend='databricks'."
        ),
    )
    databricks_sql_warehouse_id: str = Field(
        default="",
        description=(
            "Databricks SQL warehouse ID used for remote "
            "parent-chunk lookup when no active Spark session is available."
        ),
    )
# Module-level singleton â€” import this instance in other modules.
# Constructed once at import time using environment / .env file values.
config = AppConfig()
