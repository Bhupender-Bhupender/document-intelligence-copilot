"""
Indexing package: LlamaIndex-based index build and persistence layer.

Public API:
    from src.indexing.index_builder import build_indexes, load_child_index, load_parent_store, IndexManifest
    from src.indexing.embed_config import get_embed_model, configure_settings
    from src.indexing.indexing_pipeline import run_indexing_pipeline
"""
from src.indexing.embed_config import configure_settings, get_embed_model
from src.indexing.index_builder import (
    IndexManifest,
    build_indexes,
    load_child_index,
    load_parent_store,
)
from src.indexing.indexing_pipeline import run_indexing_pipeline

__all__ = [
    "IndexManifest",
    "build_indexes",
    "configure_settings",
    "get_embed_model",
    "load_child_index",
    "load_parent_store",
    "run_indexing_pipeline",
]
