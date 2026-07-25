"""
Embedding configuration helper.

This is the single module in the project that knows about HuggingFaceEmbedding.
All other indexing modules receive embed_model as an injected parameter and
remain embedding-agnostic.

Public API
----------
    get_embed_model(model_name: str | None = None) -> BaseEmbedding
    configure_settings(embed_model: BaseEmbedding | None = None) -> BaseEmbedding
"""
from __future__ import annotations

from llama_index.core.embeddings import BaseEmbedding

from src.core.config import config


def get_embed_model(model_name: str | None = None) -> BaseEmbedding:
    """
    Return a HuggingFaceEmbedding instance.

    Uses model_name if provided, otherwise falls back to config.embedding_model
    (default: ``Qwen/Qwen3-Embedding-0.6B``).

    The HuggingFace embedding import is lazy so this module can be safely
    imported even when llama-index-embeddings-huggingface is not installed —
    only *calling* this function will fail in that case.

    Args:
        model_name: HuggingFace model ID. Defaults to config.embedding_model.

    Returns:
        Configured HuggingFaceEmbedding instance.

    Raises:
        ImportError: if llama-index-embeddings-huggingface is not installed.
    """
    from llama_index.embeddings.huggingface import HuggingFaceEmbedding  # lazy

    return HuggingFaceEmbedding(model_name=model_name or config.embedding_model)


def configure_settings(embed_model: BaseEmbedding | None = None) -> BaseEmbedding:
    """
    Set LlamaIndex global Settings.embed_model and return the model.

    If embed_model is None, calls get_embed_model() using config.embedding_model.
    Pass an explicit embed_model to avoid triggering a HuggingFace model load.

    This helper is intended for startup / CLI use where a single global model
    is appropriate. Pipeline code should prefer explicit embed_model injection
    into run_indexing_pipeline() rather than relying on this global mutation.

    Args:
        embed_model: Pre-built embed model, or None to create from config.

    Returns:
        The embed model that was assigned to Settings.embed_model.
    """
    from llama_index.core import Settings

    model = embed_model if embed_model is not None else get_embed_model()
    Settings.embed_model = model
    return model
