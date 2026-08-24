"""
Retrieval package public API.

Retrieval implementations are imported lazily so cloud backends do not
implicitly load local-only dependencies such as LlamaIndex or rank_bm25.

This preserves the historical package-level API while keeping backend
selection import-light.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


__all__ = [
    "lookup_parents",
    "retrieve_children",
    "retrieve_children_bm25",
    "retrieve_hybrid",
    "route_query",
]


_LAZY_IMPORTS = {
    "lookup_parents": (
        "src.retrieval.vector_retriever",
        "lookup_parents",
    ),
    "retrieve_children": (
        "src.retrieval.vector_retriever",
        "retrieve_children",
    ),
    "retrieve_children_bm25": (
        "src.retrieval.bm25_retriever",
        "retrieve_children_bm25",
    ),
    "retrieve_hybrid": (
        "src.retrieval.hybrid_retriever",
        "retrieve_hybrid",
    ),
    "route_query": (
        "src.retrieval.query_router",
        "route_query",
    ),
}


def __getattr__(name: str) -> Any:
    """
    Resolve retrieval exports only when they are actually requested.

    This prevents importing local retrieval dependencies when using
    Databricks or another cloud retrieval backend.
    """
    try:
        module_name, attribute_name = _LAZY_IMPORTS[name]
    except KeyError as exc:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        ) from exc

    module = import_module(module_name)
    value = getattr(module, attribute_name)

    # Cache the resolved object so subsequent access behaves like a
    # normal module-level import.
    globals()[name] = value

    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))