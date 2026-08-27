"""
Application package.

Legacy local service exports are resolved lazily so importing the
Databricks serving runtime does not load local indexing dependencies.
"""

from __future__ import annotations

from typing import Any


__all__ = [
    "ServiceError",
    "answer_query",
    "index_document",
]


def __getattr__(
    name: str,
) -> Any:
    if name not in __all__:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )

    from app.service import (
        ServiceError,
        answer_query,
        index_document,
    )

    exports = {
        "ServiceError": ServiceError,
        "answer_query": answer_query,
        "index_document": index_document,
    }

    return exports[name]
