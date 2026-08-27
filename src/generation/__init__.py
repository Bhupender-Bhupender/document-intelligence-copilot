"""
Generation package.

Legacy synthesis exports are resolved lazily so importing Phase 12
evidence-generation modules does not eagerly load the older generation
pipeline.
"""

from __future__ import annotations

from typing import Any


__all__ = [
    "synthesise",
]


def __getattr__(
    name: str,
) -> Any:
    if name != "synthesise":
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )

    from src.generation.answer_engine import (
        synthesise,
    )

    return synthesise
