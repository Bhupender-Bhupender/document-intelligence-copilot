"""
Shared text processing utilities.

Used by ingestion readers and the chunking layer. Kept simple and
dependency-free so any module can import without side effects.
"""
from __future__ import annotations

import re
from typing import Literal


# Type alias for extraction quality classification.
ExtractionStatus = Literal["ok", "weak", "empty"]

# Default word-count thresholds for extraction quality.
# These match the defaults in AppConfig and can be overridden per call.
_EMPTY_THRESHOLD = 0   # 0 words → empty
_WEAK_THRESHOLD = 20   # < 20 words → weak


def clean_text(text: str) -> str:
    """
    Remove non-breaking spaces and collapse runs of whitespace.

    Safe to call on empty or None-ish input. Returns a stripped string.
    Used at raw-extraction time before any normalisation pass.
    """
    if not text:
        return ""
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_text(text: str) -> str:
    """
    Normalise text for chunking and embedding.

    Functionally identical to clean_text in Phase 1. The two names are
    kept separate because they represent different pipeline concerns:
      - clean_text  → applied during raw extraction (readers)
      - normalize_text → applied during chunk preparation (chunker)

    Later phases may diverge these (e.g. unicode normalisation, lowercasing
    for BM25 index) without changing every call site.
    """
    return clean_text(text)


def classify_extraction_status(
    text: str,
    empty_threshold: int = _EMPTY_THRESHOLD,
    weak_threshold: int = _WEAK_THRESHOLD,
) -> ExtractionStatus:
    """
    Classify the quality of extracted page text for downstream routing.

    This is the primary signal used by Phase 2 to decide whether OCR
    is needed for a given page.

    Args:
        text: The normalised text extracted from a page.
        empty_threshold: Word count at or below which the page is "empty".
        weak_threshold:  Word count below which the page is "weak".

    Returns:
        "empty"  — no usable words (triggers OCR in Phase 2)
        "weak"   — very few words (may benefit from OCR in Phase 2)
        "ok"     — sufficient text for chunking and retrieval
    """
    word_count = len(text.split()) if text and text.strip() else 0
    if word_count <= empty_threshold:
        return "empty"
    if word_count < weak_threshold:
        return "weak"
    return "ok"
