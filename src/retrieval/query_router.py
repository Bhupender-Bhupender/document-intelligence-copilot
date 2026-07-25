"""
Query router: deterministic, heuristic-only classification of user queries
into routing plans that drive retrieval and synthesis parameters.

Public API
----------
    route_query(query: str) -> RoutingPlan

Design
------
No LLM is invoked. Classification uses only stdlib string operations:
lowercasing, splitting, and keyword-set membership tests. The same input
always produces the same output (pure function, no side effects).

Query types and their retrieval parameters
------------------------------------------
    exact_lookup              retrieval_top_k= 5  rerank_top_k= 3  parent=False
    focused_question          retrieval_top_k=10  rerank_top_k= 5  parent=False
    broad_summary             retrieval_top_k=15  rerank_top_k= 8  parent=True
    comparison_or_multi_aspect retrieval_top_k=20 rerank_top_k=10  parent=True
    insufficient_or_ambiguous retrieval_top_k= 5  rerank_top_k= 3  parent=True

Classification priority (first match wins)
------------------------------------------
    1. insufficient_or_ambiguous — word count < 3
    2. comparison_or_multi_aspect — comparison keyword present
    3. broad_summary — summary keyword present OR word count > 15
    4. exact_lookup — lookup-style opener OR short (≤ 6 words) + ends with "?"
    5. focused_question — default

The ``notes`` field of the returned RoutingPlan carries plain-English strings
that explain why a particular route was chosen, making the decision
self-documenting and trivially loggable.
"""
from __future__ import annotations

from src.schema.models import RoutingPlan

# --------------------------------------------------------------------------- #
# Keyword sets                                                                 #
# --------------------------------------------------------------------------- #

_COMPARISON_KEYWORDS: frozenset[str] = frozenset(
    {
        "vs",
        "versus",
        "compare",
        "comparison",
        "contrast",
        "difference",
        "differences",
        "differ",
        "differs",
        "between",
        "pros and cons",
        "which is better",
        "which are better",
    }
)

_SUMMARY_KEYWORDS: frozenset[str] = frozenset(
    {
        "summarize",
        "summarise",
        "summary",
        "overview",
        "explain",
        "describe",
        "tell me about",
        "what is",
        "how does",
        "how do",
    }
)

_EXACT_LOOKUP_OPENERS: frozenset[str] = frozenset(
    {
        "when",
        "when did",
        "when was",
        "when is",
        "who",
        "who is",
        "who was",
        "who are",
        "where",
        "where is",
        "where was",
        "where are",
        "how many",
        "how much",
        "what time",
        "which year",
        "which date",
    }
)

# --------------------------------------------------------------------------- #
# Parameter table                                                              #
# --------------------------------------------------------------------------- #

_PARAMS: dict[str, dict] = {
    "exact_lookup": {
        "retrieval_top_k": 5,
        "rerank_top_k": 3,
        "emphasize_parent_context": False,
    },
    "focused_question": {
        "retrieval_top_k": 10,
        "rerank_top_k": 5,
        "emphasize_parent_context": False,
    },
    "broad_summary": {
        "retrieval_top_k": 15,
        "rerank_top_k": 8,
        "emphasize_parent_context": True,
    },
    "comparison_or_multi_aspect": {
        "retrieval_top_k": 20,
        "rerank_top_k": 10,
        "emphasize_parent_context": True,
    },
    "insufficient_or_ambiguous": {
        "retrieval_top_k": 5,
        "rerank_top_k": 3,
        "emphasize_parent_context": True,
    },
}

# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #


def _normalise(query: str) -> str:
    """Return lowercased, stripped query."""
    return query.strip().lower()


def _word_count(normalised: str) -> int:
    return len(normalised.split())


def _contains_comparison(normalised: str) -> tuple[bool, str]:
    """Return (found, matched_keyword)."""
    for kw in _COMPARISON_KEYWORDS:
        if kw in normalised:
            return True, kw
    return False, ""


def _contains_summary(normalised: str) -> tuple[bool, str]:
    """Return (found, matched_keyword)."""
    for kw in _SUMMARY_KEYWORDS:
        if normalised.startswith(kw) or f" {kw} " in normalised or normalised == kw:
            return True, kw
    return False, ""


def _has_exact_opener(normalised: str) -> tuple[bool, str]:
    """Return (found, matched_opener)."""
    for opener in _EXACT_LOOKUP_OPENERS:
        if normalised.startswith(opener):
            return True, opener
    return False, ""


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #


def route_query(query: str) -> RoutingPlan:
    """
    Classify *query* and return a deterministic RoutingPlan.

    Parameters
    ----------
    query:
        The raw user query string.

    Returns
    -------
    RoutingPlan
        Carries query_type, retrieval_top_k, rerank_top_k,
        emphasize_parent_context, and notes.
    """
    normalised = _normalise(query)
    word_count = _word_count(normalised)
    notes: list[str] = []

    # Priority 1: insufficient or ambiguous
    if word_count < 3:
        notes.append(f"word count {word_count} < 3: treated as insufficient/ambiguous")
        return _build_plan("insufficient_or_ambiguous", notes)

    # Priority 2: comparison or multi-aspect
    found_comparison, comparison_kw = _contains_comparison(normalised)
    if found_comparison:
        notes.append(f"comparison keyword detected: '{comparison_kw}'")
        return _build_plan("comparison_or_multi_aspect", notes)

    # Priority 3: broad summary
    found_summary, summary_kw = _contains_summary(normalised)
    if found_summary:
        notes.append(f"summary keyword detected: '{summary_kw}'")
        return _build_plan("broad_summary", notes)
    if word_count > 15:
        notes.append(f"word count {word_count} > 15: treated as broad summary")
        return _build_plan("broad_summary", notes)

    # Priority 4: exact lookup
    found_opener, opener = _has_exact_opener(normalised)
    if found_opener:
        notes.append(f"exact-lookup opener detected: '{opener}'")
        return _build_plan("exact_lookup", notes)
    if word_count <= 6 and normalised.endswith("?"):
        notes.append(f"short ({word_count} words) query ending with '?': exact lookup")
        return _build_plan("exact_lookup", notes)

    # Priority 5: focused question (default)
    notes.append("no strong signal detected: default focused question")
    return _build_plan("focused_question", notes)


def _build_plan(query_type: str, notes: list[str]) -> RoutingPlan:
    params = _PARAMS[query_type]
    return RoutingPlan(
        query_type=query_type,  # type: ignore[arg-type]
        retrieval_top_k=params["retrieval_top_k"],
        rerank_top_k=params["rerank_top_k"],
        emphasize_parent_context=params["emphasize_parent_context"],
        notes=notes,
    )
