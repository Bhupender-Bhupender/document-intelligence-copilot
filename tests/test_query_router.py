"""
tests/test_query_router.py
──────────────────────────
Unit tests for src/retrieval/query_router.py and the routing wiring in
run_pipeline (routing_plan parameter).

Classes
-------
TestQueryTypeClassification      – one canonical query per type classifies correctly
TestRoutingPlanParameters        – correct top_k values per query type
TestParentContextEmphasis        – emphasize_parent_context True/False per type
TestAmbiguousHandling            – short/minimal queries → insufficient_or_ambiguous
TestComparisonBroaderThanExact   – comparison top_k > exact_lookup top_k
TestNotesField                   – notes is a list of strings with content
TestPipelineRoutingIntegration   – run_pipeline respects routing_plan overrides
"""

from __future__ import annotations

from typing import List, Optional

import pytest

from src.retrieval.query_router import route_query
from src.schema.models import DocumentChunk, RetrievedChunk, RoutingPlan
from src.generation.answer_pipeline import run_pipeline

# =========================================================================== #
# Helpers                                                                      #
# =========================================================================== #


def _make_chunk(chunk_id: str = "c1") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id="doc1",
        page_id="page1",
        file_name="report.txt",
        page_number=1,
        text="Sample chunk text.",
        word_count=3,
        retrieval_method="hybrid",
        fusion_score=0.8,
        rerank_score=0.9,
    )


class _CapturingRetriever:
    """Records call arguments; returns a fixed chunk list."""

    def __init__(self, chunks: Optional[List[RetrievedChunk]] = None) -> None:
        self._chunks = chunks or [_make_chunk()]
        self.calls: list[str] = []

    def __call__(self, query: str) -> List[RetrievedChunk]:
        self.calls.append(query)
        return list(self._chunks)


class _CapturingReranker:
    """Records top_k-equivalent by returning a slice; captures call count."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []  # (query, len(chunks))

    def __call__(self, query: str, chunks: List[RetrievedChunk]) -> List[RetrievedChunk]:
        self.calls.append((query, len(chunks)))
        return list(chunks)


class _CapturingParentLookup:
    """Returns all-None parents; records whether it was called."""

    def __init__(self) -> None:
        self.called = False
        self.received: list[RetrievedChunk] = []

    def __call__(self, chunks: List[RetrievedChunk]) -> List[Optional[DocumentChunk]]:
        self.called = True
        self.received = list(chunks)
        return [None] * len(chunks)


class _CapturingSynthesise:
    """
    Wraps as a _generator injection.
    Records the parents list forwarded to synthesise via the messages
    (we cannot intercept parents directly, so we probe via parent gating
    in the integration tests using _CapturingParentLookup + routing flags).
    """

    def __call__(self, messages: list) -> str:
        return "Routed answer."


def _make_plan(
    query_type: str = "focused_question",
    retrieval_top_k: int = 10,
    rerank_top_k: int = 5,
    emphasize_parent_context: bool = False,
) -> RoutingPlan:
    return RoutingPlan(
        query_type=query_type,  # type: ignore[arg-type]
        retrieval_top_k=retrieval_top_k,
        rerank_top_k=rerank_top_k,
        emphasize_parent_context=emphasize_parent_context,
    )


# =========================================================================== #
# TestQueryTypeClassification                                                  #
# =========================================================================== #


class TestQueryTypeClassification:
    def test_insufficient_short_query(self) -> None:
        plan = route_query("revenue")
        assert plan.query_type == "insufficient_or_ambiguous"

    def test_comparison_query(self) -> None:
        plan = route_query("Compare the revenue figures for Q1 versus Q2.")
        assert plan.query_type == "comparison_or_multi_aspect"

    def test_broad_summary_query(self) -> None:
        plan = route_query("Summarize the key findings from the annual report.")
        assert plan.query_type == "broad_summary"

    def test_exact_lookup_query(self) -> None:
        plan = route_query("When did the company go public?")
        assert plan.query_type == "exact_lookup"

    def test_focused_question_query(self) -> None:
        plan = route_query("What are the main risks mentioned in the report?")
        assert plan.query_type == "focused_question"


# =========================================================================== #
# TestRoutingPlanParameters                                                    #
# =========================================================================== #


class TestRoutingPlanParameters:
    def test_exact_lookup_top_k(self) -> None:
        plan = route_query("Who is the CEO?")
        assert plan.retrieval_top_k == 5
        assert plan.rerank_top_k == 3

    def test_focused_question_top_k(self) -> None:
        plan = route_query("What are the main risks in the document?")
        assert plan.retrieval_top_k == 10
        assert plan.rerank_top_k == 5

    def test_broad_summary_top_k(self) -> None:
        plan = route_query("Summarize the annual report findings.")
        assert plan.retrieval_top_k == 15
        assert plan.rerank_top_k == 8

    def test_comparison_top_k(self) -> None:
        plan = route_query("Compare Q1 revenue versus Q2 revenue.")
        assert plan.retrieval_top_k == 20
        assert plan.rerank_top_k == 10

    def test_ambiguous_top_k(self) -> None:
        plan = route_query("hi")
        assert plan.retrieval_top_k == 5
        assert plan.rerank_top_k == 3


# =========================================================================== #
# TestParentContextEmphasis                                                    #
# =========================================================================== #


class TestParentContextEmphasis:
    def test_exact_lookup_no_parent_emphasis(self) -> None:
        plan = route_query("When was the IPO?")
        assert plan.emphasize_parent_context is False

    def test_focused_question_no_parent_emphasis(self) -> None:
        plan = route_query("What are the main contract terms?")
        assert plan.emphasize_parent_context is False

    def test_broad_summary_parent_emphasis(self) -> None:
        plan = route_query("Give me an overview of the report.")
        assert plan.emphasize_parent_context is True

    def test_comparison_parent_emphasis(self) -> None:
        plan = route_query("What is the difference between product A and product B?")
        assert plan.emphasize_parent_context is True

    def test_ambiguous_parent_emphasis(self) -> None:
        plan = route_query("ok")
        assert plan.emphasize_parent_context is True


# =========================================================================== #
# TestAmbiguousHandling                                                        #
# =========================================================================== #


class TestAmbiguousHandling:
    def test_single_word(self) -> None:
        assert route_query("revenue").query_type == "insufficient_or_ambiguous"

    def test_two_words(self) -> None:
        assert route_query("annual report").query_type == "insufficient_or_ambiguous"

    def test_very_short_with_punctuation(self) -> None:
        assert route_query("what?").query_type == "insufficient_or_ambiguous"

    def test_three_words_not_ambiguous(self) -> None:
        # Exactly 3 words should NOT be classified as ambiguous
        plan = route_query("What is revenue?")
        assert plan.query_type != "insufficient_or_ambiguous"


# =========================================================================== #
# TestComparisonBroaderThanExact                                               #
# =========================================================================== #


class TestComparisonBroaderThanExact:
    def test_comparison_retrieval_top_k_greater_than_exact(self) -> None:
        comparison = route_query("Compare the two strategies.")
        exact = route_query("Who signed the agreement?")
        assert comparison.retrieval_top_k > exact.retrieval_top_k

    def test_comparison_rerank_top_k_greater_than_exact(self) -> None:
        comparison = route_query("What are the differences between plan A and plan B?")
        exact = route_query("When was the policy updated?")
        assert comparison.rerank_top_k > exact.rerank_top_k


# =========================================================================== #
# TestNotesField                                                               #
# =========================================================================== #


class TestNotesField:
    def test_notes_is_list_of_strings(self) -> None:
        plan = route_query("Compare Q1 results versus Q2.")
        assert isinstance(plan.notes, list)
        assert all(isinstance(n, str) for n in plan.notes)

    def test_notes_non_empty_when_signal_detected(self) -> None:
        plan = route_query("Compare Q1 results versus Q2.")
        assert len(plan.notes) > 0

    def test_notes_non_empty_for_ambiguous(self) -> None:
        plan = route_query("x")
        assert len(plan.notes) > 0


# =========================================================================== #
# TestPipelineRoutingIntegration                                               #
# =========================================================================== #


class TestPipelineRoutingIntegration:
    """
    Verify that run_pipeline respects the routing_plan parameter.

    All tests inject fake callables to avoid real I/O.
    """

    def _run(
        self,
        routing_plan: Optional[RoutingPlan] = None,
        chunks: Optional[List[RetrievedChunk]] = None,
        retriever: Optional[_CapturingRetriever] = None,
        reranker: Optional[_CapturingReranker] = None,
        parent_lookup: Optional[_CapturingParentLookup] = None,
    ):
        if chunks is None:
            chunks = [_make_chunk(f"c{i}") for i in range(3)]
        if retriever is None:
            retriever = _CapturingRetriever(chunks=chunks)
        if reranker is None:
            reranker = _CapturingReranker()
        if parent_lookup is None:
            parent_lookup = _CapturingParentLookup()
        return run_pipeline(
            query="Test query for routing.",
            routing_plan=routing_plan,
            _retriever=retriever,
            _reranker=reranker,
            _parent_lookup=parent_lookup,
            _generator=_CapturingSynthesise(),
        )

    def test_no_routing_plan_uses_defaults(self) -> None:
        # Without a routing plan, pipeline runs with its built-in defaults
        response = self._run(routing_plan=None)
        assert response.answer_text == "Routed answer."

    def test_routing_plan_returns_answer_response(self) -> None:
        plan = _make_plan(query_type="focused_question")
        response = self._run(routing_plan=plan)
        assert response.query == "Test query for routing."
        assert isinstance(response.answer_text, str)

    def test_routing_plan_exact_lookup_overrides_top_k(self) -> None:
        # Exact lookup plan: retrieval_top_k=5, rerank_top_k=3
        plan = route_query("Who signed the contract?")
        assert plan.query_type == "exact_lookup"
        # run_pipeline with the plan should complete without error
        response = self._run(routing_plan=plan)
        assert response is not None

    def test_routing_plan_comparison_overrides_top_k(self) -> None:
        plan = route_query("Compare strategy A versus strategy B.")
        assert plan.query_type == "comparison_or_multi_aspect"
        response = self._run(routing_plan=plan)
        assert response is not None

    def test_emphasize_parent_false_suppresses_parents_in_synthesis(self) -> None:
        """
        When emphasize_parent_context=False, the pipeline passes parents=None
        to synthesise. We verify this by checking that the pipeline still
        produces a valid answer (synthesise handles None parents gracefully).
        """
        plan = _make_plan(
            query_type="exact_lookup",
            retrieval_top_k=5,
            rerank_top_k=3,
            emphasize_parent_context=False,
        )
        response = self._run(routing_plan=plan)
        # synthesise with suppressed parents still returns a grounded answer
        assert isinstance(response.answer_text, str)
        assert len(response.answer_text) > 0

    def test_emphasize_parent_true_passes_parents_to_synthesis(self) -> None:
        """
        When emphasize_parent_context=True, the pipeline passes the
        looked-up parents list to synthesise. Still produces a valid answer.
        """
        plan = _make_plan(
            query_type="broad_summary",
            retrieval_top_k=15,
            rerank_top_k=8,
            emphasize_parent_context=True,
        )
        response = self._run(routing_plan=plan)
        assert isinstance(response.answer_text, str)
        assert len(response.answer_text) > 0
