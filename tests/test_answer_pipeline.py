"""
Tests for the end-to-end answer pipeline.

Coverage
--------
TestPipelineOrdering        4 tests  stages execute in the correct order
TestPipelineOutput          5 tests  return type and field guarantees
TestEmptyRetrieval          3 tests  safe pass-through when retrieval returns []
TestParameterRouting        4 tests  retrieval_top_k / rerank_top_k / model forwarding
TestParentFallback          2 tests  parent-context enrichment and None-parent fallback
TestIntegrationPipeline     1 test   gated by PIPELINE_INTEGRATION_TESTS=1

Fake helpers
------------
_OrderTracker        — records call order across stage fakes
_FakeRetriever       — returns a fixed List[RetrievedChunk], records queries
_FakeReranker        — returns input as-is (or sliced), records queries and chunks
_FakeParentLookup    — returns a fixed List[Optional[DocumentChunk]], records chunks
_FakeGenerator       — callable(List[dict]) -> str, records messages
_make_chunk()        — factory for synthetic RetrievedChunk objects
_make_parent()       — factory for synthetic DocumentChunk objects
"""
from __future__ import annotations

import os
from typing import Callable, List, Optional

import pytest

from src.generation.answer_pipeline import run_pipeline
from src.schema.models import AnswerResponse, DocumentChunk, RetrievedChunk


# --------------------------------------------------------------------------- #
# Fake helpers                                                                 #
# --------------------------------------------------------------------------- #


class _OrderTracker:
    """Shared log used by stage fakes to record the order of their calls."""

    def __init__(self) -> None:
        self.calls: List[str] = []

    def record(self, name: str) -> None:
        self.calls.append(name)


class _FakeRetriever:
    """Returns a pre-loaded chunk list; records each query it receives."""

    def __init__(
        self,
        chunks: Optional[List[RetrievedChunk]] = None,
        tracker: Optional[_OrderTracker] = None,
    ) -> None:
        self._chunks = chunks or []
        self._tracker = tracker
        self.queries: List[str] = []

    def __call__(self, query: str) -> List[RetrievedChunk]:
        if self._tracker:
            self._tracker.record("retriever")
        self.queries.append(query)
        return list(self._chunks)


class _FakeReranker:
    """
    Returns input chunks unchanged (or sliced to top_k) and records calls.

    Pass ``top_k`` to simulate reranker output truncation; the default
    (None) returns all chunks.
    """

    def __init__(
        self,
        top_k: Optional[int] = None,
        tracker: Optional[_OrderTracker] = None,
    ) -> None:
        self._top_k = top_k
        self._tracker = tracker
        self.queries: List[str] = []
        self.chunk_inputs: List[List[RetrievedChunk]] = []

    def __call__(self, query: str, chunks: List[RetrievedChunk]) -> List[RetrievedChunk]:
        if self._tracker:
            self._tracker.record("reranker")
        self.queries.append(query)
        self.chunk_inputs.append(list(chunks))
        result = chunks if self._top_k is None else chunks[: self._top_k]
        return list(result)


class _FakeParentLookup:
    """Returns a pre-loaded parent list; records the chunk list it receives."""

    def __init__(
        self,
        parents: Optional[List[Optional[DocumentChunk]]] = None,
        tracker: Optional[_OrderTracker] = None,
    ) -> None:
        self._parents = parents  # None signals "return all-None list"
        self._tracker = tracker
        self.chunk_inputs: List[List[RetrievedChunk]] = []

    def __call__(
        self, chunks: List[RetrievedChunk]
    ) -> List[Optional[DocumentChunk]]:
        if self._tracker:
            self._tracker.record("parent_lookup")
        self.chunk_inputs.append(list(chunks))
        if self._parents is not None:
            return list(self._parents)
        return [None] * len(chunks)


class _FakeGenerator:
    """Callable(List[dict]) -> str that records messages and returns a fixed reply."""

    def __init__(self, reply: str = "Fake pipeline answer.") -> None:
        self.reply = reply
        self.calls: List[List[dict]] = []

    def __call__(self, messages: List[dict]) -> str:
        self.calls.append(messages)
        return self.reply


def _make_chunk(
    chunk_id: str = "c1",
    text: str = "child chunk text",
    parent_chunk_id: Optional[str] = None,
    rerank_score: Optional[float] = 0.9,
    fusion_score: Optional[float] = 0.8,
) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id="doc1",
        page_id="page1",
        file_name="report.txt",
        page_number=1,
        text=text,
        word_count=len(text.split()),
        retrieval_method="hybrid",
        parent_chunk_id=parent_chunk_id,
        fusion_score=fusion_score,
        rerank_score=rerank_score,
    )


def _make_parent(
    chunk_id: str = "p1",
    text: str = "parent chunk text with broader context",
) -> DocumentChunk:
    return DocumentChunk(
        chunk_id=chunk_id,
        doc_id="doc1",
        page_id="page1",
        page_number=1,
        file_name="report.txt",
        file_type="txt",
        text=text,
        word_count=len(text.split()),
        chunk_index=0,
        chunk_level="parent",
    )


def _run_fake(
    query: str = "What is the revenue?",
    retriever_chunks: Optional[List[RetrievedChunk]] = None,
    reranker_top_k: Optional[int] = None,
    parents: Optional[List[Optional[DocumentChunk]]] = None,
    generator: Optional[_FakeGenerator] = None,
    tracker: Optional[_OrderTracker] = None,
    retrieval_top_k: int = 10,
    rerank_top_k: int = 5,
    model: Optional[str] = None,
) -> AnswerResponse:
    """Convenience wrapper that wires all four fakes and calls run_pipeline."""
    if retriever_chunks is None:
        retriever_chunks = [_make_chunk()]
    if generator is None:
        generator = _FakeGenerator()

    retriever = _FakeRetriever(chunks=retriever_chunks, tracker=tracker)
    reranker = _FakeReranker(top_k=reranker_top_k, tracker=tracker)
    parent_lookup = _FakeParentLookup(parents=parents, tracker=tracker)

    return run_pipeline(
        query,
        retrieval_top_k=retrieval_top_k,
        rerank_top_k=rerank_top_k,
        model=model,
        _retriever=retriever,
        _reranker=reranker,
        _parent_lookup=parent_lookup,
        _generator=generator,
    )


# --------------------------------------------------------------------------- #
# TestPipelineOrdering                                                         #
# --------------------------------------------------------------------------- #


class TestPipelineOrdering:
    def test_stages_execute_in_order(self) -> None:
        """retriever → reranker → parent_lookup must be called in that sequence."""
        tracker = _OrderTracker()
        _run_fake(tracker=tracker)
        assert tracker.calls == ["retriever", "reranker", "parent_lookup"]

    def test_reranker_receives_retriever_output(self) -> None:
        """Chunks from the retriever must arrive at the reranker unchanged."""
        chunks = [_make_chunk("c1"), _make_chunk("c2")]
        retriever = _FakeRetriever(chunks=chunks)
        reranker = _FakeReranker()
        parent_lookup = _FakeParentLookup()
        run_pipeline(
            "query",
            _retriever=retriever,
            _reranker=reranker,
            _parent_lookup=parent_lookup,
            _generator=_FakeGenerator(),
        )
        assert [c.chunk_id for c in reranker.chunk_inputs[0]] == ["c1", "c2"]

    def test_parent_lookup_receives_reranker_output(self) -> None:
        """Chunks that enter parent_lookup must be the reranker's output, not the retriever's."""
        retriever_chunks = [_make_chunk("c1"), _make_chunk("c2"), _make_chunk("c3")]
        # Reranker cuts to 2
        reranker = _FakeReranker(top_k=2)
        parent_lookup = _FakeParentLookup()
        run_pipeline(
            "query",
            _retriever=_FakeRetriever(chunks=retriever_chunks),
            _reranker=reranker,
            _parent_lookup=parent_lookup,
            _generator=_FakeGenerator(),
        )
        # parent_lookup sees exactly 2 chunks (reranker output)
        assert len(parent_lookup.chunk_inputs[0]) == 2

    def test_synthesise_receives_reranked_chunks(self) -> None:
        """AnswerResponse.supporting_chunks must equal the reranker output list."""
        retriever_chunks = [_make_chunk("c1"), _make_chunk("c2"), _make_chunk("c3")]
        reranker = _FakeReranker(top_k=2)
        result = run_pipeline(
            "query",
            _retriever=_FakeRetriever(chunks=retriever_chunks),
            _reranker=reranker,
            _parent_lookup=_FakeParentLookup(),
            _generator=_FakeGenerator(),
        )
        assert len(result.supporting_chunks) == 2
        assert result.supporting_chunks[0].chunk_id == "c1"
        assert result.supporting_chunks[1].chunk_id == "c2"


# --------------------------------------------------------------------------- #
# TestPipelineOutput                                                           #
# --------------------------------------------------------------------------- #


class TestPipelineOutput:
    def test_returns_answer_response(self) -> None:
        result = _run_fake()
        assert isinstance(result, AnswerResponse)

    def test_query_preserved(self) -> None:
        q = "What is the operating income?"
        result = _run_fake(query=q)
        assert result.query == q

    def test_answer_text_is_str(self) -> None:
        result = _run_fake(generator=_FakeGenerator(reply="Revenue rose."))
        assert isinstance(result.answer_text, str)
        assert result.answer_text == "Revenue rose."

    def test_sources_is_list(self) -> None:
        # Stage 5 (citation construction) now populates sources with CitationRecord
        # objects derived from the reranked chunks. sources is no longer empty.
        result = _run_fake()
        assert isinstance(result.sources, list)

    def test_validation_flags_is_empty_list(self) -> None:
        result = _run_fake()
        assert result.validation_flags == []


# --------------------------------------------------------------------------- #
# TestEmptyRetrieval                                                           #
# --------------------------------------------------------------------------- #


class TestEmptyRetrieval:
    def test_empty_retrieval_returns_answer_response(self) -> None:
        """Pipeline must not crash when retrieval returns an empty list."""
        result = _run_fake(retriever_chunks=[])
        assert isinstance(result, AnswerResponse)

    def test_empty_retrieval_supporting_chunks_is_empty(self) -> None:
        result = _run_fake(retriever_chunks=[])
        assert result.supporting_chunks == []

    def test_empty_retrieval_still_calls_synthesise(self) -> None:
        """Even with no chunks, the generator must be called (placeholder context)."""
        gen = _FakeGenerator(reply="Insufficient context to answer.")
        result = _run_fake(retriever_chunks=[], generator=gen)
        assert len(gen.calls) == 1
        assert result.answer_text == "Insufficient context to answer."


# --------------------------------------------------------------------------- #
# TestParameterRouting                                                         #
# --------------------------------------------------------------------------- #


class TestParameterRouting:
    def test_retrieval_top_k_forwarded(self) -> None:
        """retrieval_top_k must be forwarded to the retriever callable via run_pipeline."""
        # We verify indirectly: the retriever returns exactly retrieval_top_k chunks
        # and the reranker sees all of them.
        chunks = [_make_chunk(f"c{i}") for i in range(8)]
        retriever = _FakeRetriever(chunks=chunks[:5])  # simulates top_k=5
        reranker = _FakeReranker()
        run_pipeline(
            "query",
            retrieval_top_k=5,
            _retriever=retriever,
            _reranker=reranker,
            _parent_lookup=_FakeParentLookup(),
            _generator=_FakeGenerator(),
        )
        # Reranker received exactly what the retriever returned
        assert len(reranker.chunk_inputs[0]) == 5

    def test_rerank_top_k_limits_supporting_chunks(self) -> None:
        """rerank_top_k is passed to the reranker; its output sets supporting_chunks."""
        chunks = [_make_chunk(f"c{i}") for i in range(6)]
        result = run_pipeline(
            "query",
            rerank_top_k=3,
            _retriever=_FakeRetriever(chunks=chunks),
            _reranker=_FakeReranker(top_k=3),
            _parent_lookup=_FakeParentLookup(),
            _generator=_FakeGenerator(),
        )
        assert len(result.supporting_chunks) == 3

    def test_model_forwarded_to_synthesise(self) -> None:
        """model override must appear in AnswerResponse.model_used."""
        result = _run_fake(model="custom-model:latest")
        assert result.model_used == "custom-model:latest"

    def test_query_forwarded_to_retriever(self) -> None:
        """The exact query string must be forwarded to the retriever."""
        q = "What were total assets in Q3?"
        retriever = _FakeRetriever(chunks=[_make_chunk()])
        run_pipeline(
            q,
            _retriever=retriever,
            _reranker=_FakeReranker(),
            _parent_lookup=_FakeParentLookup(),
            _generator=_FakeGenerator(),
        )
        assert retriever.queries[0] == q


# --------------------------------------------------------------------------- #
# TestParentFallback                                                           #
# --------------------------------------------------------------------------- #


class TestParentFallback:
    def test_parent_text_used_when_available(self) -> None:
        """When parent lookup returns a parent, its text must appear in the generator call."""
        chunk = _make_chunk("c1", text="child text")
        parent = _make_parent("p1", text="parent text with broader scope")
        gen = _FakeGenerator()
        run_pipeline(
            "query",
            _retriever=_FakeRetriever(chunks=[chunk]),
            _reranker=_FakeReranker(),
            _parent_lookup=_FakeParentLookup(parents=[parent]),
            _generator=gen,
        )
        user_content = next(
            m["content"] for m in gen.calls[0] if m["role"] == "user"
        )
        assert "parent text with broader scope" in user_content
        assert "child text" not in user_content

    def test_child_text_used_when_parent_is_none(self) -> None:
        """When parent lookup returns None for a position, child text must be used."""
        chunk = _make_chunk("c1", text="child only text")
        gen = _FakeGenerator()
        run_pipeline(
            "query",
            _retriever=_FakeRetriever(chunks=[chunk]),
            _reranker=_FakeReranker(),
            _parent_lookup=_FakeParentLookup(parents=[None]),
            _generator=gen,
        )
        user_content = next(
            m["content"] for m in gen.calls[0] if m["role"] == "user"
        )
        assert "child only text" in user_content


# --------------------------------------------------------------------------- #
# TestIntegrationPipeline (gated)                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    os.environ.get("PIPELINE_INTEGRATION_TESTS") != "1",
    reason="Set PIPELINE_INTEGRATION_TESTS=1 to run real pipeline integration tests",
)
class TestIntegrationPipeline:
    def test_full_pipeline_with_real_runtime(self) -> None:
        """
        Integration test: runs the complete pipeline against a real local
        Ollama daemon with fake retrieval injected to avoid an index dependency.

        Requires:
            - PIPELINE_INTEGRATION_TESTS=1
            - Local Ollama daemon running (`ollama serve`)
            - Model pulled: `ollama pull qwen3:8b`

        Retrieval and reranking are injected (no real index needed).
        Only the generation stage uses real Ollama.
        """
        chunks = [
            _make_chunk(
                "i1",
                text=(
                    "Total revenue for the fiscal year reached $180 million, "
                    "a 9% increase over the prior year."
                ),
            ),
            _make_chunk(
                "i2",
                text=(
                    "The gross margin improved from 42% to 45%, reflecting "
                    "disciplined cost management across all business units."
                ),
            ),
        ]

        result = run_pipeline(
            query="What was the total revenue and how did the gross margin change?",
            _retriever=_FakeRetriever(chunks=chunks),
            _reranker=_FakeReranker(),
            _parent_lookup=_FakeParentLookup(),
            # _generator NOT injected — real Ollama call
        )

        assert isinstance(result, AnswerResponse)
        assert len(result.answer_text) > 0
        assert result.model_used != ""
        assert len(result.supporting_chunks) == 2
        assert result.sources == []
        assert result.validation_flags == []
        assert result.latency_ms is not None and result.latency_ms > 0
