"""
Tests for the answer synthesis layer.

Coverage
--------
TestPromptBuilder        5 tests  build_grounded_messages structure and content
TestAnswerEngineContract 6 tests  synthesise() return type and field guarantees
TestContextStrategy      4 tests  parent-context enrichment and fallback rules
TestEdgeCases            4 tests  empty input, top_k variants
TestIntegrationOllama    1 test   gated by OLLAMA_INTEGRATION_TESTS=1

Fake helpers
------------
_FakeGenerator  — callable(List[dict]) -> str that records all calls for inspection
_make_chunk()   — factory for synthetic RetrievedChunk objects
_make_parent()  — factory for synthetic DocumentChunk objects (parent-level)
"""
from __future__ import annotations

import os
from typing import List, Optional

import pytest

from src.generation.prompt_templates import (
    build_grounded_messages,
    _NO_CONTEXT_PLACEHOLDER,
    _SYSTEM_INSTRUCTION,
)
from src.generation.answer_engine import synthesise
from src.schema.models import AnswerResponse, DocumentChunk, RetrievedChunk


# --------------------------------------------------------------------------- #
# Fake helpers                                                                 #
# --------------------------------------------------------------------------- #


class _FakeGenerator:
    """
    Callable stub that records every message list it receives.

    Use ``last_user_content`` to inspect the user turn content that
    the answer engine assembled.
    """

    def __init__(self, reply: str = "Fake answer from stub.") -> None:
        self.reply = reply
        self.calls: List[List[dict]] = []

    def __call__(self, messages: List[dict]) -> str:
        self.calls.append(messages)
        return self.reply

    @property
    def last_messages(self) -> List[dict]:
        return self.calls[-1]

    @property
    def last_user_content(self) -> str:
        for m in self.last_messages:
            if m["role"] == "user":
                return m["content"]
        return ""


def _make_chunk(
    chunk_id: str = "c1",
    text: str = "child text about revenue",
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
    text: str = "parent context about quarterly revenue growth",
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


# --------------------------------------------------------------------------- #
# TestPromptBuilder                                                            #
# --------------------------------------------------------------------------- #


class TestPromptBuilder:
    def test_returns_two_messages(self) -> None:
        messages = build_grounded_messages("What is revenue?", ["Revenue was $10M."])
        assert len(messages) == 2

    def test_roles_are_system_and_user(self) -> None:
        messages = build_grounded_messages("query", ["block"])
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"

    def test_query_in_user_message(self) -> None:
        query = "What were the Q3 results?"
        messages = build_grounded_messages(query, ["Some context."])
        assert query in messages[1]["content"]

    def test_context_blocks_in_user_message(self) -> None:
        block = "Revenue grew 15 percent year-over-year."
        messages = build_grounded_messages("What grew?", [block])
        assert block in messages[1]["content"]

    def test_empty_context_uses_placeholder(self) -> None:
        messages = build_grounded_messages("Any question?", [])
        assert _NO_CONTEXT_PLACEHOLDER in messages[1]["content"]

    def test_system_message_has_grounding_instruction(self) -> None:
        messages = build_grounded_messages("q", ["ctx"])
        assert messages[0]["content"] == _SYSTEM_INSTRUCTION

    def test_multiple_blocks_all_present(self) -> None:
        blocks = ["Block alpha.", "Block beta.", "Block gamma."]
        messages = build_grounded_messages("q", blocks)
        for block in blocks:
            assert block in messages[1]["content"]


# --------------------------------------------------------------------------- #
# TestAnswerEngineContract                                                     #
# --------------------------------------------------------------------------- #


class TestAnswerEngineContract:
    def test_returns_answer_response(self) -> None:
        chunks = [_make_chunk()]
        result = synthesise("query", chunks, _generator=_FakeGenerator())
        assert isinstance(result, AnswerResponse)

    def test_answer_text_is_set(self) -> None:
        reply = "The answer is 42."
        chunks = [_make_chunk()]
        result = synthesise("query", chunks, _generator=_FakeGenerator(reply=reply))
        assert result.answer_text == reply

    def test_model_used_is_set(self) -> None:
        chunks = [_make_chunk()]
        result = synthesise(
            "query", chunks, model="qwen3:8b", _generator=_FakeGenerator()
        )
        assert result.model_used == "qwen3:8b"

    def test_supporting_chunks_equals_input(self) -> None:
        chunks = [_make_chunk("c1"), _make_chunk("c2"), _make_chunk("c3")]
        result = synthesise("query", chunks, _generator=_FakeGenerator())
        assert len(result.supporting_chunks) == 3
        assert [c.chunk_id for c in result.supporting_chunks] == ["c1", "c2", "c3"]

    def test_latency_ms_is_non_negative(self) -> None:
        chunks = [_make_chunk()]
        result = synthesise("query", chunks, _generator=_FakeGenerator())
        assert result.latency_ms is not None
        assert result.latency_ms >= 0.0

    def test_run_id_is_generated(self) -> None:
        chunks = [_make_chunk()]
        result = synthesise("query", chunks, _generator=_FakeGenerator())
        assert isinstance(result.run_id, str)
        assert len(result.run_id) > 0

    def test_sources_is_empty_list(self) -> None:
        chunks = [_make_chunk()]
        result = synthesise("query", chunks, _generator=_FakeGenerator())
        assert result.sources == []

    def test_validation_flags_is_empty_list(self) -> None:
        chunks = [_make_chunk()]
        result = synthesise("query", chunks, _generator=_FakeGenerator())
        assert result.validation_flags == []

    def test_query_preserved_in_response(self) -> None:
        q = "What is the net income?"
        chunks = [_make_chunk()]
        result = synthesise(q, chunks, _generator=_FakeGenerator())
        assert result.query == q


# --------------------------------------------------------------------------- #
# TestContextStrategy                                                          #
# --------------------------------------------------------------------------- #


class TestContextStrategy:
    def test_parent_text_used_when_available(self) -> None:
        child = _make_chunk(text="child text only")
        parent = _make_parent(text="broad parent context passage")
        fake = _FakeGenerator()
        synthesise("q", [child], parents=[parent], _generator=fake)
        assert "broad parent context passage" in fake.last_user_content
        assert "child text only" not in fake.last_user_content

    def test_child_text_used_when_parent_is_none(self) -> None:
        child = _make_chunk(text="only child text available")
        fake = _FakeGenerator()
        synthesise("q", [child], parents=[None], _generator=fake)
        assert "only child text available" in fake.last_user_content

    def test_child_text_when_parents_shorter_than_chunks(self) -> None:
        # parents has 1 entry but chunks has 2 — second chunk uses child text
        c1 = _make_chunk("c1", text="child one text")
        c2 = _make_chunk("c2", text="child two fallback")
        p1 = _make_parent(text="parent one context")
        fake = _FakeGenerator()
        synthesise("q", [c1, c2], parents=[p1], _generator=fake)
        assert "parent one context" in fake.last_user_content
        assert "child two fallback" in fake.last_user_content

    def test_child_text_when_parents_is_none(self) -> None:
        child = _make_chunk(text="direct child text no parents")
        fake = _FakeGenerator()
        synthesise("q", [child], parents=None, _generator=fake)
        assert "direct child text no parents" in fake.last_user_content

    def test_input_chunks_not_mutated(self) -> None:
        chunk = _make_chunk(text="original text")
        original_text = chunk.text
        original_score = chunk.rerank_score
        synthesise("q", [chunk], _generator=_FakeGenerator())
        assert chunk.text == original_text
        assert chunk.rerank_score == original_score


# --------------------------------------------------------------------------- #
# TestEdgeCases                                                                #
# --------------------------------------------------------------------------- #


class TestEdgeCases:
    def test_empty_chunks_returns_answer_response(self) -> None:
        result = synthesise("query", [], _generator=_FakeGenerator())
        assert isinstance(result, AnswerResponse)
        assert result.supporting_chunks == []

    def test_empty_chunks_uses_placeholder_context(self) -> None:
        fake = _FakeGenerator()
        synthesise("query", [], _generator=fake)
        assert _NO_CONTEXT_PLACEHOLDER in fake.last_user_content

    def test_top_k_limits_context_blocks(self) -> None:
        chunks = [
            _make_chunk("c1", text="block one"),
            _make_chunk("c2", text="block two"),
            _make_chunk("c3", text="block three"),
        ]
        fake = _FakeGenerator()
        synthesise("q", chunks, top_k=2, _generator=fake)
        assert "block one" in fake.last_user_content
        assert "block two" in fake.last_user_content
        assert "block three" not in fake.last_user_content

    def test_top_k_does_not_truncate_supporting_chunks(self) -> None:
        chunks = [_make_chunk(f"c{i}") for i in range(5)]
        result = synthesise("q", chunks, top_k=2, _generator=_FakeGenerator())
        assert len(result.supporting_chunks) == 5

    def test_top_k_larger_than_chunks_uses_all(self) -> None:
        chunks = [_make_chunk("c1"), _make_chunk("c2")]
        fake = _FakeGenerator()
        synthesise("q", chunks, top_k=10, _generator=fake)
        assert "child text about revenue" in fake.last_user_content

    def test_single_chunk_no_parents(self) -> None:
        chunk = _make_chunk(text="single passage text")
        result = synthesise("query", [chunk], _generator=_FakeGenerator())
        assert len(result.supporting_chunks) == 1

    def test_answer_text_is_string(self) -> None:
        result = synthesise("q", [_make_chunk()], _generator=_FakeGenerator("answer"))
        assert isinstance(result.answer_text, str)


# --------------------------------------------------------------------------- #
# TestIntegrationOllama                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    os.getenv("OLLAMA_INTEGRATION_TESTS") != "1",
    reason="Set OLLAMA_INTEGRATION_TESTS=1 to run real Ollama integration tests",
)
class TestIntegrationOllama:
    def test_synthesise_with_real_ollama(self) -> None:
        """
        Integration test: calls the real Ollama daemon with a grounded
        finance query. Verifies that a non-empty AnswerResponse is returned.

        Requires:
            - OLLAMA_INTEGRATION_TESTS=1
            - Local Ollama daemon running (`ollama serve`)
            - Model pulled: `ollama pull qwen3:8b`
        """
        chunks = [
            _make_chunk(
                chunk_id="int1",
                text=(
                    "The company reported net revenue of $42.5 million for Q3, "
                    "representing a 12% increase year-over-year driven by "
                    "strong performance in the enterprise segment."
                ),
            ),
            _make_chunk(
                chunk_id="int2",
                text=(
                    "Operating expenses decreased by 3% to $18.2 million, "
                    "resulting in an operating income of $24.3 million."
                ),
            ),
        ]
        result = synthesise(
            query="What was the company's net revenue and how did it change year-over-year?",
            chunks=chunks,
        )
        assert isinstance(result, AnswerResponse)
        assert len(result.answer_text) > 0
        assert result.latency_ms is not None and result.latency_ms > 0
        assert result.model_used != ""
        assert len(result.supporting_chunks) == 2
        assert result.sources == []
