"""
Tests for the semantic evaluation harness.

Covers:
    TestSemanticScore         — model defaults, field contracts
    TestSemanticEvalReport    — field types, rate semantics, threshold stored
    TestJudgePrompts          — build_judge_messages structure and content
    TestParseScores           — all normalization paths and failure modes
    TestAggregateScores       — mean calculations, threshold logic, zero-denom
    TestRunSemanticEvaluation — injection, length guard, scoring, empty inputs
    TestIntegrationGated      — real Ollama judge (gated; requires env var)

Default tests never call a live LLM.
The integration test requires SEMANTIC_EVAL_INTEGRATION=1 in the environment.
"""
from __future__ import annotations

import os
from typing import List, Optional

import pytest

from src.evaluation.judge_prompts import build_judge_messages
from src.evaluation.semantic_evaluator import (
    _aggregate_scores,
    _build_context_text,
    _parse_scores,
    _score_one,
    run_semantic_evaluation,
)
from src.schema.eval_models import EvalExample
from src.schema.models import AnswerResponse, RetrievedChunk
from src.schema.semantic_eval_models import SemanticEvalReport, SemanticScore


# --------------------------------------------------------------------------- #
# Shared helpers                                                               #
# --------------------------------------------------------------------------- #

_QUERY = "What is the return policy?"
_ANSWER = "Returns are accepted within 30 days."
_CHUNK_TEXT = "The return policy allows returns within 30 days of purchase."


def _make_response(
    answer_text: str = _ANSWER,
    supporting_chunks: Optional[List[RetrievedChunk]] = None,
) -> AnswerResponse:
    return AnswerResponse(
        query=_QUERY,
        answer_text=answer_text,
        model_used="test-model",
        supporting_chunks=supporting_chunks or [],
    )


def _make_chunk(text: str = _CHUNK_TEXT, chunk_id: str = "c1") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id="doc1",
        page_id="doc1-p1",
        file_name="policy.txt",
        page_number=1,
        text=text,
        word_count=len(text.split()),
    )


def _make_example(query: str = _QUERY) -> EvalExample:
    return EvalExample(query=query)


def _fake_judge(raw_output: str):
    """Return a callable that always returns raw_output."""
    def _call(messages: list) -> str:  # noqa: ANN001
        return raw_output
    return _call


def _good_judge_output() -> str:
    return '{"groundedness": 0.9, "answer_relevance": 0.8, "context_relevance": 0.7, "completeness": 0.6}'


def _make_semantic_score(
    groundedness: float = 0.8,
    answer_relevance: float = 0.8,
    context_relevance: float = 0.8,
    completeness: float = 0.8,
    parse_failed: bool = False,
    example_id: str = "ex-1",
    query: str = _QUERY,
) -> SemanticScore:
    return SemanticScore(
        example_id=example_id,
        query=query,
        groundedness_score=groundedness,
        answer_relevance_score=answer_relevance,
        context_relevance_score=context_relevance,
        completeness_score=completeness,
        parse_failed=parse_failed,
        judge_notes="parse_error" if parse_failed else "",
    )


# --------------------------------------------------------------------------- #
# TestSemanticScore                                                            #
# --------------------------------------------------------------------------- #


class TestSemanticScore:
    """SemanticScore model: defaults and field contracts."""

    def test_required_fields_accepted(self) -> None:
        s = SemanticScore(example_id="e1", query="q")
        assert s.example_id == "e1"
        assert s.query == "q"

    def test_score_fields_default_to_zero(self) -> None:
        s = SemanticScore(example_id="e1", query="q")
        assert s.groundedness_score == 0.0
        assert s.answer_relevance_score == 0.0
        assert s.context_relevance_score == 0.0
        assert s.completeness_score == 0.0

    def test_parse_failed_defaults_false(self) -> None:
        s = SemanticScore(example_id="e1", query="q")
        assert s.parse_failed is False

    def test_judge_notes_defaults_empty(self) -> None:
        s = SemanticScore(example_id="e1", query="q")
        assert s.judge_notes == ""

    def test_parse_failed_score_pattern(self) -> None:
        s = SemanticScore(
            example_id="e1",
            query="q",
            parse_failed=True,
            judge_notes="parse_error",
        )
        assert s.parse_failed is True
        assert s.judge_notes == "parse_error"
        assert s.groundedness_score == 0.0
        assert s.answer_relevance_score == 0.0


# --------------------------------------------------------------------------- #
# TestSemanticEvalReport                                                       #
# --------------------------------------------------------------------------- #


class TestSemanticEvalReport:
    """SemanticEvalReport model: field types, threshold, rate semantics."""

    def _make_report(self, **kwargs) -> SemanticEvalReport:
        defaults = dict(
            total=1,
            threshold=0.7,
            mean_groundedness=0.8,
            mean_answer_relevance=0.8,
            mean_context_relevance=0.8,
            mean_completeness=0.8,
            above_threshold_count=1,
            above_threshold_rate=1.0,
            parse_failure_count=0,
            per_example=[_make_semantic_score()],
        )
        defaults.update(kwargs)
        return SemanticEvalReport(**defaults)

    def test_report_id_auto_generated(self) -> None:
        r = self._make_report()
        assert isinstance(r.report_id, str)
        assert len(r.report_id) > 0

    def test_threshold_stored(self) -> None:
        r = self._make_report(threshold=0.5)
        assert r.threshold == 0.5

    def test_rate_fields_are_floats(self) -> None:
        r = self._make_report()
        assert isinstance(r.mean_groundedness, float)
        assert isinstance(r.above_threshold_rate, float)

    def test_per_example_contains_semantic_scores(self) -> None:
        r = self._make_report()
        assert len(r.per_example) == 1
        assert isinstance(r.per_example[0], SemanticScore)


# --------------------------------------------------------------------------- #
# TestJudgePrompts                                                             #
# --------------------------------------------------------------------------- #


class TestJudgePrompts:
    """build_judge_messages: structure and content."""

    def test_returns_two_messages(self) -> None:
        msgs = build_judge_messages("q", "ctx", "ans")
        assert len(msgs) == 2

    def test_first_message_is_system(self) -> None:
        msgs = build_judge_messages("q", "ctx", "ans")
        assert msgs[0]["role"] == "system"

    def test_second_message_is_user(self) -> None:
        msgs = build_judge_messages("q", "ctx", "ans")
        assert msgs[1]["role"] == "user"

    def test_query_appears_in_user_turn(self) -> None:
        msgs = build_judge_messages("unique_query_string", "ctx", "ans")
        assert "unique_query_string" in msgs[1]["content"]

    def test_context_appears_in_user_turn(self) -> None:
        msgs = build_judge_messages("q", "unique_context_block", "ans")
        assert "unique_context_block" in msgs[1]["content"]

    def test_answer_appears_in_user_turn(self) -> None:
        msgs = build_judge_messages("q", "ctx", "unique_answer_text")
        assert "unique_answer_text" in msgs[1]["content"]

    def test_all_four_scoring_criteria_in_system(self) -> None:
        msgs = build_judge_messages("q", "ctx", "ans")
        system_text = msgs[0]["content"].lower()
        assert "groundedness" in system_text
        assert "answer_relevance" in system_text
        assert "context_relevance" in system_text
        assert "completeness" in system_text

    def test_empty_context_uses_placeholder(self) -> None:
        msgs = build_judge_messages("q", "", "ans")
        assert "[No context provided.]" in msgs[1]["content"]

    def test_whitespace_only_context_uses_placeholder(self) -> None:
        msgs = build_judge_messages("q", "   ", "ans")
        assert "[No context provided.]" in msgs[1]["content"]

    def test_empty_answer_included_without_error(self) -> None:
        msgs = build_judge_messages("q", "ctx", "")
        assert isinstance(msgs[1]["content"], str)

    def test_json_format_instruction_in_system(self) -> None:
        msgs = build_judge_messages("q", "ctx", "ans")
        system_text = msgs[0]["content"]
        assert "JSON" in system_text or "json" in system_text


# --------------------------------------------------------------------------- #
# TestParseScores                                                              #
# --------------------------------------------------------------------------- #


class TestParseScores:
    """_parse_scores: all normalization paths and failure modes."""

    def test_clean_json_parsed_correctly(self) -> None:
        raw = '{"groundedness": 0.9, "answer_relevance": 0.8, "context_relevance": 0.7, "completeness": 0.6}'
        result = _parse_scores(raw)
        assert result.get("groundedness") == pytest.approx(0.9)
        assert result.get("answer_relevance") == pytest.approx(0.8)
        assert result.get("context_relevance") == pytest.approx(0.7)
        assert result.get("completeness") == pytest.approx(0.6)
        assert not result.get("parse_failed")

    def test_surrounding_whitespace_stripped(self) -> None:
        raw = '  \n  {"groundedness": 0.5, "answer_relevance": 0.5, "context_relevance": 0.5, "completeness": 0.5}  \n  '
        result = _parse_scores(raw)
        assert result.get("groundedness") == pytest.approx(0.5)
        assert not result.get("parse_failed")

    def test_json_fence_stripped(self) -> None:
        raw = '```json\n{"groundedness": 0.8, "answer_relevance": 0.7, "context_relevance": 0.6, "completeness": 0.5}\n```'
        result = _parse_scores(raw)
        assert result.get("groundedness") == pytest.approx(0.8)
        assert not result.get("parse_failed")

    def test_bare_fence_stripped(self) -> None:
        raw = '```\n{"groundedness": 0.8, "answer_relevance": 0.7, "context_relevance": 0.6, "completeness": 0.5}\n```'
        result = _parse_scores(raw)
        assert result.get("groundedness") == pytest.approx(0.8)
        assert not result.get("parse_failed")

    def test_extra_keys_ignored(self) -> None:
        raw = '{"groundedness": 0.9, "answer_relevance": 0.8, "context_relevance": 0.7, "completeness": 0.6, "extra_key": "ignored"}'
        result = _parse_scores(raw)
        assert not result.get("parse_failed")
        assert "extra_key" in result  # present but harmless

    def test_missing_keys_not_in_result(self) -> None:
        raw = '{"groundedness": 0.9}'
        result = _parse_scores(raw)
        assert not result.get("parse_failed")
        assert result.get("groundedness") == pytest.approx(0.9)
        # missing keys are absent — callers treat absent → 0.0
        assert "answer_relevance" not in result

    def test_arbitrary_prose_fails(self) -> None:
        raw = "The answer is well grounded and relevant to the question."
        result = _parse_scores(raw)
        assert result.get("parse_failed") is True

    def test_empty_string_fails(self) -> None:
        result = _parse_scores("")
        assert result.get("parse_failed") is True

    def test_partial_json_fails(self) -> None:
        result = _parse_scores('{"groundedness": 0.9,')
        assert result.get("parse_failed") is True

    def test_json_array_fails(self) -> None:
        # Top-level array is not a dict — should fail.
        result = _parse_scores("[0.9, 0.8, 0.7, 0.6]")
        assert result.get("parse_failed") is True


# --------------------------------------------------------------------------- #
# TestAggregateScores                                                          #
# --------------------------------------------------------------------------- #


class TestAggregateScores:
    """_aggregate_scores: means, threshold, zero-denominator."""

    def test_means_computed_correctly(self) -> None:
        scores = [
            _make_semantic_score(groundedness=0.6, answer_relevance=0.4,
                                 context_relevance=0.8, completeness=0.2),
            _make_semantic_score(groundedness=0.8, answer_relevance=0.6,
                                 context_relevance=0.4, completeness=0.6),
        ]
        report = _aggregate_scores(scores, threshold=0.7)
        assert report.mean_groundedness == pytest.approx(0.7)
        assert report.mean_answer_relevance == pytest.approx(0.5)
        assert report.mean_context_relevance == pytest.approx(0.6)
        assert report.mean_completeness == pytest.approx(0.4)

    def test_above_threshold_count(self) -> None:
        scores = [
            _make_semantic_score(0.8, 0.8, 0.8, 0.8),   # above 0.7
            _make_semantic_score(0.5, 0.5, 0.5, 0.5),   # below 0.7
        ]
        report = _aggregate_scores(scores, threshold=0.7)
        assert report.above_threshold_count == 1
        assert report.above_threshold_rate == pytest.approx(0.5)

    def test_threshold_boundary_inclusive(self) -> None:
        # exactly at threshold should count as above
        scores = [_make_semantic_score(0.7, 0.7, 0.7, 0.7)]
        report = _aggregate_scores(scores, threshold=0.7)
        assert report.above_threshold_count == 1

    def test_zero_examples_all_zero(self) -> None:
        report = _aggregate_scores([], threshold=0.7)
        assert report.total == 0
        assert report.mean_groundedness == 0.0
        assert report.mean_answer_relevance == 0.0
        assert report.above_threshold_count == 0
        assert report.above_threshold_rate == 0.0

    def test_all_below_threshold(self) -> None:
        scores = [_make_semantic_score(0.3, 0.3, 0.3, 0.3)]
        report = _aggregate_scores(scores, threshold=0.7)
        assert report.above_threshold_count == 0
        assert report.above_threshold_rate == 0.0

    def test_parse_failures_counted(self) -> None:
        scores = [
            _make_semantic_score(parse_failed=True),
            _make_semantic_score(parse_failed=False),
        ]
        report = _aggregate_scores(scores, threshold=0.7)
        assert report.parse_failure_count == 1

    def test_parse_failures_penalise_means(self) -> None:
        # One perfect score + one parse failure (all 0.0) → mean = 0.5
        scores = [
            _make_semantic_score(1.0, 1.0, 1.0, 1.0),
            _make_semantic_score(0.0, 0.0, 0.0, 0.0, parse_failed=True),
        ]
        report = _aggregate_scores(scores, threshold=0.7)
        assert report.mean_groundedness == pytest.approx(0.5)

    def test_threshold_stored_in_report(self) -> None:
        report = _aggregate_scores([], threshold=0.5)
        assert report.threshold == pytest.approx(0.5)

    def test_total_correct(self) -> None:
        scores = [_make_semantic_score() for _ in range(5)]
        report = _aggregate_scores(scores, threshold=0.7)
        assert report.total == 5

    def test_per_example_list_preserved(self) -> None:
        scores = [_make_semantic_score(example_id=f"ex-{i}") for i in range(3)]
        report = _aggregate_scores(scores, threshold=0.7)
        assert len(report.per_example) == 3


# --------------------------------------------------------------------------- #
# TestRunSemanticEvaluation                                                    #
# --------------------------------------------------------------------------- #


class TestRunSemanticEvaluation:
    """run_semantic_evaluation: injection, guard, scoring contract."""

    def test_returns_semantic_eval_report(self) -> None:
        ex = _make_example()
        resp = _make_response()
        report = run_semantic_evaluation(
            [ex], [resp], _judge=_fake_judge(_good_judge_output())
        )
        assert isinstance(report, SemanticEvalReport)

    def test_length_mismatch_raises_value_error(self) -> None:
        ex = _make_example()
        resp = _make_response()
        with pytest.raises(ValueError, match="same length"):
            run_semantic_evaluation([ex, ex], [resp])

    def test_too_many_responses_raises_value_error(self) -> None:
        ex = _make_example()
        resp = _make_response()
        with pytest.raises(ValueError, match="same length"):
            run_semantic_evaluation([ex], [resp, resp])

    def test_empty_inputs_returns_zero_report(self) -> None:
        report = run_semantic_evaluation([], [], _judge=_fake_judge("{}"))
        assert report.total == 0
        assert report.mean_groundedness == 0.0
        assert report.above_threshold_rate == 0.0

    def test_judge_called_once_per_example(self) -> None:
        call_count = {"n": 0}

        def _counting_judge(messages: list) -> str:  # noqa: ANN001
            call_count["n"] += 1
            return _good_judge_output()

        examples = [_make_example(f"q{i}") for i in range(3)]
        responses = [_make_response() for _ in range(3)]
        run_semantic_evaluation(examples, responses, _judge=_counting_judge)
        assert call_count["n"] == 3

    def test_scores_in_range(self) -> None:
        ex = _make_example()
        resp = _make_response()
        report = run_semantic_evaluation(
            [ex], [resp], _judge=_fake_judge(_good_judge_output())
        )
        s = report.per_example[0]
        assert 0.0 <= s.groundedness_score <= 1.0
        assert 0.0 <= s.answer_relevance_score <= 1.0
        assert 0.0 <= s.context_relevance_score <= 1.0
        assert 0.0 <= s.completeness_score <= 1.0

    def test_custom_threshold_stored(self) -> None:
        ex = _make_example()
        resp = _make_response()
        report = run_semantic_evaluation(
            [ex], [resp], _judge=_fake_judge(_good_judge_output()), threshold=0.5
        )
        assert report.threshold == pytest.approx(0.5)

    def test_parse_failure_surfaces_in_report(self) -> None:
        ex = _make_example()
        resp = _make_response()
        report = run_semantic_evaluation(
            [ex], [resp], _judge=_fake_judge("not valid json at all")
        )
        assert report.parse_failure_count == 1
        assert report.per_example[0].parse_failed is True
        assert report.per_example[0].judge_notes == "parse_error"

    def test_judge_exception_treated_as_parse_failure(self) -> None:
        def _broken_judge(messages: list) -> str:  # noqa: ANN001
            raise RuntimeError("Ollama unreachable")

        ex = _make_example()
        resp = _make_response()
        report = run_semantic_evaluation([ex], [resp], _judge=_broken_judge)
        assert report.parse_failure_count == 1
        assert report.per_example[0].parse_failed is True

    def test_context_built_from_supporting_chunks(self) -> None:
        """Judge receives chunk text in the messages."""
        received: dict = {}

        def _capturing_judge(messages: list) -> str:  # noqa: ANN001
            received["user"] = messages[1]["content"]
            return _good_judge_output()

        chunk = _make_chunk("unique_chunk_text_abc")
        resp = _make_response(supporting_chunks=[chunk])
        ex = _make_example()
        run_semantic_evaluation([ex], [resp], _judge=_capturing_judge)
        assert "unique_chunk_text_abc" in received["user"]

    def test_query_forwarded_to_judge(self) -> None:
        received: dict = {}

        def _capturing_judge(messages: list) -> str:  # noqa: ANN001
            received["user"] = messages[1]["content"]
            return _good_judge_output()

        ex = _make_example(query="unique_query_xyz")
        resp = _make_response()
        run_semantic_evaluation([ex], [resp], _judge=_capturing_judge)
        assert "unique_query_xyz" in received["user"]

    def test_semantic_report_separate_from_eval_report(self) -> None:
        """SemanticEvalReport is a distinct type from EvalReport."""
        from src.schema.eval_models import EvalReport
        ex = _make_example()
        resp = _make_response()
        report = run_semantic_evaluation(
            [ex], [resp], _judge=_fake_judge(_good_judge_output())
        )
        assert not isinstance(report, EvalReport)
        assert isinstance(report, SemanticEvalReport)

    def test_out_of_range_scores_clamped(self) -> None:
        raw = '{"groundedness": 1.5, "answer_relevance": -0.3, "context_relevance": 0.5, "completeness": 0.5}'
        ex = _make_example()
        resp = _make_response()
        report = run_semantic_evaluation([ex], [resp], _judge=_fake_judge(raw))
        s = report.per_example[0]
        assert s.groundedness_score == pytest.approx(1.0)
        assert s.answer_relevance_score == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# TestBuildContextText                                                         #
# --------------------------------------------------------------------------- #


class TestBuildContextText:
    """_build_context_text: context assembly from AnswerResponse."""

    def test_no_chunks_returns_placeholder(self) -> None:
        resp = _make_response(supporting_chunks=[])
        result = _build_context_text(resp)
        assert result == "[No context provided.]"

    def test_single_chunk_text_returned(self) -> None:
        chunk = _make_chunk("return policy text")
        resp = _make_response(supporting_chunks=[chunk])
        result = _build_context_text(resp)
        assert "return policy text" in result

    def test_multiple_chunks_joined(self) -> None:
        c1 = _make_chunk("first passage", chunk_id="c1")
        c2 = _make_chunk("second passage", chunk_id="c2")
        resp = _make_response(supporting_chunks=[c1, c2])
        result = _build_context_text(resp)
        assert "first passage" in result
        assert "second passage" in result


# --------------------------------------------------------------------------- #
# TestIntegrationGated                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    os.getenv("SEMANTIC_EVAL_INTEGRATION") != "1",
    reason="Set SEMANTIC_EVAL_INTEGRATION=1 to run live Ollama judge tests.",
)
class TestIntegrationGated:
    """
    Integration test — requires a running Ollama daemon.

    Enable with: $env:SEMANTIC_EVAL_INTEGRATION = "1"
    """

    def test_real_judge_returns_semantic_report(self) -> None:
        ex = _make_example(query="What is the refund policy?")
        resp = _make_response(
            answer_text="Customers can request a refund within 30 days of purchase.",
            supporting_chunks=[_make_chunk("Refunds are available within 30 days.")],
        )
        report = run_semantic_evaluation([ex], [resp])
        assert isinstance(report, SemanticEvalReport)
        assert report.total == 1
        # Scores may vary but must be in range
        s = report.per_example[0]
        assert 0.0 <= s.groundedness_score <= 1.0
        assert 0.0 <= s.answer_relevance_score <= 1.0
        assert 0.0 <= s.context_relevance_score <= 1.0
        assert 0.0 <= s.completeness_score <= 1.0
