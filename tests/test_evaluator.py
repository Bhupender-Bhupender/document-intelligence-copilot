"""
Tests for the deterministic evaluation harness.

Covers:
    - EvalExample: model defaults, required fields, expectation fields
    - EvalReport: field types, rate semantics
    - _compute_metrics: every metric in isolation
    - Source / file / page hit metrics with and without expectations
    - citations_all_valid metric
    - run_evaluation: pipeline injection, empty input, per_example contract
"""
from __future__ import annotations

from typing import List, Optional

import pytest

from src.schema.eval_models import EvalExample, EvalReport
from src.schema.models import AnswerResponse, CitationRecord, RetrievedChunk
from src.evaluation.evaluator import _compute_metrics, run_evaluation


# --------------------------------------------------------------------------- #
# Shared test fixtures                                                         #
# --------------------------------------------------------------------------- #

_QUERY = "What is the refund policy?"
_TEXT = "The refund policy is 30 days."


def _make_response(
    answer_text: str = _TEXT,
    sources: Optional[List[CitationRecord]] = None,
    supporting_chunks: Optional[List[RetrievedChunk]] = None,
    validation_flags: Optional[List[str]] = None,
) -> AnswerResponse:
    return AnswerResponse(
        query=_QUERY,
        answer_text=answer_text,
        model_used="test-model",
        sources=sources or [],
        supporting_chunks=supporting_chunks or [],
        validation_flags=validation_flags or [],
    )


def _make_citation(
    source_chunk_id: Optional[str] = "c1",
    file_name: str = "policy.txt",
    page_number: int = 1,
    status: str = "valid",
) -> CitationRecord:
    return CitationRecord(
        doc_id="doc1",
        file_name=file_name,
        page_number=page_number,
        quote_text="sample quote",
        source_chunk_id=source_chunk_id,
        validation_status=status,  # type: ignore[arg-type]
    )


def _make_chunk(chunk_id: str = "c1") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id="doc1",
        page_id="doc1-p1",
        file_name="policy.txt",
        page_number=1,
        text=_TEXT,
        word_count=len(_TEXT.split()),
    )


def _make_example(
    query: str = _QUERY,
    expected_source_chunk_ids: Optional[List[str]] = None,
    expected_file_names: Optional[List[str]] = None,
    expected_page_numbers: Optional[List[int]] = None,
    expect_non_empty_answer: bool = True,
    expect_citations_valid: bool = False,
) -> EvalExample:
    return EvalExample(
        query=query,
        expected_source_chunk_ids=expected_source_chunk_ids or [],
        expected_file_names=expected_file_names or [],
        expected_page_numbers=expected_page_numbers or [],
        expect_non_empty_answer=expect_non_empty_answer,
        expect_citations_valid=expect_citations_valid,
    )


def _fake_pipeline(response: AnswerResponse):
    """Return a callable that always returns the given response."""
    def _call(query: str) -> AnswerResponse:
        return response
    return _call


# --------------------------------------------------------------------------- #
# TestEvalExample                                                              #
# --------------------------------------------------------------------------- #


class TestEvalExample:
    """EvalExample model: defaults and field contracts."""

    def test_example_id_auto_generated(self) -> None:
        ex = EvalExample(query="test")
        assert isinstance(ex.example_id, str)
        assert len(ex.example_id) > 0

    def test_two_instances_have_different_ids(self) -> None:
        ex1 = EvalExample(query="test")
        ex2 = EvalExample(query="test")
        assert ex1.example_id != ex2.example_id

    def test_expected_lists_default_empty(self) -> None:
        ex = EvalExample(query="test")
        assert ex.expected_source_chunk_ids == []
        assert ex.expected_file_names == []
        assert ex.expected_page_numbers == []

    def test_expect_citations_valid_defaults_false(self) -> None:
        ex = EvalExample(query="test")
        assert ex.expect_citations_valid is False

    def test_expect_non_empty_answer_defaults_true(self) -> None:
        ex = EvalExample(query="test")
        assert ex.expect_non_empty_answer is True

    def test_notes_defaults_empty_string(self) -> None:
        ex = EvalExample(query="test")
        assert ex.notes == ""


# --------------------------------------------------------------------------- #
# TestEvalReport                                                               #
# --------------------------------------------------------------------------- #


class TestEvalReport:
    """EvalReport model: field types."""

    def _minimal_report(self) -> EvalReport:
        return EvalReport(
            total=1,
            answer_non_empty_count=1,
            answer_non_empty_rate=1.0,
            citation_valid_count=0,
            citation_valid_rate=0.0,
            invalid_citation_count=0,
            invalid_citation_rate=0.0,
            no_source_count=0,
            no_source_rate=0.0,
            no_supporting_chunk_count=0,
            no_supporting_chunk_rate=0.0,
            source_hit_count=0,
            source_hit_rate=0.0,
            file_hit_count=0,
            file_hit_rate=0.0,
            page_hit_count=0,
            page_hit_rate=0.0,
            citations_all_valid_count=0,
            citations_all_valid_rate=0.0,
            flag_frequency={},
            per_example=[],
        )

    def test_report_id_auto_generated(self) -> None:
        r = self._minimal_report()
        assert isinstance(r.report_id, str)
        assert len(r.report_id) > 0

    def test_rates_are_floats(self) -> None:
        r = self._minimal_report()
        assert isinstance(r.answer_non_empty_rate, float)
        assert isinstance(r.citation_valid_rate, float)
        assert isinstance(r.source_hit_rate, float)
        assert isinstance(r.citations_all_valid_rate, float)

    def test_flag_frequency_is_dict(self) -> None:
        r = self._minimal_report()
        assert isinstance(r.flag_frequency, dict)

    def test_per_example_is_list(self) -> None:
        r = self._minimal_report()
        assert isinstance(r.per_example, list)


# --------------------------------------------------------------------------- #
# TestMetricComputation                                                        #
# --------------------------------------------------------------------------- #


class TestMetricComputation:
    """_compute_metrics: total-based metrics in isolation."""

    def test_empty_input_returns_zero_counts(self) -> None:
        report = _compute_metrics([], [])
        assert report.total == 0
        assert report.answer_non_empty_count == 0
        assert report.answer_non_empty_rate == 0.0
        assert report.citation_valid_rate == 0.0
        assert report.source_hit_rate == 0.0

    def test_answer_non_empty_rate_all_non_empty(self) -> None:
        examples = [_make_example(), _make_example()]
        responses = [_make_response("answer one"), _make_response("answer two")]
        report = _compute_metrics(examples, responses)
        assert report.answer_non_empty_count == 2
        assert report.answer_non_empty_rate == 1.0

    def test_answer_non_empty_rate_all_empty(self) -> None:
        examples = [_make_example()]
        responses = [_make_response(answer_text="   ")]
        report = _compute_metrics(examples, responses)
        assert report.answer_non_empty_count == 0
        assert report.answer_non_empty_rate == 0.0

    def test_no_source_rate(self) -> None:
        examples = [_make_example(), _make_example()]
        responses = [_make_response(sources=[]), _make_response(sources=[_make_citation()])]
        report = _compute_metrics(examples, responses)
        assert report.no_source_count == 1
        assert report.no_source_rate == pytest.approx(0.5)

    def test_no_supporting_chunk_rate(self) -> None:
        examples = [_make_example()]
        responses = [_make_response(supporting_chunks=[])]
        report = _compute_metrics(examples, responses)
        assert report.no_supporting_chunk_count == 1
        assert report.no_supporting_chunk_rate == 1.0

    def test_citation_valid_rate(self) -> None:
        # 2 valid, 0 invalid → rate = 1.0
        examples = [_make_example()]
        c1 = _make_citation(status="valid")
        c2 = _make_citation(status="valid")
        responses = [_make_response(sources=[c1, c2])]
        report = _compute_metrics(examples, responses)
        assert report.citation_valid_count == 2
        assert report.citation_valid_rate == 1.0

    def test_invalid_citation_rate(self) -> None:
        # 1 valid, 1 invalid → invalid rate = 0.5
        examples = [_make_example()]
        c_valid = _make_citation(status="valid")
        c_invalid = _make_citation(status="invalid")
        responses = [_make_response(sources=[c_valid, c_invalid])]
        report = _compute_metrics(examples, responses)
        assert report.invalid_citation_count == 1
        assert report.invalid_citation_rate == pytest.approx(0.5)

    def test_flag_frequency_aggregated(self) -> None:
        examples = [_make_example(), _make_example()]
        responses = [
            _make_response(validation_flags=["no_sources", "no_supporting_chunks"]),
            _make_response(validation_flags=["no_sources"]),
        ]
        report = _compute_metrics(examples, responses)
        assert report.flag_frequency["no_sources"] == 2
        assert report.flag_frequency["no_supporting_chunks"] == 1


# --------------------------------------------------------------------------- #
# TestSourceFilePageHitMetrics                                                 #
# --------------------------------------------------------------------------- #


class TestSourceFilePageHitMetrics:
    """Restricted-denominator hit metrics."""

    def test_source_hit_when_chunk_id_present(self) -> None:
        ex = _make_example(expected_source_chunk_ids=["c1"])
        resp = _make_response(sources=[_make_citation(source_chunk_id="c1")])
        report = _compute_metrics([ex], [resp])
        assert report.source_hit_count == 1
        assert report.source_hit_rate == 1.0

    def test_source_miss_when_chunk_id_absent(self) -> None:
        ex = _make_example(expected_source_chunk_ids=["c99"])
        resp = _make_response(sources=[_make_citation(source_chunk_id="c1")])
        report = _compute_metrics([ex], [resp])
        assert report.source_hit_count == 0
        assert report.source_hit_rate == 0.0

    def test_source_hit_rate_zero_when_no_expectations(self) -> None:
        ex = _make_example()  # no expected_source_chunk_ids
        resp = _make_response(sources=[_make_citation(source_chunk_id="c1")])
        report = _compute_metrics([ex], [resp])
        assert report.source_hit_rate == 0.0

    def test_file_hit_when_file_name_present(self) -> None:
        ex = _make_example(expected_file_names=["policy.txt"])
        resp = _make_response(sources=[_make_citation(file_name="policy.txt")])
        report = _compute_metrics([ex], [resp])
        assert report.file_hit_count == 1
        assert report.file_hit_rate == 1.0

    def test_file_hit_rate_zero_when_no_expectations(self) -> None:
        ex = _make_example()  # no expected_file_names
        resp = _make_response(sources=[_make_citation(file_name="policy.txt")])
        report = _compute_metrics([ex], [resp])
        assert report.file_hit_rate == 0.0

    def test_page_hit_when_pair_present(self) -> None:
        ex = _make_example(
            expected_file_names=["policy.txt"],
            expected_page_numbers=[1],
        )
        resp = _make_response(
            sources=[_make_citation(file_name="policy.txt", page_number=1)]
        )
        report = _compute_metrics([ex], [resp])
        assert report.page_hit_count == 1
        assert report.page_hit_rate == 1.0

    def test_page_hit_miss_when_pair_absent(self) -> None:
        ex = _make_example(
            expected_file_names=["policy.txt"],
            expected_page_numbers=[5],
        )
        resp = _make_response(
            sources=[_make_citation(file_name="policy.txt", page_number=1)]
        )
        report = _compute_metrics([ex], [resp])
        assert report.page_hit_count == 0
        assert report.page_hit_rate == 0.0

    def test_page_hit_rate_zero_when_no_file_expectations(self) -> None:
        # page_numbers present but file_names absent → not scored
        ex = _make_example(
            expected_file_names=[],
            expected_page_numbers=[1],
        )
        resp = _make_response(
            sources=[_make_citation(file_name="policy.txt", page_number=1)]
        )
        report = _compute_metrics([ex], [resp])
        assert report.page_hit_rate == 0.0

    def test_page_hit_rate_zero_when_no_page_expectations(self) -> None:
        # file_names present but page_numbers absent → not scored for pages
        ex = _make_example(
            expected_file_names=["policy.txt"],
            expected_page_numbers=[],
        )
        resp = _make_response(
            sources=[_make_citation(file_name="policy.txt", page_number=1)]
        )
        report = _compute_metrics([ex], [resp])
        assert report.page_hit_rate == 0.0


# --------------------------------------------------------------------------- #
# TestCitationsAllValidMetric                                                  #
# --------------------------------------------------------------------------- #


class TestCitationsAllValidMetric:
    """citations_all_valid metric: restricted to expect_citations_valid=True."""

    def test_all_valid_when_all_citations_valid(self) -> None:
        ex = _make_example(expect_citations_valid=True)
        resp = _make_response(sources=[_make_citation(status="valid")])
        report = _compute_metrics([ex], [resp])
        assert report.citations_all_valid_count == 1
        assert report.citations_all_valid_rate == 1.0

    def test_not_counted_when_any_citation_invalid(self) -> None:
        ex = _make_example(expect_citations_valid=True)
        resp = _make_response(
            sources=[_make_citation(status="valid"), _make_citation(status="invalid")]
        )
        report = _compute_metrics([ex], [resp])
        assert report.citations_all_valid_count == 0
        assert report.citations_all_valid_rate == 0.0

    def test_rate_zero_when_no_examples_set_flag(self) -> None:
        ex = _make_example(expect_citations_valid=False)
        resp = _make_response(sources=[_make_citation(status="valid")])
        report = _compute_metrics([ex], [resp])
        assert report.citations_all_valid_rate == 0.0

    def test_empty_sources_not_counted_as_all_valid(self) -> None:
        # Empty source list: cannot confirm all citations are valid
        ex = _make_example(expect_citations_valid=True)
        resp = _make_response(sources=[])
        report = _compute_metrics([ex], [resp])
        assert report.citations_all_valid_count == 0
        assert report.citations_all_valid_rate == 0.0

    def test_mixed_batch_rate(self) -> None:
        # 2 examples with expect_citations_valid=True; 1 passes, 1 fails
        ex1 = _make_example(expect_citations_valid=True)
        ex2 = _make_example(expect_citations_valid=True)
        resp1 = _make_response(sources=[_make_citation(status="valid")])
        resp2 = _make_response(sources=[_make_citation(status="invalid")])
        report = _compute_metrics([ex1, ex2], [resp1, resp2])
        assert report.citations_all_valid_count == 1
        assert report.citations_all_valid_rate == pytest.approx(0.5)


# --------------------------------------------------------------------------- #
# TestRunEvaluation                                                            #
# --------------------------------------------------------------------------- #


class TestRunEvaluation:
    """run_evaluation: pipeline injection, contract, per_example."""

    def test_returns_eval_report(self) -> None:
        ex = _make_example()
        resp = _make_response()
        report = run_evaluation([ex], _pipeline=_fake_pipeline(resp))
        assert isinstance(report, EvalReport)

    def test_pipeline_called_once_per_example(self) -> None:
        calls: list = []

        def counting_pipeline(query: str) -> AnswerResponse:
            calls.append(query)
            return _make_response()

        examples = [_make_example(), _make_example(), _make_example()]
        run_evaluation(examples, _pipeline=counting_pipeline)
        assert len(calls) == 3

    def test_empty_example_list_returns_zero_total(self) -> None:
        report = run_evaluation([], _pipeline=lambda q: _make_response())
        assert report.total == 0
        assert report.answer_non_empty_rate == 0.0

    def test_per_example_length_matches_input(self) -> None:
        examples = [_make_example(), _make_example()]
        resp = _make_response()
        report = run_evaluation(examples, _pipeline=_fake_pipeline(resp))
        assert len(report.per_example) == 2

    def test_per_example_contains_required_keys(self) -> None:
        ex = _make_example()
        resp = _make_response()
        report = run_evaluation([ex], _pipeline=_fake_pipeline(resp))
        entry = report.per_example[0]
        for key in ("example_id", "query", "answer_non_empty",
                    "source_count", "supporting_chunk_count", "validation_flags"):
            assert key in entry

    def test_mixed_batch_total_and_rates(self) -> None:
        # 2 examples: one with valid citation, one with empty sources
        ex1 = _make_example(expected_source_chunk_ids=["c1"])
        ex2 = _make_example()
        resp1 = _make_response(sources=[_make_citation(source_chunk_id="c1", status="valid")])
        resp2 = _make_response(sources=[], answer_text="")

        responses_iter = iter([resp1, resp2])
        report = run_evaluation(
            [ex1, ex2],
            _pipeline=lambda q: next(responses_iter),
        )

        assert report.total == 2
        assert report.answer_non_empty_count == 1
        assert report.answer_non_empty_rate == pytest.approx(0.5)
        assert report.no_source_count == 1
        assert report.source_hit_count == 1
        assert report.source_hit_rate == 1.0  # 1/1 example with expectations
