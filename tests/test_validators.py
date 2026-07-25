"""
Tests for rule-based citation and response validation.

Covers:
    - _validate_citation: all 7 rules, section-title conditionality,
      verbatim span checks
    - _build_flags: each response-level flag in isolation
    - validate_response: contract, immutability, end-to-end
    - Edge cases: empty sources, empty supporting_chunks
    - Pipeline integration: Stage 6 wired correctly, _validator injection
"""
from __future__ import annotations

from typing import List, Optional

import pytest

from src.schema.models import AnswerResponse, CitationRecord, RetrievedChunk
from src.validation.validators import _build_flags, _validate_citation, validate_response


# --------------------------------------------------------------------------- #
# Shared fixtures                                                              #
# --------------------------------------------------------------------------- #

_TEXT = "Sample passage for validation testing."
_TEXT_LEN = len(_TEXT)  # 38


def _make_chunk(
    chunk_id: str = "c1",
    doc_id: str = "doc1",
    file_name: str = "report.txt",
    page_number: int = 1,
    text: str = _TEXT,
    section_title: Optional[str] = None,
) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id=f"{doc_id}-p{page_number}",
        file_name=file_name,
        page_number=page_number,
        text=text,
        word_count=len(text.split()),
        section_title=section_title,
    )


def _make_citation(
    source_chunk_id: Optional[str] = "c1",
    doc_id: str = "doc1",
    file_name: str = "report.txt",
    page_number: int = 1,
    quote_text: str = _TEXT,
    quote_start_char: Optional[int] = 0,
    quote_end_char: Optional[int] = None,
    is_verbatim: bool = True,
    section_title: Optional[str] = None,
) -> CitationRecord:
    end = quote_end_char if quote_end_char is not None else len(quote_text)
    return CitationRecord(
        source_chunk_id=source_chunk_id,
        doc_id=doc_id,
        file_name=file_name,
        page_number=page_number,
        quote_text=quote_text,
        quote_start_char=quote_start_char,
        quote_end_char=end,
        is_verbatim=is_verbatim,
        section_title=section_title,
    )


def _make_response(
    sources: Optional[List[CitationRecord]] = None,
    supporting_chunks: Optional[List[RetrievedChunk]] = None,
    answer_text: str = "Generated answer.",
) -> AnswerResponse:
    return AnswerResponse(
        query="What is this about?",
        answer_text=answer_text,
        model_used="test-model",
        sources=sources or [],
        supporting_chunks=supporting_chunks or [],
    )


# --------------------------------------------------------------------------- #
# TestCitationValidation                                                       #
# --------------------------------------------------------------------------- #


class TestCitationValidation:
    """Unit tests for _validate_citation (all 7 rules)."""

    def test_all_rules_pass_returns_valid(self) -> None:
        chunk = _make_chunk()
        citation = _make_citation()
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "valid"

    def test_missing_source_chunk_id_is_invalid(self) -> None:
        # Rule 1: source_chunk_id is None
        chunk = _make_chunk()
        citation = _make_citation(source_chunk_id=None)
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_unmatched_chunk_id_is_invalid(self) -> None:
        # Rule 2: source_chunk_id not in lookup
        chunk = _make_chunk(chunk_id="c1")
        citation = _make_citation(source_chunk_id="does_not_exist")
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_doc_id_mismatch_is_invalid(self) -> None:
        # Rule 3
        chunk = _make_chunk(doc_id="doc1")
        citation = _make_citation(doc_id="doc_WRONG")
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_file_name_mismatch_is_invalid(self) -> None:
        # Rule 4
        chunk = _make_chunk(file_name="report.txt")
        citation = _make_citation(file_name="other_file.txt")
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_page_number_mismatch_is_invalid(self) -> None:
        # Rule 5
        chunk = _make_chunk(page_number=1)
        citation = _make_citation(page_number=99)
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_verbatim_span_out_of_bounds_is_invalid(self) -> None:
        # Rule 7a: end > len(text)
        chunk = _make_chunk()
        citation = _make_citation(
            quote_start_char=0,
            quote_end_char=_TEXT_LEN + 100,
            is_verbatim=True,
        )
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_verbatim_negative_start_is_invalid(self) -> None:
        # Rule 7a: start < 0
        chunk = _make_chunk()
        citation = _make_citation(
            quote_start_char=-1,
            quote_end_char=_TEXT_LEN,
            is_verbatim=True,
        )
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_verbatim_slice_mismatch_is_invalid(self) -> None:
        # Rule 7b: slice text != quote_text
        chunk = _make_chunk()
        citation = _make_citation(
            quote_text="WRONG TEXT",
            quote_start_char=0,
            quote_end_char=len("WRONG TEXT"),
            is_verbatim=True,
        )
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_not_verbatim_skips_span_check(self) -> None:
        # is_verbatim=False → span rules not applied even with mismatched quote
        chunk = _make_chunk()
        citation = _make_citation(
            quote_text="completely wrong text",
            quote_start_char=0,
            quote_end_char=_TEXT_LEN + 500,  # out of bounds — but not checked
            is_verbatim=False,
        )
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "valid"


# --------------------------------------------------------------------------- #
# TestSectionTitleValidation                                                   #
# --------------------------------------------------------------------------- #


class TestSectionTitleValidation:
    """Rule 6: conditional section_title check."""

    def test_both_match_returns_valid(self) -> None:
        chunk = _make_chunk(section_title="Introduction")
        citation = _make_citation(section_title="Introduction")
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "valid"

    def test_both_present_mismatch_is_invalid(self) -> None:
        chunk = _make_chunk(section_title="Introduction")
        citation = _make_citation(section_title="Conclusion")
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "invalid"

    def test_citation_section_title_none_skips_check(self) -> None:
        # citation.section_title is None → rule 6 not fired regardless of chunk
        chunk = _make_chunk(section_title="Introduction")
        citation = _make_citation(section_title=None)
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "valid"

    def test_chunk_section_title_none_skips_check(self) -> None:
        # chunk.section_title is None → rule 6 not fired
        chunk = _make_chunk(section_title=None)
        citation = _make_citation(section_title="Introduction")
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "valid"


# --------------------------------------------------------------------------- #
# TestResponseFlags                                                            #
# --------------------------------------------------------------------------- #


class TestResponseFlags:
    """Unit tests for _build_flags — each flag exercised in isolation."""

    def test_no_supporting_chunks_flag(self) -> None:
        response = _make_response(sources=[], supporting_chunks=[])
        flags = _build_flags(response, [])
        assert "no_supporting_chunks" in flags

    def test_no_sources_flag(self) -> None:
        chunk = _make_chunk()
        response = _make_response(sources=[], supporting_chunks=[chunk])
        flags = _build_flags(response, [])
        assert "no_sources" in flags

    def test_citation_chunk_count_mismatch_flag(self) -> None:
        # 1 citation, 2 supporting chunks
        chunk1 = _make_chunk(chunk_id="c1")
        chunk2 = _make_chunk(chunk_id="c2")
        citation = _make_citation(source_chunk_id="c1")
        response = _make_response(
            sources=[citation],
            supporting_chunks=[chunk1, chunk2],
        )
        # validated: citation is valid (matches c1)
        lookup = {"c1": chunk1, "c2": chunk2}
        validated = [_validate_citation(citation, lookup)]
        flags = _build_flags(response, validated)
        assert "citation_chunk_count_mismatch" in flags

    def test_missing_source_chunk_id_flag(self) -> None:
        chunk = _make_chunk()
        citation = _make_citation(source_chunk_id=None)
        response = _make_response(
            sources=[citation],
            supporting_chunks=[chunk],
        )
        validated = [_validate_citation(citation, {"c1": chunk})]
        flags = _build_flags(response, validated)
        assert "missing_source_chunk_id" in flags

    def test_invalid_citation_present_flag(self) -> None:
        chunk = _make_chunk(doc_id="doc1")
        citation = _make_citation(doc_id="WRONG_DOC")
        response = _make_response(
            sources=[citation],
            supporting_chunks=[chunk],
        )
        validated = [_validate_citation(citation, {"c1": chunk})]
        flags = _build_flags(response, validated)
        assert "invalid_citation_present" in flags

    def test_clean_response_produces_no_flags(self) -> None:
        chunk = _make_chunk()
        citation = _make_citation()
        response = _make_response(
            sources=[citation],
            supporting_chunks=[chunk],
        )
        validated = [_validate_citation(citation, {"c1": chunk})]
        flags = _build_flags(response, validated)
        assert flags == []


# --------------------------------------------------------------------------- #
# TestValidateResponseContract                                                 #
# --------------------------------------------------------------------------- #


class TestValidateResponseContract:
    """validate_response: output type, immutability, field invariants."""

    def test_returns_answer_response(self) -> None:
        chunk = _make_chunk()
        citation = _make_citation()
        response = _make_response(sources=[citation], supporting_chunks=[chunk])
        result = validate_response(response)
        assert isinstance(result, AnswerResponse)

    def test_sources_all_have_set_validation_status(self) -> None:
        chunk = _make_chunk()
        citation = _make_citation()
        response = _make_response(sources=[citation], supporting_chunks=[chunk])
        result = validate_response(response)
        assert all(
            c.validation_status in ("valid", "invalid")
            for c in result.sources
        )

    def test_validation_flags_is_list_of_strings(self) -> None:
        chunk = _make_chunk()
        citation = _make_citation()
        response = _make_response(sources=[citation], supporting_chunks=[chunk])
        result = validate_response(response)
        assert isinstance(result.validation_flags, list)
        assert all(isinstance(f, str) for f in result.validation_flags)

    def test_original_response_not_mutated(self) -> None:
        chunk = _make_chunk()
        citation = _make_citation()
        response = _make_response(sources=[citation], supporting_chunks=[chunk])
        # pre-validation status must still be "unverified" in the original
        assert citation.validation_status == "unverified"
        validate_response(response)
        # original object unchanged
        assert response.sources[0].validation_status == "unverified"


# --------------------------------------------------------------------------- #
# TestEdgeCases                                                                #
# --------------------------------------------------------------------------- #


class TestEdgeCases:
    """Boundary conditions: empty inputs, non-verbatim citations."""

    def test_empty_sources_no_crash(self) -> None:
        chunk = _make_chunk()
        response = _make_response(sources=[], supporting_chunks=[chunk])
        result = validate_response(response)
        assert result.sources == []
        assert isinstance(result.validation_flags, list)

    def test_empty_supporting_chunks_citations_become_invalid(self) -> None:
        # No chunks in lookup → every citation hits rule 2
        citation = _make_citation(source_chunk_id="c1")
        response = _make_response(sources=[citation], supporting_chunks=[])
        result = validate_response(response)
        assert result.sources[0].validation_status == "invalid"

    def test_span_none_not_verbatim_is_still_valid(self) -> None:
        # Non-verbatim citation with None span coords should not trigger rule 7
        chunk = _make_chunk()
        citation = _make_citation(
            quote_start_char=None,
            quote_end_char=None,
            is_verbatim=False,
        )
        result = _validate_citation(citation, {"c1": chunk})
        assert result.validation_status == "valid"


# --------------------------------------------------------------------------- #
# TestPipelineValidation                                                       #
# --------------------------------------------------------------------------- #


class TestPipelineValidation:
    """Integration tests: Stage 6 wired into run_pipeline."""

    @staticmethod
    def _run_fake(
        chunks: Optional[List[RetrievedChunk]] = None,
        validator=None,
    ) -> AnswerResponse:
        from src.generation.answer_pipeline import run_pipeline

        if chunks is None:
            chunks = [_make_chunk()]

        kw = {}
        if validator is not None:
            kw["_validator"] = validator

        return run_pipeline(
            query="What is this about?",
            _retriever=lambda q: chunks,
            _reranker=lambda q, cs: cs,
            _parent_lookup=lambda cs: [None] * len(cs),
            _generator=lambda msgs: "Generated answer.",
            **kw,
        )

    def test_pipeline_sources_have_validation_status_set(self) -> None:
        result = self._run_fake()
        assert all(
            c.validation_status in ("valid", "invalid")
            for c in result.sources
        )

    def test_pipeline_flags_empty_for_valid_run(self) -> None:
        # 1 chunk, 1 matching citation → all valid, no flags
        result = self._run_fake()
        assert result.validation_flags == []

    def test_validator_injection_is_called(self) -> None:
        calls: List[AnswerResponse] = []

        def capture_validator(resp: AnswerResponse) -> AnswerResponse:
            calls.append(resp)
            return resp

        self._run_fake(validator=capture_validator)
        assert len(calls) == 1

    def test_phase7a_style_citation_passes_all_rules(self) -> None:
        """Citations from citation_builder (quote_text=chunk.text, start=0,
        end=len) must survive all 7 validation rules with status='valid'."""
        chunk = _make_chunk(
            chunk_id="c1",
            doc_id="doc1",
            file_name="report.txt",
            page_number=1,
            text=_TEXT,
        )
        result = self._run_fake(chunks=[chunk])
        assert len(result.sources) == 1
        assert result.sources[0].validation_status == "valid"
        assert result.validation_flags == []
