"""
Tests for the OCR routing module (src/ocr/ocr_router.py).

Design: all tests that exercise OCR-recovery logic use unittest.mock to
patch parse_with_docling. This isolates the router's orchestration logic
from Docling's model loading — no warm-up cost, no sample-PDF dependency.

One integration-path smoke test uses the real router → real PDF to confirm
the two-pass wiring in src/ingestion/router.py does not break the born-digital
path (which should complete after Pass 1 with no OCR invoked).

Test classes:
    TestNoOp                — zero empty pages → function is a no-op
    TestPageIdentityPreserved — page_id, doc_id, page_number never changed
    TestListLengthInvariant — output always same length as input
    TestNonEmptyPagesUnchanged — "ok" and "weak" pages left untouched
    TestOcrRecovery         — empty pages updated with correct field values
    TestOcrMetadataRules    — parse_method, ocr_engine, ocr_confidence rules
    TestDoclingFailure      — Docling crash → original pages returned unchanged
    TestRouterIntegration   — end-to-end PDF path via route_file (no OCR expected)
"""
from __future__ import annotations

from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from src.ocr.ocr_router import route_pdf_pages_through_ocr
from src.schema.models import ParsedPage, RawDocument

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SAMPLE_DIR = _PROJECT_ROOT / "docs" / "sample_docs"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raw_doc(**kwargs) -> RawDocument:
    """Minimal RawDocument for testing."""
    defaults = dict(
        source_path="/tmp/test.pdf",
        file_name="test.pdf",
        file_type="pdf",
        byte_size=1024,
        checksum="a" * 64,
    )
    defaults.update(kwargs)
    return RawDocument(**defaults)


def _parsed_page(
    doc_id: str,
    page_number: int = 1,
    status: str = "ok",
    word_count: int = 100,
    parse_method: str = "pypdf",
) -> ParsedPage:
    """Minimal ParsedPage for testing."""
    text = "word " * word_count if word_count else ""
    norm = text.strip()
    return ParsedPage(
        doc_id=doc_id,
        page_number=page_number,
        raw_text=text,
        normalized_text=norm,
        word_count=word_count,
        char_count=len(norm),
        parse_method=parse_method,
        extraction_status=status,
    )


def _docling_page(doc_id: str, page_number: int, word_count: int = 50) -> ParsedPage:
    """Simulate a ParsedPage as returned by parse_with_docling for a recovered page."""
    text = "recovered " * word_count
    norm = text.strip()
    return ParsedPage(
        doc_id=doc_id,
        page_number=page_number,
        raw_text=text,
        normalized_text=norm,
        word_count=word_count,
        char_count=len(norm),
        parse_method="docling",   # docling_parser always sets "docling"
        extraction_status="ok",
        section_title="Test Section",
    )


# ---------------------------------------------------------------------------
# TestNoOp — zero empty pages
# ---------------------------------------------------------------------------


class TestNoOp:
    """When no pages have extraction_status == 'empty', the router is a no-op."""

    def test_all_ok_pages_returns_same_list(self):
        """No empty pages → the returned list object is the same instance."""
        doc = _raw_doc()
        pages = [
            _parsed_page(doc.doc_id, page_number=1, status="ok"),
            _parsed_page(doc.doc_id, page_number=2, status="ok"),
        ]
        result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, pages)
        assert result is pages  # identity — no copy made

    def test_weak_pages_not_routed(self):
        """'weak' pages are not OCR candidates in this step — returned unchanged."""
        doc = _raw_doc()
        pages = [
            _parsed_page(doc.doc_id, page_number=1, status="weak", word_count=5),
        ]
        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, pages)
        mock_run_local.assert_not_called()
        assert result[0].extraction_status == "weak"

    def test_empty_page_list(self):
        """Zero pages → no OCR invoked, empty list returned."""
        doc = _raw_doc()
        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [])
        mock_run_local.assert_not_called()
        assert result == []


# ---------------------------------------------------------------------------
# TestPageIdentityPreserved
# ---------------------------------------------------------------------------


class TestPageIdentityPreserved:
    """page_id, doc_id, and page_number must survive an OCR pass."""

    def test_identity_fields_unchanged_after_ocr(self):
        doc = _raw_doc()
        original_page = _parsed_page(doc.doc_id, page_number=3, status="empty", word_count=0)
        original_page_id = original_page.page_id

        docling_result = _docling_page(doc.doc_id, page_number=3)
        raw_doc_mock = _raw_doc()

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (raw_doc_mock, [docling_result])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [original_page])

        updated = result[0]
        assert updated.page_id == original_page_id, "page_id must not change"
        assert updated.doc_id == doc.doc_id, "doc_id must not change"
        assert updated.page_number == 3, "page_number must not change"

    def test_non_routed_pages_identity_unchanged(self):
        """Non-empty pages that are not routed through OCR keep all their fields."""
        doc = _raw_doc()
        ok_page = _parsed_page(doc.doc_id, page_number=1, status="ok")
        empty_page = _parsed_page(doc.doc_id, page_number=2, status="empty", word_count=0)
        ok_page_id = ok_page.page_id

        docling_result = _docling_page(doc.doc_id, page_number=2)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [
                _docling_page(doc.doc_id, page_number=1),  # Docling also has page 1
                docling_result,
            ])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [ok_page, empty_page])

        # The ok_page object at index 0 must be the same instance (not a copy).
        assert result[0] is ok_page
        assert result[0].page_id == ok_page_id


# ---------------------------------------------------------------------------
# TestListLengthInvariant
# ---------------------------------------------------------------------------


class TestListLengthInvariant:
    """Output list is always the same length as the input list."""

    def test_length_preserved_no_ocr(self):
        doc = _raw_doc()
        pages = [_parsed_page(doc.doc_id, page_number=i, status="ok") for i in range(1, 6)]
        result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, pages)
        assert len(result) == len(pages)

    def test_length_preserved_with_ocr(self):
        doc = _raw_doc()
        pages = [
            _parsed_page(doc.doc_id, page_number=1, status="ok"),
            _parsed_page(doc.doc_id, page_number=2, status="empty", word_count=0),
            _parsed_page(doc.doc_id, page_number=3, status="ok"),
        ]
        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [_docling_page(doc.doc_id, page_number=2)])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, pages)
        assert len(result) == 3

    def test_length_preserved_docling_fails(self):
        doc = _raw_doc()
        pages = [
            _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0),
        ]
        with patch("src.ocr.ocr_router._run_local_ocr", side_effect=RuntimeError("boom")):
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, pages)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# TestNonEmptyPagesUnchanged
# ---------------------------------------------------------------------------


class TestNonEmptyPagesUnchanged:
    """'ok' and 'weak' pages must not be modified even when OCR pass runs."""

    def test_ok_page_not_touched(self):
        doc = _raw_doc()
        ok_page = _parsed_page(doc.doc_id, page_number=1, status="ok", word_count=200)
        empty_page = _parsed_page(doc.doc_id, page_number=2, status="empty", word_count=0)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [_docling_page(doc.doc_id, page_number=2)])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [ok_page, empty_page])

        assert result[0] is ok_page
        assert result[0].word_count == 200
        assert result[0].parse_method == "pypdf"
        assert result[0].extraction_status == "ok"

    def test_weak_page_not_touched(self):
        doc = _raw_doc()
        weak_page = _parsed_page(doc.doc_id, page_number=1, status="weak", word_count=5)
        empty_page = _parsed_page(doc.doc_id, page_number=2, status="empty", word_count=0)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [_docling_page(doc.doc_id, page_number=2)])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [weak_page, empty_page])

        assert result[0] is weak_page
        assert result[0].word_count == 5
        assert result[0].parse_method == "pypdf"
        assert result[0].extraction_status == "weak"


# ---------------------------------------------------------------------------
# TestOcrRecovery
# ---------------------------------------------------------------------------


class TestOcrRecovery:
    """Verify the OCR-routed page has correct updated content."""

    def test_text_fields_updated_on_recovery(self):
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0)

        docling_result = _docling_page(doc.doc_id, page_number=1, word_count=30)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [docling_result])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        updated = result[0]
        assert updated.word_count > 0, "recovered page must have word_count > 0"
        assert updated.char_count > 0
        assert updated.raw_text.strip() != ""
        assert updated.normalized_text.strip() != ""

    def test_extraction_status_updated(self):
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0)
        docling_result = _docling_page(doc.doc_id, page_number=1, word_count=30)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [docling_result])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        assert result[0].extraction_status in ("ok", "weak")

    def test_docling_returns_no_matching_page(self):
        """If Docling has no page for the empty page number, it stays empty."""
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=5, status="empty", word_count=0)

        # Docling returns page 1 only — page 5 is missing.
        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [_docling_page(doc.doc_id, page_number=1)])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        assert result[0].extraction_status == "empty"
        assert result[0].parse_method == "pypdf"  # not overwritten


# ---------------------------------------------------------------------------
# TestOcrMetadataRules
# ---------------------------------------------------------------------------


class TestOcrMetadataRules:
    """Verify OCR metadata fields are set exactly as specified."""

    def test_parse_method_set_to_rapidocr(self):
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0)
        docling_result = _docling_page(doc.doc_id, page_number=1)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [docling_result])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        assert result[0].parse_method == "rapidocr"

    def test_ocr_engine_set_to_rapidocr(self):
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0)
        docling_result = _docling_page(doc.doc_id, page_number=1)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [docling_result])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        assert result[0].ocr_engine == "rapidocr"

    def test_ocr_confidence_is_none(self):
        """ocr_confidence must be None — RapidOCR confidence not exposed by Docling API."""
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0)
        docling_result = _docling_page(doc.doc_id, page_number=1)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [docling_result])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        assert result[0].ocr_confidence is None, (
            "ocr_confidence must remain None: Docling/RapidOCR does not expose "
            "per-page confidence through its public API."
        )

    def test_non_routed_pages_have_no_ocr_metadata(self):
        """Pages that were not OCR-routed must not have ocr_engine or ocr_confidence set."""
        doc = _raw_doc()
        ok_page = _parsed_page(doc.doc_id, page_number=1, status="ok")
        empty_page = _parsed_page(doc.doc_id, page_number=2, status="empty", word_count=0)

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [_docling_page(doc.doc_id, page_number=2)])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [ok_page, empty_page])

        assert result[0].ocr_engine is None
        assert result[0].ocr_confidence is None

    def test_section_title_propagated_from_docling(self):
        """section_title from Docling is propagated to the recovered page."""
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0)
        docling_result = _docling_page(doc.doc_id, page_number=1)
        assert docling_result.section_title == "Test Section"  # set in helper

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [docling_result])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        assert result[0].section_title == "Test Section"

    def test_section_title_none_when_docling_has_none(self):
        """section_title stays None if Docling found no heading for the page."""
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0)

        docling_result = _docling_page(doc.doc_id, page_number=1)
        docling_result = docling_result.model_copy(update={"section_title": None})

        with patch("src.ocr.ocr_router._run_local_ocr") as mock_run_local:
            mock_run_local.return_value = (_raw_doc(), [docling_result])
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        assert result[0].section_title is None


# ---------------------------------------------------------------------------
# TestDoclingFailure
# ---------------------------------------------------------------------------


class TestDoclingFailure:
    """When Docling raises, the original pages are returned unchanged."""

    def test_docling_crash_returns_original_pages(self):
        doc = _raw_doc()
        empty_page = _parsed_page(doc.doc_id, page_number=1, status="empty", word_count=0)
        original_page_id = empty_page.page_id

        with patch("src.ocr.ocr_router._run_local_ocr", side_effect=RuntimeError("crash")):
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, [empty_page])

        assert len(result) == 1
        assert result[0].page_id == original_page_id
        assert result[0].extraction_status == "empty"
        assert result[0].parse_method == "pypdf"

    def test_docling_crash_does_not_raise(self):
        """Docling failure must not propagate as an exception to callers."""
        doc = _raw_doc()
        pages = [_parsed_page(doc.doc_id, status="empty", word_count=0)]

        with patch("src.ocr.ocr_router._run_local_ocr", side_effect=Exception("boom")):
            # Must not raise.
            result = route_pdf_pages_through_ocr(Path("test.pdf"), doc, pages)

        assert result is not None


# ---------------------------------------------------------------------------
# TestRouterIntegration — real route_file(.pdf), no mocking
# ---------------------------------------------------------------------------


_PDF_SAMPLE = _SAMPLE_DIR / "Operations_report.pdf"


@pytest.mark.skipif(
    not _PDF_SAMPLE.exists(),
    reason="Operations_report.pdf not present in docs/sample_docs/",
)
class TestRouterIntegration:
    """
    End-to-end smoke test using the real router and a born-digital PDF.

    For a born-digital PDF (all pages have extractable text), the OCR router
    must be a no-op: parse_with_docling is never called for OCR recovery.
    """

    def test_born_digital_pdf_skips_ocr(self):
        """Born-digital PDF: all pages "ok" → OCR router is never invoked."""
        from src.ingestion.router import route_file

        with patch("src.ingestion.router.route_pdf_pages_through_ocr",
                   wraps=route_pdf_pages_through_ocr) as spy:
            doc, pages = route_file(_PDF_SAMPLE)

        # The router was called (it's always called for PDFs).
        spy.assert_called_once()
        # But parse_with_docling was not called inside it (no empty pages).
        # We verify this indirectly: all pages have parse_method="pypdf".
        assert all(p.parse_method == "pypdf" for p in pages), (
            "Born-digital PDF pages should remain parse_method='pypdf' — "
            "OCR recovery should not have run."
        )

    def test_router_returns_raw_document(self):
        """route_file returns a valid RawDocument for the PDF."""
        from src.ingestion.router import route_file

        doc, pages = route_file(_PDF_SAMPLE)
        assert isinstance(doc, RawDocument)
        assert doc.file_type == "pdf"
        assert len(pages) >= 1

    def test_router_pages_have_valid_doc_id(self):
        """All pages share the RawDocument's doc_id."""
        from src.ingestion.router import route_file

        doc, pages = route_file(_PDF_SAMPLE)
        assert all(p.doc_id == doc.doc_id for p in pages)
