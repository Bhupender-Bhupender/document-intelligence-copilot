"""
Smoke tests for the ingestion layer.

Covers the router, text reader, PDF reader (born-digital and complex),
and unsupported format handling.

Tests use the actual sample documents in docs/sample_docs/. Tests that
depend on a specific file will skip gracefully if the file is missing.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.ingestion.router import UnsupportedFormatError, route_file
from src.schema.models import ParsedPage, RawDocument

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SAMPLE_DIR = _PROJECT_ROOT / "docs" / "sample_docs"


def _sample(name: str) -> Path:
    return _SAMPLE_DIR / name


# --------------------------------------------------------------------------- #
# Text and Markdown                                                            #
# --------------------------------------------------------------------------- #


class TestTextIngestion:
    def test_txt_contract(self):
        """Route a .txt file; expect 1 RawDocument + 1 ParsedPage."""
        doc, pages = route_file(_sample("company_policy.txt"))

        assert isinstance(doc, RawDocument)
        assert doc.file_type == "txt"
        assert doc.file_name == "company_policy.txt"
        assert doc.total_pages == 1
        assert doc.byte_size > 0
        assert len(doc.checksum) == 64  # SHA-256 hex

        assert len(pages) == 1
        page = pages[0]
        assert isinstance(page, ParsedPage)
        assert page.doc_id == doc.doc_id
        assert page.page_number == 1
        assert page.parse_method == "text_read"
        assert page.extraction_status == "ok"
        assert page.word_count > 0
        assert page.layout_blocks == []

    def test_md_contract(self):
        """Route a .md file; expect the same contract as .txt."""
        doc, pages = route_file(_sample("quarterly_summary.md"))

        assert isinstance(doc, RawDocument)
        assert doc.file_type == "md"
        assert len(pages) == 1
        page = pages[0]
        assert page.parse_method == "text_read"
        assert page.extraction_status == "ok"
        assert page.doc_id == doc.doc_id

    def test_page_text_is_non_empty_for_known_files(self):
        """The sample txt file is known to have content."""
        _, pages = route_file(_sample("company_policy.txt"))
        assert pages[0].word_count > 10
        assert "policy" in pages[0].normalized_text.lower()


# --------------------------------------------------------------------------- #
# Born-digital PDF                                                             #
# --------------------------------------------------------------------------- #


class TestPdfIngestion:
    def test_born_digital_pdf_contract(self):
        """Route a born-digital PDF; expect 1 RawDocument + N ParsedPages."""
        path = _sample("Operations_report.pdf")
        if not path.exists():
            pytest.skip("Operations_report.pdf not found in sample_docs")

        doc, pages = route_file(path)

        assert isinstance(doc, RawDocument)
        assert doc.file_type == "pdf"
        assert doc.total_pages is not None
        assert doc.total_pages >= 1
        assert len(pages) == doc.total_pages

        # All pages link back to the same document.
        for page in pages:
            assert page.doc_id == doc.doc_id
            assert page.parse_method == "pypdf"
            assert page.extraction_status in ("ok", "weak", "empty")

        # A born-digital PDF should have at least one page with ok/weak text.
        readable = [p for p in pages if p.extraction_status in ("ok", "weak")]
        assert len(readable) > 0

    def test_complex_pdf_does_not_crash(self):
        """
        prof-services-agrmt.pdf may be scanned or have complex layout.
        The reader must not raise; it should return valid ParsedPage records
        with extraction_status "weak" or "empty" where text cannot be extracted.
        """
        path = _sample("prof-services-agrmt.pdf")
        if not path.exists():
            pytest.skip("prof-services-agrmt.pdf not found in sample_docs")

        doc, pages = route_file(path)

        assert isinstance(doc, RawDocument)
        assert doc.file_type == "pdf"
        # Pages may be empty (unreadable) or contain weak/empty records.
        # Either outcome is valid — the important thing is no exception.
        for page in pages:
            assert page.doc_id == doc.doc_id
            assert page.extraction_status in ("ok", "weak", "empty")

    def test_pdf_page_ids_are_unique(self):
        """Every ParsedPage within a multi-page PDF must have a unique page_id."""
        path = _sample("Operations_report.pdf")
        if not path.exists():
            pytest.skip("Operations_report.pdf not found in sample_docs")

        _, pages = route_file(path)
        if len(pages) < 2:
            pytest.skip("PDF has fewer than 2 pages; cannot test uniqueness")

        page_ids = [p.page_id for p in pages]
        assert len(page_ids) == len(set(page_ids))


# --------------------------------------------------------------------------- #
# Unsupported formats                                                          #
# --------------------------------------------------------------------------- #


class TestUnsupportedFormats:
    def test_docx_routes_through_docling(self):
        """
        .docx is now routed through the Docling parser. The sample file
        must return (RawDocument, list[ParsedPage]) without raising.
        """
        from src.schema.models import ParsedPage, RawDocument

        path = _sample("Operations_report.docx")
        if not path.exists():
            pytest.skip("Operations_report.docx not found in sample_docs")

        doc, pages = route_file(path)
        assert isinstance(doc, RawDocument)
        assert doc.file_type == "docx"
        assert isinstance(pages, list)
        assert len(pages) > 0
        for page in pages:
            assert isinstance(page, ParsedPage)
            assert page.parse_method == "docling"

    def test_unknown_extension_raises_unsupported_format_error(self, tmp_path):
        fake_file = tmp_path / "data.xyz"
        fake_file.write_bytes(b"unknown content")
        with pytest.raises(UnsupportedFormatError):
            route_file(fake_file)

    def test_missing_file_raises_file_not_found(self, tmp_path):
        missing = tmp_path / "does_not_exist.txt"
        with pytest.raises(FileNotFoundError):
            route_file(missing)
