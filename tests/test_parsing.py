"""
Tests for the Docling parser module (src/parsing/docling_parser.py).

Normal test run (pytest tests/ or pytest tests/test_parsing.py):
    All tests run EXCEPT the complex-PDF integration test.
    Expected duration: < 30 s on a warm converter (models cached).

Slow integration test:
    The 18-page prof-services-agrmt.pdf test is skipped by default.
    To run it explicitly:

        $env:DOCLING_INTEGRATION_TESTS=1
        pytest tests/test_parsing.py -v -k "complex"

    Expected duration: ~70 s on CPU (full Docling layout analysis).

Sample files used:
    docs/sample_docs/quarterly_summary.md   — flat markdown with 1 KPI table
    docs/sample_docs/Operations_report.pdf  — born-digital 1-page PDF
    docs/sample_docs/Operations_report.docx — born-digital DOCX
    docs/sample_docs/prof-services-agrmt.pdf — 18-page born-digital PDF (slow)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List

import pytest

from src.parsing.docling_parser import parse_with_docling
from src.schema.models import ParsedBlock, ParsedPage, RawDocument

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SAMPLE_DIR = _PROJECT_ROOT / "docs" / "sample_docs"

_RUN_INTEGRATION = bool(os.environ.get("DOCLING_INTEGRATION_TESTS"))


def _sample(name: str) -> Path:
    return _SAMPLE_DIR / name


# --------------------------------------------------------------------------- #
# Return-type contract                                                         #
# --------------------------------------------------------------------------- #


class TestReturnTypes:
    """Verify the public API always returns the declared types."""

    def test_returns_rawdocument_and_list(self):
        doc, pages = parse_with_docling(_sample("quarterly_summary.md"))
        assert isinstance(doc, RawDocument)
        assert isinstance(pages, list)
        assert len(pages) > 0

    def test_pages_are_parsed_pages(self):
        _, pages = parse_with_docling(_sample("quarterly_summary.md"))
        assert all(isinstance(p, ParsedPage) for p in pages)

    def test_layout_blocks_are_parsed_blocks(self):
        _, pages = parse_with_docling(_sample("quarterly_summary.md"))
        for page in pages:
            assert isinstance(page.layout_blocks, list)
            assert all(isinstance(b, ParsedBlock) for b in page.layout_blocks)

    def test_pages_link_to_doc(self):
        doc, pages = parse_with_docling(_sample("quarterly_summary.md"))
        for page in pages:
            assert page.doc_id == doc.doc_id


# --------------------------------------------------------------------------- #
# Markdown parsing                                                             #
# --------------------------------------------------------------------------- #


class TestMarkdownParsing:
    """quarterly_summary.md is a flat markdown file with a KPI table."""

    def test_parse_method_is_docling(self):
        _, pages = parse_with_docling(_sample("quarterly_summary.md"))
        for page in pages:
            assert page.parse_method == "docling"

    def test_single_page_for_flat_doc(self):
        doc, pages = parse_with_docling(_sample("quarterly_summary.md"))
        assert doc.total_pages == 1
        assert len(pages) == 1
        assert pages[0].page_number == 1

    def test_has_readable_text(self):
        _, pages = parse_with_docling(_sample("quarterly_summary.md"))
        assert pages[0].word_count > 0
        assert "quarterly" in pages[0].normalized_text.lower()

    def test_extraction_status_ok(self):
        _, pages = parse_with_docling(_sample("quarterly_summary.md"))
        assert pages[0].extraction_status == "ok"

    def test_has_layout_blocks(self):
        _, pages = parse_with_docling(_sample("quarterly_summary.md"))
        assert len(pages[0].layout_blocks) > 0

    def test_has_heading_block(self):
        _, pages = parse_with_docling(_sample("quarterly_summary.md"))
        block_types = [b.block_type for p in pages for b in p.layout_blocks]
        assert "heading" in block_types

    def test_has_table_block(self):
        """quarterly_summary.md contains a KPI table; Docling must detect it."""
        _, pages = parse_with_docling(_sample("quarterly_summary.md"))
        block_types = [b.block_type for p in pages for b in p.layout_blocks]
        assert "table" in block_types

    def test_rawdoc_metadata(self):
        doc, _ = parse_with_docling(_sample("quarterly_summary.md"))
        assert doc.file_type == "md"
        assert doc.file_name == "quarterly_summary.md"
        assert doc.byte_size > 0
        assert len(doc.checksum) == 64  # SHA-256 hex


# --------------------------------------------------------------------------- #
# Born-digital PDF                                                             #
# --------------------------------------------------------------------------- #


class TestBornDigitalPdf:
    """Operations_report.pdf is a single-page born-digital PDF."""

    @pytest.fixture(autouse=True)
    def skip_if_missing(self):
        if not _sample("Operations_report.pdf").exists():
            pytest.skip("Operations_report.pdf not found in sample_docs")

    def test_pdf_contract(self):
        doc, pages = parse_with_docling(_sample("Operations_report.pdf"))
        assert isinstance(doc, RawDocument)
        assert doc.file_type == "pdf"
        assert doc.total_pages >= 1
        assert len(pages) == doc.total_pages

    def test_pdf_parse_method(self):
        _, pages = parse_with_docling(_sample("Operations_report.pdf"))
        for page in pages:
            assert page.parse_method == "docling"

    def test_pdf_extraction_status(self):
        _, pages = parse_with_docling(_sample("Operations_report.pdf"))
        for page in pages:
            assert page.extraction_status in ("ok", "weak", "empty")

    def test_pdf_has_readable_content(self):
        _, pages = parse_with_docling(_sample("Operations_report.pdf"))
        readable = [p for p in pages if p.extraction_status in ("ok", "weak")]
        assert len(readable) > 0

    def test_pdf_page_ids_are_unique(self):
        _, pages = parse_with_docling(_sample("Operations_report.pdf"))
        page_ids = [p.page_id for p in pages]
        assert len(page_ids) == len(set(page_ids))

    def test_pdf_rawdoc_metadata(self):
        doc, _ = parse_with_docling(_sample("Operations_report.pdf"))
        assert doc.file_name == "Operations_report.pdf"
        assert doc.byte_size > 0
        assert len(doc.checksum) == 64


# --------------------------------------------------------------------------- #
# DOCX parsing                                                                 #
# --------------------------------------------------------------------------- #


class TestDocxParsing:
    """Operations_report.docx should parse through Docling without errors."""

    @pytest.fixture(autouse=True)
    def skip_if_missing(self):
        if not _sample("Operations_report.docx").exists():
            pytest.skip("Operations_report.docx not found in sample_docs")

    def test_docx_contract(self):
        doc, pages = parse_with_docling(_sample("Operations_report.docx"))
        assert isinstance(doc, RawDocument)
        assert doc.file_type == "docx"
        assert doc.total_pages >= 1
        assert len(pages) > 0

    def test_docx_parse_method(self):
        _, pages = parse_with_docling(_sample("Operations_report.docx"))
        for page in pages:
            assert page.parse_method == "docling"

    def test_docx_has_text(self):
        _, pages = parse_with_docling(_sample("Operations_report.docx"))
        total_words = sum(p.word_count for p in pages)
        assert total_words > 0

    def test_docx_page_ids_are_unique(self):
        _, pages = parse_with_docling(_sample("Operations_report.docx"))
        page_ids = [p.page_id for p in pages]
        assert len(page_ids) == len(set(page_ids))

    def test_docx_rawdoc_metadata(self):
        doc, _ = parse_with_docling(_sample("Operations_report.docx"))
        assert doc.file_name == "Operations_report.docx"
        assert doc.byte_size > 0
        assert len(doc.checksum) == 64


# --------------------------------------------------------------------------- #
# Error handling                                                               #
# --------------------------------------------------------------------------- #


class TestErrorHandling:
    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            parse_with_docling(tmp_path / "nonexistent.pdf")

    def test_unsupported_format_raises_value_error(self, tmp_path):
        """Parser raises ValueError for any format not in _SUPPORTED_FORMATS."""
        fake = tmp_path / "test.xyz"
        fake.write_bytes(b"not a real document")
        with pytest.raises(ValueError, match="does not support format"):
            parse_with_docling(fake)

    def test_unsupported_format_error_message_names_format(self, tmp_path):
        fake = tmp_path / "test.csv"
        fake.write_bytes(b"a,b,c")
        with pytest.raises(ValueError) as exc_info:
            parse_with_docling(fake)
        assert ".csv" in str(exc_info.value)


# --------------------------------------------------------------------------- #
# Complex PDF — slow integration test, gated on env var                       #
# --------------------------------------------------------------------------- #


class TestComplexPdf:
    @pytest.mark.skipif(
        not _RUN_INTEGRATION,
        reason=(
            "Slow Docling integration test (~70 s on CPU). "
            "Set DOCLING_INTEGRATION_TESTS=1 to enable."
        ),
    )
    def test_complex_pdf_18_pages(self):
        """
        prof-services-agrmt.pdf is an 18-page born-digital PDF.

        Validates: correct page count, all pages parsed, word coverage,
        no pages marked empty (born-digital document with full text layer).
        """
        path = _sample("prof-services-agrmt.pdf")
        if not path.exists():
            pytest.skip("prof-services-agrmt.pdf not found in sample_docs")

        doc, pages = parse_with_docling(path)

        assert doc.total_pages == 18
        assert len(pages) == 18

        for page in pages:
            assert page.parse_method == "docling"
            assert page.doc_id == doc.doc_id

        total_words = sum(p.word_count for p in pages)
        assert total_words > 1000

        # Born-digital document: no page should be empty.
        empty_pages = [p for p in pages if p.extraction_status == "empty"]
        assert len(empty_pages) == 0
