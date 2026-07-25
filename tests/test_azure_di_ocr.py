"""
Tests for Azure AI Document Intelligence OCR adapter and backend routing.

Test structure
--------------
TestAzureDiOcrAdapterText      (5) — text reconstruction, page mapping, identity
TestAzureDiOcrAdapterConfidence (3) — mean confidence logic
TestAzureDiOcrAdapterErrors    (2) — SDK failure surfacing, missing-page fallback
TestAzureDiOcrAdapterMetadata  (2) — parse_method and ocr_engine values
TestOcrRouterBackendSelection  (5) — backend-switch routing (import-light module)

Import notes
------------
ocr_router is imported directly in TestOcrRouterBackendSelection because the
module is now import-light (no torch, Docling, or Azure SDK at module level).
AzureDiOcrAdapter tests bypass __init__ using object.__new__ — identical to
the BlobArtifactWriter test pattern.
"""
from __future__ import annotations

from pathlib import Path
from typing import List
from unittest.mock import MagicMock

import pytest

from src.ocr.azure_di_ocr import AzureDiOcrAdapter, AzureDiOcrError
from src.schema.models import ParsedPage


# --------------------------------------------------------------------------- #
# Shared helpers                                                               #
# --------------------------------------------------------------------------- #


def _make_page(
    page_number: int = 1,
    extraction_status: str = "empty",
    doc_id: str = "doc-1",
) -> ParsedPage:
    return ParsedPage(
        doc_id=doc_id,
        page_number=page_number,
        raw_text="",
        normalized_text="",
        word_count=0,
        char_count=0,
        extraction_status=extraction_status,
    )


def _make_di_word(content: str, confidence: float) -> MagicMock:
    w = MagicMock()
    w.content = content
    w.confidence = confidence
    return w


def _make_di_line(content: str) -> MagicMock:
    line = MagicMock()
    line.content = content
    return line


def _make_di_page(page_number: int, words: list, lines: list) -> MagicMock:
    p = MagicMock()
    p.page_number = page_number
    p.words = words
    p.lines = lines
    return p


def _make_di_result(di_pages: list) -> MagicMock:
    result = MagicMock()
    result.pages = di_pages
    return result


def _make_mock_client(di_result: MagicMock) -> MagicMock:
    client = MagicMock()
    client.begin_analyze_document.return_value.result.return_value = di_result
    return client


class _FakeRequest:
    """Minimal AnalyzeDocumentRequest stand-in — stores kwargs as attributes."""

    def __init__(self, **kwargs: object) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


def _adapter(mock_client: MagicMock) -> AzureDiOcrAdapter:
    """Bypass __init__ (no Azure SDK import) and inject mock client + request class."""
    adapter = object.__new__(AzureDiOcrAdapter)
    adapter._client = mock_client
    adapter._AnalyzeDocumentRequest = _FakeRequest
    return adapter


def _make_raw_doc():
    """Minimal RawDocument for router tests."""
    from src.schema.models import RawDocument

    return RawDocument(
        source_path="test.pdf",
        file_name="test.pdf",
        file_type="pdf",
        byte_size=1024,
        checksum="abc123",
    )


# --------------------------------------------------------------------------- #
# TestAzureDiOcrAdapterText                                                    #
# --------------------------------------------------------------------------- #


class TestAzureDiOcrAdapterText:

    def test_api_called_with_file_bytes(self, tmp_path: Path) -> None:
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"fake-pdf-bytes")

        di_page = _make_di_page(1, words=[], lines=[_make_di_line("hello")])
        client = _make_mock_client(_make_di_result([di_page]))
        adapter = _adapter(client)

        adapter.recover_pages(pdf, [_make_page(1)])

        call_args = client.begin_analyze_document.call_args
        assert call_args is not None
        # model_id is the first positional argument
        assert call_args.args[0] == "prebuilt-read"
        # bytes_source must equal the file bytes
        request_obj = call_args.args[1]
        assert request_obj.bytes_source == b"fake-pdf-bytes"

    def test_text_reconstructed_from_lines(self, tmp_path: Path) -> None:
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        lines = [_make_di_line("First line."), _make_di_line("Second line.")]
        di_page = _make_di_page(1, words=[], lines=lines)
        adapter = _adapter(_make_mock_client(_make_di_result([di_page])))

        result = adapter.recover_pages(pdf, [_make_page(1)])

        assert "First line." in result[0].raw_text
        assert "Second line." in result[0].raw_text

    def test_page_number_matched_correctly(self, tmp_path: Path) -> None:
        """Page-number lookup maps DI page 2 to the page with page_number=2."""
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        di_p1 = _make_di_page(1, words=[], lines=[_make_di_line("page one text")])
        di_p2 = _make_di_page(2, words=[], lines=[_make_di_line("page two text")])
        adapter = _adapter(_make_mock_client(_make_di_result([di_p1, di_p2])))

        result = adapter.recover_pages(pdf, [_make_page(2)])

        assert "page two text" in result[0].raw_text
        assert "page one text" not in result[0].raw_text

    def test_empty_azure_di_page_keeps_empty_status(self, tmp_path: Path) -> None:
        """A DI page with no lines and no words keeps extraction_status='empty'."""
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        di_page = _make_di_page(1, words=[], lines=[])
        adapter = _adapter(_make_mock_client(_make_di_result([di_page])))

        result = adapter.recover_pages(pdf, [_make_page(1)])

        assert result[0].extraction_status == "empty"

    def test_identity_fields_never_changed(self, tmp_path: Path) -> None:
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        di_page = _make_di_page(3, words=[], lines=[_make_di_line("some text here today")])
        adapter = _adapter(_make_mock_client(_make_di_result([di_page])))

        original = _make_page(3, doc_id="orig-doc-99")
        result = adapter.recover_pages(pdf, [original])

        assert result[0].page_id == original.page_id
        assert result[0].doc_id == "orig-doc-99"
        assert result[0].page_number == 3


# --------------------------------------------------------------------------- #
# TestAzureDiOcrAdapterConfidence                                              #
# --------------------------------------------------------------------------- #


class TestAzureDiOcrAdapterConfidence:

    def test_mean_confidence_computed(self, tmp_path: Path) -> None:
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        words = [_make_di_word("hello", 0.9), _make_di_word("world", 0.8)]
        lines = [_make_di_line("hello world")]
        di_page = _make_di_page(1, words=words, lines=lines)
        adapter = _adapter(_make_mock_client(_make_di_result([di_page])))

        result = adapter.recover_pages(pdf, [_make_page(1)])

        assert result[0].ocr_confidence is not None
        assert abs(result[0].ocr_confidence - 0.85) < 1e-9

    def test_confidence_none_when_no_words(self, tmp_path: Path) -> None:
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        # Lines present but words list is empty
        di_page = _make_di_page(1, words=[], lines=[_make_di_line("some text")])
        adapter = _adapter(_make_mock_client(_make_di_result([di_page])))

        result = adapter.recover_pages(pdf, [_make_page(1)])

        assert result[0].ocr_confidence is None

    def test_confidence_per_page_not_cross_contaminated(self, tmp_path: Path) -> None:
        """Page 1 and page 2 each compute confidence from their own words only."""
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        di_p1 = _make_di_page(
            1,
            words=[_make_di_word("a", 1.0)],
            lines=[_make_di_line("a")],
        )
        di_p2 = _make_di_page(
            2,
            words=[_make_di_word("b", 0.5)],
            lines=[_make_di_line("b")],
        )
        adapter = _adapter(_make_mock_client(_make_di_result([di_p1, di_p2])))

        result = adapter.recover_pages(pdf, [_make_page(1), _make_page(2)])

        assert abs(result[0].ocr_confidence - 1.0) < 1e-9
        assert abs(result[1].ocr_confidence - 0.5) < 1e-9


# --------------------------------------------------------------------------- #
# TestAzureDiOcrAdapterErrors                                                  #
# --------------------------------------------------------------------------- #


class TestAzureDiOcrAdapterErrors:

    def test_sdk_exception_raises_adapter_error(self, tmp_path: Path) -> None:
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        client = MagicMock()
        client.begin_analyze_document.side_effect = RuntimeError("network error")
        adapter = _adapter(client)

        with pytest.raises(AzureDiOcrError, match="network error"):
            adapter.recover_pages(pdf, [_make_page(1)])

    def test_missing_page_in_result_keeps_original(self, tmp_path: Path) -> None:
        """When DI result has no entry for page_number=5, original is returned."""
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        # DI returns page 1 only; we ask for page 5
        di_page = _make_di_page(1, words=[], lines=[_make_di_line("irrelevant")])
        adapter = _adapter(_make_mock_client(_make_di_result([di_page])))

        original = _make_page(5)
        result = adapter.recover_pages(pdf, [original])

        assert result[0] is original  # exact same object, not a copy


# --------------------------------------------------------------------------- #
# TestAzureDiOcrAdapterMetadata                                                #
# --------------------------------------------------------------------------- #


class TestAzureDiOcrAdapterMetadata:

    def test_parse_method_is_azure_di(self, tmp_path: Path) -> None:
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        di_page = _make_di_page(1, words=[], lines=[_make_di_line("some words here")])
        adapter = _adapter(_make_mock_client(_make_di_result([di_page])))

        result = adapter.recover_pages(pdf, [_make_page(1)])

        assert result[0].parse_method == "azure_di"

    def test_ocr_engine_is_azure_document_intelligence(self, tmp_path: Path) -> None:
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF")

        di_page = _make_di_page(1, words=[], lines=[_make_di_line("engine check text")])
        adapter = _adapter(_make_mock_client(_make_di_result([di_page])))

        result = adapter.recover_pages(pdf, [_make_page(1)])

        assert result[0].ocr_engine == "azure-document-intelligence"


# --------------------------------------------------------------------------- #
# TestOcrRouterBackendSelection                                                #
# --------------------------------------------------------------------------- #


class TestOcrRouterBackendSelection:
    """
    Router-level backend-selection tests.

    ocr_router is import-light (no torch, Docling, or Azure SDK at module
    level after this chunk's changes), so these tests import it directly.
    Both backends are monkeypatched at the _run_local_ocr / _run_azure_di_ocr
    wrapper level — no live Azure, no live Docling, no torch.
    """

    def test_azure_di_backend_selected(self, monkeypatch, tmp_path: Path) -> None:
        from src.core.config import config as app_config
        from src.ocr import ocr_router

        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF")
        empty = _make_page(1)

        azure_log: list = []
        local_log: list = []

        def fake_azure_di(endpoint, file_path, empty_pages):
            azure_log.append("called")
            return empty_pages  # return pages unchanged

        monkeypatch.setattr(ocr_router, "_run_azure_di_ocr", fake_azure_di)
        monkeypatch.setattr(ocr_router, "_run_local_ocr", lambda p: local_log.append("x"))
        monkeypatch.setattr(app_config, "ocr_backend", "azure_di")

        ocr_router.route_pdf_pages_through_ocr(pdf, _make_raw_doc(), [empty])

        assert azure_log == ["called"]
        assert local_log == []

    def test_local_backend_selected(self, monkeypatch, tmp_path: Path) -> None:
        from src.core.config import config as app_config
        from src.ocr import ocr_router

        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF")
        empty = _make_page(1)

        local_log: list = []
        azure_log: list = []

        def fake_local(file_path):
            local_log.append("called")
            return (MagicMock(), [])  # (RawDocument, List[ParsedPage])

        monkeypatch.setattr(ocr_router, "_run_local_ocr", fake_local)
        monkeypatch.setattr(ocr_router, "_run_azure_di_ocr", lambda *a: azure_log.append("x"))
        monkeypatch.setattr(app_config, "ocr_backend", "local")

        ocr_router.route_pdf_pages_through_ocr(pdf, _make_raw_doc(), [empty])

        assert local_log == ["called"]
        assert azure_log == []

    def test_azure_di_error_returns_original_pages(self, monkeypatch, tmp_path: Path) -> None:
        from src.core.config import config as app_config
        from src.ocr import ocr_router

        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF")
        empty = _make_page(1)

        def fake_azure_di_error(endpoint, file_path, empty_pages):
            raise AzureDiOcrError("service unavailable")

        monkeypatch.setattr(ocr_router, "_run_azure_di_ocr", fake_azure_di_error)
        monkeypatch.setattr(app_config, "ocr_backend", "azure_di")

        result = ocr_router.route_pdf_pages_through_ocr(
            pdf, _make_raw_doc(), [empty]
        )

        assert result == [empty]  # original page list returned on failure

    def test_identity_preserved_through_router(self, monkeypatch, tmp_path: Path) -> None:
        from src.core.config import config as app_config
        from src.ocr import ocr_router

        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF")
        original = _make_page(1, doc_id="doc-identity-check")

        def fake_azure_di(endpoint, file_path, empty_pages):
            # Simulate an adapter that returns an updated page
            p = empty_pages[0]
            return [
                ParsedPage(
                    page_id=p.page_id,
                    doc_id=p.doc_id,
                    page_number=p.page_number,
                    raw_text="recovered text content",
                    normalized_text="recovered text content",
                    word_count=3,
                    char_count=24,
                    parse_method="azure_di",
                    extraction_status="ok",
                    ocr_engine="azure-document-intelligence",
                )
            ]

        monkeypatch.setattr(ocr_router, "_run_azure_di_ocr", fake_azure_di)
        monkeypatch.setattr(app_config, "ocr_backend", "azure_di")

        result = ocr_router.route_pdf_pages_through_ocr(
            pdf, _make_raw_doc(), [original]
        )

        assert result[0].page_id == original.page_id
        assert result[0].doc_id == "doc-identity-check"
        assert result[0].page_number == 1

    def test_no_empty_pages_skips_both_backends(self, monkeypatch, tmp_path: Path) -> None:
        from src.core.config import config as app_config
        from src.ocr import ocr_router

        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF")
        ok_page = _make_page(1, extraction_status="ok")

        azure_log: list = []
        local_log: list = []

        monkeypatch.setattr(ocr_router, "_run_azure_di_ocr", lambda *a: azure_log.append("x"))
        monkeypatch.setattr(ocr_router, "_run_local_ocr", lambda *a: local_log.append("x"))
        monkeypatch.setattr(app_config, "ocr_backend", "azure_di")

        result = ocr_router.route_pdf_pages_through_ocr(
            pdf, _make_raw_doc(), [ok_page]
        )

        assert azure_log == []
        assert local_log == []
        assert result == [ok_page]
