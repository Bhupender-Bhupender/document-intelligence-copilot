import pytest

from databricks.src.document_routing import (
    route_for_extension,
)


def test_text_routes_to_native_reader():
    strategy, ocr, _ = route_for_extension(".txt")

    assert strategy == "text_reader"
    assert ocr is False


def test_markdown_routes_to_native_reader():
    strategy, ocr, _ = route_for_extension(".md")

    assert strategy == "text_reader"
    assert ocr is False


def test_docx_routes_to_docling():
    strategy, ocr, _ = route_for_extension(".docx")

    assert strategy == "docling"
    assert ocr is False


def test_pdf_requires_inspection():
    strategy, ocr, _ = route_for_extension(".pdf")

    assert strategy == "pdf_inspect"
    assert ocr is None


def test_unsupported_extension_fails():
    with pytest.raises(ValueError):
        route_for_extension(".csv")
