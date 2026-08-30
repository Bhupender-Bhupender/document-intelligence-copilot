from pathlib import Path
import ast


ROOT = Path(__file__).resolve().parents[1]


def test_ingestion_router_does_not_eagerly_import_docling():
    path = (
        ROOT
        / "src"
        / "ingestion"
        / "router.py"
    )

    tree = ast.parse(
        path.read_text(
            encoding="utf-8"
        )
    )

    module_imports = [
        node
        for node in tree.body
        if isinstance(
            node,
            ast.ImportFrom,
        )
    ]

    assert not any(
        node.module
        == "src.parsing.docling_parser"
        for node in module_imports
    )


def test_databricks_pdf_path_skips_local_ocr(
    monkeypatch,
    tmp_path,
):
    from src.ingestion import router

    raw_document = object()
    pages = [object()]

    monkeypatch.setattr(
        router.config,
        "runtime_mode",
        "databricks",
    )

    monkeypatch.setattr(
        router,
        "read_pdf_file",
        lambda _: (
            raw_document,
            pages,
        ),
    )

    def local_ocr_must_not_run(*args, **kwargs):
        raise AssertionError(
            "Local OCR must not run in "
            "Databricks runtime."
        )

    monkeypatch.setattr(
        router,
        "route_pdf_pages_through_ocr",
        local_ocr_must_not_run,
    )

    sample_pdf = (
        tmp_path
        / "sample.pdf"
    )

    sample_pdf.touch()

    returned_document, returned_pages = (
        router.route_file(
            sample_pdf
        )
    )

    assert (
        returned_document
        is raw_document
    )

    assert (
        returned_pages
        is pages
    )


def test_local_pdf_path_preserves_existing_ocr_behavior(
    monkeypatch,
    tmp_path,
):
    from src.ingestion import router

    raw_document = object()
    native_pages = [object()]
    recovered_pages = [object()]

    monkeypatch.setattr(
        router.config,
        "runtime_mode",
        "local",
    )

    monkeypatch.setattr(
        router,
        "read_pdf_file",
        lambda _: (
            raw_document,
            native_pages,
        ),
    )

    monkeypatch.setattr(
        router,
        "route_pdf_pages_through_ocr",
        lambda file_path, raw_doc, pages: (
            recovered_pages
        ),
    )

    sample_pdf = (
        tmp_path
        / "sample.pdf"
    )

    sample_pdf.touch()

    returned_document, returned_pages = (
        router.route_file(
            sample_pdf
        )
    )

    assert (
        returned_document
        is raw_document
    )

    assert (
        returned_pages
        is recovered_pages
    )


def test_extract_job_sets_databricks_runtime_before_imports():
    text = (
        ROOT
        / "databricks"
        / "jobs"
        / "02_extract_silver.py"
    ).read_text(
        encoding="utf-8"
    )

    runtime_position = text.index(
        'os.environ["RUNTIME_MODE"] = "databricks"'
    )

    processing_import_position = text.index(
        "from databricks.src.document_processing"
    )

    assert (
        runtime_position
        < processing_import_position
    )


def test_serverless_environment_excludes_local_ml_ocr_stack():
    text = (
        ROOT
        / "resources"
        / "document_intelligence_job.yml"
    ).read_text(
        encoding="utf-8"
    )

    environment = text.split(
        "environments:",
        1,
    )[1].split(
        "tasks:",
        1,
    )[0]

    assert "pypdf==6.10.2" in environment
    assert "pydantic-settings==2.14.0" in environment
    assert "structlog==25.5.0" in environment

    assert "torch==" not in environment
    assert "docling==" not in environment
    assert "docling-slim" not in environment
