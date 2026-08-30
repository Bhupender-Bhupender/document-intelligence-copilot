import ast
import runpy
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
JOB_DIR = ROOT / "databricks" / "jobs"


def _runtime_namespace():
    return runpy.run_path(
        str(JOB_DIR / "_runtime.py")
    )


def _call_keywords(
    filename: str,
    function_name: str,
) -> set[str]:
    tree = ast.parse(
        (JOB_DIR / filename).read_text(
            encoding="utf-8"
        )
    )

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        func = node.func

        if (
            isinstance(func, ast.Name)
            and func.id == function_name
        ):
            return {
                keyword.arg
                for keyword in node.keywords
                if keyword.arg is not None
            }

    raise AssertionError(
        f"Call not found: {function_name}"
    )


def test_table_contract_derives_pipeline_objects():
    build = (
        _runtime_namespace()[
            "build_table_contract"
        ]
    )

    tables = build(
        "test_catalog"
    )

    assert tables.landing_root == (
        "/Volumes/test_catalog/"
        "bronze/document_landing"
    )

    assert tables.bronze_manifest == (
        "test_catalog.bronze."
        "document_manifest"
    )

    assert tables.bronze_runs == (
        "test_catalog.bronze."
        "ingestion_runs"
    )

    assert tables.silver_documents == (
        "test_catalog.silver.documents"
    )

    assert tables.silver_pages == (
        "test_catalog.silver.pages"
    )

    assert tables.silver_blocks == (
        "test_catalog.silver.blocks"
    )

    assert tables.quality_results == (
        "test_catalog.monitoring."
        "silver_quality_results"
    )

    assert tables.quality_runs == (
        "test_catalog.monitoring."
        "silver_quality_runs"
    )

    assert tables.gold_parent_chunks == (
        "test_catalog.gold.parent_chunks"
    )

    assert tables.gold_child_chunks == (
        "test_catalog.gold.child_chunks"
    )

    assert tables.gold_chunking_manifest == (
        "test_catalog.gold."
        "chunking_manifest"
    )


def test_table_contract_rejects_blank_catalog():
    build = (
        _runtime_namespace()[
            "build_table_contract"
        ]
    )

    with pytest.raises(ValueError):
        build("   ")


def test_ingest_passes_required_runtime_contract():
    assert _call_keywords(
        "01_ingest.py",
        "run_incremental_ingestion",
    ) >= {
        "landing_root",
        "manifest_table",
        "runs_table",
    }


def test_routing_passes_required_runtime_contract():
    assert _call_keywords(
        "02_extract_silver.py",
        "run_document_routing",
    ) >= {
        "manifest_table",
        "silver_documents_table",
    }


def test_extraction_passes_required_runtime_contract():
    assert _call_keywords(
        "02_extract_silver.py",
        "run_document_extraction",
    ) >= {
        "manifest_table",
        "documents_table",
        "pages_table",
        "blocks_table",
    }


def test_ocr_passes_required_runtime_contract():
    assert _call_keywords(
        "02_extract_silver.py",
        "run_managed_ocr_recovery",
    ) >= {
        "manifest_table",
        "documents_table",
        "pages_table",
        "blocks_table",
    }


def test_quality_passes_required_runtime_contract():
    assert _call_keywords(
        "03_quality_gate.py",
        "run_silver_quality_checks",
    ) >= {
        "manifest_table",
        "documents_table",
        "pages_table",
        "blocks_table",
        "results_table",
        "runs_table",
    }


def test_gold_passes_required_runtime_contract():
    assert _call_keywords(
        "04_publish_gold.py",
        "run_gold_chunking",
    ) >= {
        "bronze_manifest_table",
        "documents_table",
        "pages_table",
        "blocks_table",
        "parent_table",
        "child_table",
        "chunking_manifest_table",
    }


def test_first_four_bundle_tasks_receive_catalog():
    text = (
        ROOT
        / "resources"
        / "document_intelligence_job.yml"
    ).read_text(
        encoding="utf-8"
    )

    task_keys = [
        "ingest",
        "extract_silver",
        "quality_gate",
        "publish_gold",
    ]

    for index, key in enumerate(task_keys):
        start = text.index(
            f"- task_key: {key}"
        )

        if index + 1 < len(task_keys):
            end = text.index(
                f"- task_key: "
                f"{task_keys[index + 1]}",
                start + 1,
            )
        else:
            end = text.index(
                "- task_key: "
                "search_sync_validate",
                start + 1,
            )

        section = text[start:end]

        assert "- --catalog" in section
        assert "- ${var.catalog}" in section


def test_serverless_runtime_declares_shared_extraction_dependencies():
    text = (
        ROOT
        / "resources"
        / "document_intelligence_job.yml"
    ).read_text(
        encoding="utf-8"
    )

    assert "pypdf==6.10.2" in text
    assert "pydantic-settings==2.14.0" in text
    assert "structlog==25.5.0" in text

    # Optional local OCR/model stacks must not be pulled into
    # the standard serverless environment by default.
    assert "torch==" not in text
    assert "docling" not in text


def test_search_task_validates_gold_structure():
    script = (
        ROOT
        / "databricks"
        / "jobs"
        / "05_search_sync_validate.py"
    ).read_text(
        encoding="utf-8"
    )

    assert "validate_gold_contract" in script
    assert "duplicate_child_count" in script
    assert "duplicate_parent_count" in script
    assert "orphan_child_count" in script
    assert (
        "duplicate_current_manifest_count"
        in script
    )


def test_search_task_receives_gold_contract_tables():
    text = (
        ROOT
        / "resources"
        / "document_intelligence_job.yml"
    ).read_text(
        encoding="utf-8"
    )

    assert "- --parent-table" in text
    assert (
        "${var.catalog}.gold.parent_chunks"
        in text
    )

    assert (
        "- --chunking-manifest-table"
        in text
    )

    assert (
        "${var.catalog}.gold.chunking_manifest"
        in text
    )
