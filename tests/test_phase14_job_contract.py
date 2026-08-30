from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

JOB_DIR = ROOT / "databricks" / "jobs"

EXPECTED_TASK_FILES = [
    "01_ingest.py",
    "02_extract_silver.py",
    "03_quality_gate.py",
    "04_publish_gold.py",
    "05_search_sync_validate.py",
]


def test_phase14_task_files_exist():
    for filename in EXPECTED_TASK_FILES:
        assert (
            JOB_DIR / filename
        ).is_file()


def test_phase14_bundle_has_five_tasks():
    import re

    text = (
        ROOT
        / "resources"
        / "document_intelligence_job.yml"
    ).read_text(
        encoding="utf-8-sig"
    )

    task_declarations = re.findall(
        r"^ {8}- task_key:",
        text,
        flags=re.MULTILINE,
    )

    assert len(task_declarations) == 5


def test_phase14_job_serializes_runs():
    text = (
        ROOT
        / "resources"
        / "document_intelligence_job.yml"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "max_concurrent_runs: 1"
        in text
    )


def test_phase14_quality_gate_is_not_retried():
    text = (
        ROOT
        / "resources"
        / "document_intelligence_job.yml"
    ).read_text(
        encoding="utf-8"
    )

    section = text.split(
        "- task_key: quality_gate",
        1,
    )[1].split(
        "- task_key: publish_gold",
        1,
    )[0]

    assert "max_retries: 0" in section


def test_phase14_search_uses_existing_index():
    text = (
        JOB_DIR
        / "05_search_sync_validate.py"
    ).read_text(
        encoding="utf-8"
    )

    assert "sync_index" in text
    assert "get_index" in text
    assert "create" not in text.lower()


def test_phase14_quality_task_reuses_existing_gate():
    text = (
        JOB_DIR
        / "03_quality_gate.py"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "run_silver_quality_checks"
        in text
    )

    assert (
        "enforce_quality_gate"
        in text
    )
