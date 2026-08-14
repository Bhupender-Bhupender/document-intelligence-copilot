from databricks.src.silver_quality import (
    build_quality_checks,
)


def _checks():
    return build_quality_checks(
        manifest_table="bronze.manifest",
        documents_table="silver.documents",
        pages_table="silver.pages",
        blocks_table="silver.blocks",
    )


def test_quality_check_names_are_unique():
    checks = _checks()

    names = [
        check.name
        for check in checks
    ]

    assert len(names) == len(set(names))


def test_quality_suite_has_critical_checks():
    checks = _checks()

    assert any(
        check.severity == "CRITICAL"
        for check in checks
    )


def test_quality_suite_covers_core_entities():
    checks = _checks()

    entities = {
        check.entity
        for check in checks
    }

    assert {
        "documents",
        "pages",
        "blocks",
        "bronze_to_silver",
        "ocr",
    }.issubset(entities)


def test_all_checks_return_violation_alias():
    checks = _checks()

    assert all(
        "violations"
        in check.sql.lower()
        for check in checks
    )
