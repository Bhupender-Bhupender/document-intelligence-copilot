from __future__ import annotations

import uuid

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass(frozen=True)
class QualityCheck:
    name: str
    entity: str
    severity: str
    sql: str


RESULT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("checked_at", TimestampType(), False),
    StructField("check_name", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("passed", BooleanType(), False),
    StructField("violation_count", LongType(), False),
    StructField("expected_value", StringType(), False),
    StructField("observed_value", StringType(), False),
])


RUN_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), False),

    StructField("total_checks", IntegerType(), False),
    StructField("passed_checks", IntegerType(), False),
    StructField("failed_checks", IntegerType(), False),
    StructField("critical_failures", IntegerType(), False),

    StructField("run_status", StringType(), False),
    StructField("notes", StringType(), True),
])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_quality_checks(
    *,
    manifest_table: str,
    documents_table: str,
    pages_table: str,
    blocks_table: str,
) -> list[QualityCheck]:

    return [

        QualityCheck(
            name="document_id_unique",
            entity="documents",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM (
                SELECT document_id
                FROM {documents_table}
                GROUP BY document_id
                HAVING COUNT(*) > 1
            )
            """,
        ),

        QualityCheck(
            name="document_sha256_present",
            entity="documents",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM {documents_table}
            WHERE sha256 IS NULL
               OR trim(sha256) = ''
            """,
        ),

        QualityCheck(
            name="current_sha256_unique",
            entity="documents",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM (
                SELECT sha256
                FROM {documents_table}
                WHERE is_current = true
                GROUP BY sha256
                HAVING COUNT(*) > 1
            )
            """,
        ),

        QualityCheck(
            name="current_documents_extracted",
            entity="documents",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM {documents_table}
            WHERE is_current = true
              AND extraction_status <> 'EXTRACTED'
            """,
        ),

        QualityCheck(
            name="document_page_count_matches",
            entity="documents",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM (
                SELECT
                    d.document_id,
                    d.page_count,
                    COUNT(p.page_id) AS actual_pages

                FROM {documents_table} d

                LEFT JOIN {pages_table} p
                  ON d.document_id = p.document_id

                WHERE d.is_current = true

                GROUP BY
                    d.document_id,
                    d.page_count

                HAVING COALESCE(d.page_count, -1)
                       <> COUNT(p.page_id)
            )
            """,
        ),

        QualityCheck(
            name="document_block_count_matches",
            entity="documents",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM (
                SELECT
                    d.document_id,
                    d.block_count,
                    COUNT(b.block_id) AS actual_blocks

                FROM {documents_table} d

                LEFT JOIN {blocks_table} b
                  ON d.document_id = b.document_id

                WHERE d.is_current = true

                GROUP BY
                    d.document_id,
                    d.block_count

                HAVING COALESCE(d.block_count, -1)
                       <> COUNT(b.block_id)
            )
            """,
        ),

        QualityCheck(
            name="page_id_unique",
            entity="pages",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM (
                SELECT page_id
                FROM {pages_table}
                GROUP BY page_id
                HAVING COUNT(*) > 1
            )
            """,
        ),

        QualityCheck(
            name="page_number_unique_per_document",
            entity="pages",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM (
                SELECT
                    document_id,
                    page_number

                FROM {pages_table}

                GROUP BY
                    document_id,
                    page_number

                HAVING COUNT(*) > 1
            )
            """,
        ),

        QualityCheck(
            name="pages_have_document",
            entity="pages",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations

            FROM {pages_table} p

            LEFT ANTI JOIN {documents_table} d
              ON p.document_id = d.document_id
            """,
        ),

        QualityCheck(
            name="page_number_positive",
            entity="pages",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM {pages_table}
            WHERE page_number < 1
               OR page_number IS NULL
            """,
        ),

        QualityCheck(
            name="page_word_count_nonnegative",
            entity="pages",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM {pages_table}
            WHERE word_count < 0
               OR word_count IS NULL
            """,
        ),

        QualityCheck(
            name="page_quality_state_consistent",
            entity="pages",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM {pages_table}

            WHERE NOT (
                (
                    extraction_quality = 'good'
                    AND requires_ocr = false
                )
                OR
                (
                    extraction_quality IN ('weak', 'empty')
                    AND requires_ocr = true
                )
            )
            """,
        ),

        QualityCheck(
            name="extracted_documents_have_no_pending_ocr_pages",
            entity="pages",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations

            FROM {pages_table} p

            INNER JOIN {documents_table} d
              ON p.document_id = d.document_id

            WHERE d.is_current = true
              AND d.extraction_status = 'EXTRACTED'
              AND p.requires_ocr = true
            """,
        ),

        QualityCheck(
            name="block_id_unique",
            entity="blocks",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM (
                SELECT block_id
                FROM {blocks_table}
                GROUP BY block_id
                HAVING COUNT(*) > 1
            )
            """,
        ),

        QualityCheck(
            name="blocks_have_page",
            entity="blocks",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations

            FROM {blocks_table} b

            LEFT ANTI JOIN {pages_table} p
              ON b.page_id = p.page_id
            """,
        ),

        QualityCheck(
            name="block_order_nonnegative",
            entity="blocks",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM {blocks_table}
            WHERE block_order < 0
               OR block_order IS NULL
            """,
        ),

        QualityCheck(
            name="block_order_unique_per_page",
            entity="blocks",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM (
            SELECT
            document_id,
            page_number,
            block_order

            FROM {blocks_table}

            GROUP BY
            document_id,
            page_number,
            block_order

            HAVING COUNT(*) > 1
            )
            """,
        ),

        QualityCheck(
            name="block_text_nonempty",
            entity="blocks",
            severity="WARNING",
            sql=f"""
            SELECT COUNT(*) AS violations
            FROM {blocks_table}
            WHERE text IS NULL
               OR trim(text) = ''
            """,
        ),

        QualityCheck(
            name="bronze_current_documents_exist_in_silver",
            entity="bronze_to_silver",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations

            FROM {manifest_table} b

            LEFT ANTI JOIN {documents_table} s
              ON b.document_id = s.document_id

            WHERE b.is_current = true
            """,
        ),

        QualityCheck(
            name="bronze_silver_extracted_state_aligned",
            entity="bronze_to_silver",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations

            FROM {manifest_table} b

            INNER JOIN {documents_table} s
              ON b.document_id = s.document_id

            WHERE b.is_current = true
              AND s.is_current = true
              AND (
                    b.processing_status <> 'EXTRACTED'
                    OR s.extraction_status <> 'EXTRACTED'
                  )
            """,
        ),

        QualityCheck(
            name="document_ocr_count_matches_pages",
            entity="ocr",
            severity="CRITICAL",
            sql=f"""
            SELECT COUNT(*) AS violations

            FROM (
                SELECT
                    d.document_id,
                    COALESCE(d.ocr_page_count, 0)
                        AS expected_ocr_pages,

                    SUM(
                        CASE
                            WHEN p.extraction_method IN (
                                'rapidocr',
                                'azure_di',
                                'databricks_ai_parse_document'
                            )
                            THEN 1
                            ELSE 0
                        END
                    ) AS actual_ocr_pages

                FROM {documents_table} d

                LEFT JOIN {pages_table} p
                  ON d.document_id = p.document_id

                WHERE d.is_current = true

                GROUP BY
                    d.document_id,
                    d.ocr_page_count

                HAVING
                    COALESCE(d.ocr_page_count, 0)
                    <>
                    SUM(
                        CASE
                            WHEN p.extraction_method IN (
                                'rapidocr',
                                'azure_di',
                                'databricks_ai_parse_document'
                            )
                            THEN 1
                            ELSE 0
                        END
                    )
            )
            """,
        ),
    ]


def run_silver_quality_checks(
    spark: Any,
    *,
    manifest_table: str,
    documents_table: str,
    pages_table: str,
    blocks_table: str,
    results_table: str,
    runs_table: str,
) -> dict[str, Any]:

    run_id = f"dq_{uuid.uuid4().hex[:12]}"
    started_at = utc_now()

    checks = build_quality_checks(
        manifest_table=manifest_table,
        documents_table=documents_table,
        pages_table=pages_table,
        blocks_table=blocks_table,
    )

    result_rows = []

    for check in checks:

        row = spark.sql(
            check.sql
        ).first()

        violations = int(
            row["violations"] or 0
        )

        result_rows.append({
            "run_id": run_id,
            "checked_at": utc_now(),
            "check_name": check.name,
            "entity": check.entity,
            "severity": check.severity,
            "passed": violations == 0,
            "violation_count": violations,
            "expected_value": "0 violations",
            "observed_value":
                f"{violations} violations",
        })

    (
        spark.createDataFrame(
            result_rows,
            schema=RESULT_SCHEMA,
        )
        .write
        .mode("append")
        .saveAsTable(results_table)
    )

    total = len(result_rows)

    passed = sum(
        row["passed"]
        for row in result_rows
    )

    failed = total - passed

    critical_failures = sum(
        1
        for row in result_rows
        if (
            not row["passed"]
            and row["severity"] == "CRITICAL"
        )
    )

    run_status = (
        "FAILED"
        if critical_failures
        else "PASSED_WITH_WARNINGS"
        if failed
        else "PASSED"
    )

    run_row = [{
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": utc_now(),

        "total_checks": total,
        "passed_checks": passed,
        "failed_checks": failed,
        "critical_failures":
            critical_failures,

        "run_status": run_status,
        "notes": None,
    }]

    (
        spark.createDataFrame(
            run_row,
            schema=RUN_SCHEMA,
        )
        .write
        .mode("append")
        .saveAsTable(runs_table)
    )

    return {
        "run_id": run_id,
        "total_checks": total,
        "passed_checks": passed,
        "failed_checks": failed,
        "critical_failures":
            critical_failures,
        "status": run_status,
    }

def enforce_quality_gate(
    result: dict[str, Any],
) -> None:
    """
    Stop downstream processing when canonical
    Silver has critical quality violations.
    """

    critical_failures = int(
        result.get(
            "critical_failures",
            0,
        )
    )

    if critical_failures > 0:
        raise RuntimeError(
            "Silver quality gate failed: "
            f"{critical_failures} critical "
            "quality checks failed."
        )