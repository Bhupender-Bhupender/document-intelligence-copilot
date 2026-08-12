from __future__ import annotations

from pathlib import Path
from typing import Any

from databricks.src.document_extraction import (
    BLOCK_SCHEMA,
    PAGE_SCHEMA,
    build_page_and_block_rows,
)

from src.ingestion.router import route_file


def _merge_pages(
    spark: Any,
    rows: list[dict],
    table_name: str,
) -> None:

    if not rows:
        return

    df = spark.createDataFrame(
        rows,
        schema=PAGE_SCHEMA,
    )

    df.createOrReplaceTempView(
        "_phase6_pages"
    )

    spark.sql(
        f"""
        MERGE INTO {table_name} AS target

        USING _phase6_pages AS source

        ON target.page_id = source.page_id

        WHEN MATCHED THEN
            UPDATE SET *

        WHEN NOT MATCHED THEN
            INSERT *
        """
    )


def _merge_blocks(
    spark: Any,
    rows: list[dict],
    table_name: str,
) -> None:

    if not rows:
        return

    df = spark.createDataFrame(
        rows,
        schema=BLOCK_SCHEMA,
    )

    df.createOrReplaceTempView(
        "_phase6_blocks"
    )

    spark.sql(
        f"""
        MERGE INTO {table_name} AS target

        USING _phase6_blocks AS source

        ON target.block_id = source.block_id

        WHEN MATCHED THEN
            UPDATE SET *

        WHEN NOT MATCHED THEN
            INSERT *
        """
    )


def run_document_extraction(
    spark: Any,
    *,
    manifest_table: str,
    documents_table: str,
    pages_table: str,
    blocks_table: str,
) -> dict[str, Any]:

    documents = spark.sql(
        f"""
        SELECT
            s.document_id,
            s.source_file_path

        FROM {documents_table} AS s

        INNER JOIN {manifest_table} AS b
          ON s.document_id = b.document_id

        WHERE s.routing_status = 'ROUTED'
          AND s.is_current = true
          AND b.is_current = true
          AND b.processing_status = 'ROUTED'
          AND s.extraction_status IS NULL
        """
    ).collect()

    if not documents:
        return {
            "documents_discovered": 0,
            "documents_extracted": 0,
            "documents_ocr_required": 0,
            "documents_failed": 0,
            "status": "NOOP",
        }

    extracted_count = 0
    ocr_required_count = 0
    failed_count = 0

    for document in documents:

        document_id = document["document_id"]

        try:

            _, parsed_pages = route_file(
                Path(
                    document[
                        "source_file_path"
                    ]
                )
            )

            page_rows, block_rows = (
                build_page_and_block_rows(
                    document_id=document_id,
                    parsed_pages=parsed_pages,
                )
            )

            _merge_pages(
                spark,
                page_rows,
                pages_table,
            )

            _merge_blocks(
                spark,
                block_rows,
                blocks_table,
            )

            ocr_pages = sum(
                1
                for row in page_rows
                if row["requires_ocr"]
            )

            page_count = len(page_rows)
            block_count = len(block_rows)

            if ocr_pages > 0:
                extraction_status = (
                    "OCR_REQUIRED"
                )

                bronze_status = (
                    "OCR_REQUIRED"
                )

                ocr_required_count += 1

            else:
                extraction_status = (
                    "EXTRACTED"
                )

                bronze_status = (
                    "EXTRACTED"
                )

                extracted_count += 1

            spark.sql(
                f"""
                UPDATE {documents_table}

                SET
                    extraction_status =
                        '{extraction_status}',

                    page_count =
                        {page_count},

                    block_count =
                        {block_count},

                    ocr_page_count =
                        {ocr_pages},

                    extracted_at =
                        current_timestamp(),

                    updated_at =
                        current_timestamp()

                WHERE document_id =
                    '{document_id}'
                """
            )

            spark.sql(
                f"""
                UPDATE {manifest_table}

                SET processing_status =
                    '{bronze_status}'

                WHERE document_id =
                    '{document_id}'
                  AND is_current = true
                """
            )

        except Exception:

            failed_count += 1

            spark.sql(
                f"""
                UPDATE {documents_table}

                SET
                    extraction_status =
                        'FAILED',

                    updated_at =
                        current_timestamp()

                WHERE document_id =
                    '{document_id}'
                """
            )

            spark.sql(
                f"""
                UPDATE {manifest_table}

                SET processing_status =
                    'FAILED'

                WHERE document_id =
                    '{document_id}'
                  AND is_current = true
                """
            )

    if failed_count:
        status = "PARTIAL"
    else:
        status = "SUCCESS"

    return {
        "documents_discovered":
            len(documents),

        "documents_extracted":
            extracted_count,

        "documents_ocr_required":
            ocr_required_count,

        "documents_failed":
            failed_count,

        "status":
            status,
    }
