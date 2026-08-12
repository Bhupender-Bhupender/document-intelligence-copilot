from __future__ import annotations

import html
import json
import re

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import functions as F

from databricks.src.document_extraction import (
    BLOCK_SCHEMA,
    PAGE_SCHEMA,
    stable_block_id,
    stable_page_id,
)


MANAGED_EXTRACTION_METHOD = (
    "databricks_ai_parse_document"
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def clean_managed_text(
    value: str | None,
) -> str:
    text = html.unescape(value or "")

    # Databricks parser can represent tables as HTML.
    text = re.sub(
        r"<[^>]+>",
        " ",
        text,
    )

    return " ".join(text.split())


def build_page_range(
    page_numbers: list[int],
) -> str:
    """
    Return ai_parse_document pageRange syntax.

    Example:
        [35, 36, 37] -> "35,36,37"
    """
    return ",".join(
        str(number)
        for number in sorted(set(page_numbers))
    )


def parse_candidate_pages(
    spark: Any,
    *,
    source_path: str,
    page_numbers: list[int],
) -> dict[str, Any]:

    if not page_numbers:
        raise ValueError(
            "page_numbers must not be empty"
        )

    binary_df = (
        spark.read
        .format("binaryFile")
        .load(source_path)
    )

    page_range = build_page_range(
        page_numbers
    )

    parsed_df = binary_df.select(
        F.to_json(
            F.ai_parse_document(
                F.col("content"),
                {
                    "version": "2.0",
                    "pageRange": page_range,
                    "descriptionElementTypes": "",
                },
            )
        ).alias("parsed_json")
    )

    row = parsed_df.first()

    if row is None:
        raise RuntimeError(
            "Managed document parser returned no result."
        )

    parsed_json = row["parsed_json"]

    if not parsed_json:
        raise RuntimeError(
            "Managed document parser returned empty JSON."
        )

    result = json.loads(parsed_json)

    error_status = result.get(
        "error_status"
    )

    if error_status:
        raise RuntimeError(
            "Managed document parser reported an error."
        )

    return result


def build_managed_recovery_rows(
    *,
    document_id: str,
    managed_result: dict[str, Any],
    native_word_counts: dict[int, int],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[int],
]:

    document = managed_result.get(
        "document",
        {}
    )

    pages = document.get(
        "pages",
        []
    )

    elements = document.get(
        "elements",
        []
    )

    page_ids = {
        int(page["id"])
        for page in pages
    }

    elements_by_page: dict[
        int,
        list[dict[str, Any]],
    ] = defaultdict(list)

    for element in elements:

        boxes = element.get("bbox") or []

        if not boxes:
            continue

        page_id = boxes[0].get(
            "page_id"
        )

        if page_id is None:
            continue

        page_id = int(page_id)

        if page_id not in page_ids:
            continue

        elements_by_page[
            page_id
        ].append(element)

    created_at = utc_now()

    page_rows = []
    block_rows = []
    accepted_pages = []

    for page_id in sorted(page_ids):

        # ai_parse_document page IDs are 0-based.
        page_number = page_id + 1

        page_elements = sorted(
            elements_by_page.get(
                page_id,
                []
            ),
            key=lambda item: int(
                item.get("id", 0)
            ),
        )

        text_parts = []

        for element in page_elements:

            content = clean_managed_text(
                element.get("content")
            )

            if content:
                text_parts.append(
                    content
                )

        managed_text = "\n".join(
            text_parts
        )

        managed_words = len(
            managed_text.split()
        )

        native_words = int(
            native_word_counts.get(
                page_number,
                0,
            )
        )

        # Never replace native extraction with
        # an equal or worse result.
        if managed_words <= native_words:
            continue

        quality = (
            "good"
            if managed_words >= 20
            else "weak"
        )

        requires_ocr = (
            managed_words < 20
        )

        page_id_value = stable_page_id(
            document_id,
            page_number,
        )

        page_rows.append({
            "page_id":
                page_id_value,

            "document_id":
                document_id,

            "page_number":
                page_number,

            "text":
                managed_text,

            "word_count":
                managed_words,

            "extraction_method":
                MANAGED_EXTRACTION_METHOD,

            "extraction_quality":
                quality,

            "requires_ocr":
                requires_ocr,

            "created_at":
                created_at,
        })

        accepted_pages.append(
            page_number
        )

        current_section = None

        for block_order, element in enumerate(
            page_elements,
            start=1,
        ):

            block_text = clean_managed_text(
                element.get("content")
            )

            if not block_text:
                continue

            block_type = (
                element.get("type")
                or "unknown"
            )

            if block_type in {
                "title",
                "section_header",
            }:
                current_section = (
                    block_text
                )

            block_rows.append({
                "block_id":
                    stable_block_id(
                        document_id,
                        page_number,
                        block_order,
                    ),

                "document_id":
                    document_id,

                "page_id":
                    page_id_value,

                "page_number":
                    page_number,

                "block_order":
                    block_order,

                "block_type":
                    block_type,

                "section_title":
                    current_section,

                "text":
                    block_text,

                "created_at":
                    created_at,
            })

    return (
        page_rows,
        block_rows,
        accepted_pages,
    )

def _merge_recovered_pages(
    spark: Any,
    *,
    page_rows: list[dict[str, Any]],
    pages_table: str,
) -> None:

    if not page_rows:
        return

    df = spark.createDataFrame(
        page_rows,
        schema=PAGE_SCHEMA,
    )

    df.createOrReplaceTempView(
        "_phase6_recovered_pages"
    )

    spark.sql(
        f"""
        MERGE INTO {pages_table} AS target

        USING _phase6_recovered_pages AS source

        ON target.page_id = source.page_id

        WHEN MATCHED THEN
          UPDATE SET

            target.text =
                source.text,

            target.word_count =
                source.word_count,

            target.extraction_method =
                source.extraction_method,

            target.extraction_quality =
                source.extraction_quality,

            target.requires_ocr =
                source.requires_ocr

        WHEN NOT MATCHED THEN
          INSERT *
        """
    )


def _replace_recovered_blocks(
    spark: Any,
    *,
    block_rows: list[dict[str, Any]],
    accepted_page_ids: list[str],
    blocks_table: str,
) -> None:

    if not accepted_page_ids:
        return

    page_id_df = spark.createDataFrame(
        [(value,) for value in accepted_page_ids],
        ["page_id"],
    )

    page_id_df.createOrReplaceTempView(
        "_phase6_recovered_page_ids"
    )

    # Remove stale layout blocks for only the
    # pages whose extraction was replaced.
    spark.sql(
        f"""
        DELETE FROM {blocks_table}

        WHERE page_id IN (
            SELECT page_id
            FROM _phase6_recovered_page_ids
        )
        """
    )

    if not block_rows:
        return

    (
        spark.createDataFrame(
            block_rows,
            schema=BLOCK_SCHEMA,
        )
        .write
        .mode("append")
        .saveAsTable(blocks_table)
    )


def run_managed_ocr_recovery(
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
            document_id,
            source_file_path

        FROM {documents_table}

        WHERE extraction_status =
            'OCR_REQUIRED'

          AND is_current = true
        """
    ).collect()

    if not documents:
        return {
            "documents_discovered": 0,
            "documents_resolved": 0,
            "documents_remaining": 0,
            "pages_attempted": 0,
            "pages_improved": 0,
            "pages_resolved": 0,
            "documents_failed": 0,
            "status": "NOOP",
        }

    documents_resolved = 0
    documents_remaining = 0

    pages_attempted = 0
    pages_improved = 0
    pages_resolved = 0

    failed = 0

    for document in documents:

        document_id = document[
            "document_id"
        ]

        try:

            candidates = spark.sql(
                f"""
                SELECT
                    page_number,
                    word_count

                FROM {pages_table}

                WHERE document_id =
                    '{document_id}'

                  AND requires_ocr = true

                ORDER BY page_number
                """
            ).collect()

            candidate_numbers = [
                int(row["page_number"])
                for row in candidates
            ]

            if not candidate_numbers:
                continue

            pages_attempted += len(
                candidate_numbers
            )

            native_word_counts = {
                int(row["page_number"]):
                    int(row["word_count"] or 0)

                for row in candidates
            }

            managed_result = (
                parse_candidate_pages(
                    spark,
                    source_path=document[
                        "source_file_path"
                    ],
                    page_numbers=(
                        candidate_numbers
                    ),
                )
            )

            (
                page_rows,
                block_rows,
                accepted_pages,
            ) = build_managed_recovery_rows(
                document_id=document_id,
                managed_result=managed_result,
                native_word_counts=(
                    native_word_counts
                ),
            )

            pages_improved += len(
                accepted_pages
            )

            pages_resolved += sum(
                1
                for row in page_rows
                if not row["requires_ocr"]
            )

            _merge_recovered_pages(
                spark,
                page_rows=page_rows,
                pages_table=pages_table,
            )

            accepted_page_ids = [
                stable_page_id(
                    document_id,
                    page_number,
                )
                for page_number
                in accepted_pages
            ]

            _replace_recovered_blocks(
                spark,
                block_rows=block_rows,
                accepted_page_ids=(
                    accepted_page_ids
                ),
                blocks_table=blocks_table,
            )

            remaining = (
                spark.sql(
                    f"""
                    SELECT COUNT(*) AS n

                    FROM {pages_table}

                    WHERE document_id =
                        '{document_id}'

                      AND requires_ocr = true
                    """
                )
                .first()["n"]
            )

            total_blocks = (
                spark.sql(
                    f"""
                    SELECT COUNT(*) AS n

                    FROM {blocks_table}

                    WHERE document_id =
                        '{document_id}'
                    """
                )
                .first()["n"]
            )

            total_ocr_pages = (
                spark.sql(
                    f"""
                    SELECT COUNT(*) AS n

                    FROM {pages_table}

                    WHERE document_id =
                        '{document_id}'

                      AND extraction_method =
                        '{MANAGED_EXTRACTION_METHOD}'
                    """
                )
                .first()["n"]
            )

            if remaining == 0:

                final_status = "EXTRACTED"

                documents_resolved += 1

            else:

                final_status = (
                    "OCR_REQUIRED"
                )

                documents_remaining += 1

            spark.sql(
                f"""
                UPDATE {documents_table}

                SET
                    extraction_status =
                        '{final_status}',

                    ocr_required = true,

                    ocr_page_count =
                        {int(total_ocr_pages)},

                    block_count =
                        {int(total_blocks)},

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
                    '{final_status}'

                WHERE document_id =
                    '{document_id}'

                  AND is_current = true
                """
            )

        except Exception:

            failed += 1

    if failed:
        status = "PARTIAL"
    else:
        status = "SUCCESS"

    return {
        "documents_discovered":
            len(documents),

        "documents_resolved":
            documents_resolved,

        "documents_remaining":
            documents_remaining,

        "pages_attempted":
            pages_attempted,

        "pages_improved":
            pages_improved,

        "pages_resolved":
            pages_resolved,

        "documents_failed":
            failed,

        "status":
            status,
    }