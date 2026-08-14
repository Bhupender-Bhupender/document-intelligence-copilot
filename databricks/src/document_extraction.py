from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PAGE_SCHEMA = StructType([
    StructField("page_id", StringType(), False),
    StructField("document_id", StringType(), False),
    StructField("page_number", IntegerType(), False),
    StructField("text", StringType(), False),
    StructField("word_count", IntegerType(), False),
    StructField("extraction_method", StringType(), False),
    StructField("extraction_quality", StringType(), False),
    StructField("requires_ocr", BooleanType(), False),
    StructField("created_at", TimestampType(), False),
])


BLOCK_SCHEMA = StructType([
    StructField("block_id", StringType(), False),
    StructField("document_id", StringType(), False),
    StructField("page_id", StringType(), False),
    StructField("page_number", IntegerType(), False),
    StructField("block_order", IntegerType(), False),
    StructField("block_type", StringType(), False),
    StructField("section_title", StringType(), True),
    StructField("text", StringType(), False),
    StructField("created_at", TimestampType(), False),
])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def classify_extraction(
    word_count: int,
) -> tuple[str, bool]:

    if word_count <= 0:
        return "empty", True

    if word_count < 20:
        return "weak", True

    return "good", False


def stable_page_id(
    document_id: str,
    page_number: int,
) -> str:

    return f"{document_id}_page_{page_number:05d}"


def stable_block_id(
    document_id: str,
    page_number: int,
    block_order: int,
) -> str:

    return (
        f"{document_id}_"
        f"page_{page_number:05d}_"
        f"block_{block_order:05d}"
    )


def normalize_block(
    block: Any,
) -> dict[str, Any]:

    if hasattr(block, "model_dump"):
        return block.model_dump()

    if isinstance(block, dict):
        return dict(block)

    return {}


def build_page_and_block_rows(
    *,
    document_id: str,
    parsed_pages: list[Any],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:

    page_rows = []
    block_rows = []

    created_at = utc_now()

    for page in parsed_pages:

        data = page.model_dump()

        page_number = int(
            data.get("page_number") or 0
        )

        page_id = stable_page_id(
            document_id,
            page_number,
        )

        word_count = int(
            data.get("word_count") or 0
        )

        quality, requires_ocr = (
            classify_extraction(word_count)
        )

        text = (
            data.get("normalized_text")
            or data.get("raw_text")
            or ""
        )

        page_rows.append({
            "page_id": page_id,
            "document_id": document_id,
            "page_number": page_number,
            "text": text,
            "word_count": word_count,
            "extraction_method":
                data.get("parse_method")
                or "unknown",
            "extraction_quality": quality,
            "requires_ocr": requires_ocr,
            "created_at": created_at,
        })

        layout_blocks = (
            data.get("layout_blocks")
            or []
        )

        for fallback_order, block in enumerate(
            layout_blocks,
            start=1,
        ):

            block_data = normalize_block(block)
            block_text = (
            block_data.get("text")
            or ""
            ).strip()

            if not block_text:
                continue

            reading_order = block_data.get(
                "reading_order"
            )

            if reading_order is None:
                block_order = fallback_order
            else:
                block_order = int(reading_order)

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
                    page_id,

                "page_number":
                    page_number,

                "block_order":
                    block_order,

                "block_type":
                    block_data.get("block_type")
                    or "unknown",

                "section_title":
                    block_data.get("section_title")
                    or data.get("section_title"),

                "text":
                    block_text,

                "created_at":
                    created_at,
            })

    return page_rows, block_rows
