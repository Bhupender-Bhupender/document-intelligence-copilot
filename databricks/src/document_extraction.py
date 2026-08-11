from __future__ import annotations

import uuid

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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

        (
            quality,
            requires_ocr,
        ) = classify_extraction(
            word_count
        )

        text = (
            data.get("normalized_text")
            or data.get("raw_text")
            or ""
        )

        page_rows.append({
            "page_id":
                page_id,

            "document_id":
                document_id,

            "page_number":
                page_number,

            "text":
                text,

            "word_count":
                word_count,

            "extraction_method":
                data.get("parse_method")
                or "unknown",

            "extraction_quality":
                quality,

            "requires_ocr":
                requires_ocr,

            "created_at":
                created_at,
        })

        layout_blocks = (
            data.get("layout_blocks")
            or []
        )

        for index, block in enumerate(
            layout_blocks,
            start=1,
        ):
            block_data = normalize_block(
                block
            )

            block_rows.append({
                "block_id":
                    stable_block_id(
                        document_id,
                        page_number,
                        index,
                    ),

                "document_id":
                    document_id,

                "page_id":
                    page_id,

                "page_number":
                    page_number,

                "block_order":
                    index,

                "block_type":
                    (
                        block_data.get("block_type")
                        or block_data.get("type")
                        or block_data.get("label")
                        or "unknown"
                    ),

                "section_title":
                    (
                        block_data.get(
                            "section_title"
                        )
                        or data.get(
                            "section_title"
                        )
                    ),

                "text":
                    (
                        block_data.get("text")
                        or block_data.get(
                            "normalized_text"
                        )
                        or ""
                    ),

                "created_at":
                    created_at,
            })

    return page_rows, block_rows