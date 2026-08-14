from __future__ import annotations

from collections import defaultdict
from typing import Any

from src.schema.models import (
    ParsedBlock,
    ParsedPage,
    RawDocument,
)


BLOCK_TYPE_MAP = {
    "heading": "heading",
    "section_header": "heading",

    "paragraph": "paragraph",
    "text": "paragraph",

    "table": "table",
    "list": "list",

    "caption": "caption",
    "figure": "caption",

    "page_number": "unknown",
    "page_header": "unknown",
    "unknown": "unknown",
}


def normalize_block_type(
    block_type: str | None,
) -> str:
    return BLOCK_TYPE_MAP.get(
        (block_type or "unknown").lower(),
        "unknown",
    )


def silver_quality_to_status(
    quality: str | None,
) -> str:
    mapping = {
        "good": "ok",
        "weak": "weak",
        "empty": "empty",
    }

    return mapping.get(
        quality or "",
        "empty",
    )


def build_chunker_input(
    spark: Any,
    *,
    document_id: str,
    manifest_table: str,
    documents_table: str,
    pages_table: str,
    blocks_table: str,
) -> tuple[
    RawDocument,
    list[ParsedPage],
]:

    document = spark.sql(
        f"""
        SELECT
            s.document_id,
            s.file_name,
            s.file_extension,
            s.source_file_path,
            s.file_size_bytes,
            s.sha256,
            s.page_count,
            b.ingested_at

        FROM {documents_table} s

        INNER JOIN {manifest_table} b
          ON s.document_id = b.document_id
         AND b.is_current = true

        WHERE s.document_id = '{document_id}'
          AND s.is_current = true
          AND s.extraction_status = 'EXTRACTED'

        LIMIT 1
        """
    ).first()

    if document is None:
        raise ValueError(
            "No eligible extracted document found."
        )

    raw_document = RawDocument(
        doc_id=document["document_id"],
        source_path=document["source_file_path"],
        file_name=document["file_name"],
        file_type=document["file_extension"],
        ingested_at=document["ingested_at"],
        byte_size=int(
            document["file_size_bytes"]
            or 0
        ),
        checksum=document["sha256"],
        total_pages=int(
            document["page_count"]
            or 0
        ),
    )

    page_rows = spark.sql(
        f"""
        SELECT
            page_id,
            document_id,
            page_number,
            text,
            word_count,
            extraction_method,
            extraction_quality

        FROM {pages_table}

        WHERE document_id = '{document_id}'

        ORDER BY page_number
        """
    ).collect()

    block_rows = spark.sql(
        f"""
        SELECT
            block_id,
            document_id,
            page_number,
            block_order,
            block_type,
            section_title,
            text

        FROM {blocks_table}

        WHERE document_id = '{document_id}'

        ORDER BY
            page_number,
            block_order
        """
    ).collect()

    blocks_by_page = defaultdict(list)

    for row in block_rows:

        text = (
            row["text"] or ""
        ).strip()

        if not text:
            continue

        block = ParsedBlock(
            block_id=row["block_id"],
            doc_id=row["document_id"],
            page_number=int(
                row["page_number"]
            ),
            block_type=normalize_block_type(
                row["block_type"]
            ),
            text=text,
            reading_order=int(
                row["block_order"]
            ),
            bounding_box=None,
            section_title=row[
                "section_title"
            ],
        )

        blocks_by_page[
            int(row["page_number"])
        ].append(block)

    pages: list[ParsedPage] = []

    for row in page_rows:

        text = row["text"] or ""

        page_number = int(
            row["page_number"]
        )

        page_blocks = blocks_by_page.get(
            page_number,
            [],
        )

        section_title = next(
            (
                block.section_title
                for block in page_blocks
                if block.section_title
            ),
            None,
        )

        page = ParsedPage(
            page_id=row["page_id"],
            doc_id=row["document_id"],
            page_number=page_number,

            raw_text=text,
            normalized_text=text,

            word_count=int(
                row["word_count"]
                or 0
            ),
            char_count=len(text),

            parse_method=row[
                "extraction_method"
            ],

            extraction_status=(
                silver_quality_to_status(
                    row[
                        "extraction_quality"
                    ]
                )
            ),

            ocr_confidence=None,

            ocr_engine=(
                row["extraction_method"]
                if row[
                    "extraction_method"
                ] in {
                    "rapidocr",
                    "paddleocr",
                    "azure_di",
                    "databricks_ai_parse_document",
                }
                else None
            ),

            section_title=section_title,
            layout_blocks=page_blocks,
        )

        pages.append(page)

    return raw_document, pages