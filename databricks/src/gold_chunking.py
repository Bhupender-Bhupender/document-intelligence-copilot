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

from src.chunking.hierarchical_chunker import (
    build_hierarchical_chunks,
)

from src.core.config import config

from databricks.src.silver_chunk_adapter import (
    build_chunker_input,
)


CHUNKING_VERSION = "hierarchical_v1"


CHUNK_SCHEMA = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("document_id", StringType(), False),
    StructField("page_id", StringType(), False),
    StructField("page_number", IntegerType(), False),

    StructField("file_name", StringType(), False),
    StructField("file_type", StringType(), False),
    StructField("section_title", StringType(), True),

    StructField("text", StringType(), False),
    StructField("word_count", IntegerType(), False),
    StructField("chunk_index", IntegerType(), False),

    StructField("chunk_level", StringType(), False),
    StructField("parent_chunk_id", StringType(), True),

    StructField("source_sha256", StringType(), False),

    StructField("chunking_version", StringType(), False),
    StructField("parent_chunk_size_words", IntegerType(), False),
    StructField("child_chunk_size_words", IntegerType(), False),
    StructField("child_chunk_overlap_words", IntegerType(), False),

    StructField("embedding_model", StringType(), True),
    StructField("is_indexed", BooleanType(), False),

    StructField("created_at", TimestampType(), False),
])


MANIFEST_SCHEMA = StructType([
    StructField("document_id", StringType(), False),
    StructField("source_sha256", StringType(), False),

    StructField("chunking_version", StringType(), False),
    StructField("parent_chunk_size_words", IntegerType(), False),
    StructField("child_chunk_size_words", IntegerType(), False),
    StructField("child_chunk_overlap_words", IntegerType(), False),

    StructField("parent_chunk_count", IntegerType(), False),
    StructField("child_chunk_count", IntegerType(), False),

    StructField("chunking_status", StringType(), False),
    StructField("chunked_at", TimestampType(), False),
    StructField("is_current", BooleanType(), False),
])


def utc_now():
    return datetime.now(timezone.utc)


def chunk_to_row(
    chunk,
    *,
    source_sha256: str,
    created_at,
) -> dict:

    return {
        "chunk_id": chunk.chunk_id,
        "document_id": chunk.doc_id,
        "page_id": chunk.page_id,
        "page_number": int(chunk.page_number),

        "file_name": chunk.file_name,
        "file_type": chunk.file_type,
        "section_title": chunk.section_title,

        "text": chunk.text,
        "word_count": int(chunk.word_count),
        "chunk_index": int(chunk.chunk_index),

        "chunk_level": chunk.chunk_level,
        "parent_chunk_id": chunk.parent_chunk_id,

        "source_sha256": source_sha256,

        "chunking_version": CHUNKING_VERSION,

        "parent_chunk_size_words":
            int(config.parent_chunk_size_words),

        "child_chunk_size_words":
            int(config.child_chunk_size_words),

        "child_chunk_overlap_words":
            int(config.child_chunk_overlap_words),

        "embedding_model": None,
        "is_indexed": False,

        "created_at": created_at,
    }


def already_chunked(
    spark: Any,
    *,
    document_id: str,
    source_sha256: str,
    manifest_table: str,
) -> bool:

    count = spark.sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {manifest_table}

        WHERE document_id = '{document_id}'
          AND source_sha256 = '{source_sha256}'
          AND chunking_version = '{CHUNKING_VERSION}'
          AND parent_chunk_size_words =
              {int(config.parent_chunk_size_words)}
          AND child_chunk_size_words =
              {int(config.child_chunk_size_words)}
          AND child_chunk_overlap_words =
              {int(config.child_chunk_overlap_words)}
          AND chunking_status = 'SUCCESS'
          AND is_current = true
        """
    ).first()["n"]

    return int(count) > 0


def run_gold_chunking(
    spark: Any,
    *,
    bronze_manifest_table: str,
    documents_table: str,
    pages_table: str,
    blocks_table: str,
    parent_table: str,
    child_table: str,
    chunking_manifest_table: str,
) -> dict:

    documents = spark.sql(
        f"""
        SELECT
            document_id,
            sha256

        FROM {documents_table}

        WHERE is_current = true
          AND extraction_status = 'EXTRACTED'

        ORDER BY document_id
        """
    ).collect()

    discovered = len(documents)
    processed = 0
    unchanged = 0
    failed = 0

    total_parents = 0
    total_children = 0

    for document in documents:

        document_id = document["document_id"]
        source_sha256 = document["sha256"]

        try:

            if already_chunked(
                spark,
                document_id=document_id,
                source_sha256=source_sha256,
                manifest_table=chunking_manifest_table,
            ):
                unchanged += 1
                continue

            raw_document, pages = (
                build_chunker_input(
                    spark,
                    document_id=document_id,
                    manifest_table=bronze_manifest_table,
                    documents_table=documents_table,
                    pages_table=pages_table,
                    blocks_table=blocks_table,
                )
            )

            parents, children = (
                build_hierarchical_chunks(
                    raw_document,
                    pages,
                )
            )

            if not parents or not children:
                raise RuntimeError(
                    "Hierarchical chunking produced no usable chunks."
                )

            parent_ids = {
                chunk.chunk_id
                for chunk in parents
            }

            if any(
                child.parent_chunk_id
                not in parent_ids
                for child in children
            ):
                raise RuntimeError(
                    "Child chunk references missing parent."
                )

            created_at = utc_now()

            parent_rows = [
                chunk_to_row(
                    chunk,
                    source_sha256=source_sha256,
                    created_at=created_at,
                )
                for chunk in parents
            ]

            child_rows = [
                chunk_to_row(
                    chunk,
                    source_sha256=source_sha256,
                    created_at=created_at,
                )
                for chunk in children
            ]

            # Replace only this document's previous Gold projection.
            spark.sql(
                f"""
                DELETE FROM {parent_table}
                WHERE document_id = '{document_id}'
                """
            )

            spark.sql(
                f"""
                DELETE FROM {child_table}
                WHERE document_id = '{document_id}'
                """
            )

            (
                spark.createDataFrame(
                    parent_rows,
                    schema=CHUNK_SCHEMA,
                )
                .write
                .mode("append")
                .saveAsTable(parent_table)
            )

            (
                spark.createDataFrame(
                    child_rows,
                    schema=CHUNK_SCHEMA,
                )
                .write
                .mode("append")
                .saveAsTable(child_table)
            )

            spark.sql(
                f"""
                UPDATE {chunking_manifest_table}

                SET is_current = false

                WHERE document_id = '{document_id}'
                  AND is_current = true
                """
            )

            manifest_row = [{
                "document_id": document_id,
                "source_sha256": source_sha256,

                "chunking_version":
                    CHUNKING_VERSION,

                "parent_chunk_size_words":
                    int(config.parent_chunk_size_words),

                "child_chunk_size_words":
                    int(config.child_chunk_size_words),

                "child_chunk_overlap_words":
                    int(config.child_chunk_overlap_words),

                "parent_chunk_count":
                    len(parents),

                "child_chunk_count":
                    len(children),

                "chunking_status":
                    "SUCCESS",

                "chunked_at":
                    created_at,

                "is_current":
                    True,
            }]

            (
                spark.createDataFrame(
                    manifest_row,
                    schema=MANIFEST_SCHEMA,
                )
                .write
                .mode("append")
                .saveAsTable(
                    chunking_manifest_table
                )
            )

            processed += 1
            total_parents += len(parents)
            total_children += len(children)

        except Exception:
            failed += 1

    if failed:
        status = "PARTIAL"
    elif processed == 0:
        status = "NOOP"
    else:
        status = "SUCCESS"

    return {
        "documents_discovered": discovered,
        "documents_processed": processed,
        "documents_unchanged": unchanged,
        "documents_failed": failed,
        "parent_chunks_written": total_parents,
        "child_chunks_written": total_children,
        "status": status,
    }