from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ROUTING_SCHEMA = StructType([
    StructField("document_id", StringType(), False),
    StructField("sha256", StringType(), False),

    StructField("file_name", StringType(), False),
    StructField("source_file_path", StringType(), False),
    StructField("file_extension", StringType(), False),
    StructField("file_size_bytes", LongType(), False),

    StructField("source_system", StringType(), False),
    StructField("ingestion_batch_id", StringType(), False),

    StructField("parse_strategy", StringType(), False),
    StructField("ocr_required", BooleanType(), True),

    StructField("routing_status", StringType(), False),
    StructField("route_reason", StringType(), False),
    StructField("routed_at", TimestampType(), False),

    StructField("parser_version", StringType(), False),

    StructField("is_current", BooleanType(), False),

    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def route_for_extension(
    extension: str,
) -> tuple[str, bool | None, str]:
    """
    Return:
        parse_strategy,
        ocr_required,
        route_reason
    """

    extension = extension.lower().strip()

    if extension in {".txt", ".md"}:
        return (
            "text_reader",
            False,
            "Native text format; OCR not required.",
        )

    if extension == ".docx":
        return (
            "docling",
            False,
            "Structured document; route to Docling parser.",
        )

    if extension == ".pdf":
        return (
            "pdf_inspect",
            None,
            (
                "PDF requires extraction-quality inspection "
                "before native/OCR routing."
            ),
        )

    raise ValueError(
        f"Unsupported document extension: {extension}"
    )


def run_document_routing(
    spark: Any,
    *,
    manifest_table: str,
    silver_documents_table: str,
) -> dict[str, Any]:
    """
    Route PENDING Bronze documents into the Silver document registry.
    """

    pending = spark.sql(
        f"""
        SELECT
            document_id,
            sha256,
            file_name,
            file_path,
            file_extension,
            file_size_bytes,
            source_system,
            ingestion_batch_id,
            is_current

        FROM {manifest_table}

        WHERE ingestion_status = 'INGESTED'
          AND processing_status = 'PENDING'
          AND is_current = true
        """
    ).collect()

    if not pending:
        return {
            "documents_discovered": 0,
            "documents_routed": 0,
            "status": "NOOP",
        }

    now = utc_now()
    rows = []

    for source in pending:

        (
            strategy,
            ocr_required,
            reason,
        ) = route_for_extension(
            source["file_extension"]
        )

        rows.append({
            "document_id":
                source["document_id"],

            "sha256":
                source["sha256"],

            "file_name":
                source["file_name"],

            "source_file_path":
                source["file_path"],

            "file_extension":
                source["file_extension"],

            "file_size_bytes":
                int(source["file_size_bytes"]),

            "source_system":
                source["source_system"],

            "ingestion_batch_id":
                source["ingestion_batch_id"],

            "parse_strategy":
                strategy,

            "ocr_required":
                ocr_required,

            "routing_status":
                "ROUTED",

            "route_reason":
                reason,

            "routed_at":
                now,

            "parser_version":
                "routing-v1",

            "is_current":
                bool(source["is_current"]),

            "created_at":
                now,

            "updated_at":
                now,
        })

    routes_df = spark.createDataFrame(
        rows,
        schema=ROUTING_SCHEMA,
    )

    routes_df.createOrReplaceTempView(
        "_phase5_document_routes"
    )

    # --------------------------------------------------------------
    # Idempotent Silver upsert
    # --------------------------------------------------------------

    spark.sql(
        f"""
        MERGE INTO {silver_documents_table} AS target

        USING _phase5_document_routes AS source

        ON target.document_id = source.document_id

        WHEN MATCHED THEN
          UPDATE SET

            target.sha256 =
                source.sha256,

            target.file_name =
                source.file_name,

            target.source_file_path =
                source.source_file_path,

            target.file_extension =
                source.file_extension,

            target.file_size_bytes =
                source.file_size_bytes,

            target.source_system =
                source.source_system,

            target.ingestion_batch_id =
                source.ingestion_batch_id,

            target.parse_strategy =
                source.parse_strategy,

            target.ocr_required =
                source.ocr_required,

            target.routing_status =
                source.routing_status,

            target.route_reason =
                source.route_reason,

            target.routed_at =
                source.routed_at,

            target.parser_version =
                source.parser_version,

            target.is_current =
                source.is_current,

            target.updated_at =
                source.updated_at

        WHEN NOT MATCHED THEN
          INSERT (
            document_id,
            sha256,
            file_name,
            source_file_path,
            file_extension,
            file_size_bytes,
            source_system,
            ingestion_batch_id,
            parse_strategy,
            ocr_required,
            routing_status,
            route_reason,
            routed_at,
            parser_version,
            is_current,
            created_at,
            updated_at
          )

          VALUES (
            source.document_id,
            source.sha256,
            source.file_name,
            source.source_file_path,
            source.file_extension,
            source.file_size_bytes,
            source.source_system,
            source.ingestion_batch_id,
            source.parse_strategy,
            source.ocr_required,
            source.routing_status,
            source.route_reason,
            source.routed_at,
            source.parser_version,
            source.is_current,
            source.created_at,
            source.updated_at
          )
        """
    )

    # --------------------------------------------------------------
    # Advance Bronze state only after Silver succeeds
    # --------------------------------------------------------------

    spark.sql(
        f"""
        MERGE INTO {manifest_table} AS target

        USING (
            SELECT DISTINCT document_id
            FROM _phase5_document_routes
        ) AS source

        ON target.document_id = source.document_id

        WHEN MATCHED
          AND target.processing_status = 'PENDING'

        THEN UPDATE SET
            processing_status = 'ROUTED'
        """
    )

    return {
        "documents_discovered": len(rows),
        "documents_routed": len(rows),
        "status": "SUCCESS",
    }
