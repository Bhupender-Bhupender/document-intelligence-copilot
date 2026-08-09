from __future__ import annotations

import hashlib
import shutil
import uuid

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SUPPORTED_EXTENSIONS = {
    ".pdf",
    ".docx",
    ".txt",
    ".md",
}


MANIFEST_SCHEMA = StructType([
    StructField("document_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("file_extension", StringType(), False),
    StructField("file_size_bytes", LongType(), False),
    StructField("sha256", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("ingestion_batch_id", StringType(), False),
    StructField("discovered_at", TimestampType(), False),
    StructField("ingested_at", TimestampType(), False),
    StructField("ingestion_status", StringType(), False),
    StructField("processing_status", StringType(), False),
    StructField("is_current", BooleanType(), False),
    StructField(
        "source_metadata",
        MapType(StringType(), StringType()),
        True,
    ),
])


INGESTION_RUN_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), True),
    StructField("source_system", StringType(), False),
    StructField("files_discovered", LongType(), False),
    StructField("files_new", LongType(), False),
    StructField("files_unchanged", LongType(), False),
    StructField("files_quarantined", LongType(), False),
    StructField("files_failed", LongType(), False),
    StructField("run_status", StringType(), False),
    StructField("notes", StringType(), True),
])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for block in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def document_id_from_hash(file_hash: str) -> str:
    return f"doc_{file_hash[:16]}"


def run_incremental_ingestion(
    spark: Any,
    *,
    landing_root: str,
    manifest_table: str,
    runs_table: str,
    source_system: str = "unity_catalog_volume",
) -> dict[str, Any]:

    root = Path(landing_root)

    incoming_dir = root / "incoming"
    archive_dir = root / "archive"
    quarantine_dir = root / "quarantine"

    for directory in (
        incoming_dir,
        archive_dir,
        quarantine_dir,
    ):
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    run_id = f"ing_{uuid.uuid4().hex[:12]}"
    started_at = utc_now()

    files = sorted(
        path
        for path in incoming_dir.iterdir()
        if path.is_file()
    )

    discovered = len(files)

    existing_rows = spark.sql(
        f"""
        SELECT sha256
        FROM {manifest_table}
        WHERE sha256 IS NOT NULL
        """
    ).collect()

    seen_hashes = {
        row["sha256"]
        for row in existing_rows
    }

    new_records: list[dict[str, Any]] = []

    new_count = 0
    unchanged_count = 0
    quarantined_count = 0
    failed_count = 0

    run_status = "SUCCESS"
    error: Exception | None = None

    try:

        for source_path in files:

            extension = source_path.suffix.lower()

            # ---------------------------------------------------------
            # Unsupported source
            # ---------------------------------------------------------

            if extension not in SUPPORTED_EXTENSIONS:

                destination = (
                    quarantine_dir
                    / f"{run_id}_{source_path.name}"
                )

                shutil.move(
                    str(source_path),
                    str(destination),
                )

                quarantined_count += 1
                continue

            # ---------------------------------------------------------
            # Content fingerprint
            # ---------------------------------------------------------

            file_hash = sha256_file(source_path)

            # Already registered = idempotent skip
            if file_hash in seen_hashes:

                source_path.unlink()

                unchanged_count += 1
                continue

            document_id = document_id_from_hash(
                file_hash
            )

            # ---------------------------------------------------------
            # Content-addressed archive
            # ---------------------------------------------------------

            archive_directory = (
                archive_dir
                / file_hash[:2]
            )

            archive_directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            archive_path = (
                archive_directory
                / f"{file_hash}{extension}"
            )

            if not archive_path.exists():

                shutil.copy2(
                    source_path,
                    archive_path,
                )

            # ---------------------------------------------------------
            # Previous filename version
            # ---------------------------------------------------------

            escaped_name = (
                source_path.name
                .replace("'", "''")
            )

            spark.sql(
                f"""
                UPDATE {manifest_table}

                SET is_current = false

                WHERE file_name = '{escaped_name}'
                  AND is_current = true
                  AND sha256 <> '{file_hash}'
                """
            )

            stat = source_path.stat()

            new_records.append({
                "document_id": document_id,
                "file_name": source_path.name,
                "file_path": str(archive_path),
                "file_extension": extension,
                "file_size_bytes": int(stat.st_size),
                "sha256": file_hash,
                "source_system": source_system,
                "ingestion_batch_id": run_id,
                "discovered_at": started_at,
                "ingested_at": utc_now(),
                "ingestion_status": "INGESTED",
                "processing_status": "PENDING",
                "is_current": True,
                "source_metadata": {
                    "landing_zone": "incoming",
                    "archive_strategy":
                        "content_addressed",
                },
            })

            seen_hashes.add(file_hash)
            new_count += 1

        # -------------------------------------------------------------
        # Commit document manifest records
        # -------------------------------------------------------------

        if new_records:

            (
                spark.createDataFrame(
                    new_records,
                    schema=MANIFEST_SCHEMA,
                )
                .write
                .mode("append")
                .saveAsTable(manifest_table)
            )

        # Only remove incoming copies after Delta commit succeeds.
        for record in new_records:

            candidate = (
                incoming_dir
                / record["file_name"]
            )

            if candidate.exists():
                candidate.unlink()

    except Exception as exc:

        failed_count += 1
        run_status = "FAILED"
        error = exc

    # -----------------------------------------------------------------
    # Persist run-level observability
    # -----------------------------------------------------------------

    run_record = [{
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": utc_now(),
        "source_system": source_system,
        "files_discovered": discovered,
        "files_new": new_count,
        "files_unchanged": unchanged_count,
        "files_quarantined": quarantined_count,
        "files_failed": failed_count,
        "run_status": run_status,
        "notes": (
            None
            if error is None
            else type(error).__name__
        ),
    }]

    (
        spark.createDataFrame(
            run_record,
            schema=INGESTION_RUN_SCHEMA,
        )
        .write
        .mode("append")
        .saveAsTable(runs_table)
    )

    if error is not None:
        raise error

    return {
        "run_id": run_id,
        "files_discovered": discovered,
        "files_new": new_count,
        "files_unchanged": unchanged_count,
        "files_quarantined": quarantined_count,
        "files_failed": failed_count,
        "status": run_status,
    }
