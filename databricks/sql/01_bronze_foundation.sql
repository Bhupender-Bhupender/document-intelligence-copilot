-- Phase 3: Databricks Bronze foundation

USE CATALOG docintel_dev;

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingestion metadata and source-level records';

CREATE VOLUME IF NOT EXISTS docintel_dev.bronze.document_landing
COMMENT 'Governed landing area for source documents before processing';


CREATE TABLE IF NOT EXISTS docintel_dev.bronze.document_manifest
(
    document_id        STRING,
    file_name          STRING,
    file_path          STRING,
    file_extension     STRING,
    file_size_bytes    BIGINT,
    sha256             STRING,

    source_system      STRING,
    ingestion_batch_id STRING,

    discovered_at      TIMESTAMP,
    ingested_at        TIMESTAMP,

    ingestion_status   STRING,
    processing_status  STRING,

    is_current         BOOLEAN,

    source_metadata    MAP<STRING, STRING>
)
USING DELTA
COMMENT 'File-level manifest and provenance for ingested source documents';


CREATE TABLE IF NOT EXISTS docintel_dev.bronze.ingestion_runs
(
    run_id              STRING,
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,

    source_system       STRING,

    files_discovered    BIGINT,
    files_new           BIGINT,
    files_unchanged     BIGINT,
    files_quarantined   BIGINT,
    files_failed        BIGINT,

    run_status          STRING,
    notes               STRING
)
USING DELTA
COMMENT 'Operational history for document ingestion runs';
