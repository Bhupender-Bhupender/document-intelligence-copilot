-- Phase 5: Silver document-processing foundation

USE CATALOG docintel_dev;

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Validated and normalized document processing layer';


CREATE TABLE IF NOT EXISTS docintel_dev.silver.documents
(
    document_id        STRING,
    sha256             STRING,

    file_name          STRING,
    source_file_path   STRING,
    file_extension     STRING,
    file_size_bytes    BIGINT,

    source_system      STRING,
    ingestion_batch_id STRING,

    parse_strategy     STRING,
    ocr_required       BOOLEAN,

    routing_status     STRING,
    route_reason       STRING,
    routed_at          TIMESTAMP,

    parser_version     STRING,

    is_current         BOOLEAN,

    created_at         TIMESTAMP,
    updated_at         TIMESTAMP
)
USING DELTA
COMMENT 'Normalized document registry and processing route';


CREATE TABLE IF NOT EXISTS docintel_dev.silver.pages
(
    page_id             STRING,
    document_id         STRING,

    page_number         INT,

    text                STRING,
    word_count          INT,

    extraction_method   STRING,
    extraction_quality  STRING,

    requires_ocr        BOOLEAN,

    created_at          TIMESTAMP
)
USING DELTA
COMMENT 'Normalized page-level document content';


CREATE TABLE IF NOT EXISTS docintel_dev.silver.blocks
(
    block_id            STRING,
    document_id         STRING,
    page_id             STRING,

    page_number         INT,
    block_order         INT,

    block_type          STRING,
    section_title       STRING,
    text                STRING,

    created_at          TIMESTAMP
)
USING DELTA
COMMENT 'Layout-aware document blocks extracted from pages';
