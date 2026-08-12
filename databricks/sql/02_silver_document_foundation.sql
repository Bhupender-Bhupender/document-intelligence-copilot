-- Phase 5: Silver document-processing foundation

USE CATALOG docintel_dev;

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Validated and normalized document processing layer';


CREATE TABLE IF NOT EXISTS docintel_dev.silver.documents
(
    extraction_status STRING,
    page_count        INT,
    block_count       INT,
    ocr_page_count    INT,
    extracted_at      TIMESTAMP,
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
