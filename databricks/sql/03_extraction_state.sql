-- Phase 6: document extraction processing state

ALTER TABLE docintel_dev.silver.documents
ADD COLUMNS (
    extraction_status STRING,
    page_count INT,
    block_count INT,
    ocr_page_count INT,
    extracted_at TIMESTAMP
);
