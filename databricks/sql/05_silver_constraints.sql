-- Phase 7: enforced canonical Silver row-level constraints


-- ---------------------------------------------------------
-- Pages
-- ---------------------------------------------------------

ALTER TABLE docintel_dev.silver.pages
ADD CONSTRAINT pages_page_number_positive
CHECK (page_number >= 1);


ALTER TABLE docintel_dev.silver.pages
ADD CONSTRAINT pages_word_count_nonnegative
CHECK (word_count >= 0);


ALTER TABLE docintel_dev.silver.pages
ADD CONSTRAINT pages_quality_valid
CHECK (
    extraction_quality IN (
        'good',
        'weak',
        'empty'
    )
);


ALTER TABLE docintel_dev.silver.pages
ADD CONSTRAINT pages_quality_ocr_consistent
CHECK (
    (
        extraction_quality = 'good'
        AND requires_ocr = false
    )
    OR
    (
        extraction_quality IN (
            'weak',
            'empty'
        )
        AND requires_ocr = true
    )
);


-- ---------------------------------------------------------
-- Blocks
-- ---------------------------------------------------------

ALTER TABLE docintel_dev.silver.blocks
ADD CONSTRAINT blocks_page_number_positive
CHECK (page_number >= 1);


ALTER TABLE docintel_dev.silver.blocks
ADD CONSTRAINT blocks_order_nonnegative
CHECK (block_order >= 0);


ALTER TABLE docintel_dev.silver.blocks
ADD CONSTRAINT blocks_text_nonempty
CHECK (
    text IS NOT NULL
    AND length(trim(text)) > 0
);


-- ---------------------------------------------------------
-- Documents
-- ---------------------------------------------------------

ALTER TABLE docintel_dev.silver.documents
ADD CONSTRAINT documents_page_count_nonnegative
CHECK (
    page_count IS NULL
    OR page_count >= 0
);


ALTER TABLE docintel_dev.silver.documents
ADD CONSTRAINT documents_block_count_nonnegative
CHECK (
    block_count IS NULL
    OR block_count >= 0
);


ALTER TABLE docintel_dev.silver.documents
ADD CONSTRAINT documents_ocr_count_nonnegative
CHECK (
    ocr_page_count IS NULL
    OR ocr_page_count >= 0
);