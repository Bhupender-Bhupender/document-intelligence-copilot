USE CATALOG docintel_dev;

CREATE TABLE IF NOT EXISTS docintel_dev.gold.parent_chunks
(
    chunk_id                    STRING,
    document_id                 STRING,
    page_id                     STRING,
    page_number                 INT,

    file_name                   STRING,
    file_type                   STRING,
    section_title               STRING,

    text                        STRING,
    word_count                  INT,
    chunk_index                 INT,

    chunk_level                 STRING,
    parent_chunk_id             STRING,

    source_sha256               STRING,

    chunking_version            STRING,
    parent_chunk_size_words     INT,
    child_chunk_size_words      INT,
    child_chunk_overlap_words   INT,

    embedding_model             STRING,
    is_indexed                  BOOLEAN,

    created_at                  TIMESTAMP
)
USING DELTA;


CREATE TABLE IF NOT EXISTS docintel_dev.gold.child_chunks
(
    chunk_id                    STRING,
    document_id                 STRING,
    page_id                     STRING,
    page_number                 INT,

    file_name                   STRING,
    file_type                   STRING,
    section_title               STRING,

    text                        STRING,
    word_count                  INT,
    chunk_index                 INT,

    chunk_level                 STRING,
    parent_chunk_id             STRING,

    source_sha256               STRING,

    chunking_version            STRING,
    parent_chunk_size_words     INT,
    child_chunk_size_words      INT,
    child_chunk_overlap_words   INT,

    embedding_model             STRING,
    is_indexed                  BOOLEAN,

    created_at                  TIMESTAMP
)
USING DELTA;


CREATE TABLE IF NOT EXISTS docintel_dev.gold.chunking_manifest
(
    document_id                 STRING,
    source_sha256               STRING,

    chunking_version            STRING,
    parent_chunk_size_words     INT,
    child_chunk_size_words      INT,
    child_chunk_overlap_words   INT,

    parent_chunk_count          INT,
    child_chunk_count           INT,

    chunking_status             STRING,
    chunked_at                  TIMESTAMP,
    is_current                  BOOLEAN
)
USING DELTA;