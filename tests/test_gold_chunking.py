from datetime import datetime, timezone

from src.schema.models import DocumentChunk

from databricks.src.gold_chunking import (
    CHUNKING_VERSION,
    chunk_to_row,
)


def test_chunk_to_row_preserves_lineage():

    chunk = DocumentChunk(
        chunk_id="chunk_1",
        doc_id="doc_test",
        page_id="page_1",
        page_number=1,
        file_name="test.pdf",
        file_type=".pdf",
        section_title="Section",
        text="some useful chunk text",
        word_count=4,
        chunk_index=0,
        chunk_level="child",
        parent_chunk_id="parent_1",
    )

    row = chunk_to_row(
        chunk,
        source_sha256="abc",
        created_at=datetime.now(
            timezone.utc
        ),
    )

    assert row["document_id"] == "doc_test"
    assert row["page_id"] == "page_1"
    assert row["page_number"] == 1
    assert row["parent_chunk_id"] == "parent_1"
    assert row["chunk_level"] == "child"
    assert row["chunking_version"] == CHUNKING_VERSION
    assert row["is_indexed"] is False