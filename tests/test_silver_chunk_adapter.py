from databricks.src.silver_chunk_adapter import (
    normalize_block_type,
    silver_quality_to_status,
)


def test_block_type_mapping():

    assert (
        normalize_block_type(
            "section_header"
        )
        == "heading"
    )

    assert (
        normalize_block_type("text")
        == "paragraph"
    )

    assert (
        normalize_block_type("figure")
        == "caption"
    )

    assert (
        normalize_block_type(
            "page_header"
        )
        == "unknown"
    )


def test_unknown_block_type():

    assert (
        normalize_block_type(
            "something_new"
        )
        == "unknown"
    )


def test_quality_mapping():

    assert (
        silver_quality_to_status(
            "good"
        )
        == "ok"
    )

    assert (
        silver_quality_to_status(
            "weak"
        )
        == "weak"
    )

    assert (
        silver_quality_to_status(
            "empty"
        )
        == "empty"
    )