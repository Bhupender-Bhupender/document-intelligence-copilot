from databricks.src.document_extraction import (
    classify_extraction,
    stable_block_id,
    stable_page_id,
)


def test_good_page():
    quality, ocr = classify_extraction(200)

    assert quality == "good"
    assert ocr is False


def test_weak_page():
    quality, ocr = classify_extraction(10)

    assert quality == "weak"
    assert ocr is True


def test_empty_page():
    quality, ocr = classify_extraction(0)

    assert quality == "empty"
    assert ocr is True


def test_page_id_is_stable():
    assert (
        stable_page_id("doc_123", 7)
        == "doc_123_page_00007"
    )


def test_block_id_is_stable():
    assert (
        stable_block_id(
            "doc_123",
            7,
            3,
        )
        ==
        "doc_123_page_00007_block_00003"
    )