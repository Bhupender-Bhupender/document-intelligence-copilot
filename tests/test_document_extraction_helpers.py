from databricks.src.document_extraction import (
    classify_extraction,
    stable_block_id,
    stable_page_id,
    build_page_and_block_rows,
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

class FakeModel:
    def __init__(self, data):
        self.data = data

    def model_dump(self):
        return self.data


def test_layout_block_mapping_uses_reading_order():

    block = FakeModel({
        "block_id": "source-block",
        "block_type": "paragraph",
        "reading_order": 7,
        "section_title": "Section",
        "text": "Example",
    })

    page = FakeModel({
        "page_number": 1,
        "word_count": 100,
        "parse_method": "docling",
        "normalized_text": "Example page",
        "raw_text": "",
        "section_title": "Section",
        "layout_blocks": [block],
    })

    pages, blocks = build_page_and_block_rows(
        document_id="doc_test",
        parsed_pages=[page],
    )

    assert len(pages) == 1
    assert len(blocks) == 1

    assert blocks[0]["block_order"] == 7
    assert blocks[0]["block_type"] == "paragraph"