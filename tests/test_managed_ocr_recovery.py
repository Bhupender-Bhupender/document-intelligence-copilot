from databricks.src.managed_ocr_recovery import (
    build_managed_recovery_rows,
    build_page_range,
)


def test_page_range():
    assert (
        build_page_range(
            [37, 35, 36]
        )
        == "35,36,37"
    )


def test_managed_result_accepts_better_page():

    result = {
        "document": {
            "pages": [
                {"id": 34},
            ],
            "elements": [
                {
                    "id": 0,
                    "type": "text",
                    "content":
                        "word " * 30,
                    "bbox": [
                        {
                            "page_id": 34,
                            "coord": [],
                        }
                    ],
                }
            ],
        },
        "error_status": None,
    }

    pages, blocks, accepted = (
        build_managed_recovery_rows(
            document_id="doc_test",
            managed_result=result,
            native_word_counts={
                35: 10,
            },
        )
    )

    assert accepted == [35]

    assert len(pages) == 1
    assert pages[0]["word_count"] == 30
    assert pages[0]["requires_ocr"] is False

    assert len(blocks) == 1


def test_managed_result_rejects_worse_page():

    result = {
        "document": {
            "pages": [
                {"id": 34},
            ],
            "elements": [
                {
                    "id": 0,
                    "type": "text",
                    "content": "few words",
                    "bbox": [
                        {
                            "page_id": 34,
                            "coord": [],
                        }
                    ],
                }
            ],
        },
        "error_status": None,
    }

    pages, blocks, accepted = (
        build_managed_recovery_rows(
            document_id="doc_test",
            managed_result=result,
            native_word_counts={
                35: 10,
            },
        )
    )

    assert accepted == []
    assert pages == []
    assert blocks == []