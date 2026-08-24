from databricks.src.managed_ocr_recovery import (
    build_managed_recovery_rows,
)


def _element(
    element_id: int,
    element_type: str,
    content: str,
) -> dict:
    return {
        "id": element_id,
        "type": element_type,
        "content": content,
        "description": None,
        "confidence": 0.99,
        "bbox": [
            {
                "page_id": 0,
            }
        ],
    }


def _result(
    elements: list[dict],
) -> dict:
    return {
        "document": {
            "pages": [
                {
                    "id": 0,
                    "image_uri": "",
                }
            ],
            "elements": elements,
        }
    }


def test_normal_managed_page_above_threshold_is_good():
    content = " ".join(
        f"word{i}"
        for i in range(20)
    )

    pages, _, accepted = (
        build_managed_recovery_rows(
            document_id="doc_test",
            managed_result=_result([
                _element(
                    1,
                    "text",
                    content,
                )
            ]),
            native_word_counts={
                1: 5,
            },
        )
    )

    assert accepted == [1]
    assert len(pages) == 1
    assert pages[0]["word_count"] == 20
    assert (
        pages[0]["extraction_quality"]
        == "good"
    )
    assert (
        pages[0]["requires_ocr"]
        is False
    )


def test_structured_sparse_managed_page_is_good():
    result = _result([
        _element(
            1,
            "title",
            "Quarterly overview",
        ),
        _element(
            2,
            "text",
            "Revenue increased during period",
        ),
        _element(
            3,
            "text",
            "Costs remained stable",
        ),
    ])

    pages, blocks, accepted = (
        build_managed_recovery_rows(
            document_id="doc_test",
            managed_result=result,
            native_word_counts={
                1: 6,
            },
        )
    )

    assert accepted == [1]
    assert len(pages) == 1
    assert len(blocks) == 3
    assert pages[0]["word_count"] == 9
    assert (
        pages[0]["extraction_quality"]
        == "good"
    )
    assert (
        pages[0]["requires_ocr"]
        is False
    )


def test_too_small_sparse_page_stays_weak():
    result = _result([
        _element(
            1,
            "title",
            "Short page",
        ),
        _element(
            2,
            "text",
            "tiny text",
        ),
    ])

    pages, _, accepted = (
        build_managed_recovery_rows(
            document_id="doc_test",
            managed_result=result,
            native_word_counts={
                1: 1,
            },
        )
    )

    assert accepted == [1]
    assert len(pages) == 1
    assert pages[0]["word_count"] == 4
    assert (
        pages[0]["extraction_quality"]
        == "weak"
    )
    assert (
        pages[0]["requires_ocr"]
        is True
    )


def test_equal_unstructured_result_is_not_accepted():
    content = "one two three four five six seven eight nine"

    pages, blocks, accepted = (
        build_managed_recovery_rows(
            document_id="doc_test",
            managed_result=_result([
                _element(
                    1,
                    "text",
                    content,
                )
            ]),
            native_word_counts={
                1: 9,
            },
        )
    )

    assert pages == []
    assert blocks == []
    assert accepted == []


def test_equal_structured_sparse_result_can_be_reclassified():
    result = _result([
        _element(
            1,
            "title",
            "Quarterly overview",
        ),
        _element(
            2,
            "text",
            "Revenue increased during period",
        ),
        _element(
            3,
            "text",
            "Costs remained stable",
        ),
    ])

    pages, _, accepted = (
        build_managed_recovery_rows(
            document_id="doc_test",
            managed_result=result,
            native_word_counts={
                1: 9,
            },
        )
    )

    assert accepted == [1]
    assert len(pages) == 1
    assert pages[0]["word_count"] == 9
    assert (
        pages[0]["extraction_quality"]
        == "good"
    )
    assert (
        pages[0]["requires_ocr"]
        is False
    )
