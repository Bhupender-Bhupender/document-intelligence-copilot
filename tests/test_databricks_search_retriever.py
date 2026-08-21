import pytest
from unittest.mock import MagicMock
from src.retrieval.databricks_search_retriever import (
    DatabricksSearchRetrievalError,
    DatabricksSearchRetriever,
)


class FakeIndex:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    def similarity_search(self, **kwargs):
        self.calls.append(kwargs)

        if self.error is not None:
            raise self.error

        return self.response


def _response():
    columns = [
        "chunk_id",
        "document_id",
        "page_id",
        "page_number",
        "parent_chunk_id",
        "file_type",
        "file_name",
        "word_count",
        "chunk_level",
        "chunking_version",
        "text",
        "score",
    ]

    return {
        "manifest": {
            "columns": [{"name": name} for name in columns]
        },
        "result": {
            "row_count": 2,
            "data_array": [
                [
                    "child-1",
                    "doc-1",
                    "page-1",
                    3.0,
                    "parent-1",
                    "pdf",
                    "example.pdf",
                    120.0,
                    "child",
                    "hierarchical_v1",
                    "first chunk text",
                    0.82,
                ],
                [
                    "child-2",
                    "doc-2",
                    "page-2",
                    7.0,
                    "parent-2",
                    "docx",
                    "example.docx",
                    95.0,
                    "child",
                    "hierarchical_v1",
                    "second chunk text",
                    0.61,
                ],
            ],
        },
    }


def test_retrieve_uses_hybrid_search_and_maps_results():
    fake_index = FakeIndex(response=_response())

    retriever = DatabricksSearchRetriever(
        index_name="catalog.schema.child_chunks_index",
        index=fake_index,
    )

    results = retriever.retrieve(
        "compliance responsibilities",
        top_k=2,
    )

    assert len(results) == 2

    first = results[0]

    assert first.chunk_id == "child-1"
    assert first.doc_id == "doc-1"
    assert first.page_id == "page-1"
    assert first.page_number == 3
    assert first.word_count == 120
    assert first.parent_chunk_id == "parent-1"
    assert first.retrieval_method == "hybrid"

    assert first.vector_score is None
    assert first.bm25_score is None
    assert first.fusion_score == pytest.approx(0.82)

    assert fake_index.calls == [
        {
            "query_text": "compliance responsibilities",
            "columns": DatabricksSearchRetriever.RESULT_COLUMNS,
            "num_results": 2,
            "query_type": "hybrid",
        }
    ]


def test_retrieve_rejects_blank_query():
    retriever = DatabricksSearchRetriever(
        index_name="catalog.schema.index",
        index=FakeIndex(response=_response()),
    )

    with pytest.raises(ValueError, match="query must not be blank"):
        retriever.retrieve("   ")


def test_parse_response_rejects_missing_required_column():
    response = _response()

    response["manifest"]["columns"] = [
        column
        for column in response["manifest"]["columns"]
        if column["name"] != "parent_chunk_id"
    ]

    response["result"]["data_array"] = [
        row[:4] + row[5:]
        for row in response["result"]["data_array"]
    ]

    with pytest.raises(
        DatabricksSearchRetrievalError,
        match="missing required columns",
    ):
        DatabricksSearchRetriever._parse_response(response)


def test_non_integral_page_number_is_rejected():
    response = _response()
    response["result"]["data_array"][0][3] = 3.5

    with pytest.raises(
        DatabricksSearchRetrievalError,
        match="page_number must contain an integral value",
    ):
        DatabricksSearchRetriever._parse_response(response)


def test_sdk_failure_is_wrapped_without_query_text():
    private_query = "private enterprise query"

    fake_index = FakeIndex(
        error=RuntimeError("upstream failure")
    )

    retriever = DatabricksSearchRetriever(
        index_name="catalog.schema.index",
        index=fake_index,
    )

    with pytest.raises(DatabricksSearchRetrievalError) as exc_info:
        retriever.retrieve(private_query)

    assert private_query not in str(exc_info.value)


    


def test_retrieval_gateway_routes_databricks(monkeypatch):
        from src.retrieval import retrieval_gateway

        expected = [MagicMock()]

        monkeypatch.setattr(
            retrieval_gateway.config,
            "search_backend",
            "databricks",
        )

        monkeypatch.setattr(
            retrieval_gateway,
            "_retrieve_databricks",
            lambda query, top_k: expected,
        )

        result = retrieval_gateway.route_retrieve(
            "test query",
            top_k=5,
        )

        assert result is expected


def test_databricks_parent_lookup_does_not_fall_back_local(
        monkeypatch,
    ):
        from src.retrieval import retrieval_gateway

        monkeypatch.setattr(
            retrieval_gateway.config,
            "search_backend",
            "databricks",
        )

        with pytest.raises(
            NotImplementedError,
            match="Databricks parent lookup",
        ):
            retrieval_gateway.route_lookup_parents(
                [MagicMock()]
            )


def test_databricks_index_gateway_does_not_build_local_index(
        monkeypatch,
    ):
        from src.indexing import index_gateway

        monkeypatch.setattr(
            index_gateway.config,
            "search_backend",
            "databricks",
        )

        local_called = False

        def fake_local(*args, **kwargs):
            nonlocal local_called
            local_called = True
            return MagicMock()

        monkeypatch.setattr(
            index_gateway,
            "_run_local_indexing",
            fake_local,
        )

        with pytest.raises(
            RuntimeError,
            match="Delta Sync",
        ):
            index_gateway.route_index(
                parent_chunks=[],
                child_chunks=[],
                index_dir=None,
                embed_model=None,
            )

        assert local_called is False