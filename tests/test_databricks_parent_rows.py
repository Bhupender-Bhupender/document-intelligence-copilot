from types import SimpleNamespace

import pytest

from src.retrieval.databricks_parent_rows import (
    load_parent_rows_via_statement_api,
)


COLUMNS = [
    "chunk_id",
    "document_id",
    "page_id",
    "page_number",
    "file_name",
    "file_type",
    "section_title",
    "text",
    "word_count",
    "chunk_index",
    "chunk_level",
    "parent_chunk_id",
]


def _response(
    state="SUCCEEDED",
    data=None,
    statement_id="statement-test",
):
    return SimpleNamespace(
        statement_id=statement_id,
        status=SimpleNamespace(state=state),
        manifest=SimpleNamespace(
            schema=SimpleNamespace(
                columns=[
                    SimpleNamespace(name=name)
                    for name in COLUMNS
                ]
            )
        ),
        result=SimpleNamespace(
            data_array=data or [],
        ),
    )


def _parent_row():
    return [
        "parent-1",
        "doc-1",
        "page-1",
        "3",
        "example.pdf",
        "pdf",
        "Section",
        "Parent context.",
        "2",
        "0",
        "parent",
        None,
    ]


class FakeStatementExecution:
    def __init__(self, responses):
        self.responses = list(responses)
        self.execute_kwargs = None

    def execute_statement(self, **kwargs):
        self.execute_kwargs = kwargs
        return self.responses.pop(0)

    def get_statement(self, statement_id):
        return self.responses.pop(0)


class FakeWorkspaceClient:
    def __init__(self, responses):
        self.statement_execution = FakeStatementExecution(
            responses
        )


def test_empty_parent_ids_returns_empty():
    assert (
        load_parent_rows_via_statement_api(
            [],
            parent_table_name="cat.sch.parents",
            warehouse_id="warehouse",
        )
        == []
    )


def test_remote_parent_lookup_returns_rows():
    client = FakeWorkspaceClient(
        [
            _response(
                data=[_parent_row()],
            )
        ]
    )

    rows = load_parent_rows_via_statement_api(
        ["parent-1"],
        parent_table_name="cat.sch.parents",
        warehouse_id="warehouse",
        workspace_client=client,
    )

    assert len(rows) == 1
    assert rows[0]["chunk_id"] == "parent-1"
    assert rows[0]["page_number"] == 3
    assert rows[0]["word_count"] == 2
    assert rows[0]["chunk_index"] == 0


def test_parent_ids_are_parameterized():
    client = FakeWorkspaceClient(
        [
            _response(
                data=[_parent_row()],
            )
        ]
    )

    load_parent_rows_via_statement_api(
        ["parent-1"],
        parent_table_name="cat.sch.parents",
        warehouse_id="warehouse",
        workspace_client=client,
    )

    kwargs = (
        client.statement_execution.execute_kwargs
    )

    assert "parent-1" not in kwargs["statement"]
    assert ":parent_0" in kwargs["statement"]

    assert (
        kwargs["parameters"][0].value
        == "parent-1"
    )


def test_pending_statement_is_polled(monkeypatch):
    monkeypatch.setattr(
        "src.retrieval.databricks_parent_rows.time.sleep",
        lambda _: None,
    )

    client = FakeWorkspaceClient(
        [
            _response(
                state="PENDING",
            ),
            _response(
                state="SUCCEEDED",
                data=[_parent_row()],
            ),
        ]
    )

    rows = load_parent_rows_via_statement_api(
        ["parent-1"],
        parent_table_name="cat.sch.parents",
        warehouse_id="warehouse",
        workspace_client=client,
        timeout_seconds=5,
    )

    assert len(rows) == 1


def test_missing_warehouse_fails_closed():
    with pytest.raises(
        RuntimeError,
        match="warehouse ID",
    ):
        load_parent_rows_via_statement_api(
            ["parent-1"],
            parent_table_name="cat.sch.parents",
            warehouse_id="",
        )


def test_invalid_table_name_is_rejected():
    with pytest.raises(
        RuntimeError,
        match="three-part",
    ):
        load_parent_rows_via_statement_api(
            ["parent-1"],
            parent_table_name="invalid table",
            warehouse_id="warehouse",
        )
