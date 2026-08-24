from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    StatementParameterListItem,
)


_TABLE_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*\."
    r"[A-Za-z_][A-Za-z0-9_]*\."
    r"[A-Za-z_][A-Za-z0-9_]*$"
)

_PARENT_COLUMNS = [
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

_INTEGER_COLUMNS = {
    "page_number",
    "word_count",
    "chunk_index",
}


def _state_name(response: Any) -> str:
    status = getattr(response, "status", None)
    state = getattr(status, "state", None)

    if state is None:
        return ""

    value = getattr(state, "value", state)

    return str(value).upper().split(".")[-1]


def _rows_from_response(
    response: Any,
) -> List[Dict[str, Any]]:
    result = getattr(response, "result", None)

    if result is None:
        return []

    data = getattr(result, "data_array", None) or []

    if not data:
        return []

    manifest = getattr(response, "manifest", None)
    schema = getattr(manifest, "schema", None)
    columns = getattr(schema, "columns", None) or []

    names = [
        getattr(column, "name", None)
        for column in columns
    ]

    if not names or any(name is None for name in names):
        raise RuntimeError(
            "Statement response does not contain "
            "a usable result schema."
        )

    rows: List[Dict[str, Any]] = []

    for values in data:
        if len(values) != len(names):
            raise RuntimeError(
                "Statement result width does not "
                "match its schema."
            )

        row = dict(zip(names, values))

        for column in _INTEGER_COLUMNS:
            value = row.get(column)

            if value not in (None, ""):
                row[column] = int(value)

        rows.append(row)

    return rows


def load_parent_rows_via_statement_api(
    parent_ids: List[str],
    *,
    parent_table_name: str,
    warehouse_id: str,
    workspace_client: Optional[Any] = None,
    timeout_seconds: float = 180.0,
) -> List[Dict[str, Any]]:
    if not parent_ids:
        return []

    if not warehouse_id or not warehouse_id.strip():
        raise RuntimeError(
            "Databricks SQL warehouse ID is not configured."
        )

    if not _TABLE_NAME_PATTERN.fullmatch(
        parent_table_name
    ):
        raise RuntimeError(
            "Parent table must be a valid "
            "three-part Unity Catalog name."
        )

    unique_ids = list(dict.fromkeys(parent_ids))

    placeholders = [
        f":parent_{index}"
        for index in range(len(unique_ids))
    ]

    parameters = [
        StatementParameterListItem(
            name=f"parent_{index}",
            value=parent_id,
            type="STRING",
        )
        for index, parent_id in enumerate(unique_ids)
    ]

    columns_sql = ", ".join(_PARENT_COLUMNS)
    placeholders_sql = ", ".join(placeholders)

    statement = (
        f"SELECT {columns_sql} "
        f"FROM {parent_table_name} "
        f"WHERE chunk_id IN ({placeholders_sql})"
    )

    client = workspace_client or WorkspaceClient()

    response = (
        client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            parameters=parameters,
            row_limit=len(unique_ids),
            wait_timeout="50s",
        )
    )

    deadline = time.monotonic() + timeout_seconds
    state = _state_name(response)

    while state in {"PENDING", "RUNNING"}:
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Timed out waiting for Databricks "
                "parent lookup."
            )

        statement_id = getattr(
            response,
            "statement_id",
            None,
        )

        if not statement_id:
            raise RuntimeError(
                "Databricks statement did not return "
                "a statement ID."
            )

        time.sleep(2.0)

        response = (
            client.statement_execution.get_statement(
                statement_id
            )
        )

        state = _state_name(response)

    if state != "SUCCEEDED":
        raise RuntimeError(
            "Databricks parent lookup statement failed."
        )

    return _rows_from_response(response)
