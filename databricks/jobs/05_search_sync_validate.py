"""Lakeflow task: synchronize and validate the existing AI Search index."""

from __future__ import annotations

import argparse
import time

from _runtime import bootstrap_repo, get_spark

bootstrap_repo()

from databricks.sdk import WorkspaceClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--index-name",
        required=True,
    )

    parser.add_argument(
        "--source-table",
        required=True,
    )

    parser.add_argument(
        "--parent-table",
        required=True,
    )

    parser.add_argument(
        "--chunking-manifest-table",
        required=True,
    )

    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=900,
    )

    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=10,
    )

    return parser.parse_args()


def _status_value(
    obj,
    name: str,
    default=None,
):
    if obj is None:
        return default

    return getattr(
        obj,
        name,
        default,
    )


def wait_for_index(
    client: WorkspaceClient,
    index_name: str,
    expected_rows: int,
    timeout_seconds: int,
    poll_seconds: int,
) -> int:
    deadline = (
        time.monotonic()
        + timeout_seconds
    )

    last_count = None

    while time.monotonic() < deadline:
        index = (
            client
            .vector_search_indexes
            .get_index(
                index_name=index_name
            )
        )

        status = getattr(
            index,
            "status",
            None,
        )

        ready = bool(
            _status_value(
                status,
                "ready",
                False,
            )
        )

        indexed_rows = _status_value(
            status,
            "indexed_row_count",
            None,
        )

        if indexed_rows is not None:
            indexed_rows = int(
                indexed_rows
            )

        last_count = indexed_rows

        if (
            ready
            and indexed_rows
            == expected_rows
        ):
            return indexed_rows

        time.sleep(
            poll_seconds
        )

    raise TimeoutError(
        "AI Search index did not reach "
        "the expected ready state within "
        f"{timeout_seconds} seconds. "
        f"Expected rows={expected_rows}, "
        f"last indexed rows={last_count}."
    )



def validate_gold_contract(
    spark,
    *,
    child_table: str,
    parent_table: str,
    chunking_manifest_table: str,
) -> dict[str, int]:
    """Validate structural Gold invariants without exposing content."""

    child_df = spark.table(
        child_table
    )

    parent_df = spark.table(
        parent_table
    )

    manifest_df = spark.table(
        chunking_manifest_table
    )

    child_count = int(
        child_df.count()
    )

    parent_count = int(
        parent_df.count()
    )

    if child_count <= 0:
        raise RuntimeError(
            "Gold child chunk table is empty."
        )

    if parent_count <= 0:
        raise RuntimeError(
            "Gold parent chunk table is empty."
        )

    child_distinct = int(
        child_df
        .select("chunk_id")
        .distinct()
        .count()
    )

    parent_distinct = int(
        parent_df
        .select("chunk_id")
        .distinct()
        .count()
    )

    duplicate_child_count = (
        child_count
        - child_distinct
    )

    duplicate_parent_count = (
        parent_count
        - parent_distinct
    )

    orphan_child_count = int(
        child_df
        .select("parent_chunk_id")
        .where(
            "parent_chunk_id IS NOT NULL"
        )
        .join(
            parent_df.selectExpr(
                "chunk_id AS parent_chunk_id"
            ),
            on="parent_chunk_id",
            how="left_anti",
        )
        .count()
    )

    current_manifest = (
        manifest_df
        .where("is_current = true")
    )

    current_manifest_count = int(
        current_manifest.count()
    )

    current_manifest_distinct = int(
        current_manifest
        .select("document_id")
        .distinct()
        .count()
    )

    duplicate_current_manifest_count = (
        current_manifest_count
        - current_manifest_distinct
    )

    checks = {
        "child_count":
            child_count,

        "parent_count":
            parent_count,

        "duplicate_child_count":
            duplicate_child_count,

        "duplicate_parent_count":
            duplicate_parent_count,

        "orphan_child_count":
            orphan_child_count,

        "duplicate_current_manifest_count":
            duplicate_current_manifest_count,
    }

    violations = {
        name: value
        for name, value
        in checks.items()
        if (
            name.startswith(
                "duplicate_"
            )
            or name
            == "orphan_child_count"
        )
        and value != 0
    }

    if violations:
        raise RuntimeError(
            "Gold structural validation failed: "
            + ", ".join(
                f"{name}={value}"
                for name, value
                in violations.items()
            )
        )

    return checks

def main() -> None:
    args = parse_args()

    spark = get_spark()

    gold = validate_gold_contract(
        spark,
        child_table=args.source_table,
        parent_table=args.parent_table,
        chunking_manifest_table=(
            args.chunking_manifest_table
        ),
    )

    source_count = gold[
        "child_count"
    ]

    print(
        "PHASE14_GOLD_PARENT_COUNT:",
        gold["parent_count"],
    )

    print(
        "PHASE14_GOLD_CHILD_COUNT:",
        gold["child_count"],
    )

    print(
        "PHASE14_GOLD_DUPLICATE_PARENT_COUNT:",
        gold["duplicate_parent_count"],
    )

    print(
        "PHASE14_GOLD_DUPLICATE_CHILD_COUNT:",
        gold["duplicate_child_count"],
    )

    print(
        "PHASE14_GOLD_ORPHAN_CHILD_COUNT:",
        gold["orphan_child_count"],
    )

    print(
        "PHASE14_GOLD_DUPLICATE_CURRENT_MANIFEST_COUNT:",
        gold[
            "duplicate_current_manifest_count"
        ],
    )

    print(
        "PHASE14_GOLD_CONTRACT_PASS: True"
    )

    client = WorkspaceClient()

    index_before = (
        client
        .vector_search_indexes
        .get_index(
            index_name=args.index_name
        )
    )

    if (
        str(
            getattr(
                index_before,
                "primary_key",
                "",
            )
        )
        != "chunk_id"
    ):
        raise RuntimeError(
            "AI Search primary-key "
            "contract is invalid."
        )

    delta_spec = getattr(
        index_before,
        "delta_sync_index_spec",
        None,
    )

    if delta_spec is None:
        raise RuntimeError(
            "Expected a Delta Sync index."
        )

    source_table = str(
        getattr(
            delta_spec,
            "source_table",
            "",
        )
    )

    if (
        source_table
        != args.source_table
    ):
        raise RuntimeError(
            "AI Search source-table "
            "contract is invalid."
        )

    client.vector_search_indexes.sync_index(
        index_name=args.index_name
    )

    indexed_count = wait_for_index(
        client=client,
        index_name=args.index_name,
        expected_rows=source_count,
        timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds,
    )

    print(
        "PHASE14_SEARCH_SOURCE_COUNT:",
        source_count,
    )

    print(
        "PHASE14_SEARCH_INDEXED_COUNT:",
        indexed_count,
    )

    print(
        "PHASE14_SEARCH_COUNTS_MATCH:",
        source_count == indexed_count,
    )

    print(
        "PHASE14_TASK_SEARCH_SYNC_PASS: True"
    )


if __name__ == "__main__":
    main()
