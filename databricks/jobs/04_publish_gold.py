"""Lakeflow task: publish hierarchical Gold chunks."""

from _runtime import (
    bootstrap_repo,
    build_table_contract,
    get_spark,
    parse_catalog_args,
)

bootstrap_repo()

from databricks.src.gold_chunking import (
    run_gold_chunking,
)


def main() -> None:
    args = parse_catalog_args(
        "Publish hierarchical Gold chunks."
    )

    tables = build_table_contract(
        args.catalog
    )

    spark = get_spark()

    run_gold_chunking(
        spark,
        bronze_manifest_table=tables.bronze_manifest,
        documents_table=tables.silver_documents,
        pages_table=tables.silver_pages,
        blocks_table=tables.silver_blocks,
        parent_table=tables.gold_parent_chunks,
        child_table=tables.gold_child_chunks,
        chunking_manifest_table=(
            tables.gold_chunking_manifest
        ),
    )

    print(
        "PHASE14_TASK_PUBLISH_GOLD_PASS: True"
    )


if __name__ == "__main__":
    main()
