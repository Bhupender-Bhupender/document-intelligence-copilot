"""Lakeflow task: incremental Bronze ingestion."""

from _runtime import (
    bootstrap_repo,
    build_table_contract,
    get_spark,
    parse_catalog_args,
)

bootstrap_repo()

from databricks.src.bronze_ingestion import (
    run_incremental_ingestion,
)


def main() -> None:
    args = parse_catalog_args(
        "Incremental Bronze ingestion."
    )

    tables = build_table_contract(
        args.catalog
    )

    spark = get_spark()

    run_incremental_ingestion(
        spark,
        landing_root=tables.landing_root,
        manifest_table=tables.bronze_manifest,
        runs_table=tables.bronze_runs,
    )

    print(
        "PHASE14_TASK_INGEST_PASS: True"
    )


if __name__ == "__main__":
    main()
