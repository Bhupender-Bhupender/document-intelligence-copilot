"""Lakeflow task: execute and enforce the canonical Silver quality gate."""

from _runtime import (
    bootstrap_repo,
    build_table_contract,
    get_spark,
    parse_catalog_args,
)

bootstrap_repo()

from databricks.src.silver_quality import (
    enforce_quality_gate,
    run_silver_quality_checks,
)


def main() -> None:
    args = parse_catalog_args(
        "Execute the canonical Silver quality gate."
    )

    tables = build_table_contract(
        args.catalog
    )

    spark = get_spark()

    result = run_silver_quality_checks(
        spark,
        manifest_table=tables.bronze_manifest,
        documents_table=tables.silver_documents,
        pages_table=tables.silver_pages,
        blocks_table=tables.silver_blocks,
        results_table=tables.quality_results,
        runs_table=tables.quality_runs,
    )

    enforce_quality_gate(
        result
    )

    print(
        "PHASE14_TASK_QUALITY_GATE_PASS: True"
    )


if __name__ == "__main__":
    main()
