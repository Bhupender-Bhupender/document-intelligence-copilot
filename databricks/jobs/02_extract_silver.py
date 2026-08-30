"""Lakeflow task: route, extract and recover Silver documents."""

import os

# AppConfig is instantiated during downstream imports,
# so establish the execution environment first.
os.environ["RUNTIME_MODE"] = "databricks"

from _runtime import (
    bootstrap_repo,
    build_table_contract,
    get_spark,
    parse_catalog_args,
)

bootstrap_repo()

from databricks.src.document_processing import (
    run_document_extraction,
)
from databricks.src.document_routing import (
    run_document_routing,
)
from databricks.src.managed_ocr_recovery import (
    run_managed_ocr_recovery,
)


def main() -> None:
    args = parse_catalog_args(
        "Route and extract canonical Silver documents."
    )

    tables = build_table_contract(
        args.catalog
    )

    spark = get_spark()

    run_document_routing(
        spark,
        manifest_table=tables.bronze_manifest,
        silver_documents_table=tables.silver_documents,
    )

    print(
        "PHASE14_ROUTING_PASS: True"
    )

    run_document_extraction(
        spark,
        manifest_table=tables.bronze_manifest,
        documents_table=tables.silver_documents,
        pages_table=tables.silver_pages,
        blocks_table=tables.silver_blocks,
    )

    print(
        "PHASE14_EXTRACTION_PASS: True"
    )

    run_managed_ocr_recovery(
        spark,
        manifest_table=tables.bronze_manifest,
        documents_table=tables.silver_documents,
        pages_table=tables.silver_pages,
        blocks_table=tables.silver_blocks,
    )

    print(
        "PHASE14_OCR_RECOVERY_PASS: True"
    )

    print(
        "PHASE14_TASK_EXTRACT_SILVER_PASS: True"
    )


if __name__ == "__main__":
    main()
