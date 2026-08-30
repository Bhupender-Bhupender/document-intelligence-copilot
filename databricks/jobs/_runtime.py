"""Shared runtime helpers for Phase 14 Lakeflow Job tasks."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TableContract:
    """Unity Catalog objects used by the Document Intelligence pipeline."""

    catalog: str

    landing_root: str

    bronze_manifest: str
    bronze_runs: str

    silver_documents: str
    silver_pages: str
    silver_blocks: str

    quality_results: str
    quality_runs: str

    gold_parent_chunks: str
    gold_child_chunks: str
    gold_chunking_manifest: str


def bootstrap_repo() -> Path:
    """Ensure the bundle repository root is importable."""

    root = Path(__file__).resolve().parents[2]

    root_text = str(root)

    if root_text not in sys.path:
        sys.path.insert(0, root_text)

    return root


def get_spark():
    """Return the active serverless Spark session."""

    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def parse_catalog_args(
    description: str,
) -> argparse.Namespace:
    """Parse the common catalog argument used by pipeline tasks."""

    parser = argparse.ArgumentParser(
        description=description,
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog catalog for the pipeline.",
    )

    return parser.parse_args()


def build_table_contract(
    catalog: str,
) -> TableContract:
    """Build all pipeline object names from one catalog."""

    normalized = str(catalog).strip()

    if not normalized:
        raise ValueError(
            "Catalog must not be blank."
        )

    return TableContract(
        catalog=normalized,
        landing_root=(
            f"/Volumes/{normalized}/"
            "bronze/document_landing"
        ),
        bronze_manifest=(
            f"{normalized}.bronze."
            "document_manifest"
        ),
        bronze_runs=(
            f"{normalized}.bronze."
            "ingestion_runs"
        ),
        silver_documents=(
            f"{normalized}.silver."
            "documents"
        ),
        silver_pages=(
            f"{normalized}.silver."
            "pages"
        ),
        silver_blocks=(
            f"{normalized}.silver."
            "blocks"
        ),
        quality_results=(
            f"{normalized}.monitoring."
            "silver_quality_results"
        ),
        quality_runs=(
            f"{normalized}.monitoring."
            "silver_quality_runs"
        ),
        gold_parent_chunks=(
            f"{normalized}.gold."
            "parent_chunks"
        ),
        gold_child_chunks=(
            f"{normalized}.gold."
            "child_chunks"
        ),
        gold_chunking_manifest=(
            f"{normalized}.gold."
            "chunking_manifest"
        ),
    )
