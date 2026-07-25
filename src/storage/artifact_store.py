"""
Storage routing gateway.

All artifact writes pass through this module. The active backend is
selected by config.storage_backend:

    "local"      — writes to local disk (identical behaviour to before).
    "azure_blob" — writes to Azure Blob Storage via BlobArtifactWriter.

This is the single switch point for the storage layer. No other module
in the codebase contains backend-selection logic.

artifact_writer.write_jsonl and run_manifest.save_manifest delegate to
this module so higher-level callers remain unchanged.
"""
from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import BaseModel

from src.core.config import config
from src.schema.models import RunManifest
from src.storage.blob_artifact_writer import BlobArtifactWriter, StorageError
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def _blob_writer() -> BlobArtifactWriter:
    """Construct a BlobArtifactWriter from current config values."""
    return BlobArtifactWriter(
        account_url=config.azure_storage_account_url,
        artifacts_container=config.azure_storage_container_artifacts,
        manifests_container=config.azure_storage_container_manifests,
    )


def write_jsonl(records: List[BaseModel], output_path: Path) -> int:
    """
    Write Pydantic model instances to a JSONL artifact.

    Routes to the backend selected by config.storage_backend.
    Signature mirrors artifact_writer.write_jsonl so existing callers
    delegate here without changing their call sites.

    Args:
        records:     Pydantic model instances to serialise.
        output_path: Destination path. Used as-is in local mode; only the
                     basename is used as the blob name in azure_blob mode.

    Returns:
        Number of records written.

    Raises:
        StorageError: On backend failure or unknown storage_backend value.
    """
    if config.storage_backend == "local":
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            for record in records:
                f.write(record.model_dump_json() + "\n")
        logger.info(
            "artifact_store: wrote JSONL (local)",
            path=str(output_path),
            record_count=len(records),
        )
        return len(records)

    if config.storage_backend == "azure_blob":
        return _blob_writer().write_jsonl(records, output_path.name)

    raise StorageError(f"Unknown storage_backend: {config.storage_backend!r}")


def save_manifest(manifest: RunManifest) -> str:
    """
    Persist a RunManifest to the configured storage backend.

    Returns a stable str location in both modes:
      - local:      absolute path string — str(path)
      - azure_blob: blob URL string

    Args:
        manifest: The RunManifest to persist.

    Returns:
        Location string (path or URL) of the persisted manifest.

    Raises:
        StorageError: On backend failure or unknown storage_backend value.
    """
    if config.storage_backend == "local":
        manifest_dir = config.manifest_dir
        manifest_dir.mkdir(parents=True, exist_ok=True)
        output_path = manifest_dir / f"{manifest.run_id}.json"
        with output_path.open("w", encoding="utf-8") as f:
            f.write(manifest.model_dump_json(indent=2))
        logger.info(
            "artifact_store: manifest saved (local)",
            run_id=manifest.run_id,
            path=str(output_path),
        )
        return str(output_path)

    if config.storage_backend == "azure_blob":
        return _blob_writer().save_manifest(manifest)

    raise StorageError(f"Unknown storage_backend: {config.storage_backend!r}")
