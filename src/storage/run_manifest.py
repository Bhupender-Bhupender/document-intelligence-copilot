"""
Pipeline run manifest writer.

Creates and persists RunManifest records for observability and artifact
traceability. Manifests are written as JSON files to the configured
manifest directory (data/processed/manifests/ by default).

Typical usage:
    manifest = start_run("ingest")
    # ... do work ...
    manifest = complete_run(manifest, doc_ids=[...], artifacts=[...])
    save_manifest(manifest)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Literal, Optional

from src.schema.models import RunManifest
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def start_run(
    run_type: Literal["ingest", "chunk", "index", "retrieve", "answer", "full"],
) -> RunManifest:
    """
    Create a new RunManifest at the start of a pipeline run.

    Args:
        run_type: The type of pipeline operation being performed.

    Returns:
        A RunManifest with status="running" and started_at set to now.
    """
    manifest = RunManifest(run_type=run_type)
    logger.info(
        "run_manifest: run started",
        run_id=manifest.run_id,
        run_type=run_type,
    )
    return manifest


def complete_run(
    manifest: RunManifest,
    doc_ids: Optional[List[str]] = None,
    artifacts: Optional[List[str]] = None,
    errors: Optional[List[str]] = None,
) -> RunManifest:
    """
    Mark a RunManifest as completed and populate its result fields.

    Sets status to "failed" if any errors are present, "completed" otherwise.

    Args:
        manifest: The RunManifest created by start_run().
        doc_ids:  List of doc_ids processed during this run.
        artifacts: List of output file path strings written during this run.
        errors:   Non-fatal error messages encountered during this run.

    Returns:
        The updated RunManifest.
    """
    manifest.completed_at = datetime.now(timezone.utc)
    manifest.doc_ids_processed = doc_ids or []
    manifest.artifacts_written = artifacts or []
    manifest.errors = errors or []
    manifest.status = "failed" if manifest.errors else "completed"

    logger.info(
        "run_manifest: run completed",
        run_id=manifest.run_id,
        status=manifest.status,
        docs_processed=len(manifest.doc_ids_processed),
        errors=len(manifest.errors),
    )
    return manifest


def save_manifest(manifest: RunManifest) -> str:
    """
    Persist a RunManifest to the configured storage backend.

    Delegates to artifact_store.save_manifest so the active storage backend
    (local disk or Azure Blob) is selected from config.storage_backend.

    Returns a stable str location in both modes:
      - local:      absolute path string — str(path)
      - azure_blob: blob URL string

    Args:
        manifest: The RunManifest to persist.

    Returns:
        Location string (path or URL) of the persisted manifest.
    """
    from src.storage.artifact_store import save_manifest as _gateway  # deferred — breaks import cycle
    return _gateway(manifest)
