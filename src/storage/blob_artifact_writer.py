"""
Azure Blob Storage artifact adapter.

Writes the same artifact payloads as the local disk writers but to Azure
Blob Storage containers. Receives project-native Pydantic models; emits
no Azure types above this boundary.

Design notes:
    - BlobArtifactWriter accepts a BlobServiceClient via constructor injection
      so tests can pass a mock client directly without patching import paths.
    - Azure SDK objects (BlobServiceClient, BlobClient) are internal to this
      module. Nothing above the storage boundary receives Azure types.
    - StorageError wraps all Azure SDK failures at this boundary.
    - azure-storage-blob and azure-identity are imported inside __init__ so
      the module stays importable when those packages are absent (they are
      only required when storage_backend='azure_blob').
"""
from __future__ import annotations

from typing import Any, List

from pydantic import BaseModel

from src.schema.models import RunManifest
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class StorageError(Exception):
    """Raised when a storage backend operation fails."""


class BlobArtifactWriter:
    """
    Azure Blob Storage adapter for artifact and manifest persistence.

    Receives project-native Pydantic models; emits no Azure types above
    this boundary. All Azure SDK interactions are contained inside this class.

    Args:
        account_url:          Azure Storage account URL
                              (https://<account>.blob.core.windows.net).
        artifacts_container:  Blob container name for JSONL artifacts.
        manifests_container:  Blob container name for run manifests.
        credential:           Azure credential object. Defaults to
                              DefaultAzureCredential (Managed Identity
                              compatible). Pass a mock in tests.
    """

    def __init__(
        self,
        account_url: str,
        artifacts_container: str,
        manifests_container: str,
        credential: Any = None,
    ) -> None:
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise StorageError(
                "azure-storage-blob and azure-identity are required for blob mode. "
                "Install with: pip install azure-storage-blob azure-identity"
            ) from exc

        _credential = credential if credential is not None else DefaultAzureCredential()
        self._client = BlobServiceClient(account_url=account_url, credential=_credential)
        self._artifacts_container = artifacts_container
        self._manifests_container = manifests_container

    def write_jsonl(self, records: List[BaseModel], blob_name: str) -> int:
        """
        Upload a list of Pydantic models as JSONL to the artifacts container.

        Content is identical to what the local write_jsonl produces.

        Args:
            records:   Pydantic model instances to serialise.
            blob_name: Target blob name (e.g. "chunks.jsonl").

        Returns:
            Number of records uploaded.

        Raises:
            StorageError: If the Azure SDK upload fails.
        """
        lines = [r.model_dump_json() for r in records]
        content = ("\n".join(lines) + "\n") if lines else ""

        try:
            container_client = self._client.get_container_client(self._artifacts_container)
            container_client.upload_blob(
                name=blob_name,
                data=content.encode("utf-8"),
                overwrite=True,
            )
        except Exception as exc:
            raise StorageError(
                f"Failed to upload JSONL blob '{blob_name}': {exc}"
            ) from exc

        logger.info(
            "blob_artifact_writer: uploaded JSONL",
            blob_name=blob_name,
            container=self._artifacts_container,
            record_count=len(records),
        )
        return len(records)

    def save_manifest(self, manifest: RunManifest) -> str:
        """
        Upload a RunManifest as JSON to the manifests container.

        Content is identical to what the local save_manifest produces.

        Args:
            manifest: The RunManifest to persist.

        Returns:
            Blob URL string for the uploaded manifest.

        Raises:
            StorageError: If the Azure SDK upload fails.
        """
        blob_name = f"{manifest.run_id}.json"
        content = manifest.model_dump_json(indent=2)

        try:
            container_client = self._client.get_container_client(self._manifests_container)
            blob_client = container_client.upload_blob(
                name=blob_name,
                data=content.encode("utf-8"),
                overwrite=True,
            )
            blob_url: str = blob_client.url
        except Exception as exc:
            raise StorageError(
                f"Failed to upload manifest blob '{blob_name}': {exc}"
            ) from exc

        logger.info(
            "blob_artifact_writer: uploaded manifest",
            blob_name=blob_name,
            container=self._manifests_container,
            run_id=manifest.run_id,
        )
        return blob_url
