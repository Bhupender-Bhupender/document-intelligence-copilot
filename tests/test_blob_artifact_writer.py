"""
Tests for the Azure Blob Storage artifact adapter and storage routing gateway.

All Azure SDK calls are mocked via unittest.mock. No live Azure account or
installed azure-storage-blob package is required to run this test suite.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

import src.storage.artifact_store as artifact_store
from src.schema.models import RunManifest
from src.storage.blob_artifact_writer import BlobArtifactWriter, StorageError


# ─── helpers ─────────────────────────────────────────────────────────────────


class _Record(BaseModel):
    id: str
    value: int


def _writer(mock_client: MagicMock) -> BlobArtifactWriter:
    """Return a BlobArtifactWriter with an injected mock BlobServiceClient."""
    w = object.__new__(BlobArtifactWriter)
    w._client = mock_client
    w._artifacts_container = "artifacts"
    w._manifests_container = "manifests"
    return w


def _manifest() -> RunManifest:
    return RunManifest(run_type="ingest")


# ─── TestBlobArtifactWriterJsonl ──────────────────────────────────────────────


class TestBlobArtifactWriterJsonl:
    def test_upload_blob_called_once(self):
        mc = MagicMock()
        _writer(mc).write_jsonl([_Record(id="a", value=1)], "out.jsonl")
        mc.get_container_client.return_value.upload_blob.assert_called_once()

    def test_upload_uses_artifacts_container(self):
        mc = MagicMock()
        _writer(mc).write_jsonl([_Record(id="a", value=1)], "out.jsonl")
        mc.get_container_client.assert_called_once_with("artifacts")

    def test_upload_content_matches_model_dump_json(self):
        mc = MagicMock()
        records = [_Record(id="x", value=42), _Record(id="y", value=7)]
        _writer(mc).write_jsonl(records, "out.jsonl")
        data: bytes = mc.get_container_client.return_value.upload_blob.call_args.kwargs["data"]
        lines = [ln for ln in data.decode("utf-8").splitlines() if ln]
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"id": "x", "value": 42}
        assert json.loads(lines[1]) == {"id": "y", "value": 7}

    def test_returns_record_count(self):
        mc = MagicMock()
        records = [_Record(id=str(i), value=i) for i in range(5)]
        assert _writer(mc).write_jsonl(records, "out.jsonl") == 5

    def test_sdk_exception_raises_storage_error(self):
        mc = MagicMock()
        mc.get_container_client.return_value.upload_blob.side_effect = RuntimeError("network error")
        with pytest.raises(StorageError, match="Failed to upload JSONL blob"):
            _writer(mc).write_jsonl([_Record(id="a", value=1)], "out.jsonl")


# ─── TestBlobArtifactWriterManifest ──────────────────────────────────────────


class TestBlobArtifactWriterManifest:
    def test_upload_uses_manifests_container(self):
        mc = MagicMock()
        _writer(mc).save_manifest(_manifest())
        mc.get_container_client.assert_called_once_with("manifests")

    def test_blob_name_is_run_id_dot_json(self):
        mc = MagicMock()
        m = _manifest()
        _writer(mc).save_manifest(m)
        kwargs = mc.get_container_client.return_value.upload_blob.call_args.kwargs
        assert kwargs["name"] == f"{m.run_id}.json"

    def test_content_is_valid_json_with_run_id_and_type(self):
        mc = MagicMock()
        m = _manifest()
        _writer(mc).save_manifest(m)
        data: bytes = mc.get_container_client.return_value.upload_blob.call_args.kwargs["data"]
        parsed = json.loads(data.decode("utf-8"))
        assert parsed["run_id"] == m.run_id
        assert parsed["run_type"] == "ingest"

    def test_returns_blob_url_str(self):
        mc = MagicMock()
        mc.get_container_client.return_value.upload_blob.return_value.url = (
            "https://acct.blob.core.windows.net/manifests/run1.json"
        )
        result = _writer(mc).save_manifest(_manifest())
        assert result == "https://acct.blob.core.windows.net/manifests/run1.json"
        assert isinstance(result, str)

    def test_sdk_exception_raises_storage_error(self):
        mc = MagicMock()
        mc.get_container_client.return_value.upload_blob.side_effect = RuntimeError("timeout")
        with pytest.raises(StorageError, match="Failed to upload manifest blob"):
            _writer(mc).save_manifest(_manifest())


# ─── TestArtifactStore ────────────────────────────────────────────────────────


class TestArtifactStore:
    def test_write_jsonl_local_writes_file(self, tmp_path, monkeypatch):
        from src.core.config import config
        monkeypatch.setattr(config, "storage_backend", "local")
        out = tmp_path / "chunks.jsonl"
        n = artifact_store.write_jsonl([_Record(id="a", value=1)], out)
        assert n == 1
        assert out.exists()
        assert json.loads(out.read_text().strip()) == {"id": "a", "value": 1}

    def test_write_jsonl_blob_delegates_to_writer(self, monkeypatch):
        from src.core.config import config
        monkeypatch.setattr(config, "storage_backend", "azure_blob")
        mock_writer = MagicMock()
        mock_writer.write_jsonl.return_value = 3
        monkeypatch.setattr(artifact_store, "_blob_writer", lambda: mock_writer)
        records = [_Record(id=str(i), value=i) for i in range(3)]
        n = artifact_store.write_jsonl(records, Path("data/processed/chunks.jsonl"))
        mock_writer.write_jsonl.assert_called_once_with(records, "chunks.jsonl")
        assert n == 3

    def test_save_manifest_local_returns_str_path(self, tmp_path, monkeypatch):
        from src.core.config import config
        monkeypatch.setattr(config, "storage_backend", "local")
        monkeypatch.setattr(config, "manifest_dir", tmp_path)
        m = _manifest()
        loc = artifact_store.save_manifest(m)
        assert isinstance(loc, str)
        assert loc.endswith(f"{m.run_id}.json")
        assert Path(loc).exists()

    def test_save_manifest_blob_returns_url_str(self, monkeypatch):
        from src.core.config import config
        monkeypatch.setattr(config, "storage_backend", "azure_blob")
        mock_writer = MagicMock()
        mock_writer.save_manifest.return_value = (
            "https://acct.blob.core.windows.net/manifests/abc.json"
        )
        monkeypatch.setattr(artifact_store, "_blob_writer", lambda: mock_writer)
        loc = artifact_store.save_manifest(_manifest())
        assert loc == "https://acct.blob.core.windows.net/manifests/abc.json"
        assert isinstance(loc, str)

    def test_unknown_backend_raises_storage_error(self, monkeypatch):
        from src.core.config import config
        monkeypatch.setattr(config, "storage_backend", "unknown")
        with pytest.raises(StorageError, match="Unknown storage_backend"):
            artifact_store.write_jsonl([], Path("out.jsonl"))
