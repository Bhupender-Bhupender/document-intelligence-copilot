"""
Artifact writer.

Serialises lists of Pydantic model instances to JSONL files.
Used by all pipeline stages to persist intermediate and final artifacts.

Design notes:
    - write_jsonl() uses model.model_dump_json() for correct type serialisation
      (datetime, Path, Enum values).
    - read_jsonl_raw() returns plain dicts — callers are responsible for
      deserialising into the appropriate model type.
    - Both functions are intentionally simple; no compression or batching yet.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

from pydantic import BaseModel

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def write_jsonl(records: List[BaseModel], output_path: Path) -> int:
    """
    Serialise a list of Pydantic models to a JSONL file.

    Creates parent directories if they do not exist. Overwrites any
    existing file at output_path.

    Delegates to artifact_store.write_jsonl so the active storage backend
    (local disk or Azure Blob) is selected from config.storage_backend.

    Args:
        records: List of Pydantic model instances (any model type).
        output_path: Destination file path. Should end in .jsonl.

    Returns:
        Number of records written.
    """
    from src.storage.artifact_store import write_jsonl as _gateway  # deferred — breaks import cycle
    return _gateway(records, output_path)


def read_jsonl_raw(input_path: Path) -> List[dict]:
    """
    Read a JSONL file and return a list of raw dicts.

    Skips blank lines. Does not validate against any schema — callers
    are responsible for constructing typed models from the returned dicts.

    Args:
        input_path: Path to the .jsonl file.

    Returns:
        List of parsed JSON objects as plain dicts.

    Raises:
        FileNotFoundError: If input_path does not exist.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Artifact file not found: {input_path}")

    records: List[dict] = []
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    logger.debug(
        "artifact_writer: read JSONL",
        path=str(input_path),
        record_count=len(records),
    )
    return records
