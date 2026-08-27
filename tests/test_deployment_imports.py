from __future__ import annotations

import subprocess
import sys


def test_databricks_runtime_import_does_not_load_local_rag_stack():
    code = r"""
import sys

import app.runtime

forbidden_prefixes = (
    "llama_index",
    "chromadb",
    "sentence_transformers",
)

loaded = [
    name
    for name in sys.modules
    if any(
        name == prefix
        or name.startswith(prefix + ".")
        for prefix in forbidden_prefixes
    )
]

if loaded:
    raise RuntimeError(
        "Local RAG dependencies loaded "
        "during serving startup: "
        + ", ".join(sorted(loaded)[:10])
    )

print("CLOUD_RUNTIME_IMPORT_SAFE: True")
"""

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        result.stdout
        + "\n"
        + result.stderr
    )

    assert (
        "CLOUD_RUNTIME_IMPORT_SAFE: True"
        in result.stdout
    )



def test_cloud_runtime_does_not_eagerly_load_legacy_answer_engine():
    code = r"""
import sys

import app.runtime

print(
    "LEGACY_ANSWER_ENGINE_IMPORTED:",
    "src.generation.answer_engine"
    in sys.modules,
)

if "src.generation.answer_engine" in sys.modules:
    raise RuntimeError(
        "Legacy answer_engine was eagerly "
        "loaded by cloud runtime."
    )
"""

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        result.stdout
        + "\n"
        + result.stderr
    )


def test_cloud_requirements_include_structlog():
    from pathlib import Path

    requirements = Path(
        "requirements.txt"
    ).read_text(
        encoding="utf-8"
    ).lower()

    assert "structlog" in requirements
