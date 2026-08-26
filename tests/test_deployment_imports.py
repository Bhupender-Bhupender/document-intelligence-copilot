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
