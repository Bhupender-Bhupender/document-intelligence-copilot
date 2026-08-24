import subprocess
import sys


def test_retrieval_gateway_import_does_not_require_rank_bm25():
    code = """
import sys

sys.modules["rank_bm25"] = None

from src.retrieval.retrieval_gateway import route_retrieve

print("IMPORT_OK")
"""

    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "IMPORT_OK" in result.stdout