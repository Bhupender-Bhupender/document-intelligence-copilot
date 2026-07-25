"""
CLI entrypoint for the local KPI batch harness.

Example
-------
    python scripts/run_kpi_batch.py \\
        --pdf-dir data/raw/test_pdfs \\
        --json-dir data/eval/kpi_queries \\
        --out-dir data/eval/results
"""
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local KPI batch harness.")
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=Path("data/raw/test_pdfs"),
        help="Directory of input PDFs.",
    )
    parser.add_argument(
        "--json-dir",
        type=Path,
        default=Path("data/eval/kpi_queries"),
        help="Directory of KPI/query JSON specs (paired by exact stem).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/eval/results"),
        help="Directory for per-PDF result workbooks.",
    )
    args = parser.parse_args()

    from src.evaluation.kpi_batch import run_batch

    written = run_batch(args.pdf_dir, args.json_dir, args.out_dir)
    for path in written:
        print(path)


if __name__ == "__main__":
    main()
