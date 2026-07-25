"""
Docling preflight smoke check.

Tests Docling against one markdown file and one PDF from docs/sample_docs/.
Prints a structured summary so the result can be captured in the build log.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MD_FILE = PROJECT_ROOT / "docs" / "sample_docs" / "quarterly_summary.md"
PDF_FILE = PROJECT_ROOT / "docs" / "sample_docs" / "Operations_report.pdf"
COMPLEX_PDF = PROJECT_ROOT / "docs" / "sample_docs" / "prof-services-agrmt.pdf"


def run_check(label: str, file_path: Path, converter) -> dict:
    if not file_path.exists():
        return {"label": label, "file": str(file_path), "status": "SKIP", "reason": "file not found"}
    try:
        t0 = time.time()
        result = converter.convert(str(file_path))
        elapsed = round(time.time() - t0, 2)
        doc = result.document

        # Count pages, blocks, tables
        pages = len(doc.pages) if hasattr(doc, "pages") and doc.pages else "n/a"
        tables = len(doc.tables) if hasattr(doc, "tables") and doc.tables else 0

        # Export to Markdown to see what was extracted
        md_text = doc.export_to_markdown()
        word_count = len(md_text.split())

        # Check for any picture/image elements as an OCR proxy signal
        pictures = len(doc.pictures) if hasattr(doc, "pictures") and doc.pictures else 0

        return {
            "label": label,
            "file": file_path.name,
            "status": "OK",
            "elapsed_s": elapsed,
            "pages": pages,
            "tables": tables,
            "pictures_or_figures": pictures,
            "word_count_in_markdown_export": word_count,
            "ocr_needed_signal": pictures > 0 and word_count < 50,
        }
    except Exception as exc:
        return {"label": label, "file": file_path.name, "status": "ERROR", "error": str(exc)}


def main():
    print("=" * 60)
    print("Docling preflight smoke check")
    print("=" * 60)

    try:
        # On Windows, torch must be imported before transformers to avoid
        # a DLL initialization order conflict (WinError 1114 on c10.dll).
        import torch  # noqa: F401  — must stay first

        from docling.document_converter import DocumentConverter
        import importlib.metadata
        docling_version = importlib.metadata.version("docling")
        print(f"Docling version : {docling_version}")
    except ImportError as e:
        print(f"FATAL: Docling not importable — {e}")
        sys.exit(1)

    converter = DocumentConverter()

    results = [
        run_check("markdown", MD_FILE, converter),
        run_check("born_digital_pdf", PDF_FILE, converter),
        run_check("complex_pdf", COMPLEX_PDF, converter),
    ]

    print()
    for r in results:
        print(f"[{r['status']}] {r['label']} — {r.get('file', '?')}")
        for k, v in r.items():
            if k not in ("label", "file", "status"):
                print(f"      {k}: {v}")
    print()
    print("Smoke check complete.")


if __name__ == "__main__":
    main()
