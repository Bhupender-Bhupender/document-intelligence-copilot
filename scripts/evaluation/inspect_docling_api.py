"""Inspect Docling 2.95.0 API shape — run once, discard."""
import sys
import torch  # noqa: F401 — must stay first on Windows

from docling.document_converter import DocumentConverter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MD_FILE = PROJECT_ROOT / "docs" / "sample_docs" / "quarterly_summary.md"
PDF_FILE = PROJECT_ROOT / "docs" / "sample_docs" / "Operations_report.pdf"


def inspect(label: str, file_path: Path) -> None:
    print(f"\n{'='*60}")
    print(f"FILE: {label} — {file_path.name}")
    print("=" * 60)

    conv = DocumentConverter()
    result = conv.convert(str(file_path))
    doc = result.document

    print(f"\ndoc type      : {type(doc).__name__}")
    print(f"doc.pages type: {type(doc.pages).__name__}")
    print(f"doc.pages len : {len(doc.pages)}")

    if doc.pages:
        pk = list(doc.pages.keys())[0]
        page = doc.pages[pk]
        print(f"\npage type : {type(page).__name__}")
        print(f"page attrs: {[a for a in dir(page) if not a.startswith('_')]}")
        print(f"page.page_no: {getattr(page, 'page_no', 'N/A')}")

    # Body items (elements)
    print(f"\ndoc.body type : {type(doc.body).__name__}")
    print(f"doc.body attrs: {[a for a in dir(doc.body) if not a.startswith('_')]}")

    # Try iterating items
    if hasattr(doc, 'iterate_items'):
        print("\ndoc.iterate_items() — first 5 items:")
        for i, (item, level) in enumerate(doc.iterate_items()):
            label_val = getattr(item, 'label', 'N/A')
            text_val = getattr(item, 'text', None) or ''
            prov = getattr(item, 'prov', [])
            bbox_val = prov[0].bbox if prov else None
            print(f"  [{i}] level={level} label={label_val!r} bbox={bbox_val!r} text={text_val[:60]!r}")
            if i >= 4:
                break

    # Export methods
    print("\nExport methods on doc:")
    for attr in dir(doc):
        if 'export' in attr.lower() or 'text' in attr.lower() or 'markdown' in attr.lower():
            print(f"  {attr}")

    # Try export_to_markdown
    if hasattr(doc, 'export_to_markdown'):
        md = doc.export_to_markdown()
        print(f"\nexport_to_markdown() len: {len(md)}, first 200 chars:\n{md[:200]!r}")

    # DocItemLabel
    try:
        from docling.datamodel.base_models import ItemLabel
        print(f"\nItemLabel values: {list(ItemLabel)}")
    except Exception as e:
        print(f"\nItemLabel import error: {e}")

    try:
        from docling_core.types.doc.labels import DocItemLabel
        print(f"\nDocItemLabel values: {list(DocItemLabel)}")
    except Exception as e:
        print(f"\nDocItemLabel import error: {e}")


if __name__ == "__main__":
    inspect("markdown", MD_FILE)
    if "--pdf" in sys.argv:
        inspect("pdf", PDF_FILE)
