"""Inspect Docling page-level API on a born-digital PDF — run once, discard."""
import torch  # noqa: F401

from docling.document_converter import DocumentConverter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PDF_FILE = PROJECT_ROOT / "docs" / "sample_docs" / "Operations_report.pdf"

conv = DocumentConverter()
result = conv.convert(str(PDF_FILE))
doc = result.document

print(f"doc.pages len: {len(doc.pages)}")
for pk, page in list(doc.pages.items())[:2]:
    print(f"\npage key: {pk!r}, page.page_no: {page.page_no}, size: {page.size}")

print("\n=== iterate_items on PDF (first 8) ===")
for i, (item, level) in enumerate(doc.iterate_items()):
    label_val = getattr(item, 'label', 'N/A')
    text_val = getattr(item, 'text', None) or ''
    prov = getattr(item, 'prov', [])
    if prov:
        p = prov[0]
        print(f"  [{i}] level={level} label={label_val!r} page_no={getattr(p,'page_no','?')} bbox={getattr(p,'bbox',None)!r} text={text_val[:50]!r}")
    else:
        print(f"  [{i}] level={level} label={label_val!r} prov=[] text={text_val[:50]!r}")
    if i >= 7:
        break

print("\n=== export_to_text (first 300 chars) ===")
print(repr(doc.export_to_text()[:300]))

# Check if page-scoped text export exists
print("\n=== page-level text export? ===")
if doc.pages:
    pk = list(doc.pages.keys())[0]
    page = doc.pages[pk]
    print("page methods:", [a for a in dir(page) if 'export' in a.lower() or 'text' in a.lower()])

# Check DoclingDocument.export_to_text signature
import inspect
sig = inspect.signature(doc.export_to_text)
print(f"\nexport_to_text signature: {sig}")
sig2 = inspect.signature(doc.export_to_markdown)
print(f"export_to_markdown signature: {sig2}")
