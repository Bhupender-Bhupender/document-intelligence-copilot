from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Dict, List

from pypdf import PdfReader
from tqdm import tqdm


PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "docs" / "sample_docs"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_FILE = OUTPUT_DIR / "documents.jsonl"


def clean_text(text: str) -> str:
    """
    Clean extracted text by normalizing whitespace and removing empty noise.
    """
    if not text:
        return ""

    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    text = text.strip()
    return text


def create_doc_id(file_path: Path) -> str:
    """
    Generate a stable-ish document id for the file.
    """
    return f"{file_path.stem}_{uuid.uuid4().hex[:8]}"


def read_txt_or_md(file_path: Path) -> List[Dict]:
    """
    Read a .txt or .md file and return one record.
    """
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    cleaned = clean_text(text)

    if not cleaned:
        return []

    doc_id = create_doc_id(file_path)

    return [
        {
            "doc_id": doc_id,
            "file_name": file_path.name,
            "file_type": file_path.suffix.lower().replace(".", ""),
            "page_number": None,
            "text": cleaned,
        }
    ]


def read_pdf(file_path: Path) -> List[Dict]:
    """
    Read a text-based PDF and return one record per page.
    """
    records: List[Dict] = []
    doc_id = create_doc_id(file_path)

    try:
        reader = PdfReader(str(file_path))
    except Exception as exc:
        print(f"Failed to open PDF {file_path.name}: {exc}")
        return records

    for page_index, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception as exc:
            print(f"Failed to extract page {page_index} from {file_path.name}: {exc}")
            continue

        cleaned = clean_text(text)
        if not cleaned:
            continue

        records.append(
            {
                "doc_id": doc_id,
                "file_name": file_path.name,
                "file_type": "pdf",
                "page_number": page_index,
                "text": cleaned,
            }
        )

    return records


def ingest_documents(input_dir: Path) -> List[Dict]:
    """
    Read supported documents from the input directory.
    """
    supported_suffixes = {".txt", ".md", ".pdf"}
    all_records: List[Dict] = []

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    files = [f for f in input_dir.iterdir() if f.is_file() and f.suffix.lower() in supported_suffixes]

    if not files:
        print(f"No supported files found in {input_dir}")
        return all_records

    for file_path in tqdm(files, desc="Ingesting documents"):
        suffix = file_path.suffix.lower()

        if suffix in {".txt", ".md"}:
            all_records.extend(read_txt_or_md(file_path))
        elif suffix == ".pdf":
            all_records.extend(read_pdf(file_path))

    return all_records


def save_jsonl(records: List[Dict], output_file: Path) -> None:
    """
    Save records as JSONL.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    print(f"Reading documents from: {INPUT_DIR}")
    records = ingest_documents(INPUT_DIR)

    if not records:
        print("No records were created.")
        return

    save_jsonl(records, OUTPUT_FILE)

    print(f"\nIngestion complete.")
    print(f"Total records saved: {len(records)}")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()