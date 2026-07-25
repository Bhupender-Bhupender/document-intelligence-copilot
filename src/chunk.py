"""
DEPRECATED — Legacy prototype chunking script.

This script was the original proof-of-concept for splitting flat JSONL
record text into word-window chunks without any Pydantic schema or
hierarchical parent/child structure.

It is preserved as a reference only and is NOT part of the new pipeline.

Replacement:
    Chunking is handled by the typed chunker layer:
        src/chunking/word_chunker.py            (flat sliding-window)
        src/chunking/hierarchical_chunker.py    (parent / child, Phase 3+)
    Both produce List[DocumentChunk] as per src/schema/models.py.

Do not import or run this script in the new pipeline.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List


PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "processed" / "documents.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data" / "processed" / "chunks.jsonl"


CHUNK_SIZE_WORDS = 80
CHUNK_OVERLAP_WORDS = 20


def load_jsonl(file_path: Path) -> List[Dict]:
    records: List[Dict] = []

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))

    return records


def save_jsonl(records: List[Dict], file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def normalize_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_into_word_chunks(
    text: str,
    chunk_size_words: int = CHUNK_SIZE_WORDS,
    chunk_overlap_words: int = CHUNK_OVERLAP_WORDS,
) -> List[str]:
    text = normalize_text(text)
    if not text:
        return []

    words = text.split()

    if len(words) <= chunk_size_words:
        return [" ".join(words)]

    chunks: List[str] = []
    step = chunk_size_words - chunk_overlap_words

    if step <= 0:
        raise ValueError("chunk_size_words must be greater than chunk_overlap_words")

    for start in range(0, len(words), step):
        end = start + chunk_size_words
        chunk_words = words[start:end]

        if not chunk_words:
            continue

        chunk_text = " ".join(chunk_words).strip()
        if chunk_text:
            chunks.append(chunk_text)

        if end >= len(words):
            break

    return chunks


def build_chunk_records(documents: List[Dict]) -> List[Dict]:
    chunk_records: List[Dict] = []

    for doc in documents:
        text = doc.get("text", "")
        chunks = split_into_word_chunks(text)

        for idx, chunk_text in enumerate(chunks):
            chunk_records.append(
                {
                    "chunk_id": f"{doc['doc_id']}_chunk_{idx}",
                    "doc_id": doc["doc_id"],
                    "file_name": doc["file_name"],
                    "file_type": doc["file_type"],
                    "page_number": doc["page_number"],
                    "chunk_index": idx,
                    "text": chunk_text,
                    "word_count": len(chunk_text.split()),
                }
            )

    return chunk_records


def main() -> None:
    print(f"Loading documents from: {INPUT_FILE}")
    documents = load_jsonl(INPUT_FILE)

    if not documents:
        print("No documents found.")
        return

    chunks = build_chunk_records(documents)

    if not chunks:
        print("No chunks were created.")
        return

    save_jsonl(chunks, OUTPUT_FILE)

    print("\nChunking complete.")
    print(f"Documents processed: {len(documents)}")
    print(f"Chunks created: {len(chunks)}")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()