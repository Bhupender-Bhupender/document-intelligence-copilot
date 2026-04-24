from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import chromadb
from sentence_transformers import SentenceTransformer
from tqdm import tqdm


PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "processed" / "chunks.jsonl"
CHROMA_DIR = PROJECT_ROOT / "data" / "chroma_db"

COLLECTION_NAME = "document_chunks"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BATCH_SIZE = 32


def load_jsonl(file_path: Path) -> List[Dict]:
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    records: List[Dict] = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))

    return records


def batch_list(items: List, batch_size: int) -> List[List]:
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def build_metadata(record: Dict) -> Dict:
    metadata = {
        "doc_id": record["doc_id"],
        "file_name": record["file_name"],
        "file_type": record["file_type"],
        "chunk_index": int(record["chunk_index"]),
        "word_count": int(record["word_count"]),
    }

    if record.get("page_number") is not None:
        metadata["page_number"] = int(record["page_number"])

    return metadata


def reset_collection(client: chromadb.PersistentClient, collection_name: str) -> None:
    existing = [c.name for c in client.list_collections()]
    if collection_name in existing:
        client.delete_collection(name=collection_name)


def main() -> None:
    print(f"Loading chunks from: {INPUT_FILE}")
    chunk_records = load_jsonl(INPUT_FILE)

    if not chunk_records:
        print("No chunk records found.")
        return

    print(f"Loaded {len(chunk_records)} chunk records.")

    print(f"Loading embedding model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)

    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))

    reset_collection(client, COLLECTION_NAME)

    collection = client.create_collection(name=COLLECTION_NAME)

    ids = [record["chunk_id"] for record in chunk_records]
    documents = [record["text"] for record in chunk_records]
    metadatas = [build_metadata(record) for record in chunk_records]

    print("Generating embeddings and writing to Chroma...")

    total_written = 0

    for start_idx in tqdm(range(0, len(documents), BATCH_SIZE), desc="Embedding batches"):
        end_idx = start_idx + BATCH_SIZE

        batch_ids = ids[start_idx:end_idx]
        batch_docs = documents[start_idx:end_idx]
        batch_metas = metadatas[start_idx:end_idx]

        batch_embeddings = model.encode(
            batch_docs,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

        collection.add(
            ids=batch_ids,
            documents=batch_docs,
            metadatas=batch_metas,
            embeddings=batch_embeddings.tolist(),
        )

        total_written += len(batch_ids)

    print("\nEmbedding complete.")
    print(f"Records written to Chroma: {total_written}")
    print(f"Collection name: {COLLECTION_NAME}")
    print(f"Chroma path: {CHROMA_DIR}")
    print(f"Collection count: {collection.count()}")

    sample = collection.peek()
    print("\nSample stored record preview:")
    print(sample)


if __name__ == "__main__":
    main()