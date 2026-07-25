"""
DEPRECATED — Legacy prototype retrieval script.

This script was the original proof-of-concept for vector retrieval.
It queries ChromaDB directly using sentence-transformers/all-MiniLM-L6-v2
and prints ranked results to stdout.

It is preserved as a reference only and is NOT part of the new pipeline.

Replacement:
    Retrieval will be handled by the hybrid retriever in Phase 5, combining
    vector search, BM25 lexical search, reciprocal rank fusion, metadata
    filtering, and Qwen3-Reranker-0.6B reranking — all via LlamaIndex
    abstractions.

Do not import or run this script in the new pipeline.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CHROMA_DIR = PROJECT_ROOT / "data" / "chroma_db"

COLLECTION_NAME = "document_chunks"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
TOP_K = 3


def main() -> None:
    parser = argparse.ArgumentParser(description="Retrieve relevant chunks from Chroma.")
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="Question to search for in the vector database."
    )
    parser.add_argument(
        "--top_k",
        type=int,
        default=TOP_K,
        help="Number of results to return."
    )
    args = parser.parse_args()

    print(f"Loading embedding model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)

    print(f"Loading Chroma DB from: {CHROMA_DIR}")
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    collection = client.get_collection(name=COLLECTION_NAME)

    print(f"Collection count: {collection.count()}")

    query_embedding = model.encode(
        args.query,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    results = collection.query(
        query_embeddings=[query_embedding.tolist()],
        n_results=args.top_k,
        include=["documents", "metadatas", "distances"],
    )

    documents = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0]

    print("\n" + "=" * 80)
    print(f"QUERY: {args.query}")
    print("=" * 80)

    for i, (doc, meta, dist) in enumerate(zip(documents, metadatas, distances), start=1):
        print(f"\nResult #{i}")
        print("-" * 80)
        print(f"Distance: {dist:.4f}")
        print(f"File Name: {meta.get('file_name')}")
        print(f"Doc ID: {meta.get('doc_id')}")
        print(f"File Type: {meta.get('file_type')}")
        print(f"Chunk Index: {meta.get('chunk_index')}")
        print(f"Page Number: {meta.get('page_number', 'N/A')}")
        print(f"Word Count: {meta.get('word_count')}")
        print("\nChunk Text:")
        print(doc)
        print("-" * 80)


if __name__ == "__main__":
    main()