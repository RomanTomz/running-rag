import argparse
import os
from typing import Dict, List

import chromadb
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

from summarise.summariser import build_metadata, summarise_activity_row


load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")

openai_client = OpenAI(api_key=API_KEY)

chroma = chromadb.PersistentClient(path="chroma_db")
try:
    collection = chroma.get_collection("garmin_runs")
except Exception:
    collection = chroma.create_collection("garmin_runs")


def _clean_metadata(metadata: Dict[str, object]) -> Dict[str, object]:
    """Remove keys with ``None`` values so Chroma filters remain usable."""

    return {k: v for k, v in metadata.items() if v is not None}


def csv_to_chunks(csv_path: str) -> List[Dict[str, object]]:
    df = pd.read_csv(csv_path)
    chunks: List[Dict[str, object]] = []

    for _, row in df.iterrows():
        summary = summarise_activity_row(row)
        metadata = _clean_metadata(build_metadata(row))
        chunks.append({"summary": summary, "metadata": metadata})

    return chunks


def embed_and_store(csv_file: str):
    chunks = csv_to_chunks(csv_file)

    for i, chunk in enumerate(chunks):
        document = chunk["summary"]
        metadata = chunk["metadata"]
        resp = openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=document,
        )
        embedding = resp.data[0].embedding
        activity_id = metadata.get("activity_id")
        doc_id = str(activity_id) if activity_id is not None else f"{os.path.basename(csv_file)}_{i}"
        collection.add(
            documents=[document],
            embeddings=[embedding],
            ids=[doc_id],
            metadatas=[metadata],
        )

    print(f"Embedded: {csv_file}")


def preview_chunks(csv_file: str, limit: int = 5):
    chunks = csv_to_chunks(csv_file)
    print(f"Previewing {min(limit, len(chunks))} chunks from {csv_file} (total {len(chunks)})")
    for idx, chunk in enumerate(chunks[:limit]):
        print("-" * 60)
        print(f"Row {idx} summary:\n{chunk['summary']}")
        print("Metadata:")
        for key, value in chunk["metadata"].items():
            print(f"  {key}: {value}")


def main():
    parser = argparse.ArgumentParser(description="Embed Garmin Connect CSVs into Chroma.")
    parser.add_argument("--data-dir", default="garmin_data", help="Directory containing CSV files to ingest.")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Preview chunk summaries/metadata without embedding.",
    )
    parser.add_argument(
        "--preview-limit",
        type=int,
        default=5,
        help="Maximum number of chunks to show per file when previewing.",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.data_dir):
        raise SystemExit(f"Data directory not found: {args.data_dir}")

    csv_files = [f for f in os.listdir(args.data_dir) if f.endswith(".csv")]
    if not csv_files:
        raise SystemExit(f"No CSV files found in {args.data_dir}")

    for fname in csv_files:
        csv_path = os.path.join(args.data_dir, fname)
        if args.preview:
            preview_chunks(csv_path, limit=args.preview_limit)
        else:
            embed_and_store(csv_path)

    if not args.preview:
        print("done. Data stored in ./chroma_db (collection: garmin_runs)")


if __name__ == "__main__":
    main()