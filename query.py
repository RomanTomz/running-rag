"""Query interface for Garmin run summaries stored in ChromaDB."""
from __future__ import annotations

import os
from typing import Dict, List, Tuple

import chromadb
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")

if not API_KEY:
    raise RuntimeError("OPENAI_API_KEY not set")

openai_client = OpenAI(api_key=API_KEY)

# Persistent database created during ingestion
chroma = chromadb.PersistentClient(path="chroma_db")
try:
    collection = chroma.get_collection("garmin_runs")
except Exception as exc:  # pragma: no cover - defensive guard for CLI use
    raise RuntimeError("Collection not found, run ingest.py first") from exc


def _format_metadata(metadata: Dict[str, object]) -> str:
    """Return a compact string representation of metadata."""
    pieces: List[str] = []
    for key, value in metadata.items():
        if value is None or value == "":
            continue
        pieces.append(f"{key}={value}")
    return ", ".join(pieces) if pieces else "(no metadata)"


# ------------ Main query function ---------------------------------------------
def query(question: str, n_results: int = 5) -> Tuple[str, List[Dict[str, object]]]:
    """Answer *question* using the stored summaries.

    Returns a tuple of (answer_text, context_items) where context_items is a list
    of {"document": str, "metadata": dict} pairs used for the response.
    """

    if not question or not question.strip():
        raise ValueError("question must be a non-empty string")

    # Embed the question
    emb = openai_client.embeddings.create(
        model="text-embedding-3-small", input=question
    )
    query_vector = emb.data[0].embedding

    # Retrieve relevant documents + metadata from ChromaDB
    results = collection.query(
        query_embeddings=[query_vector],
        n_results=n_results,
        include=["documents", "metadatas"],
    )

    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]

    context: List[Dict[str, object]] = []
    summary_blocks: List[str] = []
    for idx, (doc, meta) in enumerate(zip(documents, metadatas), start=1):
        meta = meta or {}
        context.append({"document": doc, "metadata": meta})
        formatted_meta = _format_metadata(meta)
        summary_blocks.append(f"Summary {idx}: {doc}\nMetadata: {formatted_meta}")

    context_section = "\n\n".join(summary_blocks) if summary_blocks else "(no context retrieved)"

    prompt = (
        "You are a helpful running coach. Answer the user's question using the "
        "provided training summaries. Be specific when the information is "
        "available and say when it is not.\n\n"
        f"Question: {question}\n\n"
        f"Retrieved summaries:\n{context_section}"
    )

    completion = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You answer questions about running workouts."},
            {"role": "user", "content": prompt},
        ],
    )

    answer = completion.choices[0].message.content.strip()
    return answer, context
