import os
from dotenv import load_dotenv
from openai import OpenAI
import chromadb

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")

if not API_KEY:
    raise RuntimeError("OPENAI_API_KEY not set")

openai_client = OpenAI(api_key=API_KEY)

chroma = chromadb.PersistentClient(path='chroma_db')
try:
    collection = chroma.get_collection("garmin_runs")
except Exception:
    raise RuntimeError("Collection not found, run ingest.py first")

# ------------ Main query function ---------------------------------------------
def query(question: str, n_results: int = 5) -> str:
    # Embed the question
    emb = openai_client.embeddings.create(
        model="text-embedding-3-small", input=question
    )
    query_vector = emb.data[0].embedding