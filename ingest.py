import os
import pandas as pd
from openai import OpenAI
import chromadb
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")

openai_client = OpenAI(api_key=API_KEY)

chroma = chromadb.PersistentClient(path="chroma_db")
try:
    collection = chroma.get_collection("garmin_runs")
except Exception:
    collection = chroma.create_collection("garmin_runs")

def csv_to_chunks(csv_path):
    df = pd.read_csv(csv_path)
    chunks = []

    for _, row in df.iterrows():
        summary = "|".join([f"{col}: row[col]" for col in df.columns])
        chunks.append(summary)

    return chunks

def embed_and_store(csv_file: str):
    chunks = csv_to_chunks(csv_file)

    for i, chunk in enumerate(chunks):
        resp = open