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
        resp = openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=chunk
        )
        embedding = resp.data[0].embedding
        doc_id = f"{os.path.basename(csv_file)}_{i}"
        collection.add(documents=[chunk], embeddings=[embedding], ids=[doc_id])

    print(f"Embedded: {csv_file}")

# Entrypoint
if __name__=="__main__":
    data_dir = "garmin_data"
    for fname in os.listdir(data_dir):
        if fname.endswith(".csv"):
            embed_and_store(os.path.join(data_dir, fname))
    print("done. Data stored in ./chroma_db (collection: garmin_runs)")