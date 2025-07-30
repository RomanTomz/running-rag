import os
import pandas as pd
from openai import OpenAI
from chromadb import Client
from chromadb.config import Settings
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")