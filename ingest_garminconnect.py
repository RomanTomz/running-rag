import os
from datetime import date, timedelta

from dotenv import load_dotenv
from openai import OpenAI
import chromadb 
from garminconnect import Garmin

def summarise_activity_from_gc(a: dict) -> str:
    parts = []
    def add(k, v):
        if v is not None and v != "":
            parts.append(f"{k}: {v}")
    add("date")