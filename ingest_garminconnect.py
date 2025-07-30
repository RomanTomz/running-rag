import os
from datetime import date, timedelta

from dotenv import load_dotenv
from openai import OpenAI
import chromadb 
from garminconnect import Garmin

def summarise_activity_from_gc(a: )