import os
from datetime import date, timedelta

from dotenv import load_dotenv
from openai import OpenAI
import chromadb 
from garminconnect import Garmin

def get_activities(
    start_date: date = date.today() - timedelta(days=30),
    end_date: date = date.today(),
    username: str = os.getenv("GARMIN_USERNAME"),
    password: str = os.getenv("GARMIN_PASSWORD"),
) -> list:
    if not username or not password:
        raise ValueError("Please set GARMIN_USERNAME and GARMIN_PASSWORD in your environment variables.")

    gc = Garmin(username, password)
    gc.login()
    return gc.get_activities(start_date, end_date)

def create_df(activities: dict) -> pd.DataFrame:
    """Convert Garmin activities to a DataFrame."""
    df = pd.json_normalize(activities)
    # Ensure all columns are strings for consistency
    return df