import os
from datetime import date, timedelta

import pandas as pd

from dotenv import load_dotenv
from openai import OpenAI
import chromadb 
from garminconnect import Garmin

def get_activities(
    start: int,
    limit: int,
    username: str = os.getenv("GARMIN_USERNAME"),
    password: str = os.getenv("GARMIN_PASSWORD"),
) -> list:
    if not username or not password:
        raise ValueError("Please set GARMIN_USERNAME and GARMIN_PASSWORD in your environment variables.")

    gc = Garmin(username, password)
    gc.login()
    return gc.get_activities(start, limit)

def create_df(activities: dict) -> pd.DataFrame:
    """Convert Garmin activities to a DataFrame."""
    df = pd.json_normalize(activities)
    return df

def get_all_activities(start=0, page_size=50):
    all_activities = []
    while True:
        activities = get_activities(start, page_size)
        if not activities:
            break
        all_activities.extend(activities)
        start += page_size
    return all_activities

if __name__ == "__main__":
    load_dotenv()
    activities = get_activities(0, 100)
    df = create_df(activities)
    print(df.head())

