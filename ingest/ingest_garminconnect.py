# ingest/ingest_garminconnect.py

import os, time
from typing import List
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from garminconnect import Garmin
from requests.exceptions import HTTPError

# --- env/creds ---------------------------------------------------------------
def get_credentials():
    load_dotenv(find_dotenv(usecwd=True))
    user = os.getenv("GARMIN_EMAIL") or os.getenv("GARMIN_USERNAME")
    pwd = os.getenv("GARMIN_PASSWORD")
    if not user or not pwd:
        raise ValueError("Set GARMIN_EMAIL (or GARMIN_USERNAME) and GARMIN_PASSWORD in .env")
    return user, pwd

# --- single client (login once) ---------------------------------------------
def get_api() -> Garmin:
    user, pwd = get_credentials()
    api = Garmin(user, pwd)
    # login once with a couple retries in case SSO is touchy
    for attempt in range(3):
        try:
            api.login()
            return api
        except Exception as e:
            # If it's a 429, exponential backoff
            sleep_s = 2 ** attempt
            time.sleep(sleep_s)
            if attempt == 2:
                raise
    return api  # not reached

# --- paging with backoff -----------------------------------------------------
def get_activities_page(api: Garmin, start: int, limit: int, retries: int = 3, base_sleep: float = 0.8):
    for attempt in range(retries):
        try:
            return api.get_activities(start, limit)
        except Exception as e:
            # Back off on transient issues (including 429)
            time.sleep(base_sleep * (2 ** attempt))
            if attempt == retries - 1:
                raise

def get_all_activities(page_size: int = 50, pause_between_pages: float = 1) -> List[dict]:
    api = get_api()                # <-- LOGIN ONCE
    all_items: List[dict] = []
    start = 0
    while True:
        page = get_activities_page(api, start, page_size)
        if not page:
            break
        all_items.extend(page)
        start += page_size
        time.sleep(pause_between_pages)  # be polite; reduce chance of 429s
    return all_items

# --- dataframe ---------------------------------------------------------------
def create_df(activities: list) -> pd.DataFrame:
    return pd.json_normalize(activities, sep=".")

# --- main --------------------------------------------------------------------
if __name__ == "__main__":
    activities = get_all_activities(page_size=40)  # 20–50 is a safe range
    df = create_df(activities)
    print(f"Fetched {len(df)} activities")
    print(df.info())
