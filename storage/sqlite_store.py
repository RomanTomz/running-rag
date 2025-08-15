from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "garmin.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DDL = """
PRAGMA journal_mode=WAL;

create table if not exists activities_raw (
    activity_id integer primary key,
    start_time  text,
    type_key    text,
    payload_json    text not null,
    inserted_at     integer not null

);

create table if not exists activities_flat (
    activity_id integer primary key,
    start_time  text,
    type_key    text,
    distance_m  real,
    duration_s  real,
    avg_hr  real,
    location text,
    inserted_at integer not null
);
"""

def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("pragma foreign_key = on;")
    return con

def init_db() -> None:
    con = _connect()
    try:
        with con:
            for stmt in DDL.strip().split(';'):
                s = stmt.strip()
                if s:
                    con.execute(s)

    finally:
        con.close()

def get_existing_ids() -> set[int]:
    con = _connect()
    try:
        rows = con.execute("select activity_id from activities_raw").fetchall()
        return {int(r[0]) for r in rows}
    finally:
        con.close()

def upsert_activities(df: pd.DataFrame) -> tuple[int, int, list[int]]:
    now = int(time.time())
    inserted = 0
    skipped = 0
    new_ids: list[int] = []

    con = _connect()
    try:
        

if __name__ == "__main__":
    print(ROOT)
    print(DB_PATH)