from __future__ import annotations

import json
import math
import numbers
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

try:  # pragma: no cover - optional dependency at runtime
    import pandas as pd  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for smoke tests
    pd = None  # type: ignore[assignment]


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, numbers.Real):
        return math.isnan(value)  # type: ignore[arg-type]
    if pd is not None:
        try:
            return bool(pd.isna(value))
        except TypeError:
            pass
    return False


def _first_valid(row: Mapping[str, Any], keys: Iterable[str]) -> object:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if _is_missing(value):
            continue
        return value
    return None

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

def upsert_activities(df: "pd.DataFrame") -> tuple[int, int, list[int]]:
    now = int(time.time())
    inserted = 0
    skipped = 0
    new_ids: list[int] = []

    distance_keys = ["distance", "totalDistance"]
    duration_keys = ["duration", "movingDuration", "elapsedDuration"]
    avg_hr_keys = ["averageHR", "avgHR", "averageHeartRate"]
    location_keys = ["locationName", "location"]

    con = _connect()
    try:
        with con:
            for _, row in df.iterrows():
                if "activity_id" not in row or _is_missing(row["activity_id"]):
                    continue
                aid = int(row["activity_id"])
                start = str(row.get("startTimeLocal") or row.get("startTimeGMT", "") or "")
                type_key = str(row.get("activityType.typeKey") or row.get("sportTypeId") or "")
                payload = json.dumps(row.to_dict(), ensure_ascii=False, default=str)

                cur = con.execute(
                    """
                    insert into activities_raw(activity_id, start_time, type_key, payload_json, inserted_at)
                    values (?, ?, ?, ?, ?)
                    on conflict(activity_id) do nothing
                    """,
                    (aid, start, type_key, payload, now),
                )

                if cur.rowcount == 0:
                    skipped += 1
                else:
                    inserted += 1
                    new_ids.append(aid)

                distance_val = _first_valid(row, distance_keys)
                duration_val = _first_valid(row, duration_keys)
                avg_hr_val = _first_valid(row, avg_hr_keys)
                location_val = _first_valid(row, location_keys)

                con.execute(
                    """
                    insert into activities_flat(activity_id, start_time, type_key, distance_m, duration_s, avg_hr, location, inserted_at)
                    values (?, ?, ?, ?, ?, ?, ?, ?)
                    on conflict(activity_id) do update set
                        start_time = excluded.start_time,
                        type_key = excluded.type_key,
                        distance_m = excluded.distance_m,
                        duration_s = excluded.duration_s,
                        avg_hr = excluded.avg_hr,
                        location = excluded.location,
                        inserted_at = excluded.inserted_at
                    """,
                    (
                        aid,
                        start,
                        type_key,
                        float(distance_val) if distance_val is not None else 0.0,
                        float(duration_val) if duration_val is not None else 0.0,
                        float(avg_hr_val) if avg_hr_val is not None else None,
                        str(location_val) if location_val is not None else "",
                        now,
                    ),
                )
        return inserted, skipped, new_ids
    finally:
        con.close()

if __name__ == "__main__":
    print(ROOT)
    print(DB_PATH)