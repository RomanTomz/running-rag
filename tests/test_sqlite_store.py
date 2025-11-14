from __future__ import annotations

import math
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Iterable
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[1]))

from storage import sqlite_store


class DummyRow(dict):
    def to_dict(self) -> dict:
        return dict(self)


class DummyDataFrame:
    def __init__(self, rows: Iterable[dict]):
        self._rows = [DummyRow(r) for r in rows]

    def iterrows(self):  # pragma: no cover - simple helper
        for idx, row in enumerate(self._rows):
            yield idx, row


def test_upsert_activities_smoke() -> None:
    original_path = sqlite_store.DB_PATH
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "garmin.db"
        try:
            sqlite_store.DB_PATH = db_path
            sqlite_store.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            sqlite_store.init_db()

            initial_rows = [
                {
                    "activity_id": 101,
                    "startTimeLocal": "2024-01-01T07:00:00",
                    "activityType.typeKey": "running",
                    "distance": 1000,
                    "duration": 300,
                    "averageHR": 150,
                    "locationName": "London",
                },
                {
                    "activity_id": 202,
                    "startTimeLocal": "2024-01-02T08:00:00",
                    "activityType.typeKey": "cycling",
                    "distance": 2000,
                    "movingDuration": 600,
                    "avgHR": 135,
                    "locationName": "Paris",
                },
            ]
            df_initial = DummyDataFrame(initial_rows)

            with patch("storage.sqlite_store.time.time", return_value=111):
                inserted, skipped, new_ids = sqlite_store.upsert_activities(df_initial)

            assert inserted == 2
            assert skipped == 0
            assert new_ids == [101, 202]

            with sqlite3.connect(db_path) as con:
                rows = con.execute(
                    "select activity_id, distance_m, duration_s, avg_hr, location, inserted_at "
                    "from activities_flat order by activity_id"
                ).fetchall()

            assert rows == [
                (101, 1000.0, 300.0, 150.0, "London", 111),
                (202, 2000.0, 600.0, 135.0, "Paris", 111),
            ]

            update_rows = [
                {
                    "activity_id": 101,
                    "startTimeLocal": "2024-01-01T07:30:00",
                    "activityType.typeKey": "running",
                    "distance": 1250,
                    "duration": 320,
                    "avgHR": 151,
                    "location": "Berlin",
                },
                {
                    "activity_id": 202,
                    "startTimeLocal": "2024-01-02T09:00:00",
                    "activityType.typeKey": "cycling",
                    "distance": math.nan,
                    "totalDistance": 2500,
                    "duration": 620,
                    "averageHeartRate": 140,
                    "locationName": "Madrid",
                },
            ]
            df_update = DummyDataFrame(update_rows)

            with patch("storage.sqlite_store.time.time", return_value=222):
                inserted_again, skipped_again, new_ids_again = sqlite_store.upsert_activities(df_update)

            assert inserted_again == 0
            assert skipped_again == 2
            assert new_ids_again == []

            with sqlite3.connect(db_path) as con:
                rows = con.execute(
                    "select activity_id, start_time, type_key, distance_m, duration_s, avg_hr, location, inserted_at "
                    "from activities_flat order by activity_id"
                ).fetchall()

            assert rows == [
                (
                    101,
                    "2024-01-01T07:30:00",
                    "running",
                    1250.0,
                    320.0,
                    151.0,
                    "Berlin",
                    222,
                ),
                (
                    202,
                    "2024-01-02T09:00:00",
                    "cycling",
                    2500.0,
                    620.0,
                    140.0,
                    "Madrid",
                    222,
                ),
            ]
        finally:
            sqlite_store.DB_PATH = original_path
