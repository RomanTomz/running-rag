import pandas as pd
import math


def _safe(v, default=None):
    return v if (v is not None and v == v) else default

def sec_to_hms(s):
    s = _safe(s)
    if s is None: return ""
    s = int(round(float(s)))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"

def metres_to_km(m):
    m = _safe(m)
    if m is None: return None
    return round(float(m)/1000.0, 3)

def pace_min_per_km(duration_s, distance_m):
    d_km = metres_to_km(distance_m)
    if not d_km or d_km == 0:
        return ""
    pace = float(duration_s) / 60.0 / d_km
    mins = int(pace)
    secs = int(round((pace - mins) * 60))
    if secs == 60:
        mins += 1
        secs = 0
    return f"{mins:02d}:{secs:02d} min/km"