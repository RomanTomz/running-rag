import pandas as pd
import math

# --- helpers ---------------------------------------------------------------
def _safe(v, default=None):
    return v if (v is not None and v == v) else default  # handles None/NaN

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
    return round(float(m) / 1000.0, 3)

def calc_pace_min_per_km(duration_s, distance_m):
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

def clean_label(s):
    if not s: return ""
    return str(s).replace("_", " ").title()

def detect_session_tag(activity_name, training_effect_label, lactate_label):
    name = (str(activity_name) or "").lower()
    tel  = (str(training_effect_label) or "").lower()
    ltl  = (str(lactate_label) or "").lower()
    if any(k in name for k in ["tempo","threshold","lt"]): return "tempo"
    if any(k in tel for k in ["tempo","threshold"]): return "tempo"
    if "lactate_threshold" in ltl: return "tempo"
    if any(k in name for k in ["easy","recovery","z2","base"]): return "easy"
    if any(k in name for k in ["interval","vo2","reps","yasso"]): return "intervals"
    return ""

# --- main summariser --------------------------------------------------------
def summarise_activity_row(row: pd.Series) -> str:
    """
    Produce a compact natural-language summary from a Garmin Connect row.
    Optimised for embeddings (stable order, consistent wording).
    """
    sport = _safe(row.get("activityType.typeKey")) or _safe(row.get("sportTypeId"))
    sport = str(sport) if sport is not None else "unknown"

    name   = _safe(row.get("activityName"))
    start  = _safe(row.get("startTimeLocal")) or _safe(row.get("startTimeGMT"))
    dist_m = _safe(row.get("distance"))
    dur_s  = _safe(row.get("duration")) or _safe(row.get("movingDuration")) or _safe(row.get("elapsedDuration"))

    # generic metrics
    elev_gain = _safe(row.get("elevationGain"))
    avg_hr    = _safe(row.get("averageHR"))
    max_hr    = _safe(row.get("maxHR"))
    avg_spd   = _safe(row.get("averageSpeed"))
    avg_cad   = _safe(row.get("averageRunningCadenceInStepsPerMinute"))
    tl        = _safe(row.get("activityTrainingLoad")) or _safe(row.get("trainingLoad"))
    tel       = _safe(row.get("trainingEffectLabel"))  # e.g., LACTATE_THRESHOLD / TEMPO / etc.
    aer_te    = _safe(row.get("aerobicTrainingEffect"))
    an_te     = _safe(row.get("anaerobicTrainingEffect"))
    vo2       = _safe(row.get("vO2MaxValue"))
    loc       = _safe(row.get("locationName"))

    # derived
    dist_km = metres_to_km(dist_m)
    dur_hms = sec_to_hms(dur_s) if dur_s is not None else ""
    pace    = calc_pace_min_per_km(dur_s, dist_m) if (dur_s is not None and dist_m is not None) else ""
    tag     = detect_session_tag(name, tel, tel)

    # per‑sport tweaks
    pieces = []
    # header
    pieces.append(f"date: {start}" if start else "date: unknown")
    pieces.append(f"type: {sport}")
    if name: pieces.append(f"name: {name}")
    if loc:  pieces.append(f"location: {loc}")

    # distance/time
    if dist_km is not None: pieces.append(f"distance_km: {dist_km}")
    if dur_hms: pieces.append(f"duration: {dur_hms}")
    if pace and sport in ("running","trail_running","treadmill_running","hiking"):
        pieces.append(f"avg_pace: {pace}")
    elif avg_spd is not None:
        pieces.append(f"avg_speed_mps: {round(float(avg_spd),3)}")

    # cardio + load
    if avg_hr is not None: pieces.append(f"avg_hr: {int(round(float(avg_hr)))}")
    if max_hr is not None: pieces.append(f"max_hr: {int(round(float(max_hr)))}")
    if avg_cad is not None and sport.startswith("running"):
        pieces.append(f"cadence_spm: {int(round(float(avg_cad)))}")
    if elev_gain is not None: pieces.append(f"elev_gain_m: {int(round(float(elev_gain)))}")
    if tl is not None: pieces.append(f"training_load: {int(round(float(tl)))}")
    if aer_te is not None: pieces.append(f"aerobic_te: {round(float(aer_te),1)}")
    if an_te is not None: pieces.append(f"anaerobic_te: {round(float(an_te),1)}")
    if vo2 is not None: pieces.append(f"vo2max: {int(round(float(vo2)))}")

    # labels/tags
    if tel: pieces.append(f"label: {clean_label(tel)}")
    if tag: pieces.append(f"tag: {tag}")

    # Final compact sentence for LLMs:
    return " | ".join(pieces)

# --- apply to your DataFrame -------------------------------------------------
# df is your Garmin Connect DataFrame
df["natural_summary"] = df.apply(summarise_activity_row, axis=1)

# optional: metadata payload for Chroma filters
def build_metadata(row: pd.Series) -> dict:
    return {
        "source": "garmin_connect",
        "activity_id": int(row["activityId"]) if pd.notna(row.get("activityId")) else None,
        "type": str(row.get("activityType.typeKey") or ""),
        "date": str(row.get("startTimeLocal") or row.get("startTimeGMT") or ""),
        "location": str(row.get("locationName") or ""),
        "hasSplits": bool(row.get("hasSplits")) if "hasSplits" in row else None,
    }

metadata = df.apply(build_metadata, axis=1).tolist()
docs = df["natural_summary"].tolist()
ids = [f"garmin::{int(aid)}" if pd.notna(aid) else f"garmin::row_{i}" 
       for i, aid in enumerate(df.get("activityId", pd.Series([None]*len(df))))]

# (now embed `docs` and upsert with `ids` + `metadata`)
