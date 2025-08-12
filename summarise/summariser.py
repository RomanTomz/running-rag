from __future__ import annotations
"""
Summariser for Garmin Connect activities -> short, consistent natural-language strings.

Why: embeddings work best with compact text, not raw numbers. We turn each row into a
"date | type | distance | pace | HR | load" line, plus optional metadata for filtering.

Run as a module to quickly inspect output from a CSV exported (or created in-notebook)
with Garmin Connect fields.

Examples:
    # Preview top 5 summaries from a CSV
    uv run python -m garmin_rag.summariser --csv ./garmin_data/gc_export.csv --limit 5

    # Save just [activityId, natural_summary] to a new CSV
    uv run python -m garmin_rag.summariser --csv ./garmin_data/gc_export.csv --save-summaries ./summaries.csv

    # Import programmatically
    from garmin_rag.summariser import summarise_activity_row
    df["natural_summary"] = df.apply(summarise_activity_row, axis=1)
"""

import math
from typing import Any, Dict, Optional
import pandas as pd
import typer
from rich import print

app = typer.Typer(add_completion=False, help="Create natural-language summaries from Garmin Connect DataFrames.")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(v: Any, default: Any = None):
    """Return v unless it's None/NaN; otherwise default."""
    if v is None:
        return default
    try:
        # pandas uses NaN for missing numerics; NaN != NaN
        if isinstance(v, float) and math.isnan(v):
            return default
    except Exception:
        pass
    return v


def sec_to_hms(s: Any) -> str:
    s = _safe(s)
    if s is None:
        return ""
    s = int(round(float(s)))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def metres_to_km(m: Any) -> Optional[float]:
    m = _safe(m)
    if m is None:
        return None
    return round(float(m) / 1000.0, 3)


def calc_pace_min_per_km(duration_s: Any, distance_m: Any) -> str:
    """Return mm:ss per km string, or blank if not computable."""
    d_km = metres_to_km(distance_m)
    dur = _safe(duration_s)
    if d_km is None or not d_km:
        return ""
    if dur is None:
        return ""
    pace = float(dur) / 60.0 / d_km
    mins = int(pace)
    secs = int(round((pace - mins) * 60))
    if secs == 60:
        mins += 1
        secs = 0
    return f"{mins:02d}:{secs:02d} min/km"


def clean_label(s: Any) -> str:
    if not s:
        return ""
    return str(s).replace("_", " ").title()


def detect_session_tag(activity_name: Any, training_effect_label: Any) -> str:
    name = (str(activity_name) or "").lower()
    tel = (str(training_effect_label) or "").lower()
    if any(k in name for k in ["tempo", "threshold", "lt", "lactate"]):
        return "tempo"
    if any(k in tel for k in ["tempo", "threshold", "lactate"]):
        return "tempo"
    if any(k in name for k in ["easy", "recovery", "z2", "base"]):
        return "easy"
    if any(k in name for k in ["interval", "vo2", "reps", "yasso"]):
        return "intervals"
    return ""


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------

def summarise_activity_row(row: pd.Series) -> str:
    """Build a compact, RAG-friendly summary from a Garmin Connect row.

    Expected columns (any may be missing; we coalesce where sensible):
      - activityId, activityName
      - startTimeLocal / startTimeGMT
      - distance (metres), duration (seconds), movingDuration, elapsedDuration
      - elevationGain, averageHR, maxHR, averageSpeed, averageRunningCadenceInStepsPerMinute
      - activityTrainingLoad or trainingLoad
      - trainingEffectLabel, aerobicTrainingEffect, anaerobicTrainingEffect
      - vO2MaxValue, locationName, activityType.typeKey
    """
    sport = _safe(row.get("activityType.typeKey")) or _safe(row.get("sportTypeId")) or "unknown"
    name = _safe(row.get("activityName"))
    start = _safe(row.get("startTimeLocal")) or _safe(row.get("startTimeGMT"))

    dist_m = _safe(row.get("distance"))
    dur_s = _safe(row.get("duration")) or _safe(row.get("movingDuration")) or _safe(row.get("elapsedDuration"))

    elev_gain = _safe(row.get("elevationGain"))
    avg_hr = _safe(row.get("averageHR"))
    max_hr = _safe(row.get("maxHR"))
    avg_spd = _safe(row.get("averageSpeed"))
    avg_cad = _safe(row.get("averageRunningCadenceInStepsPerMinute"))

    tl = _safe(row.get("activityTrainingLoad")) or _safe(row.get("trainingLoad"))
    tel = _safe(row.get("trainingEffectLabel"))
    aer_te = _safe(row.get("aerobicTrainingEffect"))
    an_te = _safe(row.get("anaerobicTrainingEffect"))
    vo2 = _safe(row.get("vO2MaxValue"))
    loc = _safe(row.get("locationName"))

    dist_km = metres_to_km(dist_m)
    dur_hms = sec_to_hms(dur_s) if dur_s is not None else ""
    pace = calc_pace_min_per_km(dur_s, dist_m) if (dur_s is not None and dist_m is not None) else ""

    tag = detect_session_tag(name, tel)

    pieces = []
    pieces.append(f"date: {start}" if start else "date: unknown")
    pieces.append(f"type: {sport}")
    if name:
        pieces.append(f"name: {name}")
    if loc:
        pieces.append(f"location: {loc}")

    if dist_km is not None:
        pieces.append(f"distance_km: {dist_km}")
    if dur_hms:
        pieces.append(f"duration: {dur_hms}")

    # Pace for foot-travel sports; otherwise use avg speed if present
    if pace and str(sport) in ("running", "trail_running", "treadmill_running", "hiking"):
        pieces.append(f"avg_pace: {pace}")
    elif avg_spd is not None:
        pieces.append(f"avg_speed_mps: {round(float(avg_spd), 3)}")

    if avg_hr is not None:
        pieces.append(f"avg_hr: {int(round(float(avg_hr)))}")
    if max_hr is not None:
        pieces.append(f"max_hr: {int(round(float(max_hr)))}")
    if avg_cad is not None and str(sport).startswith("running"):
        pieces.append(f"cadence_spm: {int(round(float(avg_cad)))}")
    if elev_gain is not None:
        pieces.append(f"elev_gain_m: {int(round(float(elev_gain)))}")

    if tl is not None:
        pieces.append(f"training_load: {int(round(float(tl)))}")
    if aer_te is not None:
        pieces.append(f"aerobic_te: {round(float(aer_te), 1)}")
    if an_te is not None:
        pieces.append(f"anaerobic_te: {round(float(an_te), 1)}")
    if vo2 is not None:
        pieces.append(f"vo2max: {int(round(float(vo2)))}")

    if tel:
        pieces.append(f"label: {clean_label(tel)}")
    if tag:
        pieces.append(f"tag: {tag}")

    return " | ".join(pieces)


def build_metadata(row: pd.Series) -> Dict[str, Any]:
    aid = _safe(row.get("activityId"))
    return {
        "source": "garmin_connect",
        "activity_id": int(aid) if aid is not None else None,
        "type": str(_safe(row.get("activityType.typeKey")) or ""),
        "date": str(_safe(row.get("startTimeLocal")) or _safe(row.get("startTimeGMT")) or ""),
        "location": str(_safe(row.get("locationName")) or ""),
        "hasSplits": bool(_safe(row.get("hasSplits"))) if "hasSplits" in row else None,
    }


# ---------------------------------------------------------------------------
# CLI / Entrypoint for quick inspection
# ---------------------------------------------------------------------------

@app.command()
def main(
    csv: str = typer.Option(..., "--csv", help="Path to a CSV containing Garmin Connect activity rows."),
    limit: int = typer.Option(5, help="How many summaries to print."),
    save_summaries: Optional[str] = typer.Option(None, help="If set, write [activityId,natural_summary] to this CSV."),
    show_meta: bool = typer.Option(False, help="Also print first metadata dicts for inspection."),
):
    """Preview and/or save natural-language summaries from a CSV."""
    df = pd.read_csv(csv)
    # Create summaries
    df["natural_summary"] = df.apply(summarise_activity_row, axis=1)

    print("[bold]\nExamples (natural_summary):[/bold]")
    for s in df["natural_summary"].head(limit).tolist():
        print("-", s)

    if show_meta:
        print("\n[bold]Sample metadata:[/bold]")
        for i in range(min(limit, len(df))):
            print(build_metadata(df.iloc[i]))

    if save_summaries:
        out = df[["activityId", "natural_summary"]].copy() if "activityId" in df.columns else df[["natural_summary"]].copy()
        out.to_csv(save_summaries, index=False)
        print(f"\nSaved summaries to {save_summaries}")


if __name__ == "__main__":
    app()
