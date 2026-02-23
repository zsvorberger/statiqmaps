"""Precompute heatmap polylines + metadata for each activity."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import polyline

from services.bike_utils import bike_label, load_gear_lookup
from services.yearly_stats import parse_activity_dt
from user_data_pullers.activity_cache import load_activities_cached
from utils.file_lock import file_lock

HEATMAP_CACHE_DIR = Path("users_data/heatmap_cache")


def _scoped_cache_name(app_user_id: str, user_id: str) -> str:
    return f"{app_user_id}__{user_id}"


def _cache_path(user_id: str, app_user_id: str) -> Path:
    return HEATMAP_CACHE_DIR / f"{_scoped_cache_name(app_user_id, user_id)}.json"


def build_heatmap_segments(user_id: str, app_user_id: str) -> List[Dict[str, Any]]:
    HEATMAP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(user_id, app_user_id)
    lock_path = path.with_name(f"{path.name}.lock")
    with file_lock(lock_path):
        activities = load_activities_cached(user_id) or []
        gear_lookup = load_gear_lookup(user_id)

        segments: List[Dict[str, Any]] = []
        for activity in activities:
            poly = (activity.get("map") or {}).get("summary_polyline")
            if not poly:
                continue
            try:
                coords = polyline.decode(poly)
            except Exception:
                continue
            dt = parse_activity_dt(activity)
            bike_id = activity.get("gear_id") or "unknown"
            segments.append(
                {
                    "coords": [[lat, lon] for lat, lon in coords],
                    "year": dt.year if dt else None,
                    "month": dt.month if dt else None,
                    "weekday_num": dt.weekday() if dt else None,
                    "bike_id": bike_id,
                    "bike_name": bike_label(gear_lookup, bike_id),
                }
            )

        with path.open("w", encoding="utf-8") as f:
            json.dump(segments, f)
        return segments


def load_heatmap_segments(user_id: str, app_user_id: str) -> List[Dict[str, Any]] | None:
    path = _cache_path(user_id, app_user_id)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None
