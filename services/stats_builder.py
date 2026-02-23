"""Centralized stats builder for Strava Stat Tracker.

This module loads all activities for a user, computes every statistic required
by the dashboard (yearly, monthly, lifetime, personal bests, unique miles,
graphs, etc.), and writes them to a cache file. Views consume this cache
instead of recomputing from scratch per request.
"""

from __future__ import annotations

import json
import os
from contextlib import suppress
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import math

import pandas as pd
import numpy as np

from user_data_pullers.activity_cache import load_activities_cached
from user_data_pullers.stats_helpers import aggregate_better_stats
from services.yearly_stats import (
    build_yearly_detail,
    build_yearly_review_payload,
    normalize_yearly_breakdown,
    parse_activity_dt,
)
from services.bike_utils import load_gear_lookup
from services.personal_bests import build_personal_best_sections
from services.summary_builder import build_summary_payload, predefined_ranges
from user_data_pullers.resolver import atomic_write_json
from utils.file_lock import file_lock

STATS_CACHE_DIR = Path("users_data") / "stats_cache"


def _scoped_cache_name(app_user_id: str, user_id: str) -> str:
    return f"{app_user_id}__{user_id}"


def _cache_path(app_user_id: str, user_id: str) -> Path:
    return STATS_CACHE_DIR / f"{_scoped_cache_name(app_user_id, user_id)}.json"

def _sanitize_for_json(value: Any) -> Any:
    """Recursively convert pandas/numpy/datetime structures into JSON-friendly objects."""
    if value is None:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()
    if isinstance(value, (pd.Series, pd.Index)):
        return [_sanitize_for_json(v) for v in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    if value is pd.NA:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, dict):
        sanitized = {}
        for key, val in value.items():
            if isinstance(key, tuple):
                safe_key = "_".join(str(part) for part in key)
            elif isinstance(key, (str, int, float, bool)) or key is None:
                safe_key = key
            else:
                safe_key = str(key)
            sanitized[safe_key] = _sanitize_for_json(val)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [_sanitize_for_json(v) for v in value]
    if isinstance(value, set):
        return [_sanitize_for_json(v) for v in sorted(value, key=lambda x: str(x))]
    return value


@dataclass
class YearlyStats:
    year: int
    distance_km: float
    elev_m: float
    moving_hours: float
    ride_count: int
    avg_speed_kmh: float


@dataclass
class LifetimeStats:
    total_distance_km: float
    total_elev_m: float
    total_rides: int
    total_hours: float
    first_ride: str | None
    last_ride: str | None


@dataclass
class PersonalBest:
    longest_ride_km: float
    biggest_climb_m: float
    highest_speed_kmh: float
    max_moving_time_h: float


def ensure_cache_dir():
    STATS_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_dataframe(user_id: str, activities: List[Dict[str, Any]] | None = None) -> pd.DataFrame:
    if activities is None:
        activities = load_activities_cached(user_id)
    df = pd.json_normalize(activities)
    if df.empty:
        return df
    start_series = None
    if "start_date_local" in df.columns:
        start_series = df["start_date_local"]
    elif "start_date" in df.columns:
        start_series = df["start_date"]
    df["start_dt"] = pd.to_datetime(start_series, errors="coerce")
    df["date"] = df["start_dt"].dt.date
    df["year"] = df["start_dt"].dt.year
    df["month"] = df["start_dt"].dt.to_period("M").astype(str)
    if "distance" in df.columns:
        df["distance_km"] = df["distance"].fillna(0) / 1000.0
    else:
        df["distance_km"] = 0.0
    if "total_elevation_gain" in df.columns:
        df["elev_m"] = df["total_elevation_gain"].fillna(0)
    else:
        df["elev_m"] = 0.0
    if "moving_time" in df.columns:
        df["hours"] = df["moving_time"].fillna(0) / 3600.0
    else:
        df["hours"] = 0.0
    if "average_speed" in df.columns:
        df["avg_speed_kmh"] = df["average_speed"].fillna(0) * 3.6
    else:
        df["avg_speed_kmh"] = 0.0
    return df


def compute_yearly(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df.empty:
        return []
    grouped = df.groupby("year").agg(
        distance_km=("distance_km", "sum"),
        elev_m=("elev_m", "sum"),
        moving_hours=("hours", "sum"),
        ride_count=("id", "count"),
        avg_speed_kmh=("avg_speed_kmh", "mean"),
    )
    grouped = grouped.reset_index().sort_values("year")
    return grouped.to_dict("records")


def compute_lifetime(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return asdict(
            LifetimeStats(0.0, 0.0, 0, 0.0, None, None)
        )
    stats = LifetimeStats(
        total_distance_km=float(df["distance_km"].sum()),
        total_elev_m=float(df["elev_m"].sum()),
        total_rides=int(len(df)),
        total_hours=float(df["hours"].sum()),
        first_ride=df["start_dt"].min().isoformat() if df["start_dt"].notna().any() else None,
        last_ride=df["start_dt"].max().isoformat() if df["start_dt"].notna().any() else None,
    )
    return asdict(stats)


def compute_personal_bests(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return asdict(PersonalBest(0.0, 0.0, 0.0, 0.0))
    pb = PersonalBest(
        longest_ride_km=float(df["distance_km"].max()),
        biggest_climb_m=float(df["elev_m"].max()),
        highest_speed_kmh=float(df["max_speed"].fillna(df["average_speed"]).max() * 3.6)
        if "max_speed" in df else float(df["average_speed"].max() * 3.6),
        max_moving_time_h=float(df["hours"].max()),
    )
    return asdict(pb)


def compute_graph_series(df: pd.DataFrame) -> Dict[str, Any]:
    series = {}
    if df.empty:
        return series
    for grouping in ["daily", "weekly", "monthly", "yearly"]:
        grouped = group_for_graphs(df, grouping)
        series[grouping] = grouped
    return series


def group_for_graphs(df: pd.DataFrame, grouping: str) -> Dict[str, Any]:
    metrics = ["distance_km", "elev_m", "hours", "avg_speed_kmh"]
    dates = {
        "daily": df.groupby("date"),
        "weekly": df.groupby(df["start_dt"].dt.to_period("W").astype(str)),
        "monthly": df.groupby("month"),
        "yearly": df.groupby("year"),
    }
    grouped = dates[grouping]
    records = {}
    for metric in metrics:
        agg = grouped[metric].sum().reset_index()
        first_col = agg.columns[0] if not agg.empty else None
        if first_col:
            agg[first_col] = agg[first_col].apply(
                lambda val: val.isoformat() if hasattr(val, "isoformat") else val
            )
        records[metric] = agg.to_dict("records")
    return records


def compute_year_details(
    yearly_breakdown: Dict[str, Any], activities: List[Dict[str, Any]]
) -> Dict[str, Any]:
    normalized = normalize_yearly_breakdown(yearly_breakdown or {})
    years = set(normalized.keys())
    for act in activities:
        dt = parse_activity_dt(act)
        if dt:
            years.add(int(dt.year))
    details = {}
    for year in sorted(years):
        detail = build_yearly_detail(year, yearly_breakdown, activities)
        if detail:
            details[str(year)] = detail
    return details


def compute_summary_ranges(activities: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    today = datetime.utcnow().date()
    ranges = predefined_ranges(today)
    summaries = {"imperial": {}, "metric": {}}
    for key, bounds in ranges.items():
        start = bounds["start"]
        end = bounds["end"]
        for units in ("imperial", "metric"):
            summaries[units][key] = build_summary_payload(
                activities,
                units,
                start,
                end,
                since_label=key,
                custom_date_str=None,
            )
    return summaries


def build_stats_bundle(user_id: str, app_user_id: str) -> Dict[str, Any]:
    ensure_cache_dir()
    cache_path = _cache_path(str(app_user_id), str(user_id))
    lock_path = cache_path.with_name(f"{cache_path.name}.lock")
    with file_lock(lock_path):
        stats_dict, yearly_breakdown = aggregate_better_stats(user_id)
        activities = load_activities_cached(user_id)
        df = load_dataframe(user_id, activities)
        yearly_overview = build_yearly_review_payload(yearly_breakdown, activities)
        yearly_details = compute_year_details(yearly_breakdown, activities)
        gear_lookup = load_gear_lookup(user_id)
        personal_best_sections = {
            "with_virtual": build_personal_best_sections(
                activities, gear_lookup, include_virtual=True
            ),
            "no_virtual": build_personal_best_sections(
                activities, gear_lookup, include_virtual=False
            ),
        }
        summary_ranges = compute_summary_ranges(activities)
        bundle = {
            "user_id": user_id,
            "generated_at": datetime.utcnow().isoformat(),
            "activity_count": len(activities),
            "stats": stats_dict,
            "yearly_breakdown": yearly_breakdown,
            "yearly_overview": yearly_overview,
            "year_details": yearly_details,
            "lifetime": compute_lifetime(df),
            "yearly": compute_yearly(df),
            "personal_bests": compute_personal_bests(df),
            "personal_best_sections": personal_best_sections,
            "graph_series": compute_graph_series(df),
            "summary_ranges": summary_ranges,
        }
        sanitized = _sanitize_for_json(bundle)
        atomic_write_json(cache_path, sanitized, indent=2)
        return bundle


def load_stats_bundle(user_id: str, app_user_id: str) -> Dict[str, Any] | None:
    ensure_cache_dir()
    cache_path = _cache_path(str(app_user_id), str(user_id))
    if not cache_path.exists():
        return None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        try:
            os.remove(cache_path)
        except OSError:
            pass
        return None
