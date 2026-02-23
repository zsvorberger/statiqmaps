"""Precomputed summary builder for standard date ranges."""

from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional


def _act_date(activity: Dict[str, Any]) -> Optional[datetime.date]:
    dt_str = activity.get("start_date_local") or activity.get("start_date")
    if not dt_str:
        return None
    try:
        return datetime.datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").date()
    except Exception:
        try:
            return datetime.datetime.fromisoformat(dt_str).date()
        except Exception:
            return None


def _format_streak_label(start_dt: Optional[datetime.date], end_dt: Optional[datetime.date]) -> str:
    if not start_dt or not end_dt:
        return ""
    if start_dt == end_dt:
        return start_dt.strftime("%b %d, %Y")
    if start_dt.year == end_dt.year:
        return f"{start_dt.strftime('%b %d')} - {end_dt.strftime('%b %d, %Y')}"
    return f"{start_dt.strftime('%b %d, %Y')} - {end_dt.strftime('%b %d, %Y')}"


def _serialize_streak(streak: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not streak or not streak.get("length"):
        return None
    return {
        "length": streak["length"],
        "label": _format_streak_label(streak.get("start"), streak.get("end")),
    }


def _build_streaks(unique_days: List[datetime.date]):
    if not unique_days:
        return {"current": None, "longest": None}
    sorted_days = sorted(unique_days)
    longest = {"length": 0, "start": None, "end": None}
    current = {"length": 0, "start": None, "end": None}
    prev_day = None
    streak_start = None
    streak_len = 0
    for day in sorted_days:
        if prev_day and (day - prev_day).days == 1:
            streak_len += 1
        else:
            streak_len = 1
            streak_start = day
        if streak_len > longest["length"]:
            longest = {"length": streak_len, "start": streak_start, "end": day}
        prev_day = day
    if streak_len:
        current = {"length": streak_len, "start": streak_start, "end": prev_day}
    return {"current": current, "longest": longest}


def build_summary_payload(
    activities: List[Dict[str, Any]],
    units: str,
    start_date: Optional[datetime.date],
    end_date: datetime.date,
    since_label: str,
    custom_date_str: Optional[str] = None,
) -> Dict[str, Any]:
    units = (units or "imperial").lower()
    filtered = []
    for activity in activities:
        dt_val = _act_date(activity)
        if not dt_val:
            continue
        if start_date and dt_val < start_date:
            continue
        if dt_val > end_date:
            continue
        filtered.append(activity)

    total_distance_km = 0.0
    total_elev_m = 0.0
    total_time_hr = 0.0
    activity_count = len(filtered)
    distance_by_day = defaultdict(float)
    weekly_totals = {}
    avg_speed_samples = []
    cadence_samples = []
    power_samples = []
    indoor_distance_km = 0.0
    outdoor_distance_km = 0.0
    indoor_sessions = 0
    outdoor_sessions = 0
    hottest_c = None
    highest_points = []
    ride_dates = []
    longest_activity = None
    longest_km = 0.0
    max_gain_m = 0.0

    for activity in filtered:
        dist_km = (activity.get("distance", 0.0) or 0.0) / 1000.0
        elev_m = activity.get("total_elevation_gain", 0.0) or 0.0
        moving_hr = (activity.get("moving_time", 0) or 0) / 3600.0
        total_distance_km += dist_km
        total_elev_m += elev_m
        total_time_hr += moving_hr
        if dist_km > longest_km:
            longest_km = dist_km
            longest_activity = activity
        if elev_m > max_gain_m:
            max_gain_m = elev_m
        avg_temp = activity.get("average_temp")
        if isinstance(avg_temp, (int, float)):
            hottest_c = avg_temp if hottest_c is None else max(hottest_c, avg_temp)
        highest_points.append(
            activity.get("elev_high")
            or activity.get("elevation_high")
            or activity.get("elevHighest")
            or 0
        )
        dt_val = _act_date(activity)
        if dt_val:
            ride_dates.append(dt_val)
            distance_by_day[dt_val] += dist_km
            iso_year, iso_week, _ = dt_val.isocalendar()
            key = (iso_year, iso_week)
            bucket = weekly_totals.setdefault(
                key,
                {"distance": 0.0, "start": dt_val},
            )
            bucket["distance"] += dist_km
            if dt_val < bucket["start"]:
                bucket["start"] = dt_val
        avg_speed_val = activity.get("average_speed")
        if isinstance(avg_speed_val, (int, float)):
            avg_speed_samples.append(avg_speed_val * 3.6)  # km/h
        cadence_val = activity.get("average_cadence")
        if isinstance(cadence_val, (int, float)):
            cadence_samples.append(cadence_val)
        power_val = activity.get("average_watts")
        if isinstance(power_val, (int, float)):
            power_samples.append(power_val)
        is_indoor = bool(activity.get("trainer")) or (
            (activity.get("type") or "") in {"VirtualRide", "Trainer"}
        )
        if is_indoor:
            indoor_distance_km += dist_km
            indoor_sessions += 1
        else:
            outdoor_distance_km += dist_km
            outdoor_sessions += 1

    avg_speed_kmh = (total_distance_km / total_time_hr) if total_time_hr > 0 else 0.0
    top_speed_kmh = max(avg_speed_samples) if avg_speed_samples else 0.0
    highest_avg_elev_m = max(highest_points) if highest_points else 0.0

    earliest_date = min(ride_dates) if ride_dates else end_date
    span_start = start_date or earliest_date
    days_span = max(0, (end_date - span_start).days)
    total_days = days_span + 1
    weeks = max(1, days_span // 7)
    avg_hours_per_week = total_time_hr / weeks if weeks else 0.0
    rides_per_week = activity_count / weeks if weeks else 0.0
    distance_per_week_km = total_distance_km / weeks if weeks else 0.0
    unique_days = sorted(set(ride_dates))
    rest_days = max(0, total_days - len(unique_days))
    avg_distance_active_day_km = (
        (total_distance_km / len(unique_days)) if unique_days else 0.0
    )

    def longest_streak_stats():
        streaks = _build_streaks(unique_days)
        return {
            "current": _serialize_streak(streaks["current"]),
            "longest": _serialize_streak(streaks["longest"]),
        }

    longest = longest_streak_stats()

    def best_day_stats():
        if not distance_by_day:
            return 0.0, ""
        best_day, best_value = max(distance_by_day.items(), key=lambda item: item[1])
        return best_value, best_day.strftime("%b %d, %Y")

    best_day_distance_km, best_day_label = best_day_stats()

    weekly_series = sorted(
        weekly_totals.items(), key=lambda item: (item[0][0], item[0][1])
    )
    if len(weekly_series) > 12:
        weekly_series = weekly_series[-12:]
    weekly_chart_labels = [
        bucket["start"].strftime("Wk of %b %d") for _, bucket in weekly_series
    ]
    weekly_chart_values_km = [bucket["distance"] for _, bucket in weekly_series]
    best_week_distance_km = 0.0
    best_week_label = ""
    if weekly_series:
        best_week = max(weekly_series, key=lambda item: item[1]["distance"])
        best_week_distance_km = best_week[1]["distance"]
        best_week_label = best_week[1]["start"].strftime("Week of %b %d")

    avg_cadence_val = (
        sum(cadence_samples) / len(cadence_samples) if cadence_samples else None
    )
    avg_power_val = sum(power_samples) / len(power_samples) if power_samples else None

    longest_date = _act_date(longest_activity) if longest_activity else None
    indoor_pct = (
        round(indoor_distance_km / total_distance_km * 100, 1)
        if total_distance_km
        else None
    )

    def convert_distance(val_km):
        if units == "imperial":
            return round(val_km * 0.621371, 2)
        return round(val_km, 2)

    def convert_elev(val_m):
        if units == "imperial":
            return int(val_m * 3.28084)
        return int(val_m)

    def convert_speed(val_kmh):
        if units == "imperial":
            return round(val_kmh * 0.621371, 2)
        return round(val_kmh, 2)

    total_distance = convert_distance(total_distance_km)
    longest_ride = convert_distance(longest_km)
    total_elevation = convert_elev(total_elev_m)
    max_elevation = convert_elev(max_gain_m)
    highest_avg_elev = convert_elev(highest_avg_elev_m)
    avg_speed = convert_speed(avg_speed_kmh)
    top_speed = convert_speed(top_speed_kmh)
    distance_per_week = convert_distance(distance_per_week_km)
    indoor_distance = convert_distance(indoor_distance_km)
    outdoor_distance = convert_distance(outdoor_distance_km)
    best_day_distance = convert_distance(best_day_distance_km)
    avg_distance_active_day = convert_distance(avg_distance_active_day_km)
    best_week_distance = convert_distance(best_week_distance_km)
    weekly_chart_values = [convert_distance(val) for val in weekly_chart_values_km]
    hottest_value = (
        round(hottest_c * 9 / 5 + 32, 1) if hottest_c is not None and units == "imperial" else
        (round(hottest_c, 1) if hottest_c is not None else None)
    )

    weekly_chart = {
        "labels": weekly_chart_labels,
        "values": weekly_chart_values,
    }

    summary_title = _build_summary_title(since_label, custom_date_str, start_date, end_date)

    return {
        "units": units,
        "since": since_label,
        "custom_date_str": custom_date_str or "",
        "end_date_str": end_date.strftime("%Y-%m-%d"),
        "title": summary_title,
        "total_distance": total_distance,
        "total_elevation": total_elevation,
        "total_time": round(total_time_hr, 2),
        "activity_count": activity_count,
        "active_days": len(unique_days),
        "rest_days": rest_days,
        "avg_speed": avg_speed,
        "top_speed": top_speed,
        "longest_ride": longest_ride,
        "longest_ride_label": _format_streak_label(longest_date, longest_date)
        if longest_date
        else "",
        "hottest_ride": hottest_value,
        "max_elevation": max_elevation,
        "highest_avg_elev": highest_avg_elev,
        "avg_hours_per_week": round(avg_hours_per_week, 2),
        "rides_per_week": round(rides_per_week, 2),
        "distance_per_week": distance_per_week,
        "indoor_pct": indoor_pct,
        "indoor_distance": indoor_distance,
        "outdoor_distance": outdoor_distance,
        "indoor_sessions": indoor_sessions,
        "outdoor_sessions": outdoor_sessions,
        "avg_cadence": round(avg_cadence_val, 1)
        if avg_cadence_val is not None
        else None,
        "avg_power": round(avg_power_val, 1) if avg_power_val is not None else None,
        "best_day_distance": best_day_distance,
        "best_day_label": best_day_label,
        "best_week_distance": best_week_distance,
        "best_week_label": best_week_label,
        "avg_distance_active_day": avg_distance_active_day,
        "weekly_chart": weekly_chart,
        "current_streak": longest["current"],
        "longest_streak": longest["longest"],
        "start_date_str": start_date.strftime("%Y-%m-%d") if start_date else None,
    }


def _build_summary_title(
    since_label: str,
    custom_date_str: Optional[str],
    start_date: Optional[datetime.date],
    end_date: datetime.date,
) -> str:
    if since_label == "custom" and custom_date_str:
        start = custom_date_str
    elif start_date:
        start = start_date.strftime("%Y-%m-%d")
    else:
        start = "All time"
    end = end_date.strftime("%Y-%m-%d")
    if since_label == "custom":
        return f"{start} to {end} Summary"
    titles = {
        "all": "All-Time Summary",
        "week": "1 Week Summary",
        "month": "1 Month Summary",
        "3months": "3 Month Summary",
        "year": "1 Year Summary",
        "3years": "3 Year Summary",
        "5years": "5 Year Summary",
        "10years": "10 Year Summary",
    }
    return titles.get(since_label, "Summary")


def predefined_ranges(today: datetime.date) -> Dict[str, Dict[str, Optional[datetime.date]]]:
    mapping = {
        "all": {"start": None},
        "week": {"start": today - datetime.timedelta(days=7)},
        "month": {"start": today - datetime.timedelta(days=30)},
        "3months": {"start": today - datetime.timedelta(days=90)},
        "year": {"start": today - datetime.timedelta(days=365)},
        "3years": {"start": today - datetime.timedelta(days=3 * 365)},
        "5years": {"start": today - datetime.timedelta(days=5 * 365)},
        "10years": {"start": today - datetime.timedelta(days=10 * 365)},
    }
    return {k: {"start": v["start"], "end": today} for k, v in mapping.items()}

