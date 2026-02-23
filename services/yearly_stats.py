"""Shared builders for yearly review payloads and detail views.

These helpers take the normalized yearly breakdown returned by
`aggregate_better_stats` plus the full activity list and construct the rich
summaries used across the app (overview cards, deep-dive detail pages, etc.).
They live in their own module so that both the Flask app and the offline stats
builder can reuse the exact same logic without circular imports.
"""

from __future__ import annotations

import calendar
import datetime
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional


METERS_TO_MILES = 0.000621371
METERS_TO_KM = 0.001
FEET_PER_METER = 3.28084
MPH_PER_MPS = 2.236936
KMH_PER_MPS = 3.6


def parse_activity_dt(activity: Dict[str, Any]) -> Optional[datetime.datetime]:
    """Parse the start date on an activity (local preferred, fallback to UTC)."""
    raw = activity.get("start_date_local") or activity.get("start_date")
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(raw)
    except Exception:
        try:
            return datetime.datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def _format_week_range(start: datetime.date, end: datetime.date) -> str:
    if not start or not end:
        return ""
    if start.year == end.year:
        return f"{start.strftime('%b %d')} – {end.strftime('%b %d')}"
    return f"{start.strftime('%b %d, %Y')} – {end.strftime('%b %d, %Y')}"


def _best_seven_day_window(rides: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    rides = list(rides or [])
    if not rides:
        return None
    sorted_rides = sorted(rides, key=lambda r: r["dt"])
    left = 0
    dist = 0.0
    elev = 0.0
    best = None
    for right, ride in enumerate(sorted_rides):
        dist += ride["distance"]
        elev += ride["elev"]
        cutoff = ride["dt"].date() - datetime.timedelta(days=6)
        while sorted_rides[left]["dt"].date() < cutoff:
            dist -= sorted_rides[left]["distance"]
            elev -= sorted_rides[left]["elev"]
            left += 1
        current = {
            "distance": dist,
            "elevation": elev,
            "start": sorted_rides[left]["dt"].date(),
            "end": ride["dt"].date(),
            "rides": right - left + 1,
        }
        if not best or current["distance"] > best["distance"]:
            best = current
    return best


def _best_month_total(rides: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    monthly = defaultdict(float)
    for ride in rides:
        monthly[ride["dt"].month] += ride["distance"]
    if not monthly:
        return None
    month, dist = max(monthly.items(), key=lambda item: item[1])
    return {"month": month, "distance": dist}


def normalize_yearly_breakdown(yearly_breakdown: Any) -> Dict[int, Dict[str, Any]]:
    normalized: Dict[int, Dict[str, Any]] = {}
    if isinstance(yearly_breakdown, dict):
        items = yearly_breakdown.items()
    elif isinstance(yearly_breakdown, list):
        items = ((row.get("year"), row) for row in yearly_breakdown)
    else:
        items = []
    for key, val in items:
        if not val:
            continue
        year_key = key if key is not None else val.get("year")
        try:
            year = int(year_key)
        except (TypeError, ValueError):
            continue
        normalized[year] = val
    return normalized


def build_yearly_review_payload(
    yearly_breakdown: Any, activities: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    breakdown = normalize_yearly_breakdown(yearly_breakdown or {})
    rides_by_year: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for activity in activities:
        sport = activity.get("type") or activity.get("sport_type")
        if sport not in {"Ride", "VirtualRide"}:
            continue
        dt_obj = parse_activity_dt(activity)
        if not dt_obj:
            continue
        rides_by_year[dt_obj.year].append(
            {
                "dt": dt_obj,
                "distance": float(activity.get("distance") or 0.0),
                "elev": float(activity.get("total_elevation_gain") or 0.0),
                "name": activity.get("name") or "Untitled Ride",
            }
        )

    all_years = sorted(set(breakdown.keys()) | set(rides_by_year.keys()), reverse=True)
    summaries: List[Dict[str, Any]] = []
    for year in all_years:
        totals = breakdown.get(year, {})
        total_distance = float(totals.get("total_distance") or 0.0)
        total_elev = float(totals.get("total_elevation") or 0.0)
        total_moving = float(totals.get("total_moving_time") or 0.0)
        ride_count = int(totals.get("ride_count") or 0)
        rides = sorted(rides_by_year.get(year, []), key=lambda r: r["dt"])
        avg_ride_mi = (total_distance * METERS_TO_MILES / ride_count) if ride_count else 0.0

        summary = {
            "year": year,
            "distance_mi": round(total_distance * METERS_TO_MILES, 1),
            "distance_km": round(total_distance * METERS_TO_KM, 1),
            "climb_ft": int(round(total_elev * FEET_PER_METER)),
            "climb_m": int(round(total_elev)),
            "moving_hours": round(total_moving / 3600.0, 1),
            "rides": ride_count,
            "avg_ride_mi": round(avg_ride_mi, 1),
        }

        best_week = _best_seven_day_window(rides)
        if best_week:
            summary["best_week"] = {
                "label": _format_week_range(best_week["start"], best_week["end"]),
                "distance_mi": round(best_week["distance"] * METERS_TO_MILES, 1),
                "distance_km": round(best_week["distance"] * METERS_TO_KM, 1),
                "rides": best_week["rides"],
            }

        best_month = _best_month_total(rides)
        if best_month:
            summary["best_month"] = {
                "label": calendar.month_name[best_month["month"]],
                "distance_mi": round(best_month["distance"] * METERS_TO_MILES, 1),
                "distance_km": round(best_month["distance"] * METERS_TO_KM, 1),
            }

        longest = max(rides, key=lambda r: r["distance"], default=None)
        if longest and longest["distance"] > 0:
            summary["longest_ride"] = {
                "name": longest["name"],
                "date": longest["dt"].strftime("%b %d, %Y"),
                "distance_mi": round(longest["distance"] * METERS_TO_MILES, 1),
                "distance_km": round(longest["distance"] * METERS_TO_KM, 1),
            }

        climbiest = max(rides, key=lambda r: r["elev"], default=None)
        if climbiest and climbiest["elev"] > 0:
            summary["biggest_climb"] = {
                "name": climbiest["name"],
                "date": climbiest["dt"].strftime("%b %d, %Y"),
                "elev_ft": int(round(climbiest["elev"] * FEET_PER_METER)),
                "elev_m": int(round(climbiest["elev"])),
            }

        summaries.append(summary)

    return summaries


def _fmt_duration(seconds: float) -> str:
    total = int(seconds or 0)
    hours = total // 3600
    minutes = (total % 3600) // 60
    if hours and minutes:
        return f"{hours}h {minutes}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


def build_yearly_detail(
    year: int, yearly_breakdown: Any, activities: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    breakdown = normalize_yearly_breakdown(yearly_breakdown or {})
    totals = breakdown.get(year, {})
    rides = []
    for activity in activities:
        sport = activity.get("type") or activity.get("sport_type")
        if sport not in {"Ride", "VirtualRide"}:
            continue
        dt_obj = parse_activity_dt(activity)
        if not dt_obj or dt_obj.year != year:
            continue
        distance = float(activity.get("distance") or 0.0)
        elev = float(activity.get("total_elevation_gain") or 0.0)
        moving = float(activity.get("moving_time") or 0.0)
        ride = {
            "dt": dt_obj,
            "name": activity.get("name") or "Untitled Ride",
            "distance": distance,
            "elev": elev,
            "moving": moving,
            "elapsed": float(activity.get("elapsed_time") or 0.0),
            "avg_speed": float(activity.get("average_speed") or 0.0),
            "is_virtual": sport == "VirtualRide" or bool(activity.get("trainer")),
        }
        ride["distance_mi"] = distance * METERS_TO_MILES
        ride["distance_km"] = distance * METERS_TO_KM
        ride["elev_ft"] = int(round(elev * FEET_PER_METER))
        ride["elev_m"] = int(round(elev))
        ride["moving_hours"] = moving / 3600.0 if moving else 0.0
        ride["avg_speed_mph"] = ride["avg_speed"] * MPH_PER_MPS
        ride["avg_speed_kmh"] = ride["avg_speed"] * KMH_PER_MPS
        rides.append(ride)

    if not totals and not rides:
        return None

    total_distance = float(totals.get("total_distance") or sum(r["distance"] for r in rides))
    total_elev = float(totals.get("total_elevation") or sum(r["elev"] for r in rides))
    total_moving = float(totals.get("total_moving_time") or sum(r["moving"] for r in rides))
    ride_count = int(totals.get("ride_count") or len(rides))

    avg_distance = (total_distance * METERS_TO_MILES / ride_count) if ride_count else 0.0
    avg_time = (total_moving / ride_count) if ride_count else 0.0
    avg_speed_mph = (
        (total_distance * METERS_TO_MILES) / (total_moving / 3600.0)
        if total_moving > 0
        else 0.0
    )
    avg_speed_kmh = avg_speed_mph * 1.60934
    avg_climb_ft = (
        int(round((total_elev * FEET_PER_METER) / max(ride_count, 1)))
        if ride_count
        else 0
    )
    avg_climb_m = int(round(total_elev / max(ride_count, 1))) if ride_count else 0

    monthly_totals = [
        {"month": m, "distance": 0.0, "elev": 0.0, "moving": 0.0, "rides": 0} for m in range(1, 13)
    ]
    weekly_totals: Dict[tuple, Dict[str, Any]] = {}
    weekday_totals = [
        {
            "idx": idx,
            "label": calendar.day_name[idx],
            "abbr": calendar.day_abbr[idx],
            "rides": 0,
            "distance": 0.0,
            "moving": 0.0,
        }
        for idx in range(7)
    ]
    bucket_defs = [
        {"label": "<20 mi", "min_mi": 0.0, "max_mi": 20.0},
        {"label": "20-40 mi", "min_mi": 20.0, "max_mi": 40.0},
        {"label": "40-60 mi", "min_mi": 40.0, "max_mi": 60.0},
        {"label": "60+ mi", "min_mi": 60.0, "max_mi": None},
    ]
    distance_buckets = [
        {**bucket, "rides": 0, "distance": 0.0, "moving": 0.0} for bucket in bucket_defs
    ]
    ride_mix = {
        "indoor": {"rides": 0, "distance": 0.0},
        "outdoor": {"rides": 0, "distance": 0.0},
    }
    time_of_day_buckets = [
        {"label": "Early (0-6)", "start": 0, "end": 6, "rides": 0, "distance": 0.0, "moving": 0.0},
        {"label": "Morning (6-12)", "start": 6, "end": 12, "rides": 0, "distance": 0.0, "moving": 0.0},
        {"label": "Afternoon (12-18)", "start": 12, "end": 18, "rides": 0, "distance": 0.0, "moving": 0.0},
        {"label": "Evening (18-22)", "start": 18, "end": 22, "rides": 0, "distance": 0.0, "moving": 0.0},
        {"label": "Night (22-24)", "start": 22, "end": 24, "rides": 0, "distance": 0.0, "moving": 0.0},
    ]

    for ride in rides:
        idx = ride["dt"].month - 1
        monthly_totals[idx]["distance"] += ride["distance"]
        monthly_totals[idx]["elev"] += ride["elev"]
        monthly_totals[idx]["moving"] += ride["moving"]
        monthly_totals[idx]["rides"] += 1

        iso = ride["dt"].isocalendar()
        key = (iso[0], iso[1])
        bucket = weekly_totals.setdefault(
            key,
            {"distance": 0.0, "elev": 0.0, "moving": 0.0, "start": ride["dt"]},
        )
        bucket["distance"] += ride["distance"]
        bucket["elev"] += ride["elev"]
        bucket["moving"] += ride["moving"]
        if ride["dt"] < bucket["start"]:
            bucket["start"] = ride["dt"]

        weekday_bucket = weekday_totals[ride["dt"].weekday()]
        weekday_bucket["rides"] += 1
        weekday_bucket["distance"] += ride["distance"]
        weekday_bucket["moving"] += ride["moving"]

        ride_miles = ride["distance_mi"]
        for bucket in distance_buckets:
            min_mi = bucket["min_mi"] or 0.0
            max_mi = bucket["max_mi"]
            within_lower = ride_miles >= min_mi
            within_upper = True if max_mi is None else ride_miles < max_mi
            if within_lower and within_upper:
                bucket["rides"] += 1
                bucket["distance"] += ride["distance"]
                bucket["moving"] += ride["moving"]
                break

        mix_key = "indoor" if ride["is_virtual"] else "outdoor"
        ride_mix[mix_key]["rides"] += 1
        ride_mix[mix_key]["distance"] += ride["distance"]

        hour = ride["dt"].hour
        for bucket in time_of_day_buckets:
            if bucket["start"] <= hour < bucket["end"]:
                bucket["rides"] += 1
                bucket["distance"] += ride["distance"]
                bucket["moving"] += ride["moving"]
                break

    ride_dates = sorted({ride["dt"].date() for ride in rides})

    def best_month(metric):
        if not monthly_totals:
            return None
        entry = max(monthly_totals, key=lambda row: row[metric])
        if entry[metric] <= 0:
            return None
        return {
            "month": entry["month"],
            "label": calendar.month_name[entry["month"]],
            "distance_mi": round(entry["distance"] * METERS_TO_MILES, 1),
            "distance_km": round(entry["distance"] * METERS_TO_KM, 1),
            "elev_ft": int(round(entry["elev"] * FEET_PER_METER)),
            "elev_m": int(round(entry["elev"])),
            "moving_hours": round(entry["moving"] / 3600.0, 1),
        }

    longest_distance = max(rides, key=lambda r: r["distance"]) if rides else None
    longest_time = max(rides, key=lambda r: r["moving"]) if rides else None
    biggest_climb = max(rides, key=lambda r: r["elev"]) if rides else None

    def ride_snapshot(ride):
        if not ride:
            return None
        return {
            "name": ride["name"],
            "date": ride["dt"].strftime("%b %d"),
            "distance_mi": round(ride["distance"] * METERS_TO_MILES, 1),
            "distance_km": round(ride["distance"] * METERS_TO_KM, 1),
            "moving_time": _fmt_duration(ride["moving"]),
            "elapsed_time": _fmt_duration(ride["elapsed"]),
            "elev_ft": int(round(ride["elev"] * FEET_PER_METER)),
            "elev_m": int(round(ride["elev"])),
        }

    best_week = (
        _best_seven_day_window(
            [{"dt": ride["dt"], "distance": ride["distance"], "elev": ride["elev"]} for ride in rides]
        )
        if rides
        else None
    )

    def chart_ready_monthly():
        labels = [calendar.month_abbr[row["month"]] for row in monthly_totals]
        distance_mi = [round(row["distance"] * METERS_TO_MILES, 1) for row in monthly_totals]
        distance_km = [round(row["distance"] * METERS_TO_KM, 1) for row in monthly_totals]
        elev_ft = [int(round(row["elev"] * FEET_PER_METER)) for row in monthly_totals]
        elev_m = [int(round(row["elev"])) for row in monthly_totals]
        hours = [round(row["moving"] / 3600.0, 1) for row in monthly_totals]
        return {
            "labels": labels,
            "distance_mi": distance_mi,
            "distance_km": distance_km,
            "elev_ft": elev_ft,
            "elev_m": elev_m,
            "hours": hours,
        }

    def chart_ready_weekly():
        ordered = sorted(weekly_totals.items(), key=lambda item: item[1]["start"])
        labels = [item[1]["start"].strftime("Week %W (%b %d)") for item in ordered]
        distance_mi = [round(item[1]["distance"] * METERS_TO_MILES, 1) for item in ordered]
        distance_km = [round(item[1]["distance"] * METERS_TO_KM, 1) for item in ordered]
        elev_ft = [int(round(item[1]["elev"] * FEET_PER_METER)) for item in ordered]
        elev_m = [int(round(item[1]["elev"])) for item in ordered]
        hours = [round(item[1]["moving"] / 3600.0, 1) for item in ordered]
        return {
            "labels": labels,
            "distance_mi": distance_mi,
            "distance_km": distance_km,
            "elev_ft": elev_ft,
            "elev_m": elev_m,
            "hours": hours,
        }

    def serialize_top(rides_sorted):
        out = []
        for ride in rides_sorted:
            out.append(
                {
                    "name": ride["name"],
                    "date": ride["dt"].strftime("%b %d"),
                    "distance_mi": round(ride["distance"] * METERS_TO_MILES, 1),
                    "distance_km": round(ride["distance"] * METERS_TO_KM, 1),
                    "moving_time": _fmt_duration(ride["moving"]),
                    "elev_ft": int(round(ride["elev"] * FEET_PER_METER)),
                    "elev_m": int(round(ride["elev"])),
                }
            )
        return out

    def serialize_fastest(rides_sorted):
        payload = []
        for ride in rides_sorted:
            payload.append(
                {
                    "name": ride["name"],
                    "date": ride["dt"].strftime("%b %d"),
                    "avg_speed_mph": round(ride["avg_speed_mph"], 1),
                    "avg_speed_kmh": round(ride["avg_speed_kmh"], 1),
                    "distance_mi": round(ride["distance_mi"], 1),
                    "distance_km": round(ride["distance_km"], 1),
                    "moving_time": _fmt_duration(ride["moving"]),
                }
            )
        return payload

    def streak_snapshot(streak):
        if not streak or not streak.get("length"):
            return None
        start = streak.get("start")
        end = streak.get("end")
        if not start or not end:
            label = ""
        elif start == end:
            label = start.strftime("%b %d, %Y")
        elif start.year == end.year:
            label = f"{start.strftime('%b %d')} - {end.strftime('%b %d, %Y')}"
        else:
            label = f"{start.strftime('%b %d, %Y')} - {end.strftime('%b %d, %Y')}"
        return {"length": streak["length"], "label": label}

    def compute_longest_streak(dates):
        best = {"length": 0, "start": None, "end": None}
        current_start = None
        prev = None
        current_len = 0
        for day in dates:
            if prev and (day - prev).days == 1:
                current_len += 1
            else:
                current_len = 1
                current_start = day
            if current_len > best["length"]:
                best = {"length": current_len, "start": current_start, "end": day}
            prev = day
        return best

    def compute_current_streak(dates):
        if not dates:
            return {"length": 0, "start": None, "end": None}
        current_end = dates[-1]
        current_start = current_end
        length = 1
        for day in reversed(dates[:-1]):
            if (current_start - day).days == 1:
                length += 1
                current_start = day
            else:
                break
        return {"length": length, "start": current_start, "end": current_end}

    weekday_table = [
        {
            "label": entry["label"],
            "rides": entry["rides"],
            "distance_mi": round(entry["distance"] * METERS_TO_MILES, 1),
            "distance_km": round(entry["distance"] * METERS_TO_KM, 1),
            "moving_hours": round(entry["moving"] / 3600.0, 1),
            "avg_speed_mph": round(
                (entry["distance"] * METERS_TO_MILES) / (entry["moving"] / 3600.0)
                if entry["moving"] > 0
                else 0.0,
                1,
            ),
            "avg_speed_kmh": round(
                (entry["distance"] * METERS_TO_KM) / (entry["moving"] / 3600.0)
                if entry["moving"] > 0
                else 0.0,
                1,
            ),
        }
        for entry in weekday_totals
    ]
    bucket_table = [
        {
            "label": entry["label"],
            "rides": entry["rides"],
            "distance_mi": round(entry["distance"] * METERS_TO_MILES, 1),
            "distance_km": round(entry["distance"] * METERS_TO_KM, 1),
            "moving_hours": round(entry["moving"] / 3600.0, 1),
        }
        for entry in distance_buckets
    ]
    time_of_day_table = [
        {
            "label": entry["label"],
            "rides": entry["rides"],
            "distance_mi": round(entry["distance"] * METERS_TO_MILES, 1),
            "distance_km": round(entry["distance"] * METERS_TO_KM, 1),
            "moving_hours": round(entry["moving"] / 3600.0, 1),
        }
        for entry in time_of_day_buckets
    ]
    time_of_day_chart = {
        "labels": [entry["label"] for entry in time_of_day_buckets],
        "rides": [entry["rides"] for entry in time_of_day_buckets],
        "distance_mi": [round(entry["distance"] * METERS_TO_MILES, 1) for entry in time_of_day_buckets],
        "distance_km": [round(entry["distance"] * METERS_TO_KM, 1) for entry in time_of_day_buckets],
    }
    ride_mix_totals = {
        "labels": ["Outdoor", "Indoor"],
        "rides": [
            ride_mix["outdoor"]["rides"],
            ride_mix["indoor"]["rides"],
        ],
        "distance_mi": [
            round(ride_mix["outdoor"]["distance"] * METERS_TO_MILES, 1),
            round(ride_mix["indoor"]["distance"] * METERS_TO_MILES, 1),
        ],
        "distance_km": [
            round(ride_mix["outdoor"]["distance"] * METERS_TO_KM, 1),
            round(ride_mix["indoor"]["distance"] * METERS_TO_KM, 1),
        ],
    }
    ride_mix_totals["total_rides"] = sum(ride_mix_totals["rides"])
    ride_mix_totals["total_distance_mi"] = round(sum(ride_mix_totals["distance_mi"]), 1)
    ride_mix_totals["total_distance_km"] = round(sum(ride_mix_totals["distance_km"]), 1)

    quarters = []
    for idx in range(4):
        chunk = monthly_totals[idx * 3 : idx * 3 + 3]
        q_distance = sum(row["distance"] for row in chunk)
        q_elev = sum(row["elev"] for row in chunk)
        q_moving = sum(row["moving"] for row in chunk)
        q_rides = sum(row["rides"] for row in chunk)
        quarters.append(
            {
                "label": f"Q{idx + 1}",
                "distance_mi": round(q_distance * METERS_TO_MILES, 1),
                "distance_km": round(q_distance * METERS_TO_KM, 1),
                "climb_ft": int(round(q_elev * FEET_PER_METER)),
                "climb_m": int(round(q_elev)),
                "moving_hours": round(q_moving / 3600.0, 1),
                "rides": q_rides,
            }
        )

    first_half = monthly_totals[:6]
    second_half = monthly_totals[6:]

    def _half_summary(rows):
        distance = sum(row["distance"] for row in rows)
        elev = sum(row["elev"] for row in rows)
        moving = sum(row["moving"] for row in rows)
        rides = sum(row["rides"] for row in rows)
        return {
            "distance_mi": round(distance * METERS_TO_MILES, 1),
            "distance_km": round(distance * METERS_TO_KM, 1),
            "climb_ft": int(round(elev * FEET_PER_METER)),
            "climb_m": int(round(elev)),
            "moving_hours": round(moving / 3600.0, 1),
            "rides": rides,
        }

    half_split = {
        "first": _half_summary(first_half),
        "second": _half_summary(second_half),
    }
    half_split["delta_distance_mi"] = round(
        half_split["second"]["distance_mi"] - half_split["first"]["distance_mi"], 1
    )
    half_split["delta_distance_km"] = round(
        half_split["second"]["distance_km"] - half_split["first"]["distance_km"], 1
    )
    half_split["trend"] = (
        "up"
        if half_split["delta_distance_mi"] > 1.0
        else "down"
        if half_split["delta_distance_mi"] < -1.0
        else "flat"
    )

    def compute_longest_break(dates):
        if len(dates) < 2:
            return None
        best_gap = 0
        best_pair = None
        prev = dates[0]
        for date in dates[1:]:
            gap = (date - prev).days - 1
            if gap > best_gap:
                best_gap = gap
                best_pair = (prev, date)
            prev = date
        if not best_pair or best_gap <= 0:
            return None
        start, end = best_pair
        if start.year == end.year:
            label = f"{start.strftime('%b %d')} - {end.strftime('%b %d, %Y')}"
        else:
            label = f"{start.strftime('%b %d, %Y')} - {end.strftime('%b %d, %Y')}"
        return {"days": best_gap, "label": label}

    active_weeks_count = len(weekly_totals)
    active_weeks_for_avg = active_weeks_count or 1
    consistency = {
        "weekly_avg_distance_mi": round(
            (total_distance * METERS_TO_MILES) / active_weeks_for_avg, 1
        ),
        "weekly_avg_distance_km": round(
            (total_distance * METERS_TO_KM) / active_weeks_for_avg, 1
        ),
        "weekly_avg_hours": round((total_moving / 3600.0) / active_weeks_for_avg, 1),
        "rides_per_week": round(ride_count / active_weeks_for_avg, 1),
        "active_weeks": active_weeks_count,
        "longest_break": compute_longest_break(ride_dates),
    }

    detail = {
        "year": year,
        "totals": {
            "distance_mi": round(total_distance * METERS_TO_MILES, 1),
            "distance_km": round(total_distance * METERS_TO_KM, 1),
            "climb_ft": int(round(total_elev * FEET_PER_METER)),
            "climb_m": int(round(total_elev)),
            "moving_hours": round(total_moving / 3600.0, 1),
            "rides": ride_count,
        },
        "averages": {
            "ride_distance_mi": round(avg_distance, 1),
            "ride_distance_km": round(avg_distance * 1.60934, 1),
            "ride_time": _fmt_duration(avg_time),
            "avg_speed_mph": round(avg_speed_mph, 1),
            "avg_speed_kmh": round(avg_speed_kmh, 1),
            "ride_climb_ft": avg_climb_ft,
            "ride_climb_m": avg_climb_m,
        },
        "best_days": {
            "longest_distance": ride_snapshot(longest_distance),
            "longest_time": ride_snapshot(longest_time),
            "biggest_climb": ride_snapshot(biggest_climb),
        },
        "best_months": {
            "distance": best_month("distance"),
            "elevation": best_month("elev"),
            "moving": best_month("moving"),
        },
        "best_week": (
            {
                "label": _format_week_range(best_week["start"], best_week["end"]),
                "distance_mi": round(best_week["distance"] * METERS_TO_MILES, 1),
                "distance_km": round(best_week["distance"] * METERS_TO_KM, 1),
                "rides": best_week["rides"],
            }
            if best_week
            else None
        ),
        "charts": {
            "monthly": chart_ready_monthly(),
            "weekly": chart_ready_weekly(),
        },
        "distributions": {
            "weekday": {
                "labels": [entry["abbr"] for entry in weekday_totals],
                "rides": [entry["rides"] for entry in weekday_totals],
                "distance_mi": [
                    round(entry["distance"] * METERS_TO_MILES, 1) for entry in weekday_totals
                ],
                "distance_km": [
                    round(entry["distance"] * METERS_TO_KM, 1) for entry in weekday_totals
                ],
                "avg_speed_mph": [
                    round(
                        (entry["distance"] * METERS_TO_MILES) / (entry["moving"] / 3600.0)
                        if entry["moving"] > 0
                        else 0.0,
                        1,
                    )
                    for entry in weekday_totals
                ],
                "avg_speed_kmh": [
                    round(
                        (entry["distance"] * METERS_TO_KM) / (entry["moving"] / 3600.0)
                        if entry["moving"] > 0
                        else 0.0,
                        1,
                    )
                    for entry in weekday_totals
                ],
            },
            "weekday_table": weekday_table,
            "distance_buckets": {
                "labels": [entry["label"] for entry in distance_buckets],
                "rides": [entry["rides"] for entry in distance_buckets],
                "distance_mi": [
                    round(entry["distance"] * METERS_TO_MILES, 1) for entry in distance_buckets
                ],
                "distance_km": [
                    round(entry["distance"] * METERS_TO_KM, 1) for entry in distance_buckets
                ],
            },
            "distance_bucket_table": bucket_table,
            "ride_mix": ride_mix_totals,
            "time_of_day": {
                **time_of_day_chart,
                "table": time_of_day_table,
            },
        },
        "top_distance_rides": serialize_top(sorted(rides, key=lambda r: r["distance"], reverse=True)[:5]),
        "top_climb_rides": serialize_top(sorted(rides, key=lambda r: r["elev"], reverse=True)[:5]),
        "recent_rides": serialize_top(sorted(rides, key=lambda r: r["dt"], reverse=True)[:10]),
        "fastest_rides": serialize_fastest(sorted(rides, key=lambda r: r["avg_speed"], reverse=True)[:5]),
        "streaks": {
            "longest": streak_snapshot(compute_longest_streak(ride_dates)),
            "current": streak_snapshot(compute_current_streak(ride_dates)),
        },
        "quarters": quarters,
        "half_split": half_split,
        "consistency": consistency,
    }
    return detail


__all__ = [
    "parse_activity_dt",
    "normalize_yearly_breakdown",
    "build_yearly_review_payload",
    "build_yearly_detail",
]
