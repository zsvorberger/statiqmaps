"""Build the rich personal best sections used on the dashboard."""

from __future__ import annotations

import datetime
import math
from typing import Any, Dict, List

from services.bike_utils import (
    BIKE_SPORT_TYPES,
    activity_surface,
    bike_label,
)


def build_personal_best_sections(
    activities: List[Dict[str, Any]],
    gear_lookup: Dict[str, Any],
    include_virtual: bool = True,
) -> List[Dict[str, Any]]:
    METERS_TO_MILES = 0.000621371
    METERS_TO_KM = 0.001
    MPS_TO_MPH = 2.23694
    MPS_TO_KMH = 3.6
    FEET_PER_METER = 3.28084

    best: Dict[str, Dict[str, Any]] = {}

    def update_best(key, raw_value, activity, prefer_high=True):
        if raw_value is None:
            return
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return
        if math.isnan(value):
            return
        current = best.get(key)
        if not current:
            best[key] = {"value": value, "activity": activity}
            return
        if prefer_high and value > current["value"]:
            best[key] = {"value": value, "activity": activity}
        elif not prefer_high and value < current["value"]:
            best[key] = {"value": value, "activity": activity}

    def calc_calories(activity):
        if activity.get("calories"):
            return activity.get("calories")
        if activity.get("kilojoules"):
            return activity.get("kilojoules") * 0.239006
        return None

    def friendly_date(activity):
        raw = activity.get("start_date_local") or activity.get("start_date") or ""
        day = raw[:10]
        try:
            return datetime.datetime.strptime(day, "%Y-%m-%d").strftime("%b %d, %Y")
        except Exception:
            return day or "Date unknown"

    def friendly_name(activity):
        return activity.get("name") or activity.get("type") or "Unnamed Activity"

    def friendly_type(activity):
        return activity.get("type") or activity.get("sport_type") or "Activity"

    def is_bike_activity(activity):
        sport = (activity.get("sport_type") or activity.get("type") or "").lower()
        if sport not in BIKE_SPORT_TYPES:
            return False
        if not include_virtual and sport == "virtualride":
            return False
        return True

    for activity in activities:
        if not is_bike_activity(activity):
            continue
        sport = (activity.get("sport_type") or activity.get("type") or "").lower()
        surface = activity_surface(activity, gear_lookup)
        is_virtual = sport == "virtualride"
        distance_m = activity.get("distance")
        moving_time = activity.get("moving_time")
        elapsed_time = activity.get("elapsed_time")
        avg_speed = activity.get("average_speed")
        max_speed = activity.get("max_speed")
        elevation_gain = activity.get("total_elevation_gain")
        elev_high = (
            activity.get("elev_high")
            or activity.get("elevation_high")
            or activity.get("elevHighest")
        )
        avg_hr = activity.get("average_heartrate") or activity.get("avg_heartrate")
        max_hr = activity.get("max_heartrate") or activity.get("heartrate_max")
        avg_power = activity.get("average_watts")
        weighted_power = activity.get("weighted_average_watts")
        max_power = activity.get("max_watts") or activity.get("max_power")
        avg_cadence = activity.get("average_cadence")
        max_cadence = activity.get("max_cadence")
        suffer_score = activity.get("suffer_score") or activity.get("training_load")
        achievements = activity.get("achievement_count")
        pr_count = activity.get("pr_count")
        athlete_count = activity.get("athlete_count")
        avg_temp = activity.get("average_temp")

        if distance_m:
            update_best("longest_distance", distance_m, activity)
            if surface == "road":
                update_best("longest_road_distance", distance_m, activity)
            else:
                update_best("longest_offroad_distance", distance_m, activity)
            if is_virtual:
                update_best("longest_virtual_distance", distance_m, activity)
        if moving_time:
            update_best("longest_moving_time", moving_time, activity)
        if elapsed_time:
            update_best("longest_elapsed_time", elapsed_time, activity)
        if avg_speed:
            update_best("fastest_avg_speed", avg_speed, activity)
        if max_speed:
            update_best("top_speed", max_speed, activity)
        if elevation_gain:
            update_best("biggest_climb", elevation_gain, activity)
        if elev_high:
            update_best("highest_elevation", elev_high, activity)
        if avg_hr:
            update_best("highest_avg_hr", avg_hr, activity)
        if max_hr:
            update_best("max_hr", max_hr, activity)
        if avg_power:
            update_best("avg_power", avg_power, activity)
        if weighted_power:
            update_best("weighted_power", weighted_power, activity)
        if max_power:
            update_best("max_power", max_power, activity)
        if avg_cadence:
            update_best("avg_cadence", avg_cadence, activity)
        if max_cadence:
            update_best("max_cadence", max_cadence, activity)
        if avg_temp is not None:
            update_best("hottest_temp", avg_temp, activity)
            update_best("coldest_temp", avg_temp, activity, prefer_high=False)
        calories = calc_calories(activity)
        if calories:
            update_best("most_calories", calories, activity)
            if moving_time and moving_time > 0:
                hours = moving_time / 3600.0
                if hours >= 0.2:
                    rate = calories / hours
                    update_best("calorie_burn_rate", rate, activity)
        if suffer_score:
            update_best("hardest_effort", suffer_score, activity)
        if achievements:
            update_best("achievement_haul", achievements, activity)
        if pr_count:
            update_best("pr_haul", pr_count, activity)
        if athlete_count:
            update_best("biggest_group", athlete_count, activity)

        if elevation_gain and distance_m and distance_m > 0:
            distance_km = distance_m * METERS_TO_KM
            if distance_km >= 1:
                density = elevation_gain / distance_km
                update_best("steepest_density", density, activity)
        if elevation_gain and moving_time and moving_time > 0:
            hours = moving_time / 3600.0
            if hours >= 0.2:
                vertical_rate = elevation_gain / hours
                update_best("vertical_per_hour", vertical_rate, activity)

    def format_duration(seconds):
        if seconds is None:
            return "—"
        hours = int(seconds) // 3600
        minutes = (int(seconds) % 3600) // 60
        parts = []
        if hours:
            parts.append(f"{hours}h")
        if minutes or not parts:
            parts.append(f"{minutes}m")
        return " ".join(parts)

    def format_distance_card(value_m, activity):
        if value_m is None:
            return {"value": "—", "subtitle": ""}
        miles = value_m * METERS_TO_MILES
        km = value_m * METERS_TO_KM
        return {
            "value": f"{miles:.1f} mi / {km:.1f} km",
            "subtitle": friendly_date(activity),
            "details": friendly_name(activity),
        }

    def format_time_card(seconds, activity):
        if seconds is None:
            return {"value": "—", "subtitle": ""}
        hours = seconds / 3600.0
        return {
            "value": format_duration(seconds),
            "subtitle": friendly_date(activity),
            "details": f"{hours:.1f} hours • {friendly_name(activity)}",
        }

    def format_speed_card(mps, activity):
        if mps is None:
            return {"value": "—", "subtitle": ""}
        return {
            "value": f"{mps * MPS_TO_MPH:.1f} mph / {mps * MPS_TO_KMH:.1f} km/h",
            "subtitle": friendly_date(activity),
            "details": friendly_name(activity),
        }

    def format_elev_card(meters, activity):
        if meters is None:
            return {"value": "—", "subtitle": ""}
        feet = meters * FEET_PER_METER
        return {
            "value": f"{feet:,.0f} ft / {meters:,.0f} m",
            "subtitle": friendly_date(activity),
            "details": friendly_name(activity),
        }

    def format_plain_card(value, unit, activity):
        if value is None:
            return {"value": "—", "subtitle": ""}
        return {
            "value": f"{value:,.1f}{unit}",
            "subtitle": friendly_date(activity),
            "details": friendly_name(activity),
        }

    def build_card(key, title, badge, description, formatter):
        base = {
            "title": title,
            "badge": badge,
            "description": description,
            "value_metric": "—",
            "value_imperial": "—",
            "detail": "",
            "activity_name": None,
            "activity_type": None,
            "date": None,
            "bike": None,
            "subtitle": "",
        }
        if key not in best:
            return base
        activity = best[key]["activity"]
        formatted = formatter(best[key]["value"], activity)
        formatted["type"] = friendly_type(activity)
        formatted["name"] = friendly_name(activity)
        formatted["date"] = friendly_date(activity)
        formatted["bike"] = bike_label(gear_lookup, activity.get("gear_id") or "unknown")

        base.update(
            value_metric=formatted.get("value", "—"),
            value_imperial=formatted.get("value", "—"),
            detail=formatted.get("details", ""),
            activity_name=formatted.get("name"),
            activity_type=formatted.get("type"),
            date=formatted.get("date"),
            bike=formatted.get("bike"),
            subtitle=formatted.get("subtitle", ""),
        )
        return base

    distance_section = {
        "title": "Distance",
        "cards": [
            build_card(
                "longest_distance",
                "Biggest Day",
                "Dist",
                "Most distance recorded on a single ride.",
                format_distance_card,
            ),
            build_card(
                "longest_road_distance",
                "Longest Road Day",
                "RD",
                "Most distance logged on pavement setups.",
                format_distance_card,
            ),
            build_card(
                "longest_offroad_distance",
                "Longest Off-road Day",
                "OR",
                "Biggest adventure on gravel or trail bikes.",
                format_distance_card,
            ),
            build_card(
                "longest_virtual_distance",
                "Longest Trainer Session",
                "VR",
                "Your biggest indoor/virtual ride.",
                format_distance_card,
            ),
        ],
    }

    elevation_section = {
        "title": "Elevation + Time",
        "cards": [
            build_card(
                "biggest_climb",
                "Biggest Climb",
                "Climb",
                "Most elevation gain in a single ride.",
                format_elev_card,
            ),
            build_card(
                "highest_elevation",
                "Highest Point",
                "Summit",
                "Maximum altitude reached.",
                format_elev_card,
            ),
            build_card(
                "longest_moving_time",
                "Longest Moving Time",
                "Move",
                "Most time moving during one ride.",
                format_time_card,
            ),
            build_card(
                "longest_elapsed_time",
                "Longest Total Time",
                "Elapsed",
                "Door-to-door adventure time.",
                format_time_card,
            ),
        ],
    }

    heart_section = {
        "title": "Heart Rate & Power",
        "cards": [
            build_card(
                "highest_avg_hr",
                "Highest Avg HR",
                "HR",
                "Most intense sustained heart rate.",
                lambda val, act: format_plain_card(val, " bpm", act),
            ),
            build_card(
                "max_hr",
                "Max Heart Rate",
                "HR",
                "Highest observed heart rate.",
                lambda val, act: format_plain_card(val, " bpm", act),
            ),
            build_card(
                "avg_power",
                "Highest Avg Power",
                "W",
                "Most average watts pushed.",
                lambda val, act: format_plain_card(val, " W", act),
            ),
            build_card(
                "max_power",
                "Max Power",
                "W",
                "Peak power during a ride.",
                lambda val, act: format_plain_card(val, " W", act),
            ),
        ],
    }

    effort_section = {
        "title": "Effort & Energy",
        "cards": [
            build_card(
                "suffer_score",
                "Hardest Effort",
                "Effort",
                "Highest relative effort / suffer score.",
                lambda val, act: format_plain_card(val, "", act),
            ),
            build_card(
                "most_calories",
                "Most Calories",
                "kcal",
                "Calories burned in a ride.",
                lambda val, act: format_plain_card(val, " kcal", act),
            ),
            build_card(
                "calorie_burn_rate",
                "Calorie Burn Rate",
                "kcal/hr",
                "Calories burned per hour.",
                lambda val, act: format_plain_card(val, " kcal/hr", act),
            ),
            build_card(
                "vertical_per_hour",
                "Vertical per Hour",
                "Climb",
                "Climbing rate.",
                lambda val, act: format_plain_card(val * FEET_PER_METER, " ft/hr", act),
            ),
        ],
    }

    weather_section = {
        "title": "Weather",
        "cards": [
            build_card(
                "hottest_temp",
                "Hottest Ride",
                "Temp",
                "Highest temperature ride.",
                lambda val, act: format_plain_card(val, "°C", act),
            ),
            build_card(
                "coldest_temp",
                "Coldest Ride",
                "Temp",
                "Coldest temperature ride.",
                lambda val, act: format_plain_card(val, "°C", act),
            ),
        ],
    }

    surfaces_section = {
        "title": "Surface + Group",
        "cards": [
            build_card(
                "fastest_avg_speed",
                "Fastest Avg Speed",
                "Speed",
                "Highest average speed ride.",
                format_speed_card,
            ),
            build_card(
                "top_speed",
                "Top Speed",
                "Speed",
                "Peak recorded top speed.",
                format_speed_card,
            ),
            build_card(
                "achievement_haul",
                "Most Achievements",
                "Kudos",
                "Largest haul of Strava achievements.",
                lambda val, act: format_plain_card(val, "", act),
            ),
            build_card(
                "biggest_group",
                "Biggest Group Ride",
                "Group",
                "Ride with the most athletes.",
                lambda val, act: format_plain_card(val, "", act),
            ),
        ],
    }

    return [
        distance_section,
        elevation_section,
        heart_section,
        effort_section,
        weather_section,
        surfaces_section,
    ]
