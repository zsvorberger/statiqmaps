import json
import os

from user_data_pullers import resolver
from user_data_pullers.activity_cache import load_activities_cached


def build_unique_miles_context(user_id):
    """Build the bikes list used by the unique miles page."""
    bike_name_by_id = {}
    gear_path = resolver.gear_path(user_id)

    if os.path.exists(gear_path):
        try:
            with open(gear_path) as gf:
                gear = json.load(gf)
                if isinstance(gear, list):
                    for g in gear:
                        gid = g.get("id")
                        if gid:
                            bike_name_by_id[gid] = (
                                g.get("name")
                                or g.get("nickname")
                                or g.get("model_name")
                                or gid
                            )
                elif isinstance(gear, dict):
                    for gid, g in gear.items():
                        bike_name_by_id[gid] = (
                            g.get("name")
                            or g.get("nickname")
                            or g.get("model_name")
                            or gid
                        )
        except Exception:
            pass

    bikes_map = {}
    for a in load_activities_cached(user_id):
        bike_id = a.get("gear_id") or "unknown"
        bike_name = bike_name_by_id.get(
            bike_id,
            "No bike selected" if bike_id == "unknown" else bike_id
        )
        bikes_map[bike_id] = bike_name

    bikes = sorted(
        [(bid, name) for bid, name in bikes_map.items() if bid != "unknown"],
        key=lambda x: x[1].lower(),
    )
    if "unknown" in bikes_map:
        bikes.append(("unknown", "No bike selected"))

    return bikes
