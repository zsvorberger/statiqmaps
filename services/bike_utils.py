"""Shared helpers for bike/gear lookups and surface classification."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from user_data_pullers import resolver

FRAME_LABELS = {
    1: "Mountain",
    2: "Cyclocross",
    3: "Road",
    4: "Time trial",
    5: "Gravel/Hybrid",
}
ROAD_FRAME_TYPES = {2, 3, 4, 5}
ROAD_SPORT_TYPES = {
    "ride",
    "virtualride",
    "ebikeride",
    "velomobile",
    "handcycle",
}
OFFROAD_SPORT_TYPES = {
    "gravelride",
    "mountainbikeride",
    "trailride",
    "cyclocross",
}
BIKE_SPORT_TYPES = set(ROAD_SPORT_TYPES) | set(OFFROAD_SPORT_TYPES)


def _read_json(path: Path) -> Any:
    try:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def normalize_gear_data(raw: Any) -> Dict[str, Any]:
    """
    Accepts:
      - dict with bikes/parts/maintenance_log
      - dict with nested 'gear' key
      - plain list of bike dicts
    Returns a dict: {"bikes": [...], "parts": [...], "maintenance_log": [...]}
    """
    if isinstance(raw, dict):
        if "bikes" not in raw:
            if isinstance(raw.get("gear"), dict) and isinstance(raw["gear"].get("bikes"), list):
                raw["bikes"] = raw["gear"]["bikes"]
        raw.setdefault("bikes", [])
        raw.setdefault("parts", [])
        raw.setdefault("maintenance_log", [])
        return raw
    if isinstance(raw, list):
        return {"bikes": raw, "parts": [], "maintenance_log": []}
    return {"bikes": [], "parts": [], "maintenance_log": []}


def load_gear_lookup(user_id: str | None) -> Dict[str, Any]:
    if not user_id:
        return {"unknown": {"name": "No bike selected", "frame_type": None}}
    path = Path(resolver.gear_path(user_id))
    raw = _read_json(path)
    gear_data = normalize_gear_data(raw or {})
    lookup = {}
    for bike in gear_data.get("bikes", []):
        gid = str(bike.get("id") or "")
        if not gid:
            continue
        lookup[gid] = bike
    lookup.setdefault("unknown", {"name": "No bike selected", "frame_type": None})
    return lookup


def frame_label(frame_type):
    return FRAME_LABELS.get(frame_type, "Unknown")


def is_road_frame(frame_type):
    if frame_type is None:
        return False
    try:
        return int(frame_type) in ROAD_FRAME_TYPES
    except (TypeError, ValueError):
        return False


def activity_surface(activity, gear_lookup: Dict[str, Any]):
    if not activity:
        return "road"
    sport = (activity.get("sport_type") or activity.get("type") or "").lower()
    if sport in OFFROAD_SPORT_TYPES:
        return "offroad"
    if sport in ROAD_SPORT_TYPES:
        return "road"
    gear = gear_lookup.get(activity.get("gear_id") or "unknown", {})
    frame_type = gear.get("frame_type")
    if frame_type is None:
        return "road"
    return "road" if is_road_frame(frame_type) else "offroad"


def activity_is_road(activity, gear_lookup: Dict[str, Any]) -> bool:
    return activity_surface(activity, gear_lookup) == "road"


def bike_label(gear_lookup: Dict[str, Any], gear_id: str | None) -> str:
    if not gear_id or gear_id == "unknown":
        return "No bike selected"
    bike = gear_lookup.get(gear_id)
    if not bike:
        return gear_id
    return (
        bike.get("nickname")
        or bike.get("name")
        or bike.get("model_name")
        or gear_id
    )

