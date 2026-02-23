from flask import Blueprint, request, jsonify, render_template, redirect, url_for, session
import os, json, time, datetime
from datetime import datetime as dt
from collections import defaultdict
from user_data_pullers import resolver
from user_data_pullers.activity_cache import load_activities_cached
from flask_login import current_user, login_required
from utils.user_scope import get_current_app_user_id, get_strava_id_for_user
from models.models_users import (
    get_user_by_id,
    get_user_by_email,
    get_outgoing_maintenance_shares,
    get_incoming_maintenance_shares,
    upsert_maintenance_share,
    delete_maintenance_share,
    get_share_between,
)

# Blueprint for all maintenance routes
maintenance_bp = Blueprint("maintenance", __name__)

DATE_FMT = "%Y-%m-%d"


class TrackerAccessError(Exception):
    """Raised when a tracker context cannot be resolved."""


def _tracker_options_for_viewer():
    """
    Build the list of trackers the current viewer can access.
    Each entry -> {"owner_user_id","owner_name","label","strava_id","can_edit","owned"}
    """
    options = []
    seen = set()
    viewer = current_user if current_user.is_authenticated else None
    viewer_id = int(viewer.id) if viewer else None
    viewer_row = get_user_by_id(viewer_id) if viewer_id else None
    viewer_name = None
    viewer_strava_id = None
    if viewer_row:
        viewer_name = viewer_row["username"] or viewer_row["email"]
        viewer_strava_id = viewer_row["strava_athlete_id"]
    elif viewer:
        viewer_name = getattr(viewer, "username", None) or getattr(
            viewer, "email", None
        )
        viewer_strava_id = getattr(viewer, "strava_athlete_id", None)

    def _append(option):
        key = (option.get("owner_user_id"), option.get("strava_id"))
        if key in seen or not option.get("strava_id"):
            return
        seen.add(key)
        options.append(option)

    if viewer and viewer_id is not None:
        strava_id = viewer_strava_id
        if strava_id:
            _append(
                {
                    "owner_user_id": viewer_id,
                    "owner_name": viewer_name or "You",
                    "label": "Your tracker",
                    "strava_id": str(strava_id),
                    "can_edit": True,
                    "owned": True,
                }
            )
    if viewer_id is not None:
        for share in get_incoming_maintenance_shares(viewer_id):
            owner_strava_id = share.get("owner_strava_athlete_id")
            if not owner_strava_id:
                continue
            owner_label = (
                share.get("owner_username")
                or share.get("owner_email")
                or f"User {share.get('owner_user_id')}"
            )
            _append(
                {
                    "owner_user_id": share.get("owner_user_id"),
                    "owner_name": owner_label,
                    "label": f"{owner_label}'s tracker",
                    "strava_id": str(owner_strava_id),
                    "can_edit": bool(share.get("can_edit")),
                    "owned": False,
                }
            )
    return options


def _set_active_tracker(option):
    session["maintenance_tracker_owner_id"] = option.get("owner_user_id")
    session["maintenance_tracker_strava_id"] = option.get("strava_id")
    session["maintenance_tracker_can_edit"] = option.get("can_edit", True)


def _ensure_active_tracker_option(options):
    if not options:
        session.pop("maintenance_tracker_owner_id", None)
        session.pop("maintenance_tracker_strava_id", None)
        session.pop("maintenance_tracker_can_edit", None)
        return None

    stored_owner = session.get("maintenance_tracker_owner_id")
    active = None
    for opt in options:
        if stored_owner is None and opt.get("owner_user_id") is None:
            active = opt
            break
        if stored_owner is not None and opt.get("owner_user_id") == stored_owner:
            active = opt
            break
    if not active:
        active = options[0]
    _set_active_tracker(active)
    return active


def _tracker_context(require_edit=False):
    """Resolve the tracker user id + permissions for the current viewer."""
    owner_id = session.get("maintenance_tracker_owner_id")
    tracker_user_id = session.get("maintenance_tracker_strava_id")
    viewer_id = int(current_user.id) if current_user.is_authenticated else None
    can_edit = bool(session.get("maintenance_tracker_can_edit", True))
    is_owner = False

    if owner_id:
        owner_row = get_user_by_id(owner_id)
        if not owner_row or not owner_row["strava_athlete_id"]:
            raise TrackerAccessError("Tracker owner is not connected to Strava.")
        tracker_user_id = str(owner_row["strava_athlete_id"])
        if viewer_id and int(viewer_id) == int(owner_id):
            can_edit = True
            is_owner = True
        else:
            if not viewer_id:
                raise TrackerAccessError("Login required to access shared tracker.")
            share = get_share_between(owner_id, viewer_id)
            if not share:
                raise TrackerAccessError("You no longer have access to this tracker.")
            can_edit = bool(share["can_edit"])
        session["maintenance_tracker_strava_id"] = tracker_user_id
        session["maintenance_tracker_can_edit"] = can_edit
    else:
        if not tracker_user_id:
            tracker_user_id = get_current_user_id()
        if not tracker_user_id:
            raise TrackerAccessError("No tracker available for this account.")
        can_edit = True
        is_owner = True
        session["maintenance_tracker_strava_id"] = tracker_user_id

    if require_edit and not can_edit:
        raise TrackerAccessError("Editing is disabled for this tracker.")

    return {
        "tracker_user_id": str(tracker_user_id),
        "owner_user_id": owner_id,
        "viewer_user_id": viewer_id,
        "can_edit": can_edit,
        "is_owner": is_owner,
    }


def get_current_user_id():
    """
    Return the linked Strava athlete id for the logged-in user, or None.
    """
    tracker_id = session.get("maintenance_tracker_strava_id")
    if tracker_id:
        return tracker_id
    app_user_id = get_current_app_user_id()
    strava_id = get_strava_id_for_user(app_user_id)
    if strava_id:
        session["maintenance_tracker_strava_id"] = str(strava_id)
    return strava_id


def _load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return default


def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _as_dt(s):
    if isinstance(s, dt):
        return s
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", DATE_FMT):
        try:
            return dt.strptime(s, fmt)
        except Exception:
            pass
    return None


def _unique_id(prefix="part"):
    return f"{prefix}_{int(time.time() * 1000)}"


def _activities_for_bike(activities, bike_id):
    return [a for a in activities if str(a.get("gear_id")) == str(bike_id)]


def _cum_stats_for_bike(activities, up_to_dt=None):
    dist = 0.0
    elev = 0.0
    secs = 0.0
    for a in activities:
        start_dt = _as_dt(a.get("start_date"))
        if up_to_dt and start_dt and start_dt > up_to_dt:
            continue
        dist += float(a.get("distance", 0.0))
        elev += float(a.get("total_elevation_gain", 0.0))
        secs += float(a.get("moving_time", 0.0))
    return {"distance_m": dist, "elev_m": elev, "moving_time_s": secs}


def _wear_since_install(activities, install_dt, baseline):
    now_cum = _cum_stats_for_bike(activities, up_to_dt=None)
    return {
        "distance_m": max(0.0, now_cum["distance_m"] - baseline["distance_m"]),
        "elev_m": max(0.0, now_cum["elev_m"] - baseline["elev_m"]),
        "moving_time_s": max(
            0.0, now_cum["moving_time_s"] - baseline["moving_time_s"]
        ),
    }


def _safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _as_bool(val, default=False):
    if isinstance(val, bool):
        return val
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return bool(val)
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def _history_entries_for_part(part, bike_lookup):
    """
    Return normalized install history entries for a part plus a flag indicating
    whether the original data was mutated (missing ids/dates).
    """
    history = part.get("install_history") or []
    mutated = False
    if not history:
        history = [
            {
                "history_id": _unique_id("hist"),
                "bike_id": part.get("bike_id"),
                "installed_at": part.get("date_installed"),
                "removed_at": part.get("retired_at"),
            }
        ]
        part["install_history"] = history
        mutated = True

    normalized_entries = []
    for h in history:
        if not h.get("history_id"):
            h["history_id"] = _unique_id("hist")
            mutated = True
        if not h.get("installed_at"):
            h["installed_at"] = part.get("date_installed")
            mutated = True

        installed_dt = _as_dt(h.get("installed_at"))
        removed_dt = _as_dt(h.get("removed_at"))
        end_dt = removed_dt or dt.utcnow()
        duration_days = None
        if installed_dt:
            duration_days = max(0, (end_dt - installed_dt).days)

        normalized_entries.append(
            {
                "history_id": h["history_id"],
                "bike_id": h.get("bike_id"),
                "bike_name": bike_lookup.get(h.get("bike_id"), "Unknown Bike"),
                "installed_at": h.get("installed_at"),
                "removed_at": h.get("removed_at"),
                "duration_days": duration_days,
            }
        )
    return normalized_entries, mutated



########################################################################################


@maintenance_bp.route("/maintenance/add_note", methods=["POST"])
@login_required
def maintenance_add_note():
    data = request.form if request.form else request.json
    text = (data.get("text") or "").strip()
    date = (data.get("date") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Missing note text"}), 400

    try:
        ctx = _tracker_context(require_edit=True)
    except TrackerAccessError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 403
    user_id = ctx["tracker_user_id"]
    raw = _load_json(resolver.gear_path(user_id), default=[])
    gear_data = _normalize_gear_data(raw)

    note = {
        "note_id": _unique_id("note"),
        "text": text,
        "date": date or dt.utcnow().strftime("%Y-%m-%d"),
    }
    gear_data.setdefault("custom_notes", []).append(note)
    _save_json(resolver.gear_path(user_id), gear_data)
    return jsonify({"ok": True, "note": note})

########################################################################################

def _normalize_gear_data(raw):
    """
    Accepts:
      - dict with bikes/parts/maintenance_log
      - dict with nested 'gear' key
      - plain list of bike dicts
    Returns a dict: {"bikes": [...], "parts": [...], "maintenance_log": [...]}
    """
    # If it's already a dict, make sure keys exist
    if isinstance(raw, dict):
        # Some projects store bikes under raw["gear"]["bikes"]
        if "bikes" not in raw:
            if isinstance(raw.get("gear"), dict) and isinstance(
                raw["gear"].get("bikes"), list
            ):
                raw["bikes"] = raw["gear"]["bikes"]
        raw.setdefault("bikes", [])
        raw.setdefault("parts", [])
        raw.setdefault("maintenance_log", [])
        return raw

    # If it's just a list, treat it as the bikes list
    if isinstance(raw, list):
        return {"bikes": raw, "parts": [], "maintenance_log": []}

    # Fallback
    return {"bikes": [], "parts": [], "maintenance_log": []}

################################################################################

@maintenance_bp.route("/maintenance", methods=["GET"])
@login_required
def maintenance_page():
    tracker_options = _tracker_options_for_viewer()
    active_tracker = _ensure_active_tracker_option(tracker_options)
    user_id = active_tracker["strava_id"] if active_tracker else get_current_user_id()
    if not user_id:
        return redirect(url_for("index"))

    tracker_access = {
        "owner_user_id": active_tracker["owner_user_id"] if active_tracker else None,
        "owner_name": active_tracker["owner_name"] if active_tracker else "Local",
        "label": active_tracker["label"] if active_tracker else "Local tracker",
        "can_edit": active_tracker["can_edit"] if active_tracker else True,
        "is_owner": active_tracker["owned"] if active_tracker else True,
    }
    share_management_enabled = (
        tracker_access["is_owner"]
        and current_user.is_authenticated
        and tracker_access["owner_user_id"] is not None
    )
    share_records = (
        get_outgoing_maintenance_shares(active_tracker["owner_user_id"])
        if share_management_enabled and active_tracker
        else []
    )

    gear_data = _load_json(resolver.gear_path(user_id), default=[])
    activities = load_activities_cached(user_id)

    # Normalize gear_data so it always becomes a dict with bikes/parts/logs
    gear_data = _normalize_gear_data(gear_data)

    bikes = gear_data["bikes"]
    parts = gear_data["parts"]
    maint = gear_data["maintenance_log"]

    bike_lookup = {}
    for idx, b in enumerate(bikes, start=1):
        bike_id = str(b.get("gear_id") or b.get("id") or idx)
        name = (
            b.get("name")
            or b.get("model_name")
            or b.get("nickname")
            or b.get("display_name")
            or f"Bike {idx}"
        )
        bike_lookup[bike_id] = name

    logs_by_part = {}
    for m in maint:
        logs_by_part.setdefault(m.get("part_id"), []).append(m)
    for k in logs_by_part:
        logs_by_part[k] = sorted(
            logs_by_part[k],
            key=lambda x: _as_dt(x.get("date")) or dt.min,
            reverse=True,
        )


    # cache activities by bike
    activities_by_bike_cache = {}

    def get_acts(bike_id):
        if bike_id not in activities_by_bike_cache:
            activities_by_bike_cache[bike_id] = _activities_for_bike(
                activities, bike_id
            )
        return activities_by_bike_cache[bike_id]

    # compute wear + backfill baseline if missing
    changed = False
    for p in parts:
        bike_id = p.get("bike_id")
        install_dt = _as_dt(p.get("date_installed"))
        acts = get_acts(bike_id)

        if not p.get("baseline_at_install"):
            baseline = _cum_stats_for_bike(acts, up_to_dt=install_dt)
            p["baseline_at_install"] = baseline
            changed = True

        wear = _wear_since_install(acts, install_dt, p["baseline_at_install"])
        p["wear"] = wear

    parts_enriched = []
    parts_by_type = defaultdict(list)
    spend_by_bike = defaultdict(float)
    spend_by_type = defaultdict(float)
    total_part_spend = 0.0
    total_log_spend = sum(_safe_float(m.get("cost", 0.0)) for m in maint)
    retired_durations = []
    parts_by_bike_map = defaultdict(list)

    for p in parts:
        price_val = _safe_float(p.get("price", 0.0))
        if p.get("price") != price_val:
            p["price"] = price_val
            changed = True

        history_entries, hist_changed = _history_entries_for_part(p, bike_lookup)
        if hist_changed:
            changed = True

        bike_id = str(p.get("bike_id"))
        bike_name = bike_lookup.get(bike_id, "Unknown Bike")
        wear = p.get("wear", {"distance_m": 0.0, "elev_m": 0.0, "moving_time_s": 0.0})
        wear_distance_m = wear.get("distance_m", 0.0) or 0.0
        wear_miles = wear_distance_m / 1609.34 if wear_distance_m else 0.0
        wear_hours = (wear.get("moving_time_s", 0.0) or 0.0) / 3600.0
        per_mile_cost = price_val / wear_miles if wear_miles else None

        total_part_spend += price_val
        spend_by_bike[bike_name] += price_val
        spend_by_type[p.get("part_type", "other")] += price_val

        if p.get("status") == "retired":
            start_dt = _as_dt(p.get("date_installed"))
            end_dt = _as_dt(p.get("retired_at"))
            if start_dt and end_dt:
                retired_durations.append(max(0, (end_dt - start_dt).days))

        logs_for_part = logs_by_part.get(p.get("part_id"), [])
        part_view = {
            "part_id": p.get("part_id"),
            "name": p.get("name"),
            "part_type": p.get("part_type"),
            "status": p.get("status"),
            "date_installed": p.get("date_installed"),
            "retired_at": p.get("retired_at"),
            "notes": p.get("notes"),
            "bike_id": bike_id,
            "bike_name": bike_name,
            "wear": wear,
            "price": price_val,
            "per_mile_cost": per_mile_cost,
            "wear_miles": wear_miles,
            "wear_hours": wear_hours,
            "history_entries": history_entries,
            "logs": logs_for_part,
            "log_count": len(logs_for_part),
            "last_event": logs_for_part[0] if logs_for_part else None,
        }
        parts_enriched.append(part_view)
        parts_by_type[p.get("part_type", "other")].append(part_view)
        parts_by_bike_map[bike_id].append(part_view)

    active_parts = [p for p in parts_enriched if p["status"] != "retired"]
    retired_parts = [p for p in parts_enriched if p["status"] == "retired"]
    longest_parts = sorted(
        parts_enriched, key=lambda x: x["wear"].get("distance_m", 0.0), reverse=True
    )

    avg_retired_lifespan_days = (
        round(sum(retired_durations) / len(retired_durations), 1)
        if retired_durations
        else None
    )

    stats_summary = {
        "total_parts": len(parts_enriched),
        "active_parts": len(active_parts),
        "retired_parts": len(retired_parts),
        "total_part_spend": total_part_spend,
        "total_log_spend": total_log_spend,
        "avg_part_price": round(
            total_part_spend / len(parts_enriched), 2
        )
        if parts_enriched
        else 0.0,
        "avg_retired_lifespan_days": avg_retired_lifespan_days,
    }

    spend_by_bike_list = sorted(
        spend_by_bike.items(), key=lambda x: x[1], reverse=True
    )
    spend_by_type_list = sorted(
        spend_by_type.items(), key=lambda x: x[1], reverse=True
    )

    per_type_stats = []
    for part_type, entries in parts_by_type.items():
        total_type_cost = sum(p["price"] for p in entries)
        wear_sum = sum(p["wear"].get("distance_m", 0.0) for p in entries)
        per_type_stats.append(
            {
                "part_type": part_type,
                "count": len(entries),
                "active": sum(1 for p in entries if p["status"] != "retired"),
                "total_cost": total_type_cost,
                "avg_cost": total_type_cost / len(entries)
                if entries
                else 0.0,
                "avg_wear_miles": (wear_sum / len(entries) / 1609.34)
                if entries
                else 0.0,
            }
        )
    per_type_stats = sorted(
        per_type_stats, key=lambda x: x["total_cost"], reverse=True
    )

    if changed:
        gear_data["parts"] = parts
        _save_json(resolver.gear_path(user_id), gear_data)

    # group maintenance logs by part
    logs_by_part = {}
    for m in maint:
        logs_by_part.setdefault(m.get("part_id"), []).append(m)
    for k in logs_by_part:
        logs_by_part[k] = sorted(
            logs_by_part[k], key=lambda x: _as_dt(x.get("date")) or dt.min, reverse=True
        )

    bike_cards = []
    for idx, b in enumerate(bikes, start=1):
        bike_id = str(b.get("gear_id") or b.get("id") or idx)
        grouped = parts_by_bike_map.get(bike_id, [])
        active_for_bike = [p for p in grouped if p["status"] != "retired"]
        retired_for_bike = [p for p in grouped if p["status"] == "retired"]
        top_part = (
            max(grouped, key=lambda x: x["wear"].get("distance_m", 0.0), default=None)
            if grouped
            else None
        )
        bike_cards.append(
            {
                "bike_id": bike_id,
                "name": bike_lookup.get(bike_id, f"Bike {idx}"),
                "model_name": b.get("model_name"),
                "notes": b.get("notes"),
                "active_count": len(active_for_bike),
                "retired_count": len(retired_for_bike),
                "total_spend": sum(p["price"] or 0.0 for p in grouped),
                "active_parts": active_for_bike,
                "retired_parts": retired_for_bike,
                "top_part": top_part,
                "total_wear_km": sum(
                    (p["wear"].get("distance_m", 0.0) or 0.0) for p in grouped
                )
                / 1000.0
                if grouped
                else 0.0,
            }
        )

    sorted_parts = sorted(
        parts_enriched,
        key=lambda x: _as_dt(x.get("date_installed")) or dt.min,
        reverse=True,
    )
    seen_templates = set()
    part_templates = []
    for p in sorted_parts:
        key = (
            (p.get("name") or "").strip().lower(),
            (p.get("part_type") or "").strip().lower(),
        )
        if not key[0] or not key[1] or key in seen_templates:
            continue
        seen_templates.add(key)
        part_templates.append(
            {
                "name": p.get("name"),
                "part_type": p.get("part_type"),
                "notes": p.get("notes") or "",
                "price": p.get("price") or 0.0,
            }
        )
        if len(part_templates) >= 12:
            break

    service_watchlist = sorted(
        active_parts, key=lambda x: x["wear"].get("distance_m", 0.0), reverse=True
    )[:5]

    return render_template(
        "maintenance.html",
        bikes=bikes,
        parts=parts,
        parts_enriched=parts_enriched,
        bike_cards=bike_cards,
        logs_by_part=logs_by_part,
        custom_notes=gear_data.get("custom_notes", []),
        stats_summary=stats_summary,
        spend_by_bike=spend_by_bike_list,
        spend_by_type=spend_by_type_list,
        longest_parts=longest_parts,
        service_watchlist=service_watchlist,
        per_type_stats=per_type_stats,
        part_templates=part_templates,
        tracker_options=tracker_options,
        tracker_access=tracker_access,
        share_records=share_records,
        share_management_enabled=share_management_enabled,
    )

###########################################################################


@maintenance_bp.route("/maintenance/add_item", methods=["POST"])
@login_required
def maintenance_add_item():
    data = request.form if request.form else request.json
    if not data:
        return jsonify({"ok": False, "error": "No data"}), 400

    bike_id = str(data.get("bike_id") or "").strip()
    name = (data.get("name") or "").strip()
    part_type = (data.get("part_type") or "").strip()
    date_installed = (data.get("date_installed") or "").strip()
    notes = (data.get("notes") or "").strip()
    price = _safe_float(data.get("price", 0.0))
    if not (bike_id and name and part_type and date_installed):
        return jsonify({"ok": False, "error": "Missing required fields"}), 400

    try:
        ctx = _tracker_context(require_edit=True)
    except TrackerAccessError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 403
    user_id = ctx["tracker_user_id"]
    gear_path = resolver.gear_path(user_id)

    raw = _load_json(gear_path, default=[])
    gear_data = _normalize_gear_data(raw)
    activities = load_activities_cached(user_id)

    # Prevent duplicate part
    key = (bike_id, name.lower(), part_type.lower(), date_installed)
    for p in gear_data.get("parts", []):
        if (
            p.get("bike_id"),
            (p.get("name") or "").lower(),
            (p.get("part_type") or "").lower(),
            p.get("date_installed"),
        ) == key:
            return jsonify({"ok": True, "part": p, "duplicate": True})

    part_id = _unique_id("part")
    install_dt = _as_dt(date_installed)
    acts = _activities_for_bike(activities, bike_id)
    baseline = _cum_stats_for_bike(acts, up_to_dt=install_dt)

    new_part = {
        "part_id": part_id,
        "bike_id": bike_id,
        "name": name,
        "part_type": part_type,
        "status": "active",
        "date_installed": date_installed,
        "notes": notes,
        "baseline_at_install": baseline,
        "price": price,
        "install_history": [
            {
                "history_id": _unique_id("hist"),
                "bike_id": bike_id,
                "installed_at": date_installed,
                "removed_at": None,
            }
        ],
    }
    gear_data.setdefault("parts", []).append(new_part)
    _save_json(gear_path, gear_data)
    return jsonify({"ok": True, "part": new_part})


###########################################################################

@maintenance_bp.route("/maintenance/log_event", methods=["POST"])
@login_required
def maintenance_log_event():
    data = request.form if request.form else request.json
    if not data:
        return jsonify({"ok": False, "error": "No data"}), 400

    part_id = (data.get("part_id") or "").strip()
    action = (data.get("action") or "").strip()
    date = (data.get("date") or "").strip()
    cost = float(data.get("cost") or 0.0)
    notes = (data.get("notes") or "").strip()

    if not (part_id and action and date):
        return jsonify({"ok": False, "error": "Missing required fields"}), 400

    try:
        ctx = _tracker_context(require_edit=True)
    except TrackerAccessError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 403
    user_id = ctx["tracker_user_id"]
    gear_path = resolver.gear_path(user_id)

    gear_data = _load_json(
        gear_path, default={"bikes": [], "parts": [], "maintenance_log": []}
    )
    parts = gear_data.get("parts", [])
    part = next((p for p in parts if p.get("part_id") == part_id), None)
    if not part:
        return jsonify({"ok": False, "error": "Part not found"}), 404

    activities = load_activities_cached(user_id)
    acts = _activities_for_bike(activities, part.get("bike_id"))
    event_dt = _as_dt(date)

    baseline = part.get(
        "baseline_at_install", {"distance_m": 0, "elev_m": 0, "moving_time_s": 0}
    )
    cum_at_event = _cum_stats_for_bike(acts, up_to_dt=event_dt)
    odometer = {
        "distance_m": max(0.0, cum_at_event["distance_m"] - baseline["distance_m"]),
        "elev_m": max(0.0, cum_at_event["elev_m"] - baseline["elev_m"]),
        "moving_time_s": max(
            0.0, cum_at_event["moving_time_s"] - baseline["moving_time_s"]
        ),
    }

    log_id = _unique_id("log")
    new_log = {
        "log_id": log_id,
        "part_id": part_id,
        "date": date,
        "action": action,
        "cost": cost,
        "notes": notes,
        "odometer": odometer,
    }
    gear_data.setdefault("maintenance_log", []).append(new_log)
    _save_json(gear_path, gear_data)
    return jsonify({"ok": True, "log": new_log})

###########################################################################

@maintenance_bp.route("/maintenance/retire_part", methods=["POST"])
@login_required
def maintenance_retire_part():
    data = request.form if request.form else request.json
    part_id = (data.get("part_id") or "").strip()
    new_status = (data.get("status") or "retired").strip()
    retired_at = (data.get("retired_at") or "").strip()
    reinstalled_at = (data.get("reinstalled_at") or "").strip()
    if not part_id:
        return jsonify({"ok": False, "error": "Missing part_id"}), 400

    try:
        ctx = _tracker_context(require_edit=True)
    except TrackerAccessError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 403
    user_id = ctx["tracker_user_id"]
    gear_path = resolver.gear_path(user_id)

    gear_data = _load_json(
        gear_path, default={"bikes": [], "parts": [], "maintenance_log": []}
    )
    updated = False
    for p in gear_data.get("parts", []):
        if p.get("part_id") == part_id:
            p["status"] = new_status
            if new_status == "retired":
                if not retired_at:
                    retired_at = dt.utcnow().strftime("%Y-%m-%d")
                p["retired_at"] = retired_at
                history = p.setdefault("install_history", [])
                if history:
                    last = history[-1]
                    if not last.get("removed_at"):
                        last["removed_at"] = retired_at
            else:
                p.pop("retired_at", None)
                history = p.setdefault("install_history", [])
                reinstall_date = (
                    reinstalled_at if reinstalled_at else dt.utcnow().strftime(DATE_FMT)
                )
                history.append(
                    {
                        "history_id": _unique_id("hist"),
                        "bike_id": p.get("bike_id"),
                        "installed_at": reinstall_date,
                        "removed_at": None,
                    }
                )
            updated = True
            break

    if updated:
        _save_json(gear_path, gear_data)
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Part not found"}), 404

###########################################################################


@maintenance_bp.route("/maintenance/update_part", methods=["POST"])
@login_required
def maintenance_update_part():
    data = request.form if request.form else request.json
    part_id = (data.get("part_id") or "").strip()
    if not part_id:
        return jsonify({"ok": False, "error": "Missing part_id"}), 400

    try:
        ctx = _tracker_context(require_edit=True)
    except TrackerAccessError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 403
    user_id = ctx["tracker_user_id"]
    gear_path = resolver.gear_path(user_id)

    gear_data = _load_json(
        gear_path, default={"bikes": [], "parts": [], "maintenance_log": []}
    )
    updated = False
    for p in gear_data.get("parts", []):
        if p.get("part_id") == part_id:
            if "notes" in data:
                p["notes"] = (data.get("notes") or "").strip()
            if "name" in data:
                p["name"] = (data.get("name") or "").strip()
            if "part_type" in data:
                p["part_type"] = (data.get("part_type") or "").strip()
            if "price" in data:
                p["price"] = _safe_float(data.get("price", 0.0))
            updated = True
            break

    if updated:
        _save_json(gear_path, gear_data)
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Part not found"}), 404


###########################################################################
# -------- Tracker sharing + selection helpers ---------------------------


@maintenance_bp.route("/maintenance/select_tracker", methods=["POST"])
@login_required
def maintenance_select_tracker():
    data = request.get_json(silent=True) or request.form
    owner_raw = data.get("owner_user_id")
    owner_id = None
    if owner_raw not in (None, "", "legacy"):
        try:
            owner_id = int(owner_raw)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Invalid tracker id"}), 400

    options = _tracker_options_for_viewer()
    target = None
    for opt in options:
        if opt.get("owner_user_id") == owner_id:
            target = opt
            break
    if not target:
        return jsonify({"ok": False, "error": "Tracker unavailable"}), 404
    _set_active_tracker(target)
    return jsonify({"ok": True})


@maintenance_bp.route("/maintenance/share/save", methods=["POST"])
@login_required
def maintenance_share_save():
    data = request.get_json(silent=True) or request.form
    can_edit = _as_bool(data.get("can_edit", True), default=True)
    owner_id = current_user.id
    target_id = data.get("shared_with_user_id")
    email = (data.get("email") or "").strip().lower()

    target_row = None
    if target_id:
        try:
            target_row = get_user_by_id(int(target_id))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Invalid shared user id"}), 400
    elif email:
        target_row = get_user_by_email(email)
    else:
        return jsonify({"ok": False, "error": "Provide an email to share with."}), 400

    if not target_row:
        return jsonify({"ok": False, "error": "User not found."}), 404
    if int(target_row["id"]) == int(owner_id):
        return jsonify({"ok": False, "error": "Cannot share with yourself."}), 400

    owner_row = get_user_by_id(owner_id)
    if not owner_row or not owner_row["strava_athlete_id"]:
        return jsonify(
            {
                "ok": False,
                "error": "Connect your Strava account before sharing your tracker.",
            }
        ), 400

    upsert_maintenance_share(owner_id, target_row["id"], can_edit)
    return jsonify({"ok": True})


@maintenance_bp.route("/maintenance/share/remove", methods=["POST"])
@login_required
def maintenance_share_remove():
    data = request.get_json(silent=True) or request.form
    target_id = data.get("shared_with_user_id")
    if not target_id:
        return jsonify({"ok": False, "error": "Missing shared user id"}), 400
    try:
        target_id = int(target_id)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Invalid shared user id"}), 400
    delete_maintenance_share(current_user.id, target_id)
    return jsonify({"ok": True})

############################################################
# ------------------ end Maintenance Tracker routes ------------------
