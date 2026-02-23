from flask import (
    Flask,
    redirect,
    request,
    session,
    url_for,
    render_template,
    jsonify,
    send_from_directory,
    abort,
    flash,
)

import os
import json
import time
import math
import uuid
import requests
import datetime
import pandas as pd
import polyline
from pathlib import Path
from dateutil import tz
from datetime import datetime as dt
from types import SimpleNamespace
import user_data_pullers.resolver as resolver
from user_data_pullers.resolver import get_user_id
from werkzeug.utils import secure_filename


# === Local imports ===
from services.geo_index import coverage_geojson, activity_new_geojson, _hex_polygon
from user_data_pullers.segments_utils import summarize_segment, parse_date as fmt_mmss
from user_data_pullers.stats_helpers import aggregate_better_stats
from user_data_pullers.gear_helpers import load_gear, get_bike_usage, parse_dt
from user_data_pullers.foot_stats_helpers import aggregate_foot_stats, load_activities
from services.stats import load_summary
from user_data_pullers.strava_keys import ORS_API_KEY






# === Try optional imports ===
try:
    from pmtiles.reader import Reader
except Exception:
    Reader = None

# === Global paths/config ===
TILES_DIR = os.path.join(os.path.dirname(__file__), "tiles")
LOCAL_TZ = tz.gettz()  # for localizing dates
DATE_FMT = "%Y-%m-%d"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REQUESTS_PATH = os.path.join(BASE_DIR, "data", "requests.ndjson")
os.makedirs(os.path.dirname(REQUESTS_PATH), exist_ok=True)

app = Flask(__name__)
app.secret_key = "your_secret_key_here"  # <-- replace with real secret key

######## ROUTES #############
# Path to where your route data will be stored
ROUTES_DATA_FILE = os.path.join('data', 'routes_data.json')

def load_routes():
    """Load all routes from JSON file."""
    if os.path.exists(ROUTES_DATA_FILE):
        with open(ROUTES_DATA_FILE, 'r') as f:
            return json.load(f)
    return []

def save_routes(routes):
    """Save all routes to JSON file."""
    with open(ROUTES_DATA_FILE, 'w') as f:
        json.dump(routes, f, indent=4)

@app.route('/routes/all')
def all_routes():
    routes = load_routes()
    return render_template('routes/all_routes.html', routes=routes)

@app.route('/routes/my')
def my_routes():
    username = session.get('username', 'Guest')
    routes = [r for r in load_routes() if r.get('creator') == username]
    return render_template('routes/my_routes.html', routes=routes)

@app.route('/routes/favorites')
def favorite_routes():
    username = session.get('username', 'Guest')
    favs = []
    for r in load_routes():
        if username in r.get('favorites', []):
            favs.append(r)
    return render_template('routes/favorites.html', routes=favs)

@app.route('/routes/favorite/<route_id>', methods=['POST'])
def toggle_favorite(route_id):
    username = session.get('username', 'Guest')
    routes = load_routes()
    for r in routes:
        if str(r.get('id')) == route_id:
            if username in r.get('favorites', []):
                r['favorites'].remove(username)
            else:
                r.setdefault('favorites', []).append(username)
            break
    save_routes(routes)
    return redirect(request.referrer)




## ACTIVITIES PATH ADDED FRO GRAPHS

def activities_path(user_id):
    return f"users_data/ID{user_id}/strava_activities.json"




# Helper: get current user ID (session or fallback)
# -------------------------------------------------------
def get_current_user_id():
    """Return the current user_id from session, or guess from users_data folder."""
    # First check the session
    user_id = session.get("user_id")
    if user_id:
        return user_id

    # If not in session, try resolver (auto-detect from tokens.json)
    try:
        return resolver.get_user_id()
    except Exception:
        pass

    # As a last fallback, scan users_data folders
    users_dir = "users_data"
    if os.path.exists(users_dir):
        for d in os.listdir(users_dir):
            if d.startswith("ID"):
                user_id = d.replace("ID", "")
                session["user_id"] = user_id
                return user_id

    return None

    ### FOOT STATSS STUFF
@app.route("/foot_map")
def foot_map():
    user_id = get_user_id()
    activities = load_activities(user_id)

    foot_acts = [a for a in activities if a.get("type") in ("Run", "Walk", "Hike")]

    # send encoded polylines directly, just like bike map
    polylines = [a.get("map", {}).get("summary_polyline") for a in foot_acts if a.get("map")]

    return render_template("foot_map.html", polylines=polylines)






@app.route("/foot_stats")
def foot_stats():
    user_id = get_user_id()                 # -> "25512874"
    stats = aggregate_foot_stats(user_id)   # resolver will build users_data/ID25512874/...
    return render_template("foot_stats.html", stats=stats)


@app.route("/")
def index():
    return render_template("index.html")


####Wanderr style stuff


@app.route("/coverage_stats")
def coverage_stats():
    return render_template("coverage_stats.html")


@app.route("/stats/coverage_summary")
def stats_coverage_summary():
    return jsonify(load_summary())


@app.route("/stats/repeatability")
def stats_repeatability():
    s = load_summary()
    return jsonify(
        {
            "repeatability_score": s.get("repeatability_score", 0.0),
            "repeatability_cap": s.get("repeatability_cap", 3),
            "exploration_pct": s.get("exploration_pct", 0.0),
            "total_miles": s.get("total_miles", 0.0),
            "sum_new_miles": s.get("sum_new_miles", 0.0),
            "by_year": s.get("by_year", []),
            "generated_at": s.get("generated_at"),
        }
    )


@app.route("/unique_miles")
def unique_miles():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))

    # ---- BUILD BIKES (copied from heatmaps) ----
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
    activities_path = resolver.activities_path(user_id)
    if os.path.exists(activities_path):
        with open(activities_path) as f:
            data = json.load(f)
            for a in data:
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

    # -------------------------------------------

    return render_template("unique_miles.html", bikes=bikes)



def _parse_date_yyyy_mm_dd(s):
    try:
        return _dt.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _load_activities_list():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return []
    p = Path(resolver.activities_path(user_id))
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text())
    except Exception:
        return []


def _act_date(a):
    ds = a.get("start_date_local") or a.get("start_date")
    if not ds:
        return None
    try:
        # Strava strings are ISO e.g. "2025-08-09T12:34:56Z"
        return _dt.strptime(ds[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _filter_activity_ids(start=None, end=None, bike=None):
    acts = _load_activities_list()
    out = []
    for a in acts:
        d = _act_date(a)
        if start and (not d or d < start):
            continue
        if end and (not d or d > end):
            continue
        if bike and str(a.get("gear_id") or "") != str(bike):
            continue
        out.append(str(a.get("id")))
    return out


@app.route("/stats/activities")
def stats_activities():
    """List activities (minimal fields) for UI pickers."""
    start = _parse_date_yyyy_mm_dd(request.args.get("start", ""))
    end = _parse_date_yyyy_mm_dd(request.args.get("end", ""))
    bike = request.args.get("bike")
    acts = _load_activities_list()
    out = []
    for a in acts:
        d = _act_date(a)
        if start and (not d or d < start):
            continue
        if end and (not d or d > end):
            continue
        if bike and str(a.get("gear_id") or "") != str(bike):
            continue
        out.append(
            {
                "id": a.get("id"),
                "start_date_local": a.get("start_date_local") or a.get("start_date"),
                "gear_id": a.get("gear_id"),
            }
        )
    return jsonify(out)


@app.route("/coverage_range")
def coverage_range():
    """
    Union of hexes for all rides in [start,end] (and optional bike).
    Returns GeoJSON polygons.
    """
    start = _parse_date_yyyy_mm_dd(request.args.get("start", ""))
    end = _parse_date_yyyy_mm_dd(request.args.get("end", ""))
    bike = request.args.get("bike")

    ids = _filter_activity_ids(start, end, bike)
    cov_dir = Path("data/coverage/activity_hexes")
    cells = set()
    for aid in ids:
        fp = cov_dir / f"{aid}.json"
        if fp.exists():
            try:
                for c in json.loads(fp.read_text()):
                    cells.add(str(c))
            except Exception:
                pass

    # build polygons
    features = [
        {"type": "Feature", "id": c, "properties": {}, "geometry": _hex_polygon(c)}
        for c in cells
    ]
    return jsonify({"type": "FeatureCollection", "features": features})


@app.route("/new_by_range")
def new_by_range():
    """
    Union of 'new hexes' for rides in [start,end] (and optional bike).
    miles_est is the sum of per-ride new miles (already lifetime-aware).
    """
    start = _parse_date_yyyy_mm_dd(request.args.get("start", ""))
    end = _parse_date_yyyy_mm_dd(request.args.get("end", ""))
    bike = request.args.get("bike")

    ids = _filter_activity_ids(start, end, bike)
    cov_dir = Path("data/coverage/new_hexes")
    cells = set()

    # miles: from coverage_summary per-ride map
    summary_p = Path("data/coverage/coverage_summary.json")
    miles_map = {}
    if summary_p.exists():
        try:
            s = json.loads(summary_p.read_text())
            miles_map = s.get("per_ride_new_miles_est", {}) or {}
        except Exception:
            miles_map = {}

    miles_sum = 0.0
    for aid in ids:
        # hexes
        fp = cov_dir / f"{aid}.json"
        if fp.exists():
            try:
                for c in json.loads(fp.read_text()):
                    cells.add(str(c))
            except Exception:
                pass
        # miles
        try:
            miles_sum += float(miles_map.get(str(aid), 0.0))
        except Exception:
            pass

    features = [
        {"type": "Feature", "id": c, "properties": {}, "geometry": _hex_polygon(c)}
        for c in cells
    ]
    return jsonify(
        {
            "miles_est": round(miles_sum, 2),
            "geojson": {"type": "FeatureCollection", "features": features},
        }
    )


@app.route("/new_by_day")
def new_by_day():
    """
    Convenience endpoint: new miles for a single YYYY-MM-DD day (+ optional bike).
    """
    day = _parse_date_yyyy_mm_dd(request.args.get("date", ""))
    bike = request.args.get("bike")
    if not day:
        return jsonify({"error": "missing date=YYYY-MM-DD"}), 400
    return new_by_range()

    ########


@app.route("/coverage")
def coverage():
    return jsonify(coverage_geojson())


@app.route("/activity/<activity_id>/new")
def activity_new(activity_id):
    fc, miles = activity_new_geojson(activity_id)
    return jsonify({"new_miles_est": round(miles, 2), "geojson": fc})


@app.route("/gear")
def gear_tracker():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))

    raw = _load_json(resolver.gear_path(user_id), default=[])
    gear_data = _normalize_gear_data(raw)  # ensures .get("bikes", []) exists
    return render_template("gear_tracker_page.html", gear=gear_data["bikes"])


@app.route("/gear_tracker_page")
def gear_tracker_page():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))

    with open(resolver.gear_path(user_id), "r", encoding="utf-8") as f:
        data = json.load(f)
    bikes = data.get("bikes", []) if isinstance(data, dict) else data
    return render_template("gear_tracker_page.html", gear=bikes)


### TILES APP ROUTE####
@app.route("/tiles/<path:filename>")
def tiles(filename):
    return send_from_directory("tiles", filename)



@app.route("/gear/data", methods=["GET"])
def gear_data_api():
    units = request.args.get("units", "imperial")
    start_str = request.args.get("start")
    end_str = request.args.get("end")

    start_dt = parse_dt(start_str) if start_str else None
    end_dt = parse_dt(end_str) if end_str else None

    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    activities = load_activities(user_id)
    gear = load_gear(user_id)
    bikes = [b for b in gear.get("bikes", []) if b.get("status", "active") != "removed"]

    summaries = []
    for b in bikes:
        try:
            s = get_bike_usage(b, activities, start=start_dt, end=end_dt, units=units)
            summaries.append(s)
        except Exception as e:
            summaries.append(
                {"bike_id": b.get("id"), "bike_name": b.get("name"), "error": str(e)}
            )

    return jsonify(
        {
            "ok": True,
            "units": units,
            "start": start_str,
            "end": end_str,
            "count": len(summaries),
            "bikes": summaries,
        }
    )


@app.route("/api/pmtiles")
def list_pmtiles():
    """List .pmtiles files we can inspect."""
    if not os.path.isdir(TILES_DIR):
        return jsonify([])
    files = [f for f in os.listdir(TILES_DIR) if f.endswith(".pmtiles")]
    return jsonify(sorted(files))


@app.route("/api/pmtiles/<path:name>/metadata")
def pmtiles_metadata(name):
    """
    Return vector_layers and quick flags for fields we care about.
    Uses a range-reader callable as required by pmtiles.Reader.
    """
    if Reader is None:
        abort(500, description="pmtiles Python package not available")

    path = os.path.join(TILES_DIR, name)
    if not os.path.isfile(path):
        abort(404, description=f"Not found: {name}")

    try:
        with open(path, "rb") as fp:
            # Reader needs a callable: read_range(offset, length) -> bytes
            def read_range(offset: int, length: int, _fp=fp) -> bytes:
                _fp.seek(offset)
                return _fp.read(length)

            r = Reader(read_range)
            meta = r.metadata() or {}
    except Exception as e:
        abort(500, description=f"Failed to read metadata: {e}")

    vls = meta.get("vector_layers") or []
    layer_summaries = []
    for vl in vls:
        fields = vl.get("fields") or {}
        layer_summaries.append(
            {
                "id": vl.get("id"),
                "field_keys_sample": sorted(list(fields.keys()))[:30],
            }
        )

    def has_field(k: str) -> bool:
        return any((vl.get("fields") or {}).get(k) is not None for vl in vls)

    return jsonify(
        {
            "file": name,
            "has_surface": has_field("surface"),
            "has_tracktype": has_field("tracktype"),
            "has_highway": has_field("highway"),
            "vector_layers": layer_summaries,
        }
    )


# ---------------------------------------------------------------------


@app.route("/debug/draw")
def debug_draw():
    # Standalone page with no base template or extra scripts — isolates the issue
    return render_template("draw_debug.html")


def _read_submissions():
    """Reads newline-delimited JSON from data/requests.ndjson into a list."""
    items = []
    if os.path.exists(REQUESTS_PATH):
        with open(REQUESTS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except Exception:
                    continue
    return items


def _write_submissions(items):
    """Overwrites the file with the provided list (keeps the simple NDJSON format)."""
    os.makedirs(os.path.dirname(REQUESTS_PATH), exist_ok=True)
    with open(REQUESTS_PATH, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it) + "\n")


# Accept both /submit_change (existing) and /api/submissions (new) for POST
@app.route("/api/submissions", methods=["POST"])
def api_submissions_post():
    data = request.get_json(silent=True) or {}
    # Normalize a few fields for review UI
    if "id" not in data:
        data["id"] = str(uuid.uuid4())
    data.setdefault("status", "pending")
    data.setdefault("ts", datetime.datetime.utcnow().isoformat() + "Z")
    # Reuse same storage file
    existing = _read_submissions()
    existing.append(data)
    _write_submissions(existing)
    return jsonify({"ok": True, "id": data["id"]})


@app.route("/api/submissions", methods=["GET"])
def api_submissions_list():
    """
    Optional query params:
      status=pending|approved|rejected|published (default: all)
      limit=200
    """
    status = request.args.get("status")
    limit = int(request.args.get("limit", "200"))
    items = _read_submissions()
    if status:
        items = [it for it in items if str(it.get("status")) == status]
    # newest-first helps in review
    items.sort(key=lambda it: it.get("ts", ""), reverse=True)
    return jsonify(items[:limit])


@app.route("/api/submissions/<sid>/status", methods=["POST"])
def api_submissions_set_status(sid):
    """
    Body: {"status": "approved"|"rejected"|"published"}
    """
    body = request.get_json(silent=True) or {}
    new_status = body.get("status")
    if new_status not in ("pending", "approved", "rejected", "published"):
        return jsonify({"ok": False, "error": "Invalid status"}), 400

    items = _read_submissions()
    found = False
    for it in items:
        if str(it.get("id")) == str(sid):
            it["status"] = new_status
            found = True
            break
    if not found:
        return jsonify({"ok": False, "error": "Not found"}), 404

    _write_submissions(items)
    return jsonify({"ok": True, "id": sid, "status": new_status})


# Admin review page: simple template that loads a map + calls /api/submissions
@app.route("/review")
def review_page():
    # You can add a super-light auth check here if you want:
    # if not session.get("is_admin"): return "Unauthorized", 401
    return render_template("review.html")


####ADDED 8/25/2025 11:08am

#### ROute to submit changes on map####


@app.route("/submit_change", methods=["POST"])
def submit_change():
    data = request.get_json(silent=True) or {}
    change_type = data.get("change_type")
    if change_type not in ("surface_update", "new_trail", "other"):
        return jsonify({"ok": False, "error": "Unknown change_type"}), 400

    os.makedirs(os.path.dirname(REQUESTS_PATH), exist_ok=True)
    record = {"ts": datetime.datetime.utcnow().isoformat() + "Z", **data}
    with open(REQUESTS_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    return jsonify({"ok": True})


######


def _list_pmtiles():
    folder = os.path.join(app.root_path, "tiles")
    try:
        return sorted([f for f in os.listdir(folder) if f.endswith(".pmtiles")])
    except FileNotFoundError:
        return []


@app.route("/tiles/<path:filename>")
def serve_tiles(filename):
    return send_from_directory("tiles", filename)


@app.route("/sport/<sport>/map")
def map_page(sport):
    files = _list_pmtiles()
    return render_template("map.html", files=files, sport=sport)


@app.route("/map")
def map_page_default():
    files = _list_pmtiles()
    return render_template("map.html", files=files, sport=None)


@app.route("/__routes")
def __routes():
    lines = []
    for r in app.url_map.iter_rules():
        lines.append(f"{r.endpoint:20s} -> {str(r)}")
    return "<pre>" + "\n".join(sorted(lines)) + "</pre>"


### all time stats and compare stats routedss


# ---------- ALL TIME STATS (the big stats page) ----------
# ---------- ALL TIME STATS (the big stats page) ----------


@app.route("/all_time_stats", methods=["GET"])
def all_time_stats():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))

    stats_dict, yearly_breakdown = aggregate_better_stats(user_id)

    # Convert dict → object for template compatibility
    stats = SimpleNamespace(**stats_dict)

    return render_template(
        "all_time_stats.html",
        stats=stats,
        yearly_breakdown=yearly_breakdown,
        sport=request.args.get("sport"),
    )


# ---------- COMPARE STATS (the history/compare page) ----------
@app.route("/lifetime")
def lifetime():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))

    with open(resolver.activities_path(user_id)) as f:
        activities = json.load(f)

    return render_template("history.html", activities=activities)


@app.route("/compare")
def compare_alias():
    sport = request.args.get("sport")
    return redirect(url_for("lifetime", sport=sport))


### new front pages and navigation

# Map chosen sport -> Strava activity types
SPORT_MAP = {
    "bike": {"Ride", "VirtualRide", "EBikeRide"},
    "run": {"Run"},
    "hikewalk": {"Hike", "Walk"},
    "swim": {"Swim"},
    "lift": {"WeightTraining"},
    "total": set(),  # empty set => no filter (all sports)
}

LABELS = {
    "bike": "Bike",
    "run": "Run",
    "hikewalk": "Hike / Walk",
    "swim": "Swim",
    "lift": "Lifting",
    "total": "Total Stats (All Sports)",
}


def sport_filter_from_request(req):
    key = (req.args.get("sport") or "total").lower()
    if key not in SPORT_MAP:
        key = "total"
    return key, SPORT_MAP[key]


@app.route("/get_summary")
def get_summary():
    import json, datetime

    units = request.args.get("units", "metric")  # metric | imperial
    since = request.args.get(
        "since", "all"
    )  # all | week | month | 3months | year | 3years | 5years | 10years | custom
    custom_date_str = request.args.get("custom_date")  # YYYY-MM-DD
    end_date_str = request.args.get("end_date")  # YYYY-MM-DD

    # load activities
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))

    with open(resolver.activities_path(user_id)) as f:
        data = json.load(f)

    def parse_date(s):
        if not s:
            return None
        try:
            return datetime.datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    today = datetime.date.today()
    end_date = parse_date(end_date_str) or today

    # choose start date
    if since == "custom":
        start_date = parse_date(custom_date_str)
    elif since == "week":
        start_date = end_date - datetime.timedelta(days=7)
    elif since == "month":
        start_date = end_date - datetime.timedelta(days=30)
    elif since == "3months":
        start_date = end_date - datetime.timedelta(days=90)
    elif since == "year":
        start_date = end_date - datetime.timedelta(days=365)
    elif since == "3years":
        start_date = end_date - datetime.timedelta(days=3 * 365)
    elif since == "5years":
        start_date = end_date - datetime.timedelta(days=5 * 365)
    elif since == "10years":
        start_date = end_date - datetime.timedelta(days=10 * 365)
    else:
        start_date = None  # "all"

    # filter activities by local start_date
    def act_date(a):
        # prefer local; fall back to UTC
        dt_str = a.get("start_date_local") or a.get("start_date")
        if not dt_str:
            return None
        # both are ISO; local in your file ends with Z too
        try:
            return datetime.datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").date()
        except Exception:
            return None

    if start_date:
        filtered = [
            a for a in data if (d := act_date(a)) and (start_date <= d <= end_date)
        ]
    else:
        filtered = data

    # basic aggregates
    total_distance_km = sum((a.get("distance", 0.0) or 0.0) / 1000.0 for a in filtered)
    total_elev_m = sum(a.get("total_elevation_gain", 0.0) or 0.0 for a in filtered)
    total_time_hr = sum((a.get("moving_time", 0) or 0) / 3600.0 for a in filtered)

    avg_speed_kmh = (total_distance_km / total_time_hr) if total_time_hr > 0 else 0.0
    longest_km = (
        max(((a.get("distance", 0) or 0) / 1000.0) for a in filtered)
        if filtered
        else 0.0
    )
    hottest_c = (
        max((a.get("average_temp", 0) or 0) for a in filtered) if filtered else 0.0
    )
    max_elev_m = (
        max((a.get("total_elevation_gain", 0) or 0) for a in filtered)
        if filtered
        else 0
    )
    highest_avg_elev_m = (
        max((a.get("elevation_high", 0) or 0) for a in filtered) if filtered else 0
    )

    # average hours/week over the span you actually rode
    days_span = max(
        1,
        (
            end_date
            - (
                start_date
                or (filtered and min(act_date(a) for a in filtered) or end_date)
            )
        ).days,
    )
    weeks = max(1, days_span // 7)
    avg_hours_per_week = total_time_hr / weeks

    # unit convert
    if units == "imperial":
        total_distance = round(total_distance_km * 0.621371, 2)  # miles
        longest_ride = round(longest_km * 0.621371, 2)  # miles
        total_elevation = int(total_elev_m * 3.28084)  # feet
        max_elevation = int(max_elev_m * 3.28084)  # feet
        highest_avg_elev = int(highest_avg_elev_m * 3.28084)  # feet
        avg_speed = round(avg_speed_kmh * 0.621371, 2)  # mph
    else:
        total_distance = round(total_distance_km, 2)  # km
        longest_ride = round(longest_km, 2)  # km
        total_elevation = int(total_elev_m)  # m
        max_elevation = int(max_elev_m)  # m
        highest_avg_elev = int(highest_avg_elev_m)  # m
        avg_speed = round(avg_speed_kmh, 2)  # km/h

    # title
    if since == "custom" and custom_date_str:
        title = f"{custom_date_str} to {end_date.strftime('%Y-%m-%d')} Summary"
    else:
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
        title = titles.get(since, "Summary")

    return jsonify(
        {
            "units": units,
            "since": since,
            "custom_date_str": custom_date_str or "",
            "end_date_str": end_date.strftime("%Y-%m-%d"),
            "title": title,
            "total_distance": total_distance,
            "total_elevation": total_elevation,
            "total_time": round(total_time_hr, 2),
            "avg_speed": avg_speed,
            "longest_ride": longest_ride,
            "hottest_ride": round(hottest_c, 1),
            "max_elevation": max_elevation,
            "highest_avg_elev": highest_avg_elev,
            "avg_hours_per_week": round(avg_hours_per_week, 2),
        }
    )


# --- new routes ---


@app.route("/sport/<sport>")
def sport_hub(sport):
    sport = sport.lower()
    if sport not in LABELS:
        sport = "total"
    return render_template("sport_hub.html", sport=sport, sport_label=LABELS[sport])


@app.route("/elevations", methods=["POST"])
def elevations():
    data = request.get_json(force=True)
    coords = data.get("coords", [])
    if not coords:
        return {"elevations": []}
    locations = [{"latitude": lat, "longitude": lon} for (lon, lat) in coords]
    r = requests.post(
        "https://api.open-elevation.com/api/v1/lookup",
        json={"locations": locations},
        timeout=12,
    )
    r.raise_for_status()
    return {"elevations": [row["elevation"] for row in r.json().get("results", [])]}


@app.template_filter("mmss")
def mmss_filter(sec):
    return fmt_mmss(sec)


from user_data_pullers.strava_keys import (
    CLIENT_ID,
    CLIENT_SECRET,
    THUNDERFOREST_API_KEY,
    ORS_API_KEY,
    WEATHER_API_KEY,
)

from user_data_pullers.gear_helpers import load_gear, save_gear

from collections import Counter, defaultdict
import datetime

from services.custom_metrics import compute_normalized_speed


# --- NEW: Graph Builder page ---
@app.route("/graphs")
def graphs_page():
    user_id = get_user_id()
    path = activities_path(user_id)

    # Load JSON
    with open(path, "r", encoding="utf-8") as f:
        activities = json.load(f)

    # Convert to DataFrame
    df = pd.json_normalize(activities)

    # Basic unique filters
    bikes = sorted(df["gear_id"].dropna().unique().tolist()) if not df.empty else []
    types = sorted(df["type"].dropna().unique().tolist()) if not df.empty else []

    # Define fields for dropdowns
    y_fields = [
        ("distance_km", "Distance (km)"),
        ("elev_m", "Elevation Gain (m)"),
        ("hours", "Moving Time (hours)"),
        ("avg_speed_kmh", "Avg Speed (km/h)"),
        ("temp_C", "Temperature (°C)"),
    ]

    x_fields = [
        ("start_dt", "Date/Time"),
        ("distance_km", "Distance (km)"),
        ("elev_m", "Elevation (m)"),
        ("hours", "Time (h)"),
        ("avg_speed_kmh", "Avg Speed (km/h)"),
        ("temp_C", "Temperature (°C)"),
    ]

    return render_template(
        "graphs.html",
        bikes=bikes,
        types=types,
        x_fields=x_fields,
        y_fields=y_fields,
    )

@app.route("/api/graph_data", methods=["POST"])
def graph_data():
    from flask import request, jsonify

    payload = request.get_json()
    user_id = get_user_id()
    path = activities_path(user_id)

    with open(path, "r", encoding="utf-8") as f:
        activities = json.load(f)

    df = pd.json_normalize(activities)

    # Precompute some fields
    df["distance_km"] = df["distance"] / 1000.0
    df["elev_m"] = df["total_elevation_gain"]
    df["hours"] = df["moving_time"] / 3600.0
    df["avg_speed_kmh"] = df["average_speed"] * 3.6
    df["start_dt"] = pd.to_datetime(df["start_date_local"])
    df["temp_C"] = df.get("average_temp", pd.Series([None] * len(df)))

    # Apply filters
    if payload.get("gear_id"):
        df = df[df["gear_id"] == payload["gear_id"]]
    if payload.get("activity_type"):
        df = df[df["type"] == payload["activity_type"]]
    if payload.get("date_from"):
        df = df[df["start_dt"] >= payload["date_from"]]
    if payload.get("date_to"):
        df = df[df["start_dt"] <= payload["date_to"]]

    x = payload.get("x_axis", "start_dt")
    y = payload.get("y_axis", "distance_km")

    labels = df[x].astype(str).tolist()
    values = df[y].tolist()

    datasets = [{
        "label": f"{y} vs {x}",
        "data": values,
        "borderColor": "#3b82f6",
        "backgroundColor": "#60a5fa",
        "tension": 0.2,
    }]

    return jsonify({
        "labels": labels,
        "datasets": datasets,
        "chart_type": payload.get("chart_type", "line"),
    })


# --- NEW: API endpoint that returns Chart.js-ready JSON ---
@app.route("/api/graph_data", methods=["POST"])
def api_graph_data():
    payload = request.get_json(force=True, silent=True) or {}

    x_axis = payload.get(
        "x_axis", "start_dt"
    )  # not used for all groupings yet, kept for future
    y_axis = payload.get("y_axis", "distance_km")
    grouping = payload.get("grouping", "monthly")  # daily|weekly|monthly|yearly|none
    chart_type = payload.get("chart_type", "line")  # line|bar|scatter
    date_from = payload.get("date_from")  # "YYYY-MM-DD"
    date_to = payload.get("date_to")
    gear_id = payload.get("gear_id")  # optional filter
    act_type = payload.get(
        "activity_type"
    )  # optional filter like "Ride", "VirtualRide"
    only_outdoor = payload.get("only_outdoor", False)

    df = load_activities_df()
    if df.empty:
        return jsonify(
            {
                "labels": [],
                "datasets": [{"label": "No data", "data": []}],
                "chart_type": chart_type,
            }
        )

    # filters
    if date_from:
        try:
            start_d = datetime.fromisoformat(date_from).date()
            df = df[df["date"] >= start_d]
        except Exception:
            pass
    if date_to:
        try:
            end_d = datetime.fromisoformat(date_to).date()
            df = df[df["date"] <= end_d]
        except Exception:
            pass

    if gear_id:
        df = df[df["gear_id"] == gear_id]

    if act_type:
        df = df[df["type"] == act_type]

    if only_outdoor:
        # crude example: exclude Virtual rides by common labels
        df = df[~df["type"].astype(str).str.contains("Virtual", case=False, na=False)]

    # drop rows where y is NaN
    if y_axis not in df.columns:
        return jsonify(
            {
                "labels": [],
                "datasets": [{"label": f"{y_axis} missing", "data": []}],
                "chart_type": chart_type,
            }
        )

    df = df.dropna(subset=[y_axis])

    # grouping / labels / values
    labels, values = group_df(df, grouping, y_axis)

    # Return Chart.js-friendly structure
    return jsonify(
        {
            "chart_type": chart_type,
            "labels": labels,
            "datasets": [
                {
                    "label": f"{y_axis} ({grouping})",
                    "data": values,
                }
            ],
        }
    )


# --- NEW: utilities for graph data ---

LOCAL_TZ = tz.gettz("America/New_York")  # keep consistent with your site


def load_activities_df(path="strava_activities.json"):
    with open(path, "r") as f:
        data = json.load(f)

    # Expecting a list of Strava activity dicts
    df = pd.DataFrame(data)
    if df.empty:
        return df

    # Normalize/derived fields you’ll graph a lot
    # Distance: meters -> km
    if "distance" in df.columns:
        df["distance_km"] = df["distance"].fillna(0) / 1000.0
    else:
        df["distance_km"] = 0.0

    # Elev gain: already meters typically
    if "total_elevation_gain" in df.columns:
        df["elev_m"] = df["total_elevation_gain"].fillna(0)
    else:
        df["elev_m"] = 0.0

    # Moving time: seconds -> hours
    if "moving_time" in df.columns:
        df["hours"] = df["moving_time"].fillna(0) / 3600.0
    else:
        df["hours"] = 0.0

    # Average speed (m/s) -> km/h if present
    if "average_speed" in df.columns:
        df["avg_speed_kmh"] = df["average_speed"].fillna(0) * 3.6
    else:
        df["avg_speed_kmh"] = 0.0

    # Temperature if you synced it later; handle missing
    for k in ["average_temp", "avg_temp", "temp"]:
        if k in df.columns:
            df["temp_C"] = df[k]
            break
    if "temp_C" not in df.columns:
        df["temp_C"] = pd.NA

    # Start date parsing
    # Strava uses ISO8601 strings in UTC typically: "start_date"
    date_col = None
    for col in ["start_date_local", "start_date"]:
        if col in df.columns:
            date_col = col
            break
    if date_col is None:
        df["start_dt"] = pd.NaT
    else:
        df["start_dt"] = pd.to_datetime(
            df[date_col], errors="coerce", utc=True
        ).dt.tz_convert(LOCAL_TZ)

    # Simple type + gear fields
    if "type" not in df.columns:
        df["type"] = "Ride"
    if "sport_type" in df.columns:
        # prefer sport_type if present (Strava newer field)
        df["type"] = df["sport_type"].fillna(df["type"])

    if "gear_id" not in df.columns:
        df["gear_id"] = pd.NA

    # helpful breakdown columns
    df["date"] = df["start_dt"].dt.date
    df["year"] = df["start_dt"].dt.year
    df["month"] = df["start_dt"].dt.to_period("M").astype(str)  # "2025-08"
    df["week"] = (
        df["start_dt"].dt.to_period("W").astype(str)
    )  # ISO week like "2025-08-04/2025-08-10"

    return df


def group_df(df, grouping, y_key):
    """
    grouping: 'daily'|'weekly'|'monthly'|'yearly'|'none'
    y_key: column to aggregate (sum by default)
    """
    if grouping == "daily":
        g = df.groupby("date", dropna=True)[y_key].sum().reset_index()
        labels = g["date"].astype(str).tolist()
    elif grouping == "weekly":
        g = df.groupby("week", dropna=True)[y_key].sum().reset_index()
        labels = g["week"].tolist()
    elif grouping == "monthly":
        g = df.groupby("month", dropna=True)[y_key].sum().reset_index()
        labels = g["month"].tolist()
    elif grouping == "yearly":
        g = df.groupby("year", dropna=True)[y_key].sum().reset_index()
        labels = g["year"].astype(str).tolist()
    else:
        # no grouping → scatter by ride (index)
        g = df.reset_index().rename(columns={"index": "ride_index"})
        g["ride_index"] = g.index + 1
        labels = g["ride_index"].astype(str).tolist()
    values = g[y_key].fillna(0).astype(float).round(3).tolist()
    return labels, values

    # gear dataaaaaaaa


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


# Segment routes
# --- Segments Overview ---
@app.route("/segments")
def segments_overview():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))

    seg_path = resolver.segments_path(user_id)
    if not os.path.exists(seg_path):
        return render_template("segments.html", segments=[])
    with open(seg_path, "r") as f:
        data = json.load(f)

    rows = []
    for seg_id, payload in data.get("segments", {}).items():
        meta = payload.get("metadata", {})
        efforts = payload.get("efforts", [])
        summary = summarize_segment(efforts) if efforts else None
        rows.append({"segment": {"metadata": meta}, "summary": summary})

    # Sort by biggest improvement, then PR
    def _improve(row):
        sm = row["summary"]
        return sm["biggest_improvement"]["delta"] if sm else 0

    def _pr(row):
        sm = row["summary"]
        return sm["pr_time"] if sm and sm["pr_time"] is not None else 10**9

    rows.sort(key=lambda r: (-_improve(r), _pr(r)))
    return render_template("segments.html", segments=rows)


# --- Segment Detail ---
@app.route("/segments/<int:segment_id>")
def segment_detail(segment_id):
    sid = str(segment_id)
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))


    seg_path = resolver.segments_path(user_id)
    if not os.path.exists(seg_path):
        return render_template("segment_detail.html", segment=None, efforts=[], summary=None)

    with open(seg_path, "r") as f:
        data = json.load(f)

    entry = data.get("segments", {}).get(sid)
    if not entry:
        return render_template("segment_detail.html", segment=None, efforts=[], summary=None)

    efforts = entry.get("efforts", [])
    summary = summarize_segment(efforts) if efforts else None

    return render_template(
        "segment_detail.html",
        segment=entry,
        efforts=efforts,
        summary=summary,
    )


# --- Refresh Segments ---
@app.route("/segments/refresh")
def segments_refresh():
    try:
        from segments_fetch import fetch_all_segments

        fetch_all_segments(full_refresh=False, include_streams=True)
        flash("Segments refreshed.", "success")
    except Exception as e:
        flash(f"Refresh failed: {e}", "error")
    return redirect(url_for("segments_overview"))


# -------------------- Maintenance Tracker routes --------------------





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
    if isinstance(s, dt):  # use datetime class imported as dt
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
    return f"{prefix}_{int(time.time()*1000)}"


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
        "moving_time_s": max(0.0, now_cum["moving_time_s"] - baseline["moving_time_s"]),
    }


@app.template_filter("format_seconds")
def format_seconds(seconds):
    seconds = int(seconds or 0)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    sec = seconds % 60
    return f"{hours}:{minutes:02}:{sec:02}"


def parse_dt(dt_raw: str):
    """Parse many Strava-ish formats; last‑ditch: use just the date part."""
    if not dt_raw:
        return None
    # Try Z suffix
    try:
        if dt_raw.endswith("Z"):
            return datetime.strptime(dt_raw, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        pass
    # Try ISO with offset or naive ISO
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(dt_raw, fmt)
        except Exception:
            pass
    # Fallback: just the date
    try:
        y, m, d = map(int, dt_raw.split("T")[0].split("-"))
        return datetime(y, m, d)
    except Exception:
        return None


@app.route("/heatmaps")
def heatmaps():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))


    activities_path = resolver.activities_path(user_id)
    if not os.path.exists(activities_path):
        return render_template(
            "heatmaps.html", segments=[], years=[], bikes=[], months=list(range(1, 13))
        )

    with open(activities_path) as f:
        data = json.load(f)

    # Optional: map gear_id => friendly name
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

    segments = []
    years_set = set()
    bikes_map = {}

    for a in data:
        enc = (a.get("map") or {}).get("summary_polyline")
        if not enc:
            continue

        dt = parse_dt(a.get("start_date_local") or a.get("start_date"))
        # If date still missing, we’ll include the segment but it won’t match date filters
        try:
            coords = polyline.decode(enc)
        except Exception:
            continue

        year = dt.year if dt else None
        month = dt.month if dt else None
        weekday_num = dt.weekday() if dt else None  # 0=Mon..6=Sun

        bike_id = a.get("gear_id") or "unknown"
        bike_name = bike_name_by_id.get(
            bike_id, ("No bike selected" if bike_id == "unknown" else bike_id)
        )

        segments.append(
            {
                "coords": coords,
                "year": year,  # int or None
                "month": month,  # 1..12 or None
                "weekday_num": weekday_num,  # 0..6 or None
                "bike_id": bike_id,  # "unknown" allowed
                "bike_name": bike_name,
            }
        )

        if year is not None:
            years_set.add(year)
        bikes_map[bike_id] = bike_name

    years = sorted(years_set, reverse=True)
    bikes = sorted(
        [(bid, name) for bid, name in bikes_map.items() if bid != "unknown"],
        key=lambda x: x[1].lower(),
    )
    if "unknown" in bikes_map:
        bikes.append(("unknown", "No bike selected"))

    return render_template(
        "heatmaps.html",
        segments=segments,
        years=years,
        bikes=bikes,
        months=list(range(1, 13)),
    )


@app.route("/test_bikes")
def test_bikes():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return {"error": "not logged in"}, 401

    tokens = resolver.load_tokens(user_id)
    access_token = tokens.get("access_token")
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get("https://www.strava.com/api/v3/athlete/gear", headers=headers)
    return response.json()


@app.route("/activity/<int:activity_id>")
def activity_detail(activity_id):
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))


    with open(resolver.activities_path(user_id), "r") as f:
        all_activities = json.load(f)

    # Find the activity by ID
    activity = next((a for a in all_activities if a.get("id") == activity_id), None)

    if not activity:
        return "Activity with ID {activity_id} not found.", 404

    return render_template("activity_detail.html", activity=activity)


@app.route("/activities")
def activity_list():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))


    with open(resolver.activities_path(user_id), "r") as f:
        activities = json.load(f)

    return render_template("activities.html", activities=activities)


@app.route("/add_gear", methods=["POST"])
def add_gear():
    new_gear = {
        "name": request.form["name"],
        "type": request.form["type"],
        "start_date": request.form["start_date"],
        "elevation": int(request.form["elevation"]),
        "hours": float(request.form["hours"]),
        "retired": False,
    }

    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))


    gear_path = resolver.gear_path(user_id)
    with open(gear_path) as f:
        gear = json.load(f)

    gear.append(new_gear)

    with open(gear_path, "w") as f:
        json.dump(gear, f, indent=2)

    return redirect("/gear")



@app.route("/update_gear/<int:index>/<action>", methods=["POST"])
def update_gear(index, action):
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))


    gear_path = resolver.gear_path(user_id)
    with open(gear_path) as f:
        gear = json.load(f)

    if action == "retire":
        gear[index]["retired"] = True
    elif action == "reactivate":
        gear[index]["retired"] = False
    elif action == "remove":
        gear.pop(index)
    elif action == "reassign":
        new_bike = request.form["bike"]
        gear[index]["bike"] = new_bike

    with open(gear_path, "w") as f:
        json.dump(gear, f, indent=2)

    return redirect("/gear")


@app.route("/test_weather")
def test_weather():
    from user_data_pullers.strava_keys import weather_api_key
    import requests

    lat = 40.5392
    lon = -80.1803
    date = "2024-07-11"

    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{lat},{lon}/{date}?unitGroup=us&key={weather_api_key}&include=hours"
    )

    response = requests.get(url)
    data = response.json()
    return data["days"][0]["hours"][9]  # 9AM weather snapshot


@app.route("/get_thunderforest_key")
def get_thunderforest_key():
    return jsonify({"key": THUNDERFOREST_API_KEY})


@app.route("/route_builder")
def route_builder():
    return render_template("route_builder.html", ors_api_key=ORS_API_KEY)


@app.route("/get_ors_key")
def get_ors_key():
    return jsonify({"key": ORS_API_KEY})


@app.route("/authorize")
def authorize():
    return redirect(
        f"https://www.strava.com/oauth/authorize?client_id={CLIENT_ID}&response_type=code&redirect_uri=http://127.0.0.1:5000/exchange&scope=read_all,activity:read_all"
    )


@app.route("/bike_split")
def bike_split():
    bike_id = request.args.get("bike_id", None)
    left = request.args.get("left", "stats")
    right = request.args.get("right", "heatmap")
    start_date = request.args.get("start_date", None)
    end_date = request.args.get("end_date", None)

    return render_template(
        "bike_split.html",
        bike_id=bike_id,
        left=left,
        right=right,
        start_date=start_date,
        end_date=end_date
    )


@app.route("/exchange")
def exchange():
    """
    OAuth callback from Strava.
    Strava redirects here with ?code=...
    Exchange code for tokens, save them, store user_id in session.
    """
    code = request.args.get("code")
    if not code:
        return "Missing ?code from Strava", 400

    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    tokens = resp.json()
    if "athlete" not in tokens:
        return f"Error from Strava: {tokens}", 400

    user_id = str(tokens["athlete"]["id"])
    resolver.save_tokens(user_id, tokens)  # save per-user tokens
    session["user_id"] = user_id  # save logged-in user

    return redirect(url_for("history"))


@app.route("/rankings")
def rankings():
    return render_template("rankings.html")


@app.route("/get_bike_list")
def get_bike_list():
    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return []

    with open(resolver.activities_path(user_id)) as f:
        data = json.load(f)
        bikes = list({act["gear_id"] for act in data if "gear_id" in act})
        return bikes


@app.route("/personal_best")
def personal_best():
    import datetime
    import json

    user_id = get_current_user_id() or resolver.get_user_id()
    if not user_id:
        return redirect(url_for("index"))


    with open(resolver.activities_path(user_id), "r") as f:
        data = json.load(f)

    longest_ride = (0, None)
    most_elevation = (0, None)
    fastest_avg_speed = (0, None)
    coldest_temp = (float("inf"), None)
    hottest_temp = (float("-inf"), None)
    highest_elevation = (0, None)

    for activity in data:
        distance = activity.get("distance", 0) / 1609.34
        elevation = activity.get("total_elevation_gain", 0)
        if elevation is not None:
            elevation *= 3.28084

        speed = activity.get("average_speed", 0) * 2.23694
        temp = activity.get("average_temp", None)
        max_alt = activity.get("elev_high", 0)
        if max_alt is not None:
            max_alt *= 3.28084

        date = activity.get("start_date_local", "")[:10]
        try:
            formatted_date = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%b %d, %Y")
        except Exception:
            formatted_date = "Unknown"

        if distance > longest_ride[0]:
            longest_ride = (round(distance, 1), formatted_date)
        if elevation > most_elevation[0]:
            most_elevation = (int(elevation), formatted_date)
        if speed > fastest_avg_speed[0]:
            fastest_avg_speed = (round(speed, 1), formatted_date)
        if temp is not None:
            if temp < coldest_temp[0]:
                coldest_temp = (temp, formatted_date)
            if temp > hottest_temp[0]:
                hottest_temp = (temp, formatted_date)
        if max_alt > highest_elevation[0]:
            highest_elevation = (int(max_alt), formatted_date)

    stats = {
        "longest_ride": longest_ride,
        "most_elevation": most_elevation,
        "fastest_avg_speed": fastest_avg_speed,
        "coldest_temp": (
            (int(coldest_temp[0]), coldest_temp[1]) if coldest_temp[1] else ("N/A", "")
        ),
        "hottest_temp": (
            (int(hottest_temp[0]), hottest_temp[1]) if hottest_temp[1] else ("N/A", "")
        ),
        "highest_elevation": highest_elevation,
    }

    return render_template("personal_best.html", stats=stats)

# LOAD MAINTENANCE ROUTES (must be LAST)
from template_helpers.maintenance_routes import maintenance_bp

app.register_blueprint(maintenance_bp)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
