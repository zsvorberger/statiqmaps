from flask import (
    Flask,
    redirect,
    request,
    session,
    url_for,
    render_template,
    jsonify,
    send_from_directory,
    send_file,
    abort,
    flash,
)

import os
import io
import json
import time
import math
import copy
import uuid
import calendar
import requests
import smtplib
import datetime
import subprocess
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from twilio.rest import Client as TwilioClient
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
import pandas as pd
import numpy as np
import polyline
import threading
import xml.etree.ElementTree as ET
from pathlib import Path
from dateutil import tz
from datetime import datetime as dt
from types import SimpleNamespace
from collections import defaultdict, OrderedDict
from urllib.parse import urlencode
import user_data_pullers.resolver as resolver
from werkzeug.utils import secure_filename
from email.message import EmailMessage

try:
    import fcntl
except ImportError:
    fcntl = None

_CACHE_BUILD_LOCK = threading.Lock()
_CACHE_BUILD_STATES = {}
_USER_BUILD_LOCKS = {}
_USER_BUILD_LOCKS_LOCK = threading.Lock()


# === Local imports ===
from services.geo_index import (
    coverage_geojson,
    activity_new_geojson,
    rebuild_indexes,
    _hex_polygon,
)
from user_data_pullers.segments_utils import summarize_segment, parse_date as fmt_mmss
from user_data_pullers.stats_helpers import aggregate_better_stats
from user_data_pullers.gear_helpers import load_gear, get_bike_usage, parse_dt
from user_data_pullers.foot_stats_helpers import aggregate_foot_stats, load_activities
from services.stats import load_summary
from user_data_pullers.strava_keys import ORS_API_KEY
from user_data_pullers.fetch_data import main as fetch_data_main
from user_data_pullers.activity_cache import (
    load_activities_cached,
    invalidate_activities_cache,
)
from services.stats_builder import STATS_CACHE_DIR, build_stats_bundle, load_stats_bundle, _sanitize_for_json
from services.bike_utils import (
    BIKE_SPORT_TYPES,
    OFFROAD_SPORT_TYPES,
    ROAD_SPORT_TYPES,
    activity_surface as _activity_surface_helper,
    bike_label as _bike_label_helper,
    frame_label as _frame_label_helper,
    is_road_frame as _is_road_frame_helper,
    load_gear_lookup as _load_gear_lookup_base,
    normalize_gear_data as _normalize_gear_data_base,
)
from services.personal_bests import build_personal_best_sections
from services.summary_builder import build_summary_payload, predefined_ranges
from services.heatmap_builder import build_heatmap_segments, load_heatmap_segments
from services.unique_miles_service import build_unique_miles_context
from services.activity_frame_cache import (
    frame_lock_path,
    load_activity_frame,
    save_activity_frame,
)
from services.yearly_stats import (
    build_yearly_detail,
    build_yearly_review_payload,
    normalize_yearly_breakdown,
    parse_activity_dt,
)
from jobs.background_jobs import job_runner
from utils.request_cache import request_cache_context, memoize_request
from utils.user_scope import get_current_app_user_id, get_strava_id_for_user
from utils.validation import parse_yyyy_mm_dd, is_valid_date_range, is_allowed
from utils.rate_limit import check_rate_limit
from utils.file_lock import file_lock
from environment.environment_service import (
    DEFAULT_VARIABLES,
    get_air_quality_forecast_hourly,
    get_air_quality_historical_hourly,
    get_available_variables,
    get_weather_forecast_hourly as get_env_weather_forecast_hourly,
    get_weather_historical_daily as get_env_weather_historical_daily,
    get_weather_historical_hourly as get_env_weather_historical_hourly,
)
from weather.weather_service import (
    SUPPORTED_VARIABLES,
    get_forecast_hourly,
    get_historical_daily,
    get_historical_hourly,
)


###LOGINNNN
from flask_login import LoginManager, login_required
from flask_login import current_user
from auth_routes import auth_bp, init_auth
from models.models_users import (
    init_users_db,
    get_user_by_id,
    row_to_user,
    unlink_strava_account,
    update_strava_oauth,
    close_db,
    create_reminder,
    delete_reminder,
    get_reminder,
    list_reminders,
    update_reminder_status,
)





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
SUMMARY_PATH = Path("data/coverage/coverage_summary.json")
NEW_HEX_DIR = Path("data/coverage/new_hexes")
CACHE_DIR = Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
YEARLY_CACHE_VERSION = 4

STATS_BUILD_STATUS_DIR = Path("users_data") / "stats_build_status"

PMTILES_NAMES_FILE = Path(BASE_DIR) / "pmtilesnames.csv"
PMTILES_URLS_FILE = Path(BASE_DIR) / "pmtilesurls.csv"
DEFAULT_PMTILES_BASE_URL = "https://pub-8a2831e00ec443fda66477330bd94460.r2.dev"
_PMTILES_BASE_URL = None
_PMTILES_ASSET_URLS = None
_PMTILES_NAMES_CACHE = None

QUESTIONS_DATA_FILE = os.path.join(BASE_DIR, "data", "questions.json")
os.makedirs(os.path.dirname(QUESTIONS_DATA_FILE), exist_ok=True)
QUESTIONS_LOCK = threading.Lock()
INFO_DATA_FILE = os.path.join(BASE_DIR, "data", "info_posts.json")
os.makedirs(os.path.dirname(INFO_DATA_FILE), exist_ok=True)
INFO_LOCK = threading.Lock()
CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL", "statiqlab@gmail.com")
FEEDBACK_EMAIL = os.environ.get("FEEDBACK_EMAIL", "statiqlabs@gmail.com")
CONTACT_LOG_FILE = os.path.join(BASE_DIR, "data", "contact_messages.log")
FEEDBACK_LOG_FILE = os.path.join(BASE_DIR, "data", "feedback_messages.log")
os.makedirs(os.path.dirname(CONTACT_LOG_FILE), exist_ok=True)

_COVERAGE_RANGE_CACHE: OrderedDict = OrderedDict()
_NEW_RANGE_CACHE: OrderedDict = OrderedDict()
_COVERAGE_CACHE_VERSION = {}
_COVERAGE_CACHE_LIMIT = 40

_COVERAGE_SUMMARY = {}
_ACTIVITY_LOOKUP = {}
_GEAR_LOOKUP = {}


def _load_pmtiles_url_config():
    env_value = os.environ.get("PMTILES_BASE_URL", "").strip()
    base = env_value or None
    assets = {}
    if PMTILES_URLS_FILE.exists():
        try:
            with open(PMTILES_URLS_FILE, encoding="utf-8") as f:
                for raw in f:
                    stripped = raw.split("#", 1)[0].strip()
                    if not stripped:
                        continue
                    parts = stripped.split()
                    if len(parts) == 1:
                        if not base:
                            base = parts[0]
                        continue
                    name = parts[0]
                    url = parts[-1]
                    if url:
                        assets[name] = url
        except Exception:
            pass
    if not base:
        fallback = DEFAULT_PMTILES_BASE_URL.strip()
        if fallback:
            base = fallback
    return base, assets


def _ensure_pmtiles_url_config():
    global _PMTILES_BASE_URL, _PMTILES_ASSET_URLS
    if _PMTILES_BASE_URL is not None and _PMTILES_ASSET_URLS is not None:
        return
    base, assets = _load_pmtiles_url_config()
    if _PMTILES_BASE_URL is None:
        _PMTILES_BASE_URL = base
    if _PMTILES_ASSET_URLS is None:
        _PMTILES_ASSET_URLS = assets or {}


def _parse_pmtiles_names():
    if not PMTILES_NAMES_FILE.exists():
        return []
    names = []
    try:
        with open(PMTILES_NAMES_FILE, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split()
                if not parts:
                    continue
                name = parts[-1]
                if name:
                    names.append(name)
    except Exception:
        pass
    return sorted(names)


def get_pmtiles_base_url():
    _ensure_pmtiles_url_config()
    return _PMTILES_BASE_URL


def get_pmtiles_asset_urls():
    _ensure_pmtiles_url_config()
    return _PMTILES_ASSET_URLS or {}


def get_pmtiles_names():
    global _PMTILES_NAMES_CACHE
    if _PMTILES_NAMES_CACHE is None:
        _PMTILES_NAMES_CACHE = _parse_pmtiles_names()
    return _PMTILES_NAMES_CACHE


def _pmtiles_http_url(name):
    base = get_pmtiles_base_url()
    if not base:
        return None
    clean_name = os.path.basename(name)
    base = base.rstrip("/")
    return f"{base}/{clean_name}"


def _pmtiles_protocol_url(name):
    http_url = _pmtiles_http_url(name)
    if not http_url:
        return None
    return f"pmtiles://{http_url}"

app = Flask(__name__)
app.secret_key = "your_secret_key_here"  # <-- replace with real secret key
##LOGINNNN
app.secret_key = "dev-secret-change-later"

login_manager = LoginManager()
login_manager.init_app(app)

reminder_scheduler = BackgroundScheduler(daemon=True, timezone=LOCAL_TZ)
reminder_scheduler.start()
REMINDER_JOB_PREFIX = "reminder_"


def _parse_reminder_datetime(value):
    if isinstance(value, dt):
        return value
    if isinstance(value, datetime.datetime):
        return dt.fromtimestamp(value.timestamp())
    if isinstance(value, str):
        try:
            return dt.fromisoformat(value)
        except ValueError:
            pass
    return None


def _get_twilio_client():
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN") or os.environ.get("TWILIO_API_KEY")
    if not account_sid or not auth_token:
        raise RuntimeError("Twilio credentials are not configured.")
    return TwilioClient(account_sid, auth_token)


def _send_twilio_sms(phone_number, message):
    from_number = os.environ.get("TWILIO_PHONE_NUMBER")
    if not from_number:
        raise RuntimeError("TWILIO_PHONE_NUMBER is not configured.")
    client = _get_twilio_client()
    client.messages.create(body=message, from_=from_number, to=phone_number)


def _schedule_reminder_job(reminder_id, send_time):
    run_time = _parse_reminder_datetime(send_time)
    if not run_time:
        return
    if run_time.tzinfo is None:
        run_time = run_time.replace(tzinfo=LOCAL_TZ)
    job_id = f"{REMINDER_JOB_PREFIX}{reminder_id}"
    reminder_scheduler.add_job(
        send_reminder_job,
        "date",
        run_date=run_time,
        args=[reminder_id],
        id=job_id,
        replace_existing=True,
    )


def _load_pending_reminders():
    pending = list_reminders(statuses=["scheduled"])
    now = dt.now(LOCAL_TZ)
    for reminder in pending:
        send_time = _parse_reminder_datetime(reminder.get("send_time"))
        if not send_time:
            continue
        if send_time.tzinfo is None:
            send_time = send_time.replace(tzinfo=LOCAL_TZ)
        if send_time <= now:
            _schedule_reminder_job(reminder["id"], now)
        else:
            _schedule_reminder_job(reminder["id"], send_time)


def send_reminder_job(reminder_id):
    reminder = get_reminder(reminder_id)
    if not reminder:
        return
    if reminder["status"] != "scheduled":
        return
    send_time = _parse_reminder_datetime(reminder["send_time"])
    if send_time and send_time.tzinfo is None:
        send_time = send_time.replace(tzinfo=LOCAL_TZ)
    if send_time and send_time > dt.now(LOCAL_TZ):
        _schedule_reminder_job(reminder_id, send_time)
        return
    try:
        _send_twilio_sms(reminder["phone_number"], reminder["message"])
        update_reminder_status(reminder_id, "sent")
    except Exception as exc:
        app.logger.error("Reminder send failed: %s", exc)
        update_reminder_status(reminder_id, "error")


@app.teardown_appcontext
def _teardown_user_db(exception=None):
    close_db(exception)


@app.before_request
def _init_request_cache():
    ctx = request_cache_context()
    ctx.__enter__()
    request._cache_ctx = ctx


@app.teardown_request
def _teardown_request_cache(exc):
    ctx = getattr(request, "_cache_ctx", None)
    if ctx:
        if exc is None:
            ctx.__exit__(None, None, None)
        else:
            ctx.__exit__(type(exc), exc, getattr(exc, "__traceback__", None))
        request._cache_ctx = None



######## ROUTES #############
ROUTES_DATA_FILE = os.path.join("data", "routes_data.json")
ROUTE_EXPORT_FORMATS = ("geojson", "gpx", "tcx")

refresh_jobs = {}
refresh_lock = threading.RLock()
REFRESH_STATUS_DIR = Path("users_data") / "refresh_status"
REFRESH_ESTIMATE_SECONDS = 90
stats_build_jobs = {}
stats_build_lock = threading.Lock()
STATS_BUILD_ESTIMATE_SECONDS = 120
CACHE_MAX_AGE_ALL_TIME = 30 * 24 * 60 * 60
CACHE_MAX_AGE_FOOT = 30 * 60
CACHE_MAX_AGE_PERSONAL_BEST = 3 * 60 * 60
CACHE_MAX_AGE_LIFETIME = 10 * 60
CACHE_MAX_AGE_GEAR = 10 * 60
ACTIVITY_PAGE_SIZE = 25
SYNC_INLINE = True

SPORT_MAP = {
    "bike": {"Ride", "VirtualRide", "EBikeRide"},
    "run": {"Run"},
    "hikewalk": {"Hike", "Walk"},
    "swim": {"Swim"},
    "lift": {"WeightTraining"},
    "total": set(),
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


def _lock_file(file_obj, shared=True):
    if fcntl:
        flag = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        fcntl.flock(file_obj, flag)


def _unlock_file(file_obj):
    if fcntl:
        fcntl.flock(file_obj, fcntl.LOCK_UN)


def load_cache(path, max_age):
    path = Path(path)
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age >= max_age:
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            try:
                _lock_file(fh, shared=True)
                return json.load(fh)
            finally:
                _unlock_file(fh)
    except Exception:
        return None


def save_cache(path, data):
    resolver.atomic_write_json(Path(path), data, indent=2)


def _load_questions_data():
    if not os.path.exists(QUESTIONS_DATA_FILE):
        return []
    try:
        with open(QUESTIONS_DATA_FILE, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def _save_questions_data(items):
    resolver.atomic_write_json(Path(QUESTIONS_DATA_FILE), items, indent=2)


def _format_questions_timestamp(ts):
    if not ts:
        return ""
    try:
        dt_obj = dt.fromisoformat(ts)
        if not dt_obj.tzinfo and LOCAL_TZ:
            dt_obj = dt_obj.replace(tzinfo=LOCAL_TZ)
        elif LOCAL_TZ:
            dt_obj = dt_obj.astimezone(LOCAL_TZ)
        return dt_obj.strftime("%b %d, %Y %I:%M %p")
    except Exception:
        return ts


def _current_user_label():
    if not current_user.is_authenticated:
        return "Unknown user"
    if getattr(current_user, "username", None):
        return current_user.username
    if getattr(current_user, "strava_athlete_name", None):
        return current_user.strava_athlete_name
    if getattr(current_user, "email", None):
        return current_user.email
    return "Unknown user"


def _append_question(body):
    question = {
        "id": str(uuid.uuid4()),
        "user_id": getattr(current_user, "id", None),
        "author": _current_user_label(),
        "content": body,
        "created_at": dt.now(tz=LOCAL_TZ).isoformat(),
        "answers": [],
    }
    with QUESTIONS_LOCK:
        questions = _load_questions_data()
        questions.append(question)
        _save_questions_data(questions)


def _append_answer(question_id, body):
    answer = {
        "id": str(uuid.uuid4()),
        "user_id": getattr(current_user, "id", None),
        "author": _current_user_label(),
        "content": body,
        "created_at": dt.now(tz=LOCAL_TZ).isoformat(),
    }
    updated = False
    with QUESTIONS_LOCK:
        questions = _load_questions_data()
        for q in questions:
            if q.get("id") == question_id:
                q.setdefault("answers", []).append(answer)
                updated = True
                break
        if updated:
            _save_questions_data(questions)
    return updated


def _get_questions_for_display():
    questions = _load_questions_data()
    questions.sort(key=lambda q: q.get("created_at") or "", reverse=True)
    for q in questions:
        q["created_display"] = _format_questions_timestamp(q.get("created_at"))
        q.setdefault("answers", [])
        q["answers"] = sorted(
            q["answers"], key=lambda a: a.get("created_at") or "", reverse=False
        )
        for ans in q["answers"]:
            ans["created_display"] = _format_questions_timestamp(ans.get("created_at"))
    return questions


def _send_contact_email(name, email, topic, message_body):
    """Attempt to send contact email, falling back to local log."""
    timestamp = dt.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    subject_prefix = os.environ.get("CONTACT_SUBJECT_PREFIX", "[Strava Stat Tracker]")
    subject = f"{subject_prefix} {topic or 'Contact Request'}"
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("CONTACT_FROM_EMAIL", "no-reply@strava-stat-tracker")
    msg["To"] = CONTACT_EMAIL
    preview_email = email or "Unknown"
    msg.set_content(
        f"New contact submission\n"
        f"Name: {name or 'Unknown'}\n"
        f"Email: {preview_email}\n"
        f"Submitted: {timestamp}\n\n"
        f"{message_body}"
    )

    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    use_tls = os.environ.get("SMTP_USE_TLS", "1") not in {"0", "false", "False"}

    if smtp_host:
        try:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
                if use_tls:
                    server.starttls()
                if smtp_user and smtp_password:
                    server.login(smtp_user, smtp_password)
                server.send_message(msg)
            return True, None
        except Exception as exc:
            error_note = f"SMTP send failed: {exc}"
    else:
        error_note = "SMTP_HOST not configured; wrote to log file."

    log_entry = (
        f"--- {timestamp} ---\n"
        f"Name: {name or 'Unknown'}\n"
        f"Email: {preview_email}\n"
        f"Topic: {topic}\n"
        f"{message_body}\n\n"
    )
    try:
        with open(CONTACT_LOG_FILE, "a") as fh:
            fh.write(log_entry)
    except Exception as log_exc:
        return False, f"{error_note}; also failed to log: {log_exc}"
    return False, error_note


def _send_feedback_email(category, message_body):
    """Attempt to send help-improve feedback email, falling back to local log."""
    timestamp = dt.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    subject_prefix = os.environ.get("FEEDBACK_SUBJECT_PREFIX", "[Strava Stat Tracker]")
    category_label = category or "Feedback"
    subject = f"{subject_prefix} Help Improve: {category_label}"
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("FEEDBACK_FROM_EMAIL", "no-reply@strava-stat-tracker")
    msg["To"] = FEEDBACK_EMAIL
    user_label = _current_user_label()
    user_email = getattr(current_user, "email", None) or "Unknown"
    msg.set_content(
        f"New help-improve feedback\n"
        f"Category: {category_label}\n"
        f"User: {user_label}\n"
        f"Email: {user_email}\n"
        f"Submitted: {timestamp}\n\n"
        f"{message_body}"
    )

    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    use_tls = os.environ.get("SMTP_USE_TLS", "1") not in {"0", "false", "False"}

    if smtp_host:
        try:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
                if use_tls:
                    server.starttls()
                if smtp_user and smtp_password:
                    server.login(smtp_user, smtp_password)
                server.send_message(msg)
            return True, None
        except Exception as exc:
            error_note = f"SMTP send failed: {exc}"
    else:
        error_note = "SMTP_HOST not configured; wrote to log file."

    log_entry = (
        f"--- {timestamp} ---\n"
        f"Category: {category_label}\n"
        f"User: {user_label}\n"
        f"Email: {user_email}\n"
        f"{message_body}\n\n"
    )
    try:
        with open(FEEDBACK_LOG_FILE, "a") as fh:
            fh.write(log_entry)
    except Exception as log_exc:
        return False, f"{error_note}; also failed to log: {log_exc}"
    return False, error_note


def _load_info_posts():
    if not os.path.exists(INFO_DATA_FILE):
        return []
    try:
        with open(INFO_DATA_FILE, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def _save_info_posts(items):
    resolver.atomic_write_json(Path(INFO_DATA_FILE), items, indent=2)


def _append_info_post(title, description, resource_url):
    post = {
        "id": str(uuid.uuid4()),
        "title": title,
        "description": description,
        "resource_url": resource_url,
        "created_at": dt.now(tz=LOCAL_TZ).isoformat(),
        "author": _current_user_label(),
        "user_id": getattr(current_user, "id", None),
    }
    with INFO_LOCK:
        posts = _load_info_posts()
        posts.append(post)
        _save_info_posts(posts)


def _get_info_posts():
    posts = _load_info_posts()
    posts.sort(key=lambda p: p.get("created_at") or "", reverse=True)
    for post in posts:
        post["created_display"] = _format_questions_timestamp(post.get("created_at"))
    return posts


def _safe_cache_key(name):
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(name))


def _single_flight_cache(path, builder):
    key = str(path)
    with _CACHE_BUILD_LOCK:
        state = _CACHE_BUILD_STATES.get(key)
        if state is None:
            state = {"event": threading.Event(), "result": None, "error": None}
            _CACHE_BUILD_STATES[key] = state
            first = True
        else:
            first = False
    if first:
        try:
            state["result"] = builder()
            return state["result"]
        except Exception as exc:
            state["error"] = exc
            raise
        finally:
            state["event"].set()
            with _CACHE_BUILD_LOCK:
                _CACHE_BUILD_STATES.pop(key, None)
    state["event"].wait()
    if state["error"]:
        raise state["error"]
    return state["result"]


def _cache_file(name):
    return CACHE_DIR / f"{_safe_cache_key(name)}.json"


def _user_cache_name(app_user_id, user_id, base):
    return f"{base}_{app_user_id}_{user_id}"


def _cached_json(name, max_age, builder):
    path = _cache_file(name)
    cached = load_cache(path, max_age)
    if cached is not None:
        return cached

    def build_and_save():
        data = builder()
        try:
            save_cache(path, data)
        except Exception:
            pass
        return data

    return _single_flight_cache(path, build_and_save)


def _clear_user_caches(user_id, app_user_id):
    safe_id = _user_cache_name(app_user_id, user_id, "")
    for path in CACHE_DIR.glob("*.json"):
        if safe_id in path.stem:
            try:
                path.unlink()
            except OSError:
                pass
    stats_path = STATS_CACHE_DIR / f"{app_user_id}__{user_id}.json"
    if stats_path.exists():
        try:
            stats_path.unlink()
        except OSError:
            pass


def _parse_iso_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _hydrate_stats_dict(stats_dict):
    if not isinstance(stats_dict, dict):
        return stats_dict
    hydrated = stats_dict.copy()

    for attr in (
        "streak_start",
        "streak_end",
        "break_start",
        "break_end",
        "earliest_start_local",
        "latest_start_local",
    ):
        candidate = hydrated.get(attr)
        parsed = _parse_iso_datetime(candidate)
        if parsed is not None:
            hydrated[attr] = parsed

    for value in hydrated.values():
        if isinstance(value, dict) and "date" in value:
            parsed = _parse_iso_datetime(value.get("date"))
            if parsed is not None:
                value["date"] = parsed

    return hydrated


def _active_user_id():
    """Return the linked Strava athlete id for the logged-in user."""
    app_user_id = get_current_app_user_id()
    return get_strava_id_for_user(app_user_id)


def _get_user_build_lock(user_id):
    with _USER_BUILD_LOCKS_LOCK:
        lock = _USER_BUILD_LOCKS.get(str(user_id))
        if lock is None:
            lock = threading.Lock()
            _USER_BUILD_LOCKS[str(user_id)] = lock
    return lock


def _load_or_build_stats_bundle(user_id, app_user_id):
    bundle = load_stats_bundle(user_id, app_user_id)
    if bundle:
        return bundle
    _ensure_stats_build(user_id, app_user_id)
    return None


@memoize_request(lambda user_id=None, *args, **kwargs: ("activity_df", user_id or _active_user_id()))
def get_cached_activity_df(user_id=None, app_user_id=None):
    return load_activities_df(user_id, app_user_id=app_user_id)


def _refresh_status_path(app_user_id, user_id):
    REFRESH_STATUS_DIR.mkdir(parents=True, exist_ok=True)
    return REFRESH_STATUS_DIR / f"{app_user_id}__{user_id}.json"


def _log_refresh_event(message):
    REFRESH_STATUS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = REFRESH_STATUS_DIR / "refresh_debug.log"
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(f"[{stamp}] {message}\n")


def _read_refresh_status(app_user_id, user_id):
    path = _refresh_status_path(app_user_id, user_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_refresh_status(app_user_id, user_id, payload):
    path = _refresh_status_path(app_user_id, user_id)
    try:
        resolver.atomic_write_json(path, payload, indent=2)
        _log_refresh_event(
            f"refresh_status_write ok path={path} status={payload.get('status')}"
        )
    except Exception as exc:
        _log_refresh_event(f"refresh_status_write failed path={path} error={exc}")
    return payload


def _update_refresh_job(user_id, app_user_id=None, **fields):
    with refresh_lock:
        job = refresh_jobs.setdefault(
            user_id,
            {
                "job_id": None,
                "status": "idle",
                "progress": 0,
                "message": "Idle",
                "started_at": None,
                "ended_at": None,
                "estimate": REFRESH_ESTIMATE_SECONDS,
            },
        )
        job.update(fields)
        if app_user_id and user_id:
            persisted = dict(job)
            _write_refresh_status(app_user_id, user_id, persisted)
        return job


def _run_refresh_job(user_id, app_user_id, job_id=None):
    _log_refresh_event(f"refresh_job_start user_id={user_id} app_user_id={app_user_id}")
    _update_refresh_job(
        user_id,
        app_user_id=app_user_id,
        status="running",
        progress=5,
        message="Queued Strava sync…",
        started_at=time.time(),
        ended_at=None,
        job_id=job_id,
    )
    try:
        _update_refresh_job(
            user_id,
            app_user_id=app_user_id,
            progress=15,
            message="Refreshing Strava tokens…",
        )
        resolver.require_token(user_id)
        _update_refresh_job(
            user_id,
            app_user_id=app_user_id,
            progress=30,
            message="Fetching activities & gear…",
        )
        fetch_data_main(user_id)
        invalidate_activities_cache(user_id)
        _update_refresh_job(
            user_id,
            app_user_id=app_user_id,
            progress=70,
            message="Precomputing stats cache…",
        )
        _set_stats_build_status(
            app_user_id,
            user_id,
            status="running",
            message="Building stats cache…",
            started_at=time.time(),
            ended_at=None,
            error=None,
        )
        prime_precomputed_stats(user_id, app_user_id)
        _set_stats_build_status(
            app_user_id,
            user_id,
            status="completed",
            message="Stats ready.",
            ended_at=time.time(),
            error=None,
        )
        _update_refresh_job(
            user_id,
            app_user_id=app_user_id,
            status="completed",
            progress=100,
            message="Sync finished successfully.",
            ended_at=time.time(),
        )
        _log_refresh_event(f"refresh_job_done user_id={user_id} app_user_id={app_user_id}")
    except Exception as exc:
        print(f"[sync] user_id={user_id} failed: {exc}")
        _set_stats_build_status(
            app_user_id,
            user_id,
            status="error",
            message=f"Stats build failed: {exc}",
            ended_at=time.time(),
            error=str(exc),
        )
        _update_refresh_job(
            user_id,
            app_user_id=app_user_id,
            status="error",
            message=f"Sync failed: {exc}",
            progress=0,
            ended_at=time.time(),
        )
        _log_refresh_event(f"refresh_job_error user_id={user_id} error={exc}")


def _stats_job_defaults():
    return {
        "status": "idle",
        "progress": 0,
        "message": "Stats bundle not built yet.",
        "started_at": None,
        "ended_at": None,
        "job_id": None,
        "estimate": STATS_BUILD_ESTIMATE_SECONDS,
    }


def _stats_build_status_path(app_user_id, user_id):
    STATS_BUILD_STATUS_DIR.mkdir(parents=True, exist_ok=True)
    return STATS_BUILD_STATUS_DIR / f"{app_user_id}__{user_id}.json"


def _read_stats_build_status(app_user_id, user_id):
    path = _stats_build_status_path(app_user_id, user_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_stats_build_status(app_user_id, user_id, payload):
    path = _stats_build_status_path(app_user_id, user_id)
    resolver.atomic_write_json(path, payload, indent=2)
    return payload


def _set_stats_build_status(app_user_id, user_id, **fields):
    status = _read_stats_build_status(app_user_id, user_id) or {}
    status.update(fields)
    return _write_stats_build_status(app_user_id, user_id, status)


def _update_stats_build_job(user_id, **fields):
    with stats_build_lock:
        key = str(user_id)
        job = stats_build_jobs.setdefault(key, _stats_job_defaults())
        job.update(fields)
        return job


def _run_stats_build_job(user_id, app_user_id, job_id=None):
    _update_stats_build_job(
        user_id,
        status="running",
        progress=10,
        message="Building stats cache…",
        started_at=time.time(),
        ended_at=None,
        job_id=job_id,
    )
    _set_stats_build_status(
        app_user_id,
        user_id,
        status="running",
        message="Building stats cache…",
        started_at=time.time(),
        ended_at=None,
        error=None,
    )
    try:
        prime_precomputed_stats(user_id, app_user_id)
        stats_path = STATS_CACHE_DIR / f"{app_user_id}__{user_id}.json"
        all_time_path = _cache_file(_user_cache_name(app_user_id, user_id, "all_time_stats"))
        if not stats_path.exists() or not all_time_path.exists():
            raise RuntimeError("Stats cache files were not written.")
        _update_stats_build_job(
            user_id,
            status="completed",
            progress=100,
            message="Stats ready.",
            ended_at=time.time(),
        )
        _set_stats_build_status(
            app_user_id,
            user_id,
            status="completed",
            message="Stats ready.",
            ended_at=time.time(),
            error=None,
        )
    except Exception as exc:
        print(f"[stats_build] user_id={user_id} failed: {exc}")
        _update_stats_build_job(
            user_id,
            status="error",
            progress=0,
            message=f"Stats build failed: {exc}",
            ended_at=time.time(),
        )
        _set_stats_build_status(
            app_user_id,
            user_id,
            status="error",
            message=f"Stats build failed: {exc}",
            ended_at=time.time(),
            error=str(exc),
        )


def _ensure_stats_build(user_id, app_user_id):
    if not user_id:
        return None
    status = _read_stats_build_status(app_user_id, user_id)
    if status and status.get("status") in ("running", "queued"):
        return status
    with stats_build_lock:
        key = str(user_id)
        existing = stats_build_jobs.get(key)
        if existing and existing.get("status") in ("running", "queued"):
            return existing
        _set_stats_build_status(
            app_user_id,
            user_id,
            status="queued",
            message="Queued to build stats cache…",
            started_at=None,
            ended_at=None,
            error=None,
        )
        try:
            cmd = [
                sys.executable,
                "-m",
                "jobs.stats_build_cli",
                str(app_user_id),
                str(user_id),
            ]
            subprocess.Popen(
                cmd,
                cwd=str(BASE_DIR),
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return _read_stats_build_status(app_user_id, user_id)
        except Exception as exc:
            print(f"[stats_build] failed to spawn subprocess: {exc}")
        job_meta = job_runner.enqueue(
            "stats_build",
            user_id,
            _run_stats_build_job,
            user_id,
            app_user_id,
        )
        return _update_stats_build_job(
            user_id,
            status="queued",
            progress=0,
            message="Queued to build stats cache…",
            started_at=None,
            ended_at=None,
            job_id=job_meta["id"],
        )


def _ensure_routes_store():
    os.makedirs(os.path.dirname(ROUTES_DATA_FILE), exist_ok=True)
    if not os.path.exists(ROUTES_DATA_FILE):
        with open(ROUTES_DATA_FILE, "w") as f:
            json.dump([], f)


def load_routes():
    """Load all routes from JSON file."""
    _ensure_routes_store()
    try:
        with open(ROUTES_DATA_FILE, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return []


def _build_all_time_stats_payload(user_id):
    stats_dict, yearly_breakdown = aggregate_better_stats(user_id)
    return {"stats": stats_dict, "yearly_breakdown": yearly_breakdown}


def _load_cached_all_time_stats_payload(user_id, app_user_id):
    """Return the all-time stats payload if the cache file exists and is fresh."""
    return load_cache(
        _cache_file(_user_cache_name(app_user_id, user_id, "all_time_stats")),
        CACHE_MAX_AGE_ALL_TIME,
    )


def _stats_last_built_at(user_id, app_user_id):
    bundle = load_stats_bundle(user_id, app_user_id)
    if isinstance(bundle, dict):
        return bundle.get("generated_at")
    return None


@memoize_request(lambda user_id, *args, **kwargs: ("all_time_stats", user_id))
def get_cached_all_time_stats(user_id, app_user_id):
    bundle = _load_or_build_stats_bundle(user_id, app_user_id)
    if bundle:
        stats_dict = bundle.get("stats") or {}
        yearly_breakdown = bundle.get("yearly_breakdown") or {}
        return SimpleNamespace(**_hydrate_stats_dict(stats_dict)), yearly_breakdown
    payload = _load_cached_all_time_stats_payload(user_id, app_user_id)
    if payload:
        stats_dict = payload.get("stats") or {}
        yearly_breakdown = payload.get("yearly_breakdown") or {}
        return SimpleNamespace(**_hydrate_stats_dict(stats_dict)), yearly_breakdown
    _ensure_stats_build(user_id, app_user_id)
    return SimpleNamespace(), {}


@memoize_request(lambda user_id, *args, **kwargs: ("foot_stats", user_id))
def get_cached_foot_stats(user_id, app_user_id):
    cached = load_cache(
        _cache_file(_user_cache_name(app_user_id, user_id, "foot_stats")),
        CACHE_MAX_AGE_FOOT,
    )
    if cached is None:
        _ensure_stats_build(user_id, app_user_id)
        return {}
    return cached


def _compute_personal_best_sections(user_id, include_virtual=True):
    data = load_activities_cached(user_id)
    gear_lookup = _load_gear_lookup_base(user_id)
    return build_personal_best_sections(
        data, gear_lookup, include_virtual=include_virtual
    )

def _activity_matches(activity, sport=None, search=None):
    if sport:
        act_type = (activity.get("type") or "").lower()
        if act_type != sport.lower():
            return False
    if search:
        term = search.lower()
        name = (activity.get("name") or "").lower()
        act_type = (activity.get("type") or "").lower()
        if term not in name and term not in act_type:
            return False
    return True


def _serialize_activity(activity):
    allowed = [
        "id",
        "name",
        "type",
        "distance",
        "moving_time",
        "elapsed_time",
        "total_elevation_gain",
        "average_speed",
        "max_speed",
        "average_heartrate",
        "max_heartrate",
        "average_cadence",
        "kilojoules",
        "calories",
        "splits_metric",
        "laps",
        "start_date",
        "start_date_local",
    ]
    result = {k: activity.get(k) for k in allowed}
    if activity.get("map"):
        result["map"] = {
            "summary_polyline": activity["map"].get("summary_polyline"),
        }
    else:
        result["map"] = None
    return result


@memoize_request(
    lambda user_id, include_virtual=True, *args, **kwargs: (
        "personal_best_v2",
        user_id,
        include_virtual,
    )
)
def get_cached_personal_best_sections(user_id, include_virtual=True, app_user_id=None):
    bundle = _load_or_build_stats_bundle(user_id, app_user_id)
    key = "with_virtual" if include_virtual else "no_virtual"
    if bundle:
        sections = (bundle.get("personal_best_sections") or {}).get(key)
        if sections:
            return _normalize_personal_best_sections(sections)
    suffix = "with_virtual" if include_virtual else "no_virtual"
    cached_sections = load_cache(
        _cache_file(_user_cache_name(app_user_id, user_id, f"personal_best_v2_{suffix}")),
        CACHE_MAX_AGE_PERSONAL_BEST,
    )
    if cached_sections is None:
        _ensure_stats_build(user_id, app_user_id)
        return []
    return _normalize_personal_best_sections(cached_sections)


def _normalize_personal_best_sections(sections):
    """Ensure every section/card matches the template's expected keys."""
    if not isinstance(sections, list):
        return []
    normalized = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        cards = section.get("cards") or section.get("items") or []
        normalized_cards = [_normalize_personal_best_card(card) for card in cards if card]
        section_copy = section.copy()
        section_copy["cards"] = normalized_cards
        section_copy["items"] = normalized_cards
        normalized.append(section_copy)
    return normalized


def _normalize_personal_best_card(card):
    if not isinstance(card, dict):
        return card
    normalized = card.copy()

    value_blob = normalized.get("value") or {}
    metric_value = normalized.get("value_metric") or value_blob.get("value")
    imperial_value = normalized.get("value_imperial") or value_blob.get("value") or metric_value

    normalized["value_metric"] = metric_value or "—"
    normalized["value_imperial"] = imperial_value or normalized["value_metric"]
    normalized["subtitle"] = normalized.get("subtitle") or value_blob.get("subtitle", "")
    normalized["detail"] = normalized.get("detail") or value_blob.get("details", "")
    normalized["activity_name"] = normalized.get("activity_name") or value_blob.get("name")
    normalized["activity_type"] = normalized.get("activity_type") or value_blob.get("type")
    normalized["date"] = normalized.get("date") or value_blob.get("date")
    normalized["bike"] = normalized.get("bike") or value_blob.get("bike")
    normalized.setdefault("empty", False)
    if not normalized.get("icon"):
        normalized["icon"] = normalized.get("badge") or ""
    return normalized


@memoize_request(lambda user_id, *args, **kwargs: ("lifetime_activities", user_id))
def get_cached_lifetime_activities(user_id, app_user_id):
    def builder():
        return load_activities_cached(user_id)

    return _cached_json(
        _user_cache_name(app_user_id, user_id, "lifetime"),
        CACHE_MAX_AGE_LIFETIME,
        builder,
    )


@memoize_request(lambda user_id, *args, **kwargs: ("yearly_overview", user_id))
def get_cached_yearly_overview(user_id, app_user_id):
    bundle = _load_or_build_stats_bundle(user_id, app_user_id)
    if bundle and bundle.get("yearly_overview"):
        return bundle["yearly_overview"]
    cache_name = _cache_file(_user_cache_name(app_user_id, user_id, "yearly_overview"))
    cached = load_cache(cache_name, CACHE_MAX_AGE_ALL_TIME)
    if (
        cached
        and isinstance(cached, dict)
        and cached.get("version") == YEARLY_CACHE_VERSION
        and isinstance(cached.get("summaries"), list)
    ):
        return cached["summaries"]
    _ensure_stats_build(user_id, app_user_id)
    return []


@memoize_request(lambda user_id, year, *args, **kwargs: ("year_detail", user_id, year))
def get_cached_year_detail(user_id, year, app_user_id):
    bundle = _load_or_build_stats_bundle(user_id, app_user_id)
    if bundle:
        detail = None
        year_details = bundle.get("year_details")
        if isinstance(year_details, dict):
            detail = year_details.get(str(year)) or year_details.get(year)
        if detail:
            return detail
    cache_name = _cache_file(
        _user_cache_name(app_user_id, user_id, f"year_detail_{year}")
    )
    cached = load_cache(cache_name, CACHE_MAX_AGE_ALL_TIME)
    if (
        cached
        and isinstance(cached, dict)
        and cached.get("version") == YEARLY_CACHE_VERSION
        and isinstance(cached.get("detail"), dict)
    ):
        return cached["detail"]
    _ensure_stats_build(user_id, app_user_id)
    return None


def prime_precomputed_stats(user_id, app_user_id):
    """Build heavyweight stats once after a Strava sync and persist them."""
    try:
        payload = _build_all_time_stats_payload(user_id)
        sanitized_payload = _sanitize_for_json(payload)
        save_cache(
            _cache_file(_user_cache_name(app_user_id, user_id, "all_time_stats")),
            sanitized_payload,
        )
        stats_dict = payload.get("stats") or {}
        yearly_breakdown = payload.get("yearly_breakdown") or {}
    except Exception as exc:
        print(f"[prime] Failed to build all-time stats for {user_id}: {exc}")
        return

    try:
        foot_payload = aggregate_foot_stats(user_id)
        save_cache(
            _cache_file(_user_cache_name(app_user_id, user_id, "foot_stats")),
            foot_payload,
        )
    except Exception as exc:
        print(f"[prime] Failed to build foot stats for {user_id}: {exc}")

    try:
        activities = get_cached_lifetime_activities(user_id, app_user_id)
        overview = build_yearly_review_payload(yearly_breakdown, activities)
        save_cache(
            _cache_file(_user_cache_name(app_user_id, user_id, "yearly_overview")),
            {"version": YEARLY_CACHE_VERSION, "summaries": overview},
        )

        normalized_years = normalize_yearly_breakdown(yearly_breakdown)
        all_years = set(normalized_years.keys())
        if activities:
            years_from_activities = {
                dt.year
                for dt in (parse_activity_dt(act) for act in activities)
                if dt is not None
            }
            all_years |= years_from_activities
        all_years = sorted(all_years)
        for year in all_years:
            detail = build_yearly_detail(year, yearly_breakdown, activities)
            if detail:
                save_cache(
                    _cache_file(
                        _user_cache_name(app_user_id, user_id, f"year_detail_{year}")
                    ),
                    {"version": YEARLY_CACHE_VERSION, "detail": detail},
                )
    except Exception as exc:
        print(f"[prime] Failed to compute yearly overviews for {user_id}: {exc}")

    try:
        build_stats_bundle(user_id, app_user_id)
    except Exception as exc:
        print(f"[prime] Failed to build unified stats bundle for {user_id}: {exc}")

    try:
        build_heatmap_segments(user_id, app_user_id)
    except Exception as exc:
        print(f"[prime] Failed to build heatmap segments for {user_id}: {exc}")
    try:
        frame_df = _build_activity_frame(user_id)
        save_activity_frame(user_id, app_user_id, frame_df)
    except Exception as exc:
        print(f"[prime] Failed to cache activity dataframe for {user_id}: {exc}")
    _GRAPH_RESPONSE_CACHE.clear()


@memoize_request(lambda user_id, *args, **kwargs: ("foot_polylines", user_id))
def get_cached_foot_polylines(user_id, app_user_id):
    def builder():
        activities = load_activities(user_id)
        foot_acts = [a for a in activities if a.get("type") in ("Run", "Walk", "Hike")]
        return [
            a.get("map", {}).get("summary_polyline")
            for a in foot_acts
            if a.get("map") and a.get("map", {}).get("summary_polyline")
        ]

    return _cached_json(
        _user_cache_name(app_user_id, user_id, "foot_map"),
        CACHE_MAX_AGE_FOOT,
        builder,
    )


@memoize_request(lambda user_id, *args, **kwargs: ("bike_list", user_id))
def get_cached_bike_list(user_id, app_user_id):
    def builder():
        data = load_activities_cached(user_id)
        return [gear for gear in {act.get("gear_id") for act in data if act.get("gear_id")}]

    return _cached_json(
        _user_cache_name(app_user_id, user_id, "bike_list"),
        CACHE_MAX_AGE_GEAR,
        builder,
    )


def save_routes(routes):
    """Persist all routes to disk."""
    _ensure_routes_store()
    with open(ROUTES_DATA_FILE, "w") as f:
        json.dump(routes, f, indent=2)


def find_route(route_id):
    """Return a single route dict by ID."""
    for route in load_routes():
        if str(route.get("id")) == str(route_id):
            return route
    return None


def _replace_route(route_id, new_route):
    """Replace a route with updated data and save the store."""
    routes = load_routes()
    for idx, existing in enumerate(routes):
        if str(existing.get("id")) == str(route_id):
            routes[idx] = new_route
            save_routes(routes)
            return new_route
    return None


def _sanitize_points(raw_points):
    points = []
    for pair in raw_points or []:
        if not isinstance(pair, (list, tuple)) or len(pair) < 2:
            continue
        try:
            lat = float(pair[0])
            lon = float(pair[1])
        except (TypeError, ValueError):
            continue
        points.append([lat, lon])
    return points


def haversine_m(a, b):
    """Return distance in meters between two lat/lon pairs."""
    if not a or not b:
        return 0.0
    r = 6371000.0
    lat1 = math.radians(a[0])
    lat2 = math.radians(b[0])
    dlat = lat2 - lat1
    dlon = math.radians(b[1] - a[1])
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(max(h, 0.0)))


def compute_route_stats(points, elevations=None):
    """Compute baseline stats for a route."""
    if not points or len(points) < 2:
        return {
            "distance_m": 0,
            "climb_m": 0,
            "descent_m": 0,
            "avg_grade": 0,
            "max_grade": 0,
            "min_grade": 0,
            "segments": [],
            "est_time_sec": 0,
        }

    has_elev = elevations and len(elevations) == len(points)
    total = 0.0
    climb = 0.0
    descent = 0.0
    max_grade = -99.0
    min_grade = 99.0
    segments = []
    chunk_len = 800.0  # meters
    seg_dist = 0.0
    seg_gain = 0.0

    for idx in range(len(points) - 1):
        ds = haversine_m(points[idx], points[idx + 1])
        dh = 0.0
        if has_elev:
            dh = float(elevations[idx + 1]) - float(elevations[idx])
        total += ds
        seg_dist += ds
        if dh > 0:
            climb += dh
            seg_gain += dh
        elif dh < 0:
            descent -= dh

        grade = (dh / ds * 100) if ds else 0.0
        if math.isfinite(grade):
            max_grade = max(max_grade, grade)
            min_grade = min(min_grade, grade)

        # finalize segment buckets every ~0.8km or at the end
        if seg_dist >= chunk_len or idx == len(points) - 2:
            avg_grade = (seg_gain / seg_dist * 100) if seg_dist else 0.0
            segments.append(
                {
                    "length_m": seg_dist,
                    "gain_m": seg_gain,
                    "grade": avg_grade,
                }
            )
            seg_dist = 0.0
            seg_gain = 0.0

    avg_grade = (climb / total * 100) if total else 0.0
    est_time_hours = total / 1000 / 28 if total else 0  # 28 km/h default pace
    est_time_sec = int(est_time_hours * 3600)

    return {
        "distance_m": total,
        "climb_m": climb,
        "descent_m": descent,
        "avg_grade": avg_grade,
        "max_grade": max_grade if max_grade != -99 else 0,
        "min_grade": min_grade if min_grade != 99 else 0,
        "segments": segments,
        "est_time_sec": est_time_sec,
    }


def _route_summary_fields(route):
    stats = route.get("stats", {})
    dist = stats.get("distance_m", 0)
    climb = stats.get("climb_m", 0)
    descent = stats.get("descent_m", 0)

    def _fmt_distance(meters):
        miles = meters * 0.000621371
        km = meters / 1000
        if miles >= 0.5:
            return f"{miles:.2f} mi"
        return f"{km:.2f} km"

    def _fmt_elev(meters):
        if meters * 3.28084 >= 100:
            return f"{meters * 3.28084:,.0f} ft"
        return f"{meters:,.0f} m"

    est_time = stats.get("est_time_sec", 0)
    hours = est_time // 3600
    minutes = (est_time % 3600) // 60

    return {
        "distance_pretty": _fmt_distance(dist),
        "climb_pretty": _fmt_elev(climb),
        "descent_pretty": _fmt_elev(descent),
        "est_time_pretty": f"{hours}h {str(minutes).zfill(2)}m",
    }


def decorate_routes_for_view(routes):
    """Attach friendly stats for template rendering."""
    decorated = []
    for route in routes:
        enriched = route.copy()
        enriched["stats_display"] = _route_summary_fields(route)
        enriched.setdefault("favorites", [])
        enriched.setdefault("description", "")
        enriched["stats"] = enriched.get("stats", {})
        enriched["distance"] = enriched["stats_display"]["distance_pretty"]
        decorated.append(enriched)
    return decorated


def _route_api_payload(route):
    payload = {
        "id": route.get("id"),
        "name": route.get("name"),
        "description": route.get("description"),
        "creator": route.get("creator"),
        "created_at": route.get("created_at"),
        "updated_at": route.get("updated_at"),
        "waypoints": route.get("waypoints", []),
        "line": route.get("line", []),
        "elevations": route.get("elevations", []),
        "stats": route.get("stats", {}),
        "favorites": route.get("favorites", []),
    }
    return payload


def _downsample_points(points, target=40):
    if not points:
        return []
    if len(points) <= target:
        return points
    step = max(1, len(points) // target)
    trimmed = [points[0]]
    for idx in range(step, len(points) - 1, step):
        trimmed.append(points[idx])
    trimmed.append(points[-1])
    return trimmed


def _prepare_route_from_payload(payload, existing=None):
    if payload is None:
        raise ValueError("Missing route payload.")

    line = _sanitize_points(payload.get("line") or payload.get("geometry"))
    if len(line) < 2:
        raise ValueError("Route requires at least two coordinates.")

    waypoints = _sanitize_points(payload.get("waypoints"))
    if not waypoints:
        waypoints = _downsample_points(line, target=12)

    raw_elev = payload.get("elevations")
    elevations = []
    if raw_elev and len(raw_elev) == len(line):
        try:
            elevations = [float(e) for e in raw_elev]
        except (TypeError, ValueError):
            elevations = []
    if not elevations:
        elevations = [0.0] * len(line)

    stats = compute_route_stats(line, elevations)

    now = dt.utcnow().isoformat() + "Z"
    creator = existing.get("creator") if existing else session.get("username", "Guest")
    base = existing.copy() if existing else {}
    base.update(
        {
            "id": base.get("id") or str(payload.get("id") or uuid.uuid4()),
            "name": (payload.get("name") or base.get("name") or "Untitled Route").strip(),
            "description": (payload.get("description") or base.get("description") or "").strip(),
            "creator": creator or "Guest",
            "created_at": base.get("created_at") or now,
            "updated_at": now,
            "waypoints": waypoints,
            "line": line,
            "elevations": elevations,
            "stats": stats,
            "favorites": base.get("favorites", []),
        }
    )
    return base


def _slugify(value):
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in (value or "route"))
    cleaned = "-".join(filter(None, cleaned.split("-")))
    return cleaned or "route"


def _route_to_geojson(route):
    coords = [[pt[1], pt[0]] for pt in route.get("line", [])]
    feature = {
        "type": "Feature",
        "properties": {
            "name": route.get("name"),
            "creator": route.get("creator"),
            "description": route.get("description"),
        },
        "geometry": {"type": "LineString", "coordinates": coords},
    }
    collection = {"type": "FeatureCollection", "features": [feature]}
    return json.dumps(collection, indent=2)


def _route_to_gpx(route):
    gpx = ET.Element("gpx", version="1.1", creator="StravaStatTracker")
    trk = ET.SubElement(gpx, "trk")
    ET.SubElement(trk, "name").text = route.get("name") or "Route"
    if route.get("description"):
        ET.SubElement(trk, "desc").text = route.get("description")
    trkseg = ET.SubElement(trk, "trkseg")
    elevations = route.get("elevations") or []
    for idx, pt in enumerate(route.get("line", [])):
        trkpt = ET.SubElement(
            trkseg,
            "trkpt",
            lat=f"{pt[0]:.6f}",
            lon=f"{pt[1]:.6f}",
        )
        if idx < len(elevations):
            ET.SubElement(trkpt, "ele").text = f"{float(elevations[idx]):.1f}"
    return ET.tostring(gpx, encoding="unicode")


def _route_to_tcx(route):
    ns = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
    tcx = ET.Element("TrainingCenterDatabase", xmlns=ns)
    activities = ET.SubElement(tcx, "Activities")
    activity = ET.SubElement(activities, "Activity", Sport="Biking")
    start_time = route.get("created_at") or dt.utcnow().isoformat() + "Z"
    lap = ET.SubElement(activity, "Lap", StartTime=start_time)
    stats = route.get("stats") or {}
    ET.SubElement(lap, "TotalTimeSeconds").text = str(stats.get("est_time_sec", 0))
    ET.SubElement(lap, "DistanceMeters").text = f"{stats.get('distance_m', 0):.1f}"
    ET.SubElement(activity, "Notes").text = route.get("description") or ""
    track = ET.SubElement(lap, "Track")
    elevations = route.get("elevations") or []
    for idx, pt in enumerate(route.get("line", [])):
        tp = ET.SubElement(track, "Trackpoint")
        pos = ET.SubElement(tp, "Position")
        ET.SubElement(pos, "LatitudeDegrees").text = f"{pt[0]:.6f}"
        ET.SubElement(pos, "LongitudeDegrees").text = f"{pt[1]:.6f}"
        if idx < len(elevations):
            ET.SubElement(tp, "AltitudeMeters").text = f"{float(elevations[idx]):.1f}"
    return ET.tostring(tcx, encoding="unicode")


def _build_route_download(route, fmt):
    fmt = (fmt or "geojson").lower()
    if fmt not in ROUTE_EXPORT_FORMATS:
        raise ValueError(f"Unsupported format '{fmt}'.")
    if fmt == "geojson":
        return _route_to_geojson(route), "application/geo+json", "geojson"
    if fmt == "gpx":
        return _route_to_gpx(route), "application/gpx+xml", "gpx"
    return _route_to_tcx(route), "application/vnd.garmin.tcx+xml", "tcx"


def _parse_geojson_obj(data):
    if data.get("type") == "FeatureCollection":
        features = data.get("features") or []
        if not features:
            raise ValueError("GeoJSON missing features.")
        geom = features[0].get("geometry") or {}
        props = features[0].get("properties") or {}
    elif data.get("type") == "Feature":
        geom = data.get("geometry") or {}
        props = data.get("properties") or {}
    else:
        geom = data
        props = {}
    if geom.get("type") != "LineString":
        raise ValueError("GeoJSON must contain a LineString.")
    coords = geom.get("coordinates") or []
    points = []
    elevations = []
    for c in coords:
        if not isinstance(c, (list, tuple)) or len(c) < 2:
            continue
        points.append([float(c[1]), float(c[0])])
        if len(c) >= 3:
            try:
                elevations.append(float(c[2]))
            except (TypeError, ValueError):
                elevations.append(0.0)
    if len(points) < 2:
        raise ValueError("GeoJSON route needs at least two coordinates.")
    if len(elevations) != len(points):
        elevations = []
    return {
        "name": props.get("name"),
        "description": props.get("description") or props.get("desc"),
        "line": points,
        "elevations": elevations,
    }


def _parse_gpx_text(text):
    root = ET.fromstring(text)
    pts = []
    elevs = []
    for trkpt in root.findall(".//{*}trkpt"):
        lat = trkpt.attrib.get("lat")
        lon = trkpt.attrib.get("lon")
        if lat is None or lon is None:
            continue
        pts.append([float(lat), float(lon)])
        ele_el = trkpt.find("{*}ele")
        if ele_el is not None and ele_el.text:
            elevs.append(float(ele_el.text))
        else:
            elevs.append(0.0)
    if len(pts) < 2:
        raise ValueError("GPX route needs at least two points.")
    name_el = root.find(".//{*}name")
    desc_el = root.find(".//{*}desc")
    return {
        "name": name_el.text if name_el is not None else None,
        "description": desc_el.text if desc_el is not None else None,
        "line": pts,
        "elevations": elevs,
    }


def _parse_tcx_text(text):
    root = ET.fromstring(text)
    pts = []
    elevs = []
    for tp in root.findall(".//{*}Trackpoint"):
        lat_el = tp.find(".//{*}LatitudeDegrees")
        lon_el = tp.find(".//{*}LongitudeDegrees")
        if lat_el is None or lon_el is None or not lat_el.text or not lon_el.text:
            continue
        pts.append([float(lat_el.text), float(lon_el.text)])
        alt_el = tp.find(".//{*}AltitudeMeters")
        if alt_el is not None and alt_el.text:
            elevs.append(float(alt_el.text))
        else:
            elevs.append(0.0)
    if len(pts) < 2:
        raise ValueError("TCX route needs at least two coordinates.")
    name_el = root.find(".//{*}Name")
    return {
        "name": name_el.text if name_el is not None else None,
        "description": "",
        "line": pts,
        "elevations": elevs,
    }


def _parse_uploaded_route(file_storage):
    filename = secure_filename(file_storage.filename or "route")
    raw = file_storage.read()
    if not raw:
        raise ValueError("Uploaded file was empty.")
    ext = os.path.splitext(filename.lower())[1]
    text = raw.decode("utf-8", errors="ignore")
    if ext in (".json", ".geojson"):
        try:
            payload = _parse_geojson_obj(json.loads(text))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid GeoJSON: {exc}") from exc
    elif ext == ".gpx":
        payload = _parse_gpx_text(text)
    elif ext == ".tcx":
        payload = _parse_tcx_text(text)
    else:
        # try auto-detect by XML root
        try:
            if "<gpx" in text[:200]:
                payload = _parse_gpx_text(text)
            elif "TrainingCenterDatabase" in text[:200]:
                payload = _parse_tcx_text(text)
            else:
                payload = _parse_geojson_obj(json.loads(text))
        except Exception:
            raise ValueError("Unsupported route file format.")
    payload["filename"] = filename
    return payload


####LOGINNNN
@login_manager.user_loader
def load_user(user_id):
    row = get_user_by_id(user_id)
    return row_to_user(row)


init_auth(app)
app.register_blueprint(auth_bp)


def _strava_state_serializer():
    return URLSafeTimedSerializer(app.secret_key, salt="strava-oauth")


def _build_strava_state(app_user_id):
    serializer = _strava_state_serializer()
    payload = {"user_id": str(app_user_id), "nonce": uuid.uuid4().hex}
    return serializer.dumps(payload)


def _parse_strava_state(state, max_age=900):
    serializer = _strava_state_serializer()
    return serializer.loads(state, max_age=max_age)

@app.route("/routes/all")
def all_routes():
    routes = decorate_routes_for_view(load_routes())
    return render_template(
        "routes/all_routes.html",
        routes=routes,
        export_formats=ROUTE_EXPORT_FORMATS,
    )


@app.route("/routes/my")
def my_routes():
    username = session.get("username", "Guest")
    routes = [
        r for r in decorate_routes_for_view(load_routes()) if r.get("creator") == username
    ]
    return render_template(
        "routes/my_routes.html",
        routes=routes,
        export_formats=ROUTE_EXPORT_FORMATS,
    )


@app.route("/routes/favorites")
def favorite_routes():
    username = session.get("username", "Guest")
    favs = []
    for r in decorate_routes_for_view(load_routes()):
        if username in r.get("favorites", []):
            favs.append(r)
    return render_template(
        "routes/favorites.html",
        routes=favs,
        export_formats=ROUTE_EXPORT_FORMATS,
    )

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
    return redirect(request.referrer or url_for('all_routes'))


@app.route("/api/refresh_strava", methods=["POST"])
@login_required
def trigger_refresh_strava():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    _log_refresh_event(
        f"trigger_refresh_strava app_user_id={app_user_id} user_id={user_id} sync_inline={SYNC_INLINE}"
    )
    if not user_id:
        return jsonify({"ok": False, "error": "User not resolved."}), 400
    if not check_rate_limit(f"refresh_strava:{app_user_id}", 15):
        _log_refresh_event(
            f"refresh_rate_limited app_user_id={app_user_id} user_id={user_id}"
        )
        _update_refresh_job(
            user_id,
            app_user_id=app_user_id,
            status="error",
            progress=0,
            message="Please wait before starting another sync.",
            ended_at=time.time(),
        )
        return jsonify({"ok": False, "error": "Please wait before starting another sync."}), 429
    try:
        with refresh_lock:
            _log_refresh_event(
                f"refresh_lock_enter app_user_id={app_user_id} user_id={user_id}"
            )
            existing = _read_refresh_status(app_user_id, user_id)
            if not existing:
                existing = refresh_jobs.get(user_id)
            if existing and existing.get("status") == "running":
                _log_refresh_event(
                    f"refresh_already_running app_user_id={app_user_id} user_id={user_id}"
                )
                return jsonify({"ok": True, "status": existing, "already_running": True}), 202
            job_state = _update_refresh_job(
                user_id,
                app_user_id=app_user_id,
                status="queued",
                progress=1,
                message="Queued for processing…",
                started_at=time.time(),
                ended_at=None,
                job_id=None,
            )
            _log_refresh_event(
                f"refresh_job_queued app_user_id={app_user_id} user_id={user_id}"
            )
            if SYNC_INLINE:
                _log_refresh_event(
                    f"refresh_job_inline_start app_user_id={app_user_id} user_id={user_id}"
                )
                _run_refresh_job(user_id, app_user_id, job_id=None)
                job_state = _read_refresh_status(app_user_id, user_id) or job_state
                _log_refresh_event(
                    f"refresh_job_inline_done app_user_id={app_user_id} user_id={user_id}"
                )
            else:
                try:
                    cmd = [
                        sys.executable,
                        "-m",
                        "jobs.refresh_strava_cli",
                        str(app_user_id),
                        str(user_id),
                    ]
                    subprocess.Popen(
                        cmd,
                        cwd=str(BASE_DIR),
                        start_new_session=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                except Exception as exc:
                    print(f"[strava_sync] failed to spawn subprocess: {exc}")
                    job_meta = job_runner.enqueue(
                        "strava_sync",
                        user_id,
                        _run_refresh_job,
                        user_id,
                        app_user_id,
                    )
                    job_state = _update_refresh_job(
                        user_id,
                        app_user_id=app_user_id,
                        status="queued",
                        progress=1,
                        message="Queued for processing…",
                        started_at=time.time(),
                        ended_at=None,
                        job_id=job_meta["id"],
                    )
        return jsonify({"ok": True, "status": job_state})
    except Exception as exc:
        _log_refresh_event(f"refresh_trigger_error app_user_id={app_user_id} error={exc}")
        return jsonify({"ok": False, "error": "Refresh failed to start."}), 500


@app.route("/api/refresh_strava/status")
@login_required
def refresh_strava_status():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"status": "idle", "message": "No user session."})
    job = _read_refresh_status(app_user_id, user_id)
    if not job:
        job = refresh_jobs.get(
            user_id,
            {
                "status": "idle",
                "progress": 0,
                "message": "Idle",
                "started_at": None,
                "ended_at": None,
                "estimate": REFRESH_ESTIMATE_SECONDS,
            },
        )
    if not isinstance(job, dict):
        job = {
            "status": "idle",
            "progress": 0,
            "message": "Idle",
            "started_at": None,
            "ended_at": None,
            "estimate": REFRESH_ESTIMATE_SECONDS,
        }
    return jsonify(job)


@app.route("/sync-status")
@login_required
def sync_status():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify(
            {
                "status": "idle",
                "state": "idle",
                "message": "No Strava account connected.",
                "progress": 0,
                "last_sync": None,
            }
        )
    job = _read_refresh_status(app_user_id, user_id)
    if not job:
        job = refresh_jobs.get(
            user_id,
            {
                "status": "idle",
                "progress": 0,
                "message": "Idle",
                "started_at": None,
                "ended_at": None,
                "estimate": REFRESH_ESTIMATE_SECONDS,
            },
        )
    if not isinstance(job, dict):
        job = {
            "status": "idle",
            "progress": 0,
            "message": "Idle",
            "started_at": None,
            "ended_at": None,
            "estimate": REFRESH_ESTIMATE_SECONDS,
        }
    last_sync = None
    last_activity = None
    try:
        activities_file = resolver.activities_path(str(user_id))
        if activities_file.exists():
            ts = datetime.datetime.fromtimestamp(os.path.getmtime(activities_file))
            last_sync = ts.strftime("%b %d, %Y %I:%M %p")
        last_activity = _load_last_activity_from_stats_cache(str(user_id), app_user_id)
    except Exception:
        pass
    status_value = job.get("status", "idle")
    state_value = "done" if status_value == "completed" else status_value
    payload = dict(job)
    payload.update(
        {
            "status": status_value,
            "state": state_value,
            "last_sync": last_sync,
            "last_activity": last_activity,
        }
    )
    if request.args.get("debug") == "1":
        activities_path = resolver.activities_path(str(user_id))
        stats_cache_path = STATS_CACHE_DIR / f"{app_user_id}__{user_id}.json"
        refresh_status_path = _refresh_status_path(app_user_id, user_id)
        stats_status_path = _stats_build_status_path(app_user_id, user_id)
        payload["debug"] = {
            "cwd": str(Path.cwd()),
            "code_file": __file__,
            "sync_inline": SYNC_INLINE,
            "app_user_id": app_user_id,
            "user_id": user_id,
            "activities_path": str(activities_path),
            "activities_exists": activities_path.exists(),
            "activities_mtime": activities_path.stat().st_mtime if activities_path.exists() else None,
            "stats_cache_path": str(stats_cache_path),
            "stats_cache_exists": stats_cache_path.exists(),
            "refresh_status_path": str(refresh_status_path),
            "refresh_status_exists": refresh_status_path.exists(),
            "stats_status_path": str(stats_status_path),
            "stats_status_exists": stats_status_path.exists(),
        }
    return jsonify(payload)


@app.route("/api/refresh_stats", methods=["POST"])
@login_required
def trigger_refresh_stats():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"ok": False, "error": "User not resolved."}), 400
    with stats_build_lock:
        key = str(user_id)
        existing = stats_build_jobs.get(key)
        if existing and existing.get("status") in ("running", "queued"):
            return jsonify({"ok": True, "status": existing, "already_running": True}), 202
        status = _read_stats_build_status(app_user_id, user_id)
        if status and status.get("status") in ("running", "queued"):
            return jsonify({"ok": True, "status": status, "already_running": True}), 202
        if not check_rate_limit(f"refresh_stats:{app_user_id}", 15):
            return jsonify({"ok": False, "error": "Please wait before starting another build."}), 429
        _set_stats_build_status(
            app_user_id,
            user_id,
            status="queued",
            message="Queued to build stats cache…",
            started_at=None,
            ended_at=None,
            error=None,
        )
        try:
            cmd = [
                sys.executable,
                "-m",
                "jobs.stats_build_cli",
                str(app_user_id),
                str(user_id),
            ]
            subprocess.Popen(
                cmd,
                cwd=str(BASE_DIR),
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            status = _read_stats_build_status(app_user_id, user_id) or {}
            return jsonify({"ok": True, "status": status})
        except Exception as exc:
            print(f"[stats_build] failed to spawn subprocess: {exc}")
        job_meta = job_runner.enqueue(
            "stats_build",
            user_id,
            _run_stats_build_job,
            user_id,
            app_user_id,
        )
        job_state = _update_stats_build_job(
            user_id,
            status="queued",
            progress=0,
            message="Queued to build stats cache…",
            started_at=None,
            ended_at=None,
            job_id=job_meta["id"],
        )
    return jsonify({"ok": True, "status": job_state})


@app.route("/api/refresh_stats/status")
@login_required
def refresh_stats_status():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"status": "missing", "message": "User not resolved."})
    status = _read_stats_build_status(app_user_id, user_id)
    if status and status.get("status") in ("running", "queued", "error"):
        return jsonify(status)
    stats_path = STATS_CACHE_DIR / f"{app_user_id}__{user_id}.json"
    if stats_path.exists():
        return jsonify(
            {
                "status": "completed",
                "message": "Stats ready.",
                "started_at": None,
                "ended_at": None,
            }
        )
    if status:
        return jsonify(status)
    job = stats_build_jobs.get(str(user_id))
    if not job:
        job = _stats_job_defaults()
    return jsonify(job)


@app.route("/routes/import", methods=["POST"])
def import_route():
    upload = request.files.get("route_file")
    if not upload or upload.filename == "":
        flash("Select a route file to import.", "error")
        return redirect(request.referrer or url_for("all_routes"))
    try:
        parsed = _parse_uploaded_route(upload)
        payload = {
            "name": request.form.get("name") or parsed.get("name") or parsed.get("filename"),
            "description": request.form.get("description") or parsed.get("description") or "",
            "line": parsed.get("line"),
            "waypoints": _downsample_points(parsed.get("line"), target=20),
            "elevations": parsed.get("elevations"),
        }
        route = _prepare_route_from_payload(payload)
        routes = load_routes()
        routes.append(route)
        save_routes(routes)
        flash(f"Imported route '{route.get('name')}'.", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    except Exception as exc:  # pragma: no cover
        flash(f"Import failed: {exc}", "error")
    return redirect(request.referrer or url_for("all_routes"))


@app.route("/routes/export/<route_id>")
def export_route(route_id):
    route = find_route(route_id)
    if not route:
        abort(404)
    fmt = (request.args.get("fmt") or "geojson").lower()
    try:
        payload, mimetype, extension = _build_route_download(route, fmt)
    except ValueError as exc:
        abort(400, str(exc))
    filename = _slugify(route.get("name"))
    return send_file(
        io.BytesIO(payload.encode("utf-8")),
        mimetype=mimetype,
        as_attachment=True,
        download_name=f"{filename}.{extension}",
    )


@app.route("/api/routes", methods=["GET", "POST"])
def api_routes_collection():
    if request.method == "GET":
        return jsonify([_route_api_payload(r) for r in load_routes()])
    payload = request.get_json(silent=True)
    try:
        route = _prepare_route_from_payload(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    routes = load_routes()
    routes.append(route)
    save_routes(routes)
    return jsonify(_route_api_payload(route)), 201


@app.route("/api/routes/<route_id>", methods=["GET", "PUT", "DELETE"])
def api_route_detail(route_id):
    route = find_route(route_id)
    if not route:
        return jsonify({"error": "Route not found"}), 404
    if request.method == "GET":
        return jsonify(_route_api_payload(route))
    if request.method == "DELETE":
        routes = [r for r in load_routes() if str(r.get("id")) != str(route_id)]
        save_routes(routes)
        return jsonify({"ok": True})
    payload = request.get_json(silent=True)
    try:
        updated = _prepare_route_from_payload(payload, existing=route)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    _replace_route(route_id, updated)
    return jsonify(_route_api_payload(updated))


## ACTIVITIES PATH ADDED FRO GRAPHS

def activities_path(user_id):
    return str(resolver.activities_path(user_id))




# Helper: get current user ID (session or fallback)
# -------------------------------------------------------
def get_current_user_id():
    """Return the linked Strava athlete id for the logged-in user, or None."""
    app_user_id = get_current_app_user_id()
    return get_strava_id_for_user(app_user_id)

    ### FOOT STATSS STUFF
@app.route("/foot_map")
@login_required
def foot_map():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    polylines = get_cached_foot_polylines(user_id, app_user_id) if user_id else []
    return render_template("foot_map.html", polylines=polylines)


####profile

def _parse_strava_datetime(value):
    if not value or not isinstance(value, str):
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(value)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%b %d, %Y"):
        try:
            return datetime.datetime.strptime(value, fmt)
        except Exception:
            continue
    return None


def _load_last_activity_from_stats_cache(user_id, app_user_id):
    stats_path = Path("users_data") / "stats_cache" / f"{app_user_id}__{user_id}.json"
    if not stats_path.exists():
        return None
    try:
        with open(stats_path, "r", encoding="utf-8") as stats_f:
            payload = json.load(stats_f)
    except Exception:
        return None
    stats = payload.get("stats", {}) if isinstance(payload, dict) else {}
    for key in ("latest_start_local", "latest_start", "latest_activity"):
        candidate = stats.get(key)
        parsed = _parse_strava_datetime(candidate)
        if parsed:
            return parsed.strftime("%b %d, %Y %I:%M %p")
    return None


@app.route("/profile")
@login_required
def profile():
    app_user_id = get_current_app_user_id()
    row = get_user_by_id(current_user.id)
    user = row_to_user(row)
    if not user:
        user = current_user

    strava_context = {"connected": False}
    if user and user.strava_athlete_id:
        athlete_id = str(user.strava_athlete_id)
        tokens = resolver.load_tokens(athlete_id)
        athlete = tokens.get("athlete", {}) if isinstance(tokens, dict) else {}
        display_name = (
            user.strava_athlete_name
            or " ".join(
                [
                    (athlete.get("firstname") or "").strip(),
                    (athlete.get("lastname") or "").strip(),
                ]
            ).strip()
            or athlete.get("username")
            or f"Athlete {athlete_id}"
        )
        activities_file = resolver.activities_path(athlete_id)
        last_sync = None
        if activities_file.exists():
            ts = datetime.datetime.fromtimestamp(os.path.getmtime(activities_file))
            last_sync = ts.strftime("%b %d, %Y %I:%M %p")
        last_activity = _load_last_activity_from_stats_cache(athlete_id, app_user_id)
        strava_context = {
            "connected": True,
            "athlete_id": athlete_id,
            "athlete_name": display_name,
            "connected_at": user.strava_connected_display,
            "last_sync": last_sync,
            "last_activity": last_activity,
        }

    return render_template("profile.html", user=user, strava=strava_context)


@app.route("/profile/refresh_stats", methods=["POST"])
@login_required
def profile_refresh_stats():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"ok": False, "error": "User not resolved."}), 400
    with stats_build_lock:
        key = str(user_id)
        existing = stats_build_jobs.get(key)
        if existing and existing.get("status") in ("running", "queued"):
            return jsonify({"ok": True, "message": existing.get("message") or "Build already running."}), 202
    if not check_rate_limit(f"profile_refresh_stats:{app_user_id}", 15):
        return jsonify({"ok": False, "error": "Please wait before refreshing stats again."}), 429
    _clear_user_caches(user_id, app_user_id)
    if SYNC_INLINE:
        _run_stats_build_job(user_id, app_user_id, job_id=None)
        status = _read_stats_build_status(app_user_id, user_id)
        message = status.get("message") if status else "Stats build finished."
        ok = not status or status.get("status") != "error"
        return jsonify({"ok": ok, "message": message})
    job_state = _ensure_stats_build(user_id, app_user_id)
    if not job_state:
        return jsonify({"ok": False, "error": "Unable to queue stats build."}), 500
    return jsonify({"ok": True, "message": job_state.get("message") or "Queued to build stats cache…"})


@app.route("/settings")
@login_required
def settings():
    row = get_user_by_id(current_user.id)
    user = row_to_user(row) or current_user
    return render_template("settings.html", user=user)


@app.route("/about_project")
@login_required
def about_project():
    return render_template("abt_project.html")

@app.route("/how-this-works")
def how_this_works():
    return render_template("how_this_works.html", contact_email=CONTACT_EMAIL)

@app.route("/reminders", methods=["GET", "POST"])
@login_required
def reminders_page():
    default_phone = os.environ.get("REMINDER_DEFAULT_PHONE_NUMBER", "").strip()
    if request.method == "POST":
        message = (request.form.get("message") or "").strip()
        date_str = (request.form.get("date") or "").strip()
        time_str = (request.form.get("time") or "").strip()
        phone = (request.form.get("phone_number") or default_phone).strip()
        if not message or not date_str or not time_str or not phone:
            flash("All fields are required to schedule a reminder.", "error")
            return redirect(url_for("reminders_page"))
        try:
            send_time = dt.fromisoformat(f"{date_str}T{time_str}").replace(tzinfo=LOCAL_TZ)
        except ValueError:
            flash("Invalid date/time format.", "error")
            return redirect(url_for("reminders_page"))
        reminder_id = create_reminder(
            phone_number=phone,
            message=message,
            send_time=send_time.isoformat(),
            status="scheduled",
        )
        _schedule_reminder_job(reminder_id, send_time)
        flash("Reminder scheduled.", "success")
        return redirect(url_for("reminders_page"))

    reminders = list_reminders()
    for reminder in reminders:
        send_time = _parse_reminder_datetime(reminder.get("send_time"))
        if send_time and send_time.tzinfo is None:
            send_time = send_time.replace(tzinfo=LOCAL_TZ)
        reminder["send_time_display"] = (
            send_time.strftime("%b %d, %Y %I:%M %p") if send_time else "Unknown"
        )
    return render_template(
        "reminders.html",
        reminders=reminders,
        default_phone=default_phone,
    )


@app.route("/reminders/<int:reminder_id>/cancel", methods=["POST"])
@login_required
def cancel_reminder(reminder_id):
    reminder = get_reminder(reminder_id)
    if not reminder:
        flash("Reminder not found.", "error")
        return redirect(url_for("reminders_page"))
    update_reminder_status(reminder_id, "canceled")
    job_id = f"{REMINDER_JOB_PREFIX}{reminder_id}"
    try:
        reminder_scheduler.remove_job(job_id)
    except Exception:
        pass
    flash("Reminder canceled.", "info")
    return redirect(url_for("reminders_page"))


@app.route("/sms/webhook", methods=["POST"])
def sms_webhook():
    from_number = request.form.get("From")
    body = request.form.get("Body")
    app.logger.info("Inbound SMS from %s: %s", from_number, body)
    return ("", 200)



@app.route("/foot_stats")
@login_required
def foot_stats():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    stats = get_cached_foot_stats(user_id, app_user_id) if user_id else {}
    return render_template("foot_stats.html", stats=stats)



@app.route("/")
def index():
    if not current_user.is_authenticated:
        return redirect("/login")
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    stats_ready = False
    if user_id:
        bundle = load_stats_bundle(user_id, app_user_id)
        stats_ready = bool(bundle)
        if not stats_ready and _load_cached_all_time_stats_payload(user_id, app_user_id):
            _ensure_stats_build(user_id, app_user_id)
    return render_template("index.html", stats_ready=stats_ready)


@app.route("/questions", methods=["GET", "POST"])
@login_required
def questions_page():
    if request.method == "POST":
        body = (request.form.get("question_text") or "").strip()
        if not body:
            flash("Please enter a question before submitting.")
        else:
            _append_question(body)
            flash("Question posted.")
            return redirect(url_for("questions_page"))
    questions = _get_questions_for_display()
    return render_template("questions.html", questions=questions)


@app.route("/questions/<question_id>/answer", methods=["POST"])
@login_required
def answer_question(question_id):
    body = (request.form.get("answer_text") or "").strip()
    if not body:
        flash("Answer cannot be empty.")
        return redirect(url_for("questions_page") + f"#question-{question_id}")
    if _append_answer(question_id, body):
        flash("Answer added.")
    else:
        flash("Question could not be found. Please refresh and try again.")
    return redirect(url_for("questions_page") + f"#question-{question_id}")


@app.route("/info", methods=["GET", "POST"])
@login_required
def info_page():
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        description = (request.form.get("description") or "").strip()
        resource_url = (request.form.get("resource_url") or "").strip()
        if not title or not description:
            flash("Title and description are required.")
        else:
            _append_info_post(title, description, resource_url)
            flash("Info entry posted.")
            return redirect(url_for("info_page"))
    posts = _get_info_posts()
    return render_template("info.html", posts=posts)


@app.route("/contact", methods=["GET", "POST"])
@login_required
def contact_page():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip()
        topic = (request.form.get("topic") or "").strip()
        message = (request.form.get("message") or "").strip()
        if not message or not topic:
            flash("Please include both a topic and message.")
        else:
            sent, note = _send_contact_email(name, email, topic, message)
            if sent:
                flash("Message sent! I will reach out soon.")
            else:
                if note:
                    flash(f"Saved your message locally (email send pending): {note}")
                else:
                    flash("Saved your message. Email delivery is not configured yet.")
            return redirect(url_for("contact_page"))
    return render_template("contact.html", contact_email=CONTACT_EMAIL)


@app.route("/help-improve", methods=["GET", "POST"])
@login_required
def help_improve_page():
    if request.method == "POST":
        category = (request.form.get("category") or "").strip()
        message = (request.form.get("message") or "").strip()
        if not message:
            flash("Please add a note before submitting.")
        else:
            sent, note = _send_feedback_email(category, message)
            if sent:
                flash("Thanks for your feedback!")
            else:
                if note:
                    flash(f"Thanks for your feedback! (Email pending: {note})")
                else:
                    flash("Thanks for your feedback! (Email delivery not configured yet.)")
            return redirect(url_for("help_improve_page"))
    return render_template("help_improve.html", feedback_email=FEEDBACK_EMAIL)


####Wanderr style stuff


@app.route("/coverage_stats")
@login_required
def coverage_stats():
    return render_template("coverage_stats.html")


@app.route("/stats/coverage_summary")
@login_required
def stats_coverage_summary():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({}), 400
    return jsonify(load_summary(_coverage_summary_path(app_user_id, user_id)))


@app.route("/stats/repeatability")
@login_required
def stats_repeatability():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({}), 400
    s = load_summary(_coverage_summary_path(app_user_id, user_id))
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
@login_required
def unique_miles():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))
    bikes = build_unique_miles_context(user_id)
    return render_template("unique_miles.html", bikes=bikes)


@app.route("/unique_miles/stats")
@login_required
def unique_miles_stats():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"ok": False, "error": "User not resolved."}), 400
    summary = _load_coverage_summary(app_user_id, user_id)
    if not summary:
        return jsonify({"ok": False, "error": "Coverage summary missing"}), 500
    units = (request.args.get("units") or "imperial").lower()
    if not is_allowed(units, {"imperial", "metric"}):
        return jsonify({"ok": False, "error": "Invalid units"}), 400
    start_raw = request.args.get("start", "")
    end_raw = request.args.get("end", "")
    start = parse_yyyy_mm_dd(start_raw)
    end = parse_yyyy_mm_dd(end_raw)
    if (start_raw and not start) or (end_raw and not end):
        return jsonify({"ok": False, "error": "Invalid date format"}), 400
    if not is_valid_date_range(start, end):
        return jsonify({"ok": False, "error": "Invalid date range"}), 400
    bike = request.args.get("bike") or "all"
    lifetime = _build_lifetime_payload(summary, units)
    range_stats = _compute_range_stats(summary, start, end, bike, units, app_user_id, user_id)
    bikes = _build_bike_breakdown(summary, units)
    return jsonify(
        {
            "ok": True,
            "units": units,
            "lifetime": lifetime,
            "range": range_stats,
            "bikes": bikes,
        }
    )



def _parse_date_yyyy_mm_dd(s):
    try:
        return _dt.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _load_activities_list():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return []
    return load_activities_cached(user_id)


def _load_activity_lookup():
    global _ACTIVITY_LOOKUP
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return {}
    cache_key = _coverage_user_key(app_user_id, user_id)
    cached = _ACTIVITY_LOOKUP.get(cache_key)
    if cached is not None:
        return cached
    lookup = {}
    for row in _load_activities_list():
        rid = str(row.get("id"))
        if not rid:
            continue
        lookup[rid] = {
            "gear_id": str(row.get("gear_id") or "") or "unknown",
            "name": row.get("name"),
            "start_date": (row.get("start_date_local") or row.get("start_date") or "")[:10],
            "distance_m": float(row.get("distance") or 0.0),
        }
    _ACTIVITY_LOOKUP[cache_key] = lookup
    return lookup


def _load_gear_lookup(user_id=None):
    global _GEAR_LOOKUP
    resolved = user_id
    if not resolved:
        app_user_id = get_current_app_user_id()
        resolved = get_strava_id_for_user(app_user_id)
    if not resolved:
        return {"unknown": {"name": "No bike selected", "frame_type": None}}
    if resolved not in _GEAR_LOOKUP:
        _GEAR_LOOKUP[resolved] = _load_gear_lookup_base(resolved)
    return _GEAR_LOOKUP[resolved]


def _coverage_user_key(app_user_id, user_id):
    return f"{app_user_id}:{user_id}"


def _coverage_dir(app_user_id, user_id):
    return Path("data/coverage") / f"{app_user_id}__{user_id}"


def _coverage_summary_path(app_user_id, user_id):
    return _coverage_dir(app_user_id, user_id) / "coverage_summary.json"


def _coverage_activity_hex_dir(app_user_id, user_id):
    return _coverage_dir(app_user_id, user_id) / "activity_hexes"


def _coverage_new_hex_dir(app_user_id, user_id):
    return _coverage_dir(app_user_id, user_id) / "new_hexes"


def rebuild_user_coverage(app_user_id, user_id):
    activities_path = resolver.activities_path(user_id)
    output_dir = _coverage_dir(app_user_id, user_id)
    rebuild_indexes(activities_path=activities_path, output_dir=output_dir)


def _load_coverage_summary(app_user_id, user_id):
    global _COVERAGE_SUMMARY
    key = _coverage_user_key(app_user_id, user_id)
    cached = _COVERAGE_SUMMARY.get(key)
    if cached is not None:
        return cached or {}
    summary_path = _coverage_summary_path(app_user_id, user_id)
    if summary_path.exists():
        try:
            _COVERAGE_SUMMARY[key] = json.loads(summary_path.read_text())
        except Exception:
            _COVERAGE_SUMMARY[key] = {}
    else:
        _COVERAGE_SUMMARY[key] = {}
    return _COVERAGE_SUMMARY[key] or {}


def _coverage_data_mtime(app_user_id, user_id):
    try:
        return _coverage_summary_path(app_user_id, user_id).stat().st_mtime
    except FileNotFoundError:
        return 0.0
    except Exception:
        return 0.0


def _reset_coverage_caches_if_needed(app_user_id, user_id):
    global _COVERAGE_CACHE_VERSION
    key = _coverage_user_key(app_user_id, user_id)
    current = _coverage_data_mtime(app_user_id, user_id)
    if _COVERAGE_CACHE_VERSION.get(key) != current:
        _COVERAGE_RANGE_CACHE.clear()
        _NEW_RANGE_CACHE.clear()
        _COVERAGE_CACHE_VERSION[key] = current


def _cache_lookup(cache, key):
    value = cache.get(key)
    if value is None:
        return None
    cache.move_to_end(key)
    return copy.deepcopy(value)


def _cache_store(cache, key, value):
    cache[key] = copy.deepcopy(value)
    cache.move_to_end(key)
    while len(cache) > _COVERAGE_CACHE_LIMIT:
        cache.popitem(last=False)


def _frame_label(frame_type):
    return _frame_label_helper(frame_type)


def _is_road_frame(frame_type):
    return _is_road_frame_helper(frame_type)


def _activity_surface(activity, gear_lookup=None):
    gear_lookup = gear_lookup or _load_gear_lookup()
    return _activity_surface_helper(activity, gear_lookup)


def _activity_is_road(activity, gear_lookup=None):
    gear_lookup = gear_lookup or _load_gear_lookup()
    return _activity_surface_helper(activity, gear_lookup) == "road"


def _bike_label(gear_lookup, gear_id):
    return _bike_label_helper(gear_lookup, gear_id)


def _convert_miles(val, units):
    if units == "metric":
        return val * 1.60934
    return val


def _build_bike_breakdown(summary, units):
    per_ride_new = summary.get("per_ride_new_miles_est", {}) or {}
    act_lookup = _load_activity_lookup()
    gear_lookup = _load_gear_lookup()
    totals = defaultdict(float)
    for rid, miles in per_ride_new.items():
        act = act_lookup.get(str(rid))
        if not act:
            continue
        totals[act.get("gear_id") or "unknown"] += float(miles or 0.0)
    rows = []
    grand = sum(totals.values()) or 1.0
    for gid, miles in totals.items():
        bike = gear_lookup.get(gid, {})
        frame = bike.get("frame_type")
        converted = _convert_miles(miles, units)
        rows.append({
            "gear_id": gid,
            "name": _bike_label(gear_lookup, gid),
            "frame_type": frame,
            "frame_label": _frame_label(frame),
            "is_road": _is_road_frame(frame),
            "miles": converted,
            "share": (miles / grand) * 100 if grand else 0.0,
        })
    rows.sort(key=lambda r: r["miles"], reverse=True)
    return rows


def _compute_range_stats(summary, start, end, bike, units, app_user_id, user_id):
    per_ride_new = summary.get("per_ride_new_miles_est", {}) or {}
    per_ride_meta = summary.get("per_ride_meta", {}) or {}
    act_lookup = _load_activity_lookup()
    gear_lookup = _load_gear_lookup()
    ids = _filter_activity_ids(start, end, bike if bike not in ("all", "", None) else None)
    cells = set()
    rides = []
    miles_total = 0.0
    on_road = 0.0
    off_road = 0.0
    distance_by_day = defaultdict(float)
    distance_by_week = {}
    gear_lookup = _load_gear_lookup()
    active_dates = set()
    for rid in ids:
        rid = str(rid)
        miles = float(per_ride_new.get(rid, 0.0) or 0.0)
        miles_total += miles
        meta = per_ride_meta.get(rid, {})
        act = act_lookup.get(rid, {})
        gear_id = act.get("gear_id") or "unknown"
        bike_info = gear_lookup.get(gear_id, {})
        surface = _activity_surface(act, gear_lookup)
        is_road = surface == "road"
        if is_road:
            on_road += miles
        else:
            off_road += miles
        rides.append({
            "id": rid,
            "name": meta.get("name") or act.get("name") or f"Ride {rid}",
            "date": meta.get("date") or act.get("start_date"),
            "miles": _convert_miles(miles, units),
            "bike_id": gear_id,
            "bike_name": _bike_label(gear_lookup, gear_id),
            "is_road": is_road,
            "frame_label": _frame_label(bike_info.get("frame_type")),
        })
        ride_date = _act_date(act)
        if ride_date:
            active_dates.add(ride_date)
            distance_by_day[ride_date] += miles
            iso_year, iso_week, _ = ride_date.isocalendar()
            bucket = distance_by_week.setdefault(
                (iso_year, iso_week), {"distance": 0.0, "start": ride_date}
            )
            bucket["distance"] += miles
            if ride_date < bucket["start"]:
                bucket["start"] = ride_date
        hex_path = _coverage_new_hex_dir(app_user_id, user_id) / f"{rid}.json"
        if hex_path.exists():
            try:
                for cell in json.loads(hex_path.read_text()):
                    cells.add(str(cell))
            except Exception:
                pass
    rides.sort(key=lambda r: r["miles"], reverse=True)
    weekly_series = sorted(
        distance_by_week.items(), key=lambda item: (item[0][0], item[0][1])
    )
    best_week_distance = 0.0
    best_week_label = ""
    if weekly_series:
        best = max(weekly_series, key=lambda item: item[1]["distance"])
        best_week_distance = best[1]["distance"]
        best_week_label = best[1]["start"].strftime("Week of %b %d")
    best_day_distance = 0.0
    best_day_label = ""
    if distance_by_day:
        day, value = max(distance_by_day.items(), key=lambda item: item[1])
        best_day_distance = value
        best_day_label = day.strftime("%b %d, %Y")
    avg_miles_per_ride = (miles_total / len(ids)) if ids else 0.0
    avg_distance_active_day = (
        (miles_total / len(active_dates)) if active_dates else 0.0
    )
    if active_dates:
        span_start = start or min(active_dates)
        span_end = end or max(active_dates)
        total_days = (span_end - span_start).days + 1
    else:
        total_days = 0
    rest_days = max(0, total_days - len(active_dates))
    return {
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "bike": bike if bike not in ("", None) else "all",
        "ride_count": len(ids),
        "unique_cells": len(cells),
        "new_miles": _convert_miles(miles_total, units),
        "on_road_miles": _convert_miles(on_road, units),
        "off_road_miles": _convert_miles(off_road, units),
        "rides": rides[:20],
        "avg_miles_per_ride": _convert_miles(avg_miles_per_ride, units),
        "avg_distance_active_day": _convert_miles(avg_distance_active_day, units),
        "rest_days": rest_days,
        "best_day_distance": _convert_miles(best_day_distance, units),
        "best_day_label": best_day_label,
        "best_week_distance": _convert_miles(best_week_distance, units),
        "best_week_label": best_week_label,
    }


def _build_lifetime_payload(summary, units):
    unique_miles = _convert_miles(summary.get("unique_miles_est", 0.0) or 0.0, units)
    total_miles = _convert_miles(summary.get("total_miles", 0.0) or 0.0, units)
    repeated = _convert_miles(summary.get("repeated_miles", 0.0) or 0.0, units)
    exploration = summary.get("exploration_pct", 0.0)
    repeatability = summary.get("repeatability_score", 0.0)
    unique_cells = summary.get("unique_cells", 0)
    best_month_unique = summary.get("best_month_unique") or {}
    best_month_exploration = summary.get("best_month_exploration") or {}
    best_ride_unique = summary.get("best_ride_unique") or {}
    best_ride_exploration = summary.get("best_ride_exploration") or {}
    by_year = []
    for row in summary.get("by_year", []):
        by_year.append({
            "year": row.get("year"),
            "total_miles": _convert_miles(row.get("total_miles", 0.0) or 0.0, units),
            "new_miles": _convert_miles(row.get("new_miles", 0.0) or 0.0, units),
            "exploration_pct": row.get("exploration_pct", 0.0),
        })
    return {
        "unique_miles": unique_miles,
        "total_miles": total_miles,
        "repeated_miles": repeated,
        "exploration_pct": exploration,
        "repeatability_score": repeatability,
        "unique_cells": unique_cells,
        "best_month_unique": {
            "year": best_month_unique.get("year"),
            "month": best_month_unique.get("month"),
            "new_miles": _convert_miles(best_month_unique.get("new_miles", 0.0) or 0.0, units),
        }
        if best_month_unique
        else None,
        "best_month_exploration": {
            "year": best_month_exploration.get("year"),
            "month": best_month_exploration.get("month"),
            "exploration_pct": best_month_exploration.get("exploration_pct"),
            "total_miles": _convert_miles(best_month_exploration.get("total_miles", 0.0) or 0.0, units),
        }
        if best_month_exploration
        else None,
        "best_ride_unique": {
            "id": best_ride_unique.get("id"),
            "name": best_ride_unique.get("name"),
            "date": best_ride_unique.get("date"),
            "new_miles": _convert_miles(best_ride_unique.get("new_miles", 0.0) or 0.0, units),
        }
        if best_ride_unique
        else None,
        "best_ride_exploration": {
            "id": best_ride_exploration.get("id"),
            "name": best_ride_exploration.get("name"),
            "date": best_ride_exploration.get("date"),
            "pct_new": best_ride_exploration.get("pct_new"),
            "miles_total": _convert_miles(best_ride_exploration.get("miles_total", 0.0) or 0.0, units),
        }
        if best_ride_exploration
        else None,
        "by_year": by_year,
        "generated_at": summary.get("generated_at"),
    }


def _act_date(a):
    ds = a.get("start_date_local") or a.get("start_date")
    if not ds:
        return None
    try:
        # Strava strings are ISO e.g. "2025-08-09T12:34:56Z"
        return _dt.strptime(ds[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _filter_activity_ids(start=None, end=None, bike=None, surface=None):
    acts = _load_activities_list()
    out = []
    gear_lookup = None
    for a in acts:
        d = _act_date(a)
        if start and (not d or d < start):
            continue
        if end and (not d or d > end):
            continue
        if bike and str(a.get("gear_id") or "") != str(bike):
            continue
        if surface:
            gear_lookup = gear_lookup or _load_gear_lookup()
            if _activity_surface(a, gear_lookup) != surface:
                continue
        out.append(str(a.get("id")))
    return out


@app.route("/stats/activities")
@login_required
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
@login_required
def coverage_range():
    """
    Union of hexes for all rides in [start,end] (and optional bike).
    Returns GeoJSON polygons.
    """
    start = _parse_date_yyyy_mm_dd(request.args.get("start", ""))
    end = _parse_date_yyyy_mm_dd(request.args.get("end", ""))
    bike = request.args.get("bike")
    surface = request.args.get("surface")
    if surface not in (None, "road", "offroad"):
        surface = None

    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"error": "User not resolved."}), 400

    _reset_coverage_caches_if_needed(app_user_id, user_id)
    cache_key = (
        _coverage_user_key(app_user_id, user_id),
        start.isoformat() if start else "",
        end.isoformat() if end else "",
        str(bike or "all"),
        surface or "all",
    )
    cached = _cache_lookup(_COVERAGE_RANGE_CACHE, cache_key)
    if cached is not None:
        return jsonify(cached)

    ids = _filter_activity_ids(start, end, bike, surface)
    cov_dir = _coverage_activity_hex_dir(app_user_id, user_id)
    counts = defaultdict(int)
    for aid in ids:
        fp = cov_dir / f"{aid}.json"
        if not fp.exists():
            continue
        try:
            cells = json.loads(fp.read_text())
        except Exception:
            continue
        for c in cells:
            key = str(c)
            counts[key] += 1

    # build polygons with intensity metadata for heatmap styling
    features = [
        {
            "type": "Feature",
            "id": cell,
            "properties": {"weight": count},
            "geometry": _hex_polygon(cell),
        }
        for cell, count in counts.items()
    ]
    fc = {"type": "FeatureCollection", "features": features}
    _cache_store(_COVERAGE_RANGE_CACHE, cache_key, fc)
    return jsonify(fc)


@app.route("/new_by_range")
@login_required
def new_by_range():
    """
    Union of 'new hexes' for rides in [start,end] (and optional bike).
    miles_est is the sum of per-ride new miles (already lifetime-aware).
    """
    start = _parse_date_yyyy_mm_dd(request.args.get("start", ""))
    end = _parse_date_yyyy_mm_dd(request.args.get("end", ""))
    bike = request.args.get("bike")
    surface = request.args.get("surface")
    if surface not in (None, "road", "offroad"):
        surface = None

    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"error": "User not resolved."}), 400

    _reset_coverage_caches_if_needed(app_user_id, user_id)
    cache_key = (
        _coverage_user_key(app_user_id, user_id),
        start.isoformat() if start else "",
        end.isoformat() if end else "",
        str(bike or "all"),
        surface or "all",
    )
    cached = _cache_lookup(_NEW_RANGE_CACHE, cache_key)
    if cached is not None:
        return jsonify(cached)

    ids = _filter_activity_ids(start, end, bike, surface)
    cov_dir = _coverage_new_hex_dir(app_user_id, user_id)
    counts = defaultdict(int)

    # miles: from coverage_summary per-ride map
    summary_p = _coverage_summary_path(app_user_id, user_id)
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
                cells = json.loads(fp.read_text())
            except Exception:
                cells = []
            for c in cells:
                key = str(c)
                counts[key] += 1
        # miles
        try:
            miles_sum += float(miles_map.get(str(aid), 0.0))
        except Exception:
            pass

    features = [
        {
            "type": "Feature",
            "id": cell,
            "properties": {"weight": count},
            "geometry": _hex_polygon(cell),
        }
        for cell, count in counts.items()
    ]
    result = {
        "miles_est": round(miles_sum, 2),
        "geojson": {"type": "FeatureCollection", "features": features},
    }
    _cache_store(_NEW_RANGE_CACHE, cache_key, result)
    return jsonify(result)


@app.route("/new_by_day")
@login_required
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
@login_required
def coverage():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"type": "FeatureCollection", "features": []})
    return jsonify(coverage_geojson(_coverage_dir(app_user_id, user_id)))


@app.route("/activity/<activity_id>/new")
@login_required
def activity_new(activity_id):
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"new_miles_est": 0.0, "geojson": {"type": "FeatureCollection", "features": []}})
    fc, miles = activity_new_geojson(activity_id, _coverage_dir(app_user_id, user_id))
    return jsonify({"new_miles_est": round(miles, 2), "geojson": fc})


@app.route("/gear")
@login_required
def gear_tracker():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    raw = _load_json(resolver.gear_path(user_id), default=[])
    gear_data = _normalize_gear_data(raw)  # ensures bikes/parts/maintenance keys
    return render_template(
        "gear_tracker_page.html",
        gear=gear_data["bikes"],
        bikes=gear_data["bikes"],
        parts=gear_data.get("parts", []),
        maintenance=gear_data.get("maintenance_log", []),
    )


@app.route("/gear_tracker_page")
@login_required
def gear_tracker_page():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    raw = _load_json(resolver.gear_path(user_id), default=[])
    gear_data = _normalize_gear_data(raw)
    return render_template(
        "gear_tracker_page.html",
        gear=gear_data["bikes"],
        bikes=gear_data["bikes"],
        parts=gear_data.get("parts", []),
        maintenance=gear_data.get("maintenance_log", []),
    )


@app.route("/gear/data", methods=["GET"])
@login_required
def gear_data_api():
    units = request.args.get("units", "imperial")
    start_str = request.args.get("start")
    end_str = request.args.get("end")

    if not is_allowed(units, {"imperial", "metric"}):
        return jsonify({"ok": False, "error": "Invalid units"}), 400
    start_dt = parse_dt(start_str) if start_str else None
    end_dt = parse_dt(end_str) if end_str else None
    if (start_str and not start_dt) or (end_str and not end_dt):
        return jsonify({"ok": False, "error": "Invalid date format"}), 400
    if start_dt and end_dt and start_dt > end_dt:
        return jsonify({"ok": False, "error": "Invalid date range"}), 400

    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
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
    return jsonify(get_pmtiles_names())


@app.route("/api/pmtiles/<path:name>/metadata")
def pmtiles_metadata(name):
    """
    Return vector_layers and quick flags for fields we care about.
    Uses a range-reader callable as required by pmtiles.Reader.
    """
    if Reader is None:
        abort(500, description="pmtiles Python package not available")

    all_names = get_pmtiles_names()
    normalized = os.path.basename(name)
    if normalized not in all_names:
        abort(404, description=f"Not found: {name}")

    try:
        url = _pmtiles_http_url(normalized)
        if not url:
            abort(500, description="PMTiles base URL not configured")

        def read_range(offset: int, length: int, _url=url) -> bytes:
            headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
            resp = requests.get(_url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.content

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
    return get_pmtiles_names()


@app.route("/tiles/<path:filename>")
def serve_tiles(filename):
    """Local tile paths are no longer available; use the CDN instead."""
    abort(404, description="PMTiles are now hosted remotely.")


@app.route("/sport/<sport>/map")
def map_page(sport):
    files = _list_pmtiles()
    return render_template(
        "map.html",
        files=files,
        sport=sport,
        pmtiles_base_url=get_pmtiles_base_url() or "",
        pmtiles_assets=get_pmtiles_asset_urls(),
    )


@app.route("/map")
def map_page_default():
    files = _list_pmtiles()
    return render_template(
        "map.html",
        files=files,
        sport=None,
        pmtiles_base_url=get_pmtiles_base_url() or "",
        pmtiles_assets=get_pmtiles_asset_urls(),
    )


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
@login_required
def all_time_stats():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    bundle = load_stats_bundle(user_id, app_user_id)
    payload = None
    stats_ready = bool(bundle)
    stats = SimpleNamespace()
    yearly_breakdown = {}
    if bundle:
        stats_dict = bundle.get("stats") or {}
        stats = SimpleNamespace(**_hydrate_stats_dict(stats_dict))
        yearly_breakdown = bundle.get("yearly_breakdown") or {}
    else:
        payload = _load_cached_all_time_stats_payload(user_id, app_user_id)
        if payload:
            stats_ready = True
            stats_dict = payload.get("stats") or {}
            stats = SimpleNamespace(**_hydrate_stats_dict(stats_dict))
            yearly_breakdown = payload.get("yearly_breakdown") or {}
            _ensure_stats_build(user_id, app_user_id)

    return render_template(
        "all_time_stats.html",
        stats=stats,
        yearly_breakdown=yearly_breakdown,
        stats_ready=stats_ready,
        stats_last_built_at=_stats_last_built_at(user_id, app_user_id),
        sport=request.args.get("sport"),
    )


@app.route("/yearly_review")
@login_required
def yearly_review():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    payload = _load_cached_all_time_stats_payload(user_id, app_user_id)
    if not payload:
        stats = SimpleNamespace()
    else:
        stats_dict = payload.get("stats") or {}
        stats = SimpleNamespace(**_hydrate_stats_dict(stats_dict))
    year_summaries = get_cached_yearly_overview(user_id, app_user_id)

    return render_template(
        "yearly_review.html",
        year_summaries=year_summaries,
        totals=stats,
        stats_last_built_at=_stats_last_built_at(user_id, app_user_id),
    )


@app.route("/yearly_review/<int:year>")
@login_required
def yearly_review_detail(year):
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    if not _load_cached_all_time_stats_payload(user_id, app_user_id):
        flash("Stats need to be built before viewing this page. Refresh from the home page once the build finishes.", "info")
        return redirect(url_for("index"))

    detail = get_cached_year_detail(user_id, year, app_user_id)
    if not detail:
        flash(f"No ride data stored for {year}.")
        return redirect(url_for("yearly_review"))

    return render_template(
        "yearly_review_detail.html",
        detail=detail,
    )


# ---------- COMPARE STATS (the history/compare page) ----------
@app.route("/lifetime")
@login_required
def lifetime():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    bundle = load_stats_bundle(user_id, app_user_id)
    stats_ready = bool(bundle)
    if not stats_ready and _load_cached_all_time_stats_payload(user_id, app_user_id):
        _ensure_stats_build(user_id, app_user_id)

    return render_template(
        "history.html",
        stats_ready=stats_ready,
        stats_last_built_at=_stats_last_built_at(user_id, app_user_id),
    )


@app.route("/compare")
def compare_alias():
    sport = request.args.get("sport")
    return redirect(url_for("lifetime", sport=sport))


### new front pages and navigation


@app.route("/get_summary")
@login_required
def get_summary():
    import datetime

    units = request.args.get("units", "metric")
    since = request.args.get("since", "all")
    custom_date_str = request.args.get("custom_date")
    end_date_str = request.args.get("end_date")

    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    today = datetime.date.today()
    end_date = parse_yyyy_mm_dd(end_date_str) or today
    if end_date_str and not parse_yyyy_mm_dd(end_date_str):
        return redirect(url_for("index"))

    alias_map = {"1week": "week", "1month": "month", "1year": "year"}
    range_key = alias_map.get(since, since)

    if range_key == "custom":
        start_date = parse_yyyy_mm_dd(custom_date_str)
        if custom_date_str and not start_date:
            return redirect(url_for("index"))
    elif range_key == "week":
        start_date = end_date - datetime.timedelta(days=7)
    elif range_key == "month":
        start_date = end_date - datetime.timedelta(days=30)
    elif range_key == "3months":
        start_date = end_date - datetime.timedelta(days=90)
    elif range_key == "year":
        start_date = end_date - datetime.timedelta(days=365)
    elif range_key == "3years":
        start_date = end_date - datetime.timedelta(days=3 * 365)
    elif range_key == "5years":
        start_date = end_date - datetime.timedelta(days=5 * 365)
    elif range_key == "10years":
        start_date = end_date - datetime.timedelta(days=10 * 365)
    else:
        start_date = None

    bundle = _load_or_build_stats_bundle(user_id, app_user_id)
    units_key = units.lower()
    precomputed = None
    if (
        bundle
        and range_key != "custom"
        and not custom_date_str
        and not end_date_str
    ):
        summary_ranges = (bundle.get("summary_ranges") or {}).get(units_key, {})
        precomputed = summary_ranges.get(range_key)
    if precomputed:
        return jsonify(precomputed)

    _ensure_stats_build(user_id, app_user_id)
    return jsonify(
        {
            "status": "building",
            "units": units_key,
            "since": range_key,
            "custom_date_str": custom_date_str,
            "end_date_str": end_date_str,
            "message": "Stats cache not ready.",
        }
    )


@app.route("/sport/<sport>")
def sport_hub(sport):
    sport = sport.lower()
    if sport not in LABELS:
        sport = "total"
    return render_template("sport_hub.html", sport=sport, sport_label=LABELS[sport])

@app.route("/sportshub", defaults={"sport": "total"}, endpoint="sportshub")
@app.route("/sportshub/<sport>", endpoint="sportshub")
def sportshub_alias(sport):
    return redirect(url_for("sport_hub", sport=sport))

@app.route("/sport_hiub", defaults={"sport": "total"}, endpoint="sport_hiub")
@app.route("/sport_hiub/<sport>", endpoint="sport_hiub")
def sport_hiub_alias(sport):
    return redirect(url_for("sport_hub", sport=sport))

# Provide a backwards-compatible endpoint name in case templates reference the typo.
app.add_url_rule(
    "/sports/<sport>",
    view_func=sport_hub,
    endpoint="sports_huib",
)


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
    STRAVA_REDIRECT_URI,
    THUNDERFOREST_API_KEY,
    ORS_API_KEY,
    WEATHER_API_KEY,
)

from user_data_pullers.gear_helpers import load_gear, save_gear

from collections import Counter, defaultdict
import datetime

from services.custom_metrics import compute_normalized_speed


# --- NEW: Graph Builder page ---
GRAPH_METRIC_CATALOG = [
    {
        "key": "distance_km",
        "label": "Distance (km)",
        "unit": "km",
        "description": "Total distance covered.",
        "default_agg": "sum",
    },
    {
        "key": "elev_m",
        "label": "Elevation Gain (m)",
        "unit": "m",
        "description": "Climbing gain.",
        "default_agg": "sum",
    },
    {
        "key": "hours",
        "label": "Moving Time (h)",
        "unit": "h",
        "description": "Moving time converted to hours.",
        "default_agg": "sum",
    },
    {
        "key": "avg_speed_kmh",
        "label": "Avg Speed (km/h)",
        "unit": "km/h",
        "description": "Average speed for each activity.",
        "default_agg": "avg",
    },
    {
        "key": "max_speed_kmh",
        "label": "Max Speed (km/h)",
        "unit": "km/h",
        "description": "Maximum speed reached.",
        "default_agg": "max",
    },
    {
        "key": "temp_C",
        "label": "Temperature (°C)",
        "unit": "°C",
        "description": "Average temperature for the activity.",
        "default_agg": "avg",
    },
    {
        "key": "average_heartrate",
        "label": "Avg Heart Rate",
        "unit": "bpm",
        "description": "Average heart rate.",
        "default_agg": "avg",
    },
    {
        "key": "max_heartrate",
        "label": "Max Heart Rate",
        "unit": "bpm",
        "description": "Maximum recorded heart rate.",
        "default_agg": "max",
    },
    {
        "key": "average_cadence",
        "label": "Avg Cadence",
        "unit": "rpm",
        "description": "Average cadence for the ride.",
        "default_agg": "avg",
    },
    {
        "key": "average_watts",
        "label": "Avg Power (W)",
        "unit": "W",
        "description": "Average power output.",
        "default_agg": "avg",
    },
    {
        "key": "weighted_average_watts",
        "label": "Weighted Power (W)",
        "unit": "W",
        "description": "Weighted average power output.",
        "default_agg": "avg",
    },
    {
        "key": "kilojoules",
        "label": "Kilojoules",
        "unit": "kJ",
        "description": "Work done over the ride.",
        "default_agg": "sum",
    },
    {
        "key": "calories",
        "label": "Calories",
        "unit": "kcal",
        "description": "Estimated calories burned.",
        "default_agg": "sum",
    },
    {
        "key": "suffer_score",
        "label": "Suffer Score",
        "unit": "pts",
        "description": "Relative effort / suffer score.",
        "default_agg": "avg",
    },
    {
        "key": "relative_effort",
        "label": "Relative Effort",
        "unit": "pts",
        "description": "Strava relative effort metric.",
        "default_agg": "avg",
    },
    {
        "key": "moving_time",
        "label": "Moving Time (s)",
        "unit": "s",
        "description": "Moving time in seconds.",
        "default_agg": "sum",
    },
    {
        "key": "elapsed_time",
        "label": "Elapsed Time (s)",
        "unit": "s",
        "description": "Elapsed time in seconds.",
        "default_agg": "sum",
    },
    {
        "key": "achievement_count",
        "label": "Achievements",
        "unit": "count",
        "description": "Number of achievements on the ride.",
        "default_agg": "sum",
    },
    {
        "key": "kudos_count",
        "label": "Kudos",
        "unit": "count",
        "description": "Kudos received.",
        "default_agg": "sum",
    },
    {
        "key": "commute",
        "label": "Commute (0/1)",
        "unit": "flag",
        "description": "Whether the activity was tagged as a commute.",
        "default_agg": "avg",
    },
    {
        "key": "ride_count",
        "label": "Ride Count",
        "unit": "rides",
        "description": "Convenience metric for counting rides.",
        "default_agg": "count",
    },
]

GRAPH_METRIC_LOOKUP = {m["key"]: m for m in GRAPH_METRIC_CATALOG}

_GRAPH_RESPONSE_CACHE = OrderedDict()
_GRAPH_CACHE_TTL = 120
_GRAPH_CACHE_LIMIT = 40

def _graph_cache_key(app_user_id: str, user_id: str, payload: dict) -> str:
    key_payload = json.dumps(payload, sort_keys=True, default=str)
    return f"{app_user_id}:{user_id}:{key_payload}"


def _graph_cache_get(key: str):
    entry = _GRAPH_RESPONSE_CACHE.get(key)
    if not entry:
        return None
    ts, data = entry
    if time.time() - ts > _GRAPH_CACHE_TTL:
        _GRAPH_RESPONSE_CACHE.pop(key, None)
        return None
    _GRAPH_RESPONSE_CACHE.move_to_end(key)
    return data


def _graph_cache_set(key: str, data: dict):
    _GRAPH_RESPONSE_CACHE[key] = (time.time(), data)
    _GRAPH_RESPONSE_CACHE.move_to_end(key)
    while len(_GRAPH_RESPONSE_CACHE) > _GRAPH_CACHE_LIMIT:
        _GRAPH_RESPONSE_CACHE.popitem(last=False)


def _graph_response_from_bundle(bundle, payload):
    if not bundle:
        return None
    graph_series = bundle.get("graph_series") or {}
    grouping = (payload.get("grouping") or "monthly").lower()
    grouping_data = graph_series.get(grouping)
    if not grouping_data:
        return None

    metric_fields = set(GRAPH_METRIC_LOOKUP.keys())
    label_key = None
    for records in grouping_data.values():
        if not records:
            continue
        for key in records[0].keys():
            if key not in metric_fields:
                label_key = key
                break
        if label_key:
            break
    if not label_key:
        return None

    labels = sorted(
        {str(rec.get(label_key) or "") for records in grouping_data.values() for rec in records}
    )
    if not labels or labels == [""]:
        return None

    series_payload = payload.get("series")
    if not series_payload:
        series_payload = [
            {"y_axis": payload.get("y_axis", "distance_km"), "aggregation": payload.get("aggregation", "sum")}
        ]

    datasets = []
    for idx, series in enumerate(series_payload):
        metric = series.get("y_axis", "distance_km")
        records = grouping_data.get(metric)
        if not records:
            continue
        record_map = {
            str(rec.get(label_key) or ""): rec.get(metric) or 0.0 for rec in records
        }
        data = [float(record_map.get(lbl, 0.0)) for lbl in labels]
        label = series.get("label") or GRAPH_METRIC_LOOKUP.get(metric, {}).get("label", metric)
        dataset = {
            "label": label,
            "data": data,
            "yAxisID": "y2" if bool(series.get("secondary")) else "y",
            "series_id": series.get("id") or f"series_bundle_{idx}",
        }
        color = series.get("color")
        if color:
            dataset["borderColor"] = color
            dataset["backgroundColor"] = color + "55" if len(color) == 7 else color
        if series.get("chart"):
            dataset["type"] = series["chart"]
        datasets.append(dataset)

    if not datasets:
        return None

    chart_type = payload.get("chart_type", "mixed")
    stacking = bool(payload.get("stacked", False))
    return {
        "chart_type": chart_type,
        "labels": labels,
        "datasets": datasets,
        "stacked": stacking,
    }

@app.route("/graphs")
@login_required
def graphs_page():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    df = get_cached_activity_df(user_id, app_user_id=app_user_id)

    if df is None or df.empty:
        bikes = []
        types = []
    else:
        bikes = (
            df["gear_id"].dropna().astype(str).sort_values().unique().tolist()
        )
        types = df["type"].dropna().astype(str).sort_values().unique().tolist()

    derived_cols = {
        "distance_km",
        "elev_m",
        "hours",
        "avg_speed_kmh",
        "temp_C",
        "max_speed_kmh",
    }
    available_metrics = []
    for meta in GRAPH_METRIC_CATALOG:
        key = meta["key"]
        if (
            key == "ride_count"
            or (df is not None and not df.empty and key in df.columns)
            or key in derived_cols
        ):
            available_metrics.append(meta)
    if not available_metrics:
        available_metrics = GRAPH_METRIC_CATALOG[:5]

    return render_template(
        "graphs.html",
        bikes=bikes,
        types=types,
        metrics=available_metrics,
    )

@app.route("/api/graph_data", methods=["POST"])
@login_required
def api_graph_data():
    payload = request.get_json(force=True, silent=True) or {}

    chart_type = payload.get("chart_type", "mixed")

    series_payload = payload.get("series")
    if not series_payload:
        series_payload = [
            {
                "y_axis": payload.get("y_axis", "distance_km"),
                "aggregation": payload.get("aggregation", "sum"),
                "label": payload.get("label"),
                "color": payload.get("color"),
                "chart": payload.get("series_chart_type"),
            }
        ]

    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    cache_key = None
    if user_id:
        cache_key = _graph_cache_key(app_user_id, user_id, payload)
        cached = _graph_cache_get(cache_key)
        if cached:
            return jsonify(cached)

    bundle = _load_or_build_stats_bundle(user_id, app_user_id) if user_id else None
    response = _graph_response_from_bundle(bundle, payload)
    if response:
        if cache_key:
            _graph_cache_set(cache_key, response)
        return jsonify(response)

    return jsonify({"labels": [], "datasets": [], "chart_type": chart_type})


# --- NEW: utilities for graph data ---

LOCAL_TZ = tz.gettz("America/New_York")  # keep consistent with your site


def load_activities_df(user_id=None, app_user_id=None):
    if user_id is None:
        app_user_id = get_current_app_user_id()
        user_id = get_strava_id_for_user(app_user_id)
    if app_user_id is None:
        app_user_id = get_current_app_user_id()
    if not user_id:
        return pd.DataFrame()

    cached = load_activity_frame(user_id, app_user_id)
    if cached is not None:
        return cached.copy()

    lock_path = frame_lock_path(user_id, app_user_id)
    with file_lock(lock_path):
        cached = load_activity_frame(user_id, app_user_id)
        if cached is not None:
            return cached.copy()
        df = _build_activity_frame(user_id)
        save_activity_frame(user_id, app_user_id, df)
        return df


def _build_activity_frame(user_id: str) -> pd.DataFrame:
    data = load_activities_cached(user_id)
    df = pd.DataFrame(data)
    if df.empty:
        return df

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
    if "max_speed" in df.columns:
        df["max_speed_kmh"] = df["max_speed"].fillna(0) * 3.6
    else:
        df["max_speed_kmh"] = 0.0

    for k in ["average_temp", "avg_temp", "temp"]:
        if k in df.columns:
            df["temp_C"] = df[k]
            break
    if "temp_C" not in df.columns:
        df["temp_C"] = pd.NA

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

    if "type" not in df.columns:
        df["type"] = "Ride"
    if "sport_type" in df.columns:
        df["type"] = df["sport_type"].fillna(df["type"])

    if "gear_id" not in df.columns:
        df["gear_id"] = pd.NA

    df["date"] = df["start_dt"].dt.date
    df["year"] = df["start_dt"].dt.year
    df["month"] = df["start_dt"].dt.to_period("M").astype(str)
    df["week"] = df["start_dt"].dt.to_period("W").astype(str)
    df["ride_count"] = 1
    df["is_virtual"] = df["type"].astype(str).str.contains("Virtual", case=False, na=False)

    return df


def group_df(df, grouping):
    """Group the dataframe once and return aggregated stats for every numeric column."""

    base = df.copy()
    base["ride_index"] = np.arange(1, len(base) + 1)

    grouping = (grouping or "monthly").lower()
    if grouping == "daily":
        group_col = "date"
    elif grouping == "weekly":
        group_col = "week"
    elif grouping == "monthly":
        group_col = "month"
    elif grouping == "yearly":
        group_col = "year"
    elif grouping == "none":
        group_col = None
    else:
        group_col = "month"
    numeric_cols = [
        c for c in base.columns if pd.api.types.is_numeric_dtype(base[c])
    ]

    aggregated = {}
    if group_col is None:
        labels = base["ride_index"].astype(str).tolist()
        for col in numeric_cols:
            series = base[col].astype(float)
            stats = pd.DataFrame(
                {
                    "sum": series,
                    "count": np.where(series.notna(), 1, 0),
                    "max": series,
                    "min": series,
                    "median": series,
                }
            )
            aggregated[col] = stats
    else:
        grouped = base.groupby(group_col, dropna=True, sort=True)
        index = grouped.size().index
        labels = [str(v) for v in index]
        for col in numeric_cols:
            stats = grouped[col].agg(["sum", "count", "max", "min", "median"])
            aggregated[col] = stats

    return {"labels": labels, "data": aggregated}

    # gear dataaaaaaaa


def _normalize_gear_data(raw):
    return _normalize_gear_data_base(raw)


# Segment routes
# --- Segments Overview ---
@app.route("/segments")
@login_required
def segments_overview():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
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
@login_required
def segment_detail(segment_id):
    sid = str(segment_id)
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
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
@login_required
def segments_refresh():
    app_user_id = get_current_app_user_id()
    if not check_rate_limit(f"segments_refresh:{app_user_id}", 30):
        flash("Please wait before refreshing segments again.", "warning")
        return redirect(url_for("segments_overview"))
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
@login_required
def heatmaps():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    segments = None
    try:
        segments = load_heatmap_segments(user_id, app_user_id)
        if segments is None:
            segments = build_heatmap_segments(user_id, app_user_id)
    except Exception:
        segments = _build_heatmap_segments_inline(user_id)

    if not segments:
        return render_template(
            "heatmaps.html",
            segments=[],
            years=[],
            bikes=[],
            months=list(range(1, 13)),
            pmtiles_base_url=get_pmtiles_base_url() or "",
            pmtiles_assets=get_pmtiles_asset_urls(),
        )

    years_set = set()
    bikes_map = {}
    for seg in segments:
        year = seg.get("year")
        if year is not None:
            years_set.add(year)
        bike_id = seg.get("bike_id") or "unknown"
        bike_name = seg.get("bike_name") or (
            "No bike selected" if bike_id == "unknown" else bike_id
        )
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
        pmtiles_base_url=get_pmtiles_base_url() or "",
        pmtiles_assets=get_pmtiles_asset_urls(),
    )


def _build_heatmap_segments_inline(user_id):
    data = load_activities_cached(user_id)
    if not data:
        return []

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
    for a in data:
        enc = (a.get("map") or {}).get("summary_polyline")
        if not enc:
            continue
        dt = parse_dt(a.get("start_date_local") or a.get("start_date"))
        try:
            coords = polyline.decode(enc)
        except Exception:
            continue
        bike_id = a.get("gear_id") or "unknown"
        segments.append(
            {
                "coords": [[lat, lon] for lat, lon in coords],
                "year": dt.year if dt else None,
                "month": dt.month if dt else None,
                "weekday_num": dt.weekday() if dt else None,
                "bike_id": bike_id,
                "bike_name": bike_name_by_id.get(
                    bike_id, "No bike selected" if bike_id == "unknown" else bike_id
                ),
            }
        )
    return segments


@app.route("/test_bikes")
@login_required
def test_bikes():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return {"error": "not logged in"}, 401

    tokens = resolver.load_tokens(user_id)
    access_token = tokens.get("access_token")
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get("https://www.strava.com/api/v3/athlete/gear", headers=headers)
    return response.json()


@app.route("/activity/<int:activity_id>")
@login_required
def activity_detail(activity_id):
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))


    all_activities = load_activities_cached(user_id)

    # Find the activity by ID
    activity = next((a for a in all_activities if a.get("id") == activity_id), None)

    if not activity:
        return "Activity with ID {activity_id} not found.", 404

    return render_template("activity_detail.html", activity=activity)


@app.route("/activities")
@login_required
def activity_list():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    sport = (request.args.get("sport") or "").strip().lower()
    if sport not in SPORT_MAP:
        sport = "total"

    activities = load_activities_cached(user_id)
    total = len(activities)
    page_size = ACTIVITY_PAGE_SIZE
    initial = activities[:page_size]
    sports = sorted({act.get("type") for act in activities if act.get("type")})

    return render_template(
        "activities.html",
        activities=initial,
        total_activities=total,
        page_size=page_size,
        sport=sport,
        sport_options=sports,
    )


@app.route("/api/activities")
@login_required
def api_activities():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return jsonify({"ok": False, "error": "User not resolved"}), 401

    activities = load_activities_cached(user_id)
    sport = (request.args.get("sport") or "").strip()
    search = (request.args.get("search") or "").strip()

    filtered = [
        act for act in activities if _activity_matches(act, sport=sport or None, search=search or None)
    ]

    total = len(filtered)
    try:
        offset = max(0, int(request.args.get("offset", 0)))
    except ValueError:
        offset = 0
    try:
        limit = int(request.args.get("limit", ACTIVITY_PAGE_SIZE))
    except ValueError:
        limit = ACTIVITY_PAGE_SIZE
    limit = max(1, min(100, limit))

    rows = filtered[offset : offset + limit]
    next_offset = offset + len(rows)
    if next_offset >= total:
        next_offset = None

    return jsonify(
        {
            "ok": True,
            "items": [_serialize_activity(act) for act in rows],
            "total": total,
            "next_offset": next_offset,
        }
    )


@app.route("/add_gear", methods=["POST"])
@login_required
def add_gear():
    new_gear = {
        "name": request.form["name"],
        "type": request.form["type"],
        "start_date": request.form["start_date"],
        "elevation": int(request.form["elevation"]),
        "hours": float(request.form["hours"]),
        "retired": False,
    }

    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
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
@login_required
def update_gear(index, action):
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
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
    return render_template(
        "route_builder.html",
        ors_api_key=ORS_API_KEY,
        pmtiles_base_url=get_pmtiles_base_url() or "",
        pmtiles_assets=get_pmtiles_asset_urls(),
    )


@app.route("/weather")
def weather_page():
    return render_template("weather.html")


@app.route("/api/environment")
def environment_data():
    def _to_float(value, fallback):
        try:
            return float(value)
        except Exception:
            return fallback

    def _to_date(value, fallback):
        try:
            return dt.strptime(value, "%Y-%m-%d").date().isoformat()
        except Exception:
            return fallback

    latitude = _to_float(request.args.get("latitude"), 40.4406)
    longitude = _to_float(request.args.get("longitude"), -79.9959)
    category = (request.args.get("category") or "weather").lower()
    mode = (request.args.get("mode") or "forecast_hourly").lower()
    aggregation = (request.args.get("aggregation") or "raw").lower()
    days = request.args.get("days") or 7

    vars_raw = request.args.getlist("variables")
    if not vars_raw:
        vars_raw = request.args.get("variables") or ""
        vars_raw = [v.strip() for v in vars_raw.split(",") if v.strip()]

    today = dt.utcnow().date()
    default_start = (today - datetime.timedelta(days=7)).isoformat()
    default_end = (today - datetime.timedelta(days=1)).isoformat()
    start_date = _to_date(request.args.get("start_date"), default_start)
    end_date = _to_date(request.args.get("end_date"), default_end)

    available = get_available_variables(category, mode)
    if not vars_raw:
        vars_raw = DEFAULT_VARIABLES.get(category, []) or available[:3]
    variables = [v for v in vars_raw if v in available]

    try:
        if category == "air_quality":
            if mode == "forecast_hourly":
                payload = get_air_quality_forecast_hourly(
                    latitude, longitude, days=int(days), variables=variables
                )
            else:
                payload = get_air_quality_historical_hourly(
                    latitude, longitude, start_date, end_date, variables=variables
                )
        else:
            if mode == "historical_daily":
                payload = get_env_weather_historical_daily(
                    latitude, longitude, start_date, end_date, variables=variables
                )
            elif mode == "historical_hourly":
                payload = get_env_weather_historical_hourly(
                    latitude, longitude, start_date, end_date, variables=variables
                )
            else:
                payload = get_env_weather_forecast_hourly(
                    latitude, longitude, days=int(days), variables=variables
                )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    if aggregation in ("daily_avg", "weekly_avg", "monthly_avg"):
        payload["series"] = _aggregate_environment_series(payload["series"], aggregation)

    return jsonify(
        {
            "ok": True,
            "data": payload,
            "available_variables": available,
            "selected_variables": variables,
            "aggregation": aggregation,
        }
    )


@app.route("/api/weather_data")
def weather_data():
    def _to_float(value, fallback):
        try:
            return float(value)
        except Exception:
            return fallback

    def _to_date(value, fallback):
        try:
            return dt.strptime(value, "%Y-%m-%d").date().isoformat()
        except Exception:
            return fallback

    latitude = _to_float(request.args.get("latitude"), 40.4406)
    longitude = _to_float(request.args.get("longitude"), -79.9959)
    mode = (request.args.get("mode") or "forecast").lower()
    view = (request.args.get("view") or "timeline").lower()
    vars_raw = request.args.get("vars") or ""
    variables = [v.strip() for v in vars_raw.split(",") if v.strip()]

    today = dt.utcnow().date()
    default_start = (today - datetime.timedelta(days=7)).isoformat()
    default_end = (today - datetime.timedelta(days=1)).isoformat()
    start_date = _to_date(request.args.get("start_date"), default_start)
    end_date = _to_date(request.args.get("end_date"), default_end)

    rows = []
    try:
        if mode == "historical_hourly":
            rows = get_historical_hourly(latitude, longitude, start_date, end_date, variables)
        elif mode == "historical_daily":
            rows = get_historical_daily(latitude, longitude, start_date, end_date, variables)
        else:
            mode = "forecast"
            rows = get_forecast_hourly(latitude, longitude, days=10, variables=variables)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    if view in ("daily", "monthly"):
        rows = _aggregate_weather_rows(rows, view)

    return jsonify(
        {
            "ok": True,
            "rows": rows,
            "mode": mode,
            "view": view,
            "variables": variables,
        }
    )


def _aggregate_weather_rows(rows, view):
    if view not in ("daily", "monthly"):
        return rows
    buckets = {}
    for row in rows:
        ts = row.get("timestamp")
        if not ts:
            continue
        date_part = ts.split("T")[0]
        if view == "monthly":
            key = date_part[:7]
            label = key + "-01"
        else:
            key = date_part
            label = date_part
        bucket = buckets.get(key)
        if not bucket:
            bucket = {"timestamp": label, "source": row.get("source")}
            bucket["_count"] = {}
            buckets[key] = bucket
        for k, v in row.items():
            if k in ("timestamp", "source"):
                continue
            if v is None:
                continue
            count_key = bucket["_count"].get(k, 0)
            total_key = bucket.get(k, 0.0)
            bucket[k] = total_key + float(v)
            bucket["_count"][k] = count_key + 1
    aggregated = []
    for key in sorted(buckets.keys()):
        bucket = buckets[key]
        counts = bucket.pop("_count", {})
        for k, v in list(bucket.items()):
            if k in ("timestamp", "source"):
                continue
            count = counts.get(k, 0)
            if count:
                bucket[k] = v / count
            else:
                bucket[k] = None
        aggregated.append(bucket)
    return aggregated


def _aggregate_environment_series(series, aggregation):
    def _parse_ts(value):
        if not value:
            return None
        ts = value.replace("Z", "")
        try:
            return dt.fromisoformat(ts)
        except Exception:
            return None

    buckets = {}
    for var, rows in series.items():
        for row in rows:
            ts = _parse_ts(row.get("t"))
            if not ts:
                continue
            if aggregation == "monthly_avg":
                label = ts.strftime("%Y-%m-01")
            elif aggregation == "weekly_avg":
                week_start = ts.date() - datetime.timedelta(days=ts.weekday())
                label = week_start.isoformat()
            else:
                label = ts.date().isoformat()
            bucket = buckets.setdefault(label, {})
            entry = bucket.setdefault(var, {"sum": 0.0, "count": 0})
            value = row.get("v")
            if isinstance(value, (int, float)):
                entry["sum"] += float(value)
                entry["count"] += 1

    aggregated = {}
    for label in sorted(buckets.keys()):
        bucket = buckets[label]
        for var, stats in bucket.items():
            if not stats["count"]:
                continue
            aggregated.setdefault(var, []).append(
                {"t": label, "v": stats["sum"] / stats["count"]}
            )
    return aggregated


@app.route("/get_ors_key")
def get_ors_key():
    return jsonify({"key": ORS_API_KEY})


def _strava_auth_redirect(state):
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": STRAVA_REDIRECT_URI,
        "scope": "read,activity:read_all",
        "approval_prompt": "auto",
    }
    if state:
        params["state"] = state
    auth_url = f"https://www.strava.com/oauth/authorize?{urlencode(params)}"
    print(f"[strava_oauth] authorize_url={auth_url}")
    return redirect(auth_url)


@app.route("/connect-strava")
@login_required
def connect_strava_route():
    if not session.get("user_id"):
        return redirect(url_for("auth_bp.login"))
    state = _build_strava_state(current_user.id)
    session["strava_connect_state"] = state
    return _strava_auth_redirect(state)


@app.route("/authorize")
def authorize():
    if not session.get("user_id"):
        return redirect(url_for("auth_bp.login"))
    state = session.get("strava_connect_state")
    if not state and current_user.is_authenticated:
        state = _build_strava_state(current_user.id)
        session["strava_connect_state"] = state
    return _strava_auth_redirect(state)


@app.route("/disconnect-strava", methods=["POST"])
@login_required
def disconnect_strava_route():
    athlete_id = getattr(current_user, "strava_athlete_id", None)
    if not athlete_id:
        flash("No Strava account linked.", "warning")
        return redirect(url_for("profile"))
    unlink_strava_account(current_user.id)
    tokens_file = resolver.tokens_path(str(athlete_id))
    try:
        tokens_file.unlink()
    except FileNotFoundError:
        pass
    flash("Strava account disconnected.", "info")
    return redirect(url_for("profile"))


@app.route("/profile/delete_data", methods=["POST"])
@login_required
def profile_delete_data():
    app_user_id = get_current_app_user_id()
    athlete_id = get_strava_id_for_user(app_user_id)
    if not athlete_id:
        flash("No Strava account linked. Nothing to delete.", "warning")
        return redirect(url_for("profile"))
    user_dir = resolver.user_dir(str(athlete_id))
    deleted = 0
    for path in user_dir.glob("*.json"):
        if path.name == "tokens.json":
            continue
        try:
            path.unlink()
            deleted += 1
        except OSError:
            pass
    _clear_user_caches(str(athlete_id), app_user_id)
    flash(f"Deleted {deleted} data file(s) and cleared cached stats.", "info")
    return redirect(url_for("profile"))


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


@app.route("/strava/callback")
@app.route("/exchange")
def exchange():
    """
    OAuth callback from Strava.
    Strava redirects here with ?code=...
    Exchange code for tokens and save them.
    """
    code = request.args.get("code")
    if not code:
        return "Missing ?code from Strava", 400

    state = request.args.get("state") or session.pop("strava_connect_state", None)
    if not state:
        return redirect(url_for("auth_bp.login"))
    try:
        payload = _parse_strava_state(state)
        app_user_id = payload.get("user_id")
    except (BadSignature, SignatureExpired):
        return redirect(url_for("auth_bp.login"))
    if not app_user_id:
        return redirect(url_for("auth_bp.login"))
    user_row = get_user_by_id(app_user_id)
    if not user_row:
        return redirect(url_for("auth_bp.login"))

    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": STRAVA_REDIRECT_URI,
        },
        timeout=30,
    )
    tokens = resp.json()
    if "athlete" not in tokens:
        return f"Error from Strava: {tokens}", 400

    athlete = tokens["athlete"]
    athlete_id = str(athlete["id"])
    resolver.save_tokens(athlete_id, tokens)
    full_name = " ".join(
        [athlete.get("firstname", "").strip(), athlete.get("lastname", "").strip()]
    ).strip() or athlete.get("username") or f"Athlete {athlete_id}"
    update_strava_oauth(
        app_user_id,
        athlete_id,
        full_name,
        tokens.get("access_token"),
        tokens.get("refresh_token"),
        tokens.get("expires_at"),
    )
    if current_user.is_authenticated and str(current_user.id) == str(app_user_id):
        current_user.strava_athlete_id = athlete_id
        current_user.strava_athlete_name = full_name
        current_user.strava_access_token = tokens.get("access_token")
        current_user.strava_refresh_token = tokens.get("refresh_token")
        current_user.strava_expires_at = tokens.get("expires_at")
    flash(f"Connected Strava athlete {full_name}.", "success")

    return redirect(url_for("profile"))


@app.route("/rankings")
def rankings():
    return render_template("rankings.html")


@app.route("/get_bike_list")
@login_required
def get_bike_list():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return []

    return get_cached_bike_list(user_id, app_user_id)


@app.route("/personal_best")
@login_required
def personal_best():
    app_user_id = get_current_app_user_id()
    user_id = get_strava_id_for_user(app_user_id)
    if not user_id:
        return redirect(url_for("index"))

    include_virtual = (request.args.get("virtual", "include") or "include").lower() != "exclude"
    sections = get_cached_personal_best_sections(
        user_id,
        include_virtual=include_virtual,
        app_user_id=app_user_id,
    )
    return render_template(
        "personal_best.html",
        sections=sections,
        include_virtual=include_virtual,
    )

# LOAD MAINTENANCE ROUTES (must be LAST)
from template_helpers.maintenance_routes import maintenance_bp

app.register_blueprint(maintenance_bp)
init_users_db()
_load_pending_reminders()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
