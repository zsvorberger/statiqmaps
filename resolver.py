# resolver.py
import json
import os
import time
import uuid
from contextlib import suppress
from pathlib import Path

import requests
from user_data_pullers.strava_keys import CLIENT_ID, CLIENT_SECRET
from models.models_users import get_user_by_strava_id, update_tokens_for_athlete

BASE_DIR = Path("users_data")
STRAVA_BASE = "https://www.strava.com/api/v3"

def get_user_id() -> str:
    """
    Auto-detect the user_id from users_data/ID*/tokens.json.
    Returns the numeric athlete.id as a string.
    """
    # Look for ID* folders with tokens.json
    for d in sorted(BASE_DIR.glob("ID*")):
        t = d / "tokens.json"
        if t.exists():
            try:
                data = json.loads(t.read_text(encoding="utf-8"))
                uid = (data.get("athlete") or {}).get("id")
                if uid:
                    return str(uid)
            except Exception:
                continue
            # fallback: parse from folder name (strip "ID")
            return d.name[2:]

    raise RuntimeError("No user_id found. Place tokens.json under users_data/ID########/")

# ---------------- Paths ----------------
def user_dir(user_id: str) -> Path:
    """Return base folder for a given user ID (creates if missing)."""
    d = BASE_DIR / f"ID{user_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d

def tokens_path(user_id: str) -> Path:
    return user_dir(user_id) / "tokens.json"

def activities_path(user_id: str) -> Path:
    return user_dir(user_id) / "strava_activities.json"

def gear_path(user_id: str) -> Path:
    return user_dir(user_id) / "gear_data.json"

def segments_path(user_id: str) -> Path:
    return user_dir(user_id) / "segments_data.json"

# ---------------- Token Handling ----------------
def load_tokens(user_id: str) -> dict:
    """Read tokens.json for a user. Returns {} if missing."""
    row = get_user_by_strava_id(str(user_id))
    if row and (row["strava_access_token"] or row["strava_refresh_token"]):
        return {
            "access_token": row["strava_access_token"],
            "refresh_token": row["strava_refresh_token"],
            "expires_at": row["strava_expires_at"] or 0,
        }
    p = tokens_path(user_id)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))

def save_tokens(user_id: str, tokens: dict) -> None:
    """Write tokens.json for a user."""
    update_tokens_for_athlete(
        str(user_id),
        tokens.get("access_token"),
        tokens.get("refresh_token"),
        tokens.get("expires_at"),
    )
    p = tokens_path(user_id)
    atomic_write_json(p, tokens, indent=2)

def ensure_token(user_id: str) -> str:
    """
    Ensure a valid access_token for this user.
    Refreshes if expired and saves back to tokens.json.
    Returns the valid access_token.
    """
    tokens = load_tokens(user_id)
    if not tokens:
        raise RuntimeError(f"No tokens found for user {user_id}")

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_at = tokens.get("expires_at", 0)

    # If still valid, reuse
    if access_token and time.time() < expires_at - 60:
        return access_token

    # Otherwise refresh
    r = requests.post(
        f"{STRAVA_BASE}/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Token refresh failed: {r.status_code} {r.text}")

    new_tokens = r.json()
    save_tokens(user_id, new_tokens)
    return new_tokens.get("access_token")


def atomic_write_json(path: Path, data: dict, *, indent=2) -> None:
    """Atomically write JSON to disk using a temp file and rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f"{path.name}.tmp.{uuid.uuid4().hex}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=indent)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    except Exception:
        with suppress(OSError):
            tmp_path.unlink()
        raise

# ---------------- Convenience ----------------
def auth_headers(user_id: str) -> dict:
    """Return auth headers for API requests for this user."""
    return {"Authorization": f"Bearer {ensure_token(user_id)}"}
