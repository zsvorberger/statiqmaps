# resolver.py
import json, time, requests
from pathlib import Path
from user_data_pullers.strava_keys import CLIENT_ID, CLIENT_SECRET

BASE_DIR = Path("users_data")
STRAVA_BASE = "https://www.strava.com/api/v3"

def get_user_id() -> str:
    """
    Semi-hardcoded for now: read athlete.id from your tokens.json.
    Future: replace with session-based user_id after OAuth.
    """
    tokens_path = BASE_DIR / "ID25512874" / "tokens.json"   # <= your folder
    try:
        with open(tokens_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        uid = (data.get("athlete") or {}).get("id")         # <-- correct field
        if uid:
            return str(uid)
    except FileNotFoundError:
        pass

    # Fallback 1: infer from folder name ID########
    if tokens_path.parent.name.startswith("ID"):
        return tokens_path.parent.name[2:]

    # Fallback 2: auto-detect the first ID* dir that has tokens.json
    for d in sorted(BASE_DIR.glob("ID*")):
        t = d / "tokens.json"
        if t.exists():
            try:
                with open(t, "r", encoding="utf-8") as f:
                    data = json.load(f)
                uid = (data.get("athlete") or {}).get("id")
                if uid:
                    return str(uid)
            except Exception:
                continue
            return d.name[2:]  # last-resort: folder name

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
    p = tokens_path(user_id)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))

def save_tokens(user_id: str, tokens: dict) -> None:
    """Write tokens.json for a user."""
    p = tokens_path(user_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(tokens, indent=2), encoding="utf-8")

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

# ---------------- Convenience ----------------
def auth_headers(user_id: str) -> dict:
    """Return auth headers for API requests for this user."""
    return {"Authorization": f"Bearer {ensure_token(user_id)}"}
