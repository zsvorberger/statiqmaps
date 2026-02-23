from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import appp


def _status_path(app_user_id: str, user_id: str) -> Path:
    base = Path("users_data") / "stats_build_status"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{app_user_id}__{user_id}.json"


def _write_status(app_user_id: str, user_id: str, payload: dict) -> None:
    path = _status_path(app_user_id, user_id)
    appp.resolver.atomic_write_json(path, payload, indent=2)


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: python -m jobs.stats_build_cli <app_user_id> <user_id>")
        return 2
    app_user_id = str(sys.argv[1])
    user_id = str(sys.argv[2])
    started = time.time()
    _write_status(
        app_user_id,
        user_id,
        {
            "status": "running",
            "message": "Building stats cache…",
            "started_at": started,
            "ended_at": None,
            "error": None,
        },
    )
    try:
        appp.prime_precomputed_stats(user_id, app_user_id)
        stats_path = appp.STATS_CACHE_DIR / f"{app_user_id}__{user_id}.json"
        all_time_path = appp._cache_file(appp._user_cache_name(app_user_id, user_id, "all_time_stats"))
        if not stats_path.exists() or not all_time_path.exists():
            raise RuntimeError("Stats cache files were not written.")
        _write_status(
            app_user_id,
            user_id,
            {
                "status": "completed",
                "message": "Stats ready.",
                "started_at": started,
                "ended_at": time.time(),
                "error": None,
            },
        )
        return 0
    except Exception as exc:
        _write_status(
            app_user_id,
            user_id,
            {
                "status": "error",
                "message": f"Stats build failed: {exc}",
                "started_at": started,
                "ended_at": time.time(),
                "error": str(exc),
            },
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
