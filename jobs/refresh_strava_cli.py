from __future__ import annotations

import sys
import time

import appp


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: python -m jobs.refresh_strava_cli <app_user_id> <user_id>")
        return 2
    app_user_id = str(sys.argv[1])
    user_id = str(sys.argv[2])
    started = time.time()
    appp._update_refresh_job(
        user_id,
        app_user_id=app_user_id,
        status="running",
        progress=2,
        message="Starting Strava sync…",
        started_at=started,
        ended_at=None,
    )
    try:
        appp._run_refresh_job(user_id, app_user_id, job_id=None)
        return 0
    except Exception as exc:
        appp._update_refresh_job(
            user_id,
            app_user_id=app_user_id,
            status="error",
            progress=0,
            message=f"Sync failed: {exc}",
            ended_at=time.time(),
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
