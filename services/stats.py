# services/stats.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
SUMMARY_PATHS = [
    ROOT / "data" / "coverage" / "coverage_summary.json",
    ROOT / "coverage_summary.json",
]

def load_summary(summary_path: Path | None = None) -> Dict[str, Any]:
    """Load the precomputed coverage & stats summary. Always returns a dict."""
    paths = [summary_path] if summary_path else SUMMARY_PATHS
    for p in paths:
        if not p:
            continue
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    # Fallback so UI still renders if you haven't indexed yet
    return {
        "unique_miles_est": 0.0,
        "total_miles": 0.0,
        "repeated_miles": 0.0,
        "sum_new_miles": 0.0,
        "exploration_pct": 0.0,
        "repeatability_score": 0.0,
        "repeatability_cap": 3,
        "per_ride_new_miles_est": {},
        "per_ride_meta": {},
        "by_year": [],
        "by_month": [],
        "date_range_seen": {"first": None, "last": None},
        "generated_at": None,
        "streaks": {"exploration_streak_max": 0, "longest_gap_no_new_days": 0, "pct_rides_zero_new": 0.0},
        "rides_ranked_by_new": [],
        "best_month_unique": None,
        "best_month_exploration": None,
        "best_ride_unique": None,
        "best_ride_exploration": None,
    }
