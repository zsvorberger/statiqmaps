"""Disk cache helpers for the normalized activities DataFrame used by graphs."""

from __future__ import annotations

import os
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Optional

import pandas as pd

from user_data_pullers import resolver
FRAME_CACHE_DIR = Path("users_data/activity_frames")


def _scoped_cache_name(app_user_id: str, user_id: str) -> str:
    return f"{app_user_id}__{user_id}"


def _frame_cache_path(user_id: str, app_user_id: str) -> Path:
    return FRAME_CACHE_DIR / f"{_scoped_cache_name(app_user_id, user_id)}.pkl"

def _frame_meta_path(user_id: str, app_user_id: str) -> Path:
    return FRAME_CACHE_DIR / f"{_scoped_cache_name(app_user_id, user_id)}.mtime"


def _frame_lock_path(user_id: str, app_user_id: str) -> Path:
    return FRAME_CACHE_DIR / f"{_scoped_cache_name(app_user_id, user_id)}.pkl.lock"


def frame_lock_path(user_id: str, app_user_id: str) -> Path:
    FRAME_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _frame_lock_path(user_id, app_user_id)


def load_activity_frame(user_id: str, app_user_id: str) -> Optional[pd.DataFrame]:
    path = _frame_cache_path(user_id, app_user_id)
    meta_path = _frame_meta_path(user_id, app_user_id)
    if not path.exists():
        return None
    try:
        current_mtime = os.path.getmtime(resolver.activities_path(user_id))
    except OSError:
        return None
    try:
        cached_mtime = float(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else None
    except Exception:
        return None
    if cached_mtime is None or cached_mtime != current_mtime:
        return None
    try:
        return pd.read_pickle(path)
    except Exception:
        return None


def save_activity_frame(user_id: str, app_user_id: str, df: pd.DataFrame) -> None:
    FRAME_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _frame_cache_path(user_id, app_user_id)
    meta_path = _frame_meta_path(user_id, app_user_id)
    tmp_path = path.with_name(f"{path.name}.tmp.{uuid.uuid4().hex}")
    try:
        try:
            current_mtime = os.path.getmtime(resolver.activities_path(user_id))
        except OSError:
            current_mtime = None
        df.to_pickle(tmp_path)
        os.replace(tmp_path, path)
        if current_mtime is not None:
            meta_path.write_text(str(current_mtime), encoding="utf-8")
    except Exception:
        with suppress(OSError):
            tmp_path.unlink()
