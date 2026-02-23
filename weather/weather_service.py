from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests

OPEN_METEO_ERA5_URL = "https://api.open-meteo.com/v1/era5"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

SUPPORTED_VARIABLES = [
    "temperature_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
    "precipitation_probability",
    "relative_humidity_2m",
    "surface_pressure",
    "snowfall",
    "shortwave_radiation",
]

_HOURLY_VARIABLE_MAP: Dict[str, str] = {
    "temperature_2m": "temp_c",
    "wind_speed_10m": "wind_mps",
    "wind_direction_10m": "wind_dir_deg",
    "precipitation": "precip_mm",
    "precipitation_probability": "precip_prob",
    "relative_humidity_2m": "humidity_pct",
    "surface_pressure": "pressure_hpa",
    "snowfall": "snowfall_cm",
    "shortwave_radiation": "shortwave_wm2",
}

_DAILY_VARIABLE_MAP: Dict[str, Tuple[str, str]] = {
    "temperature_2m": ("temperature_2m_mean", "temp_c"),
    "wind_speed_10m": ("wind_speed_10m_max", "wind_mps"),
    "wind_direction_10m": ("wind_direction_10m_dominant", "wind_dir_deg"),
    "precipitation": ("precipitation_sum", "precip_mm"),
    "precipitation_probability": ("precipitation_probability_max", "precip_prob"),
    "relative_humidity_2m": ("relative_humidity_2m_mean", "humidity_pct"),
    "surface_pressure": ("surface_pressure_mean", "pressure_hpa"),
    "snowfall": ("snowfall_sum", "snowfall_cm"),
    "shortwave_radiation": ("shortwave_radiation_sum", "shortwave_wm2"),
}


def _to_iso(ts: str) -> str:
    if ts.endswith("Z"):
        return ts
    return ts + "Z"


def _normalize_hourly(payload: Dict[str, Any], variables: List[str]) -> List[Dict[str, Any]]:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    datasets = {}
    for var in variables:
        raw = hourly.get(var)
        if raw is not None:
            datasets[var] = raw
    rows: List[Dict[str, Any]] = []
    for idx, ts in enumerate(times):
        row = {"timestamp": _to_iso(ts), "source": "open-meteo"}
        for var, values in datasets.items():
            key = _HOURLY_VARIABLE_MAP.get(var, var)
            if key in ("wind_dir_deg", "precip_prob", "humidity_pct"):
                row[key] = _safe_int(values, idx)
            else:
                row[key] = _safe_float(values, idx)
        rows.append(row)
    return rows


def _normalize_daily(payload: Dict[str, Any], variables: List[str]) -> List[Dict[str, Any]]:
    daily = payload.get("daily") or {}
    times = daily.get("time") or []
    datasets = {}
    for var in variables:
        mapped = _DAILY_VARIABLE_MAP.get(var)
        if not mapped:
            continue
        raw_key, _ = mapped
        raw = daily.get(raw_key)
        if raw is not None:
            datasets[var] = raw
    rows: List[Dict[str, Any]] = []
    for idx, ts in enumerate(times):
        row = {"timestamp": _to_iso(ts), "source": "open-meteo"}
        for var, values in datasets.items():
            _, key = _DAILY_VARIABLE_MAP.get(var, (var, var))
            if key in ("wind_dir_deg", "precip_prob", "humidity_pct"):
                row[key] = _safe_int(values, idx)
            else:
                row[key] = _safe_float(values, idx)
        rows.append(row)
    return rows


def _safe_float(values: List[Any], idx: int) -> Optional[float]:
    try:
        val = values[idx]
    except Exception:
        return None
    try:
        return float(val)
    except Exception:
        return None


def _safe_int(values: List[Any], idx: int) -> Optional[int]:
    try:
        val = values[idx]
    except Exception:
        return None
    try:
        return int(round(float(val)))
    except Exception:
        return None


def _normalize_variable_list(variables: Optional[List[str]]) -> List[str]:
    if not variables:
        return [
            "temperature_2m",
            "wind_speed_10m",
            "wind_direction_10m",
            "precipitation",
            "precipitation_probability",
        ]
    return [v for v in variables if v in SUPPORTED_VARIABLES]


def get_historical_hourly(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    variables: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    variables = _normalize_variable_list(variables)
    hourly_vars = [v for v in variables if v in _HOURLY_VARIABLE_MAP]
    if not hourly_vars:
        hourly_vars = ["temperature_2m"]
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_vars),
        "timezone": "UTC",
        "windspeed_unit": "ms",
        "temperature_unit": "celsius",
    }
    resp = requests.get(OPEN_METEO_ERA5_URL, params=params, timeout=30)
    resp.raise_for_status()
    return _normalize_hourly(resp.json(), hourly_vars)


def get_historical_daily(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    variables: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    variables = _normalize_variable_list(variables)
    daily_vars = [v for v in variables if v in _DAILY_VARIABLE_MAP]
    if not daily_vars:
        daily_vars = ["temperature_2m"]
    daily_params = [(_DAILY_VARIABLE_MAP[v][0]) for v in daily_vars]
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(daily_params),
        "timezone": "UTC",
        "windspeed_unit": "ms",
        "temperature_unit": "celsius",
    }
    resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return _normalize_daily(resp.json(), daily_vars)


def get_forecast_hourly(
    lat: float,
    lon: float,
    days: int = 10,
    variables: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    days = max(1, min(16, int(days)))
    variables = _normalize_variable_list(variables)
    hourly_vars = [v for v in variables if v in _HOURLY_VARIABLE_MAP]
    if not hourly_vars:
        hourly_vars = ["temperature_2m"]
    params = {
        "latitude": lat,
        "longitude": lon,
        "forecast_days": days,
        "hourly": ",".join(hourly_vars),
        "timezone": "UTC",
        "windspeed_unit": "ms",
        "temperature_unit": "celsius",
    }
    resp = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=30)
    resp.raise_for_status()
    return _normalize_hourly(resp.json(), hourly_vars)
