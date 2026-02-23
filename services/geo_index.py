# services/geo_index.py
from __future__ import annotations
import json, os
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Iterable, List, Sequence, Set, Tuple, Optional

# ---- H3 compatibility (v3 and v4)
try:
    # h3-py v3
    from h3.api.basic_int import geo_to_h3 as _geo_to_h3
    from h3.api.basic_int import h3_to_geo_boundary as _cell_to_boundary
    from h3.api.basic_int import h3_indexes_are_neighbors as _are_neighbors
except Exception:
    # h3 v4
    from h3 import latlng_to_cell as _geo_to_h3
    from h3 import cell_to_boundary as _cell_to_boundary
    from h3 import are_neighbor_cells as _are_neighbors

import polyline as _polyline
from shapely.geometry import LineString
from shapely.ops import transform as _transform
from pyproj import Transformer, Geod

# =========================
# Config (env overridable)
# =========================
H3_RES            = int(os.getenv("H3_RES", "11"))         # ~20m
RESAMPLE_STEP_M   = float(os.getenv("RESAMPLE_STEP_M", "20"))
SNAPSHOT_NAME     = os.getenv("COVERAGE_SNAPSHOT", "default")

# Optional filters
REGION_BBOX_ENV   = os.getenv("REGION_BBOX", "").strip()   # "minlon,minlat,maxlon,maxlat"
START_DATE_ENV    = os.getenv("START_DATE", "").strip()    # "YYYY-MM-DD"
END_DATE_ENV      = os.getenv("END_DATE", "").strip()
PRIVACY_FILE_ENV  = os.getenv("PRIVACY_ZONES_FILE", "").strip()

# Files & folders
ROOT   = Path(__file__).resolve().parents[1]
DATA   = ROOT / "data"
COV_DIR = DATA / "coverage"
ACTIVITIES_CANDIDATES = [DATA / "strava_activities.json", ROOT / "strava_activities.json"]

# Projections / geodesy
WGS84 = "EPSG:4326"
MERC  = "EPSG:3857"
_transformer_fwd = Transformer.from_crs(WGS84, MERC, always_xy=True).transform
_transformer_inv = Transformer.from_crs(MERC, WGS84, always_xy=True).transform
_geod = Geod(ellps="WGS84")

# ---------- helpers ----------
def meters_to_miles(m: float) -> float:
    return m / 1609.344

def _parse_bbox(s: str) -> Optional[Tuple[float,float,float,float]]:
    if not s: return None
    try:
        mnx,mny,mxx,mxy = [float(x) for x in s.split(",")]
        assert mnx < mxx and mny < mxy
        return (mnx,mny,mxx,mxy)
    except Exception:
        print("[geo_index] WARNING: bad REGION_BBOX, ignoring.")
        return None

REGION_BBOX = _parse_bbox(REGION_BBOX_ENV)

def _parse_date(s: str) -> Optional[date]:
    if not s: return None
    try:
        return date.fromisoformat(s)
    except Exception:
        print("[geo_index] WARNING: bad date in START_DATE/END_DATE, ignoring.")
        return None

START_DATE = _parse_date(START_DATE_ENV)
END_DATE   = _parse_date(END_DATE_ENV)

class PrivacyZones:
    """Optional circular privacy zones (in data/privacy_zones.json)."""
    def __init__(self, zones: List[dict]):
        self.z = []
        for it in zones:
            try:
                self.z.append((float(it["lon"]), float(it["lat"]), float(it["radius_m"])))
            except Exception:
                pass

    @classmethod
    def load(cls) -> "PrivacyZones":
        p = Path(PRIVACY_FILE_ENV) if PRIVACY_FILE_ENV else (DATA / "privacy_zones.json")
        if p.exists():
            try:
                return cls(json.loads(p.read_text()))
            except Exception:
                print("[geo_index] WARNING: could not parse privacy_zones.json; continuing without.")
        return cls([])

    def inside(self, lon: float, lat: float) -> bool:
        for (plon, plat, r) in self.z:
            _,_,dist = _geod.inv(plon, plat, lon, lat)
            if dist <= r:
                return True
        return False

PRIVACY = PrivacyZones.load()

# ---------- core pipeline ----------
def _find_activities_path() -> Path:
    for p in ACTIVITIES_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError("Could not find strava_activities.json in /data or project root.")

def _iter_rides(acts: List[dict]) -> Iterable[dict]:
    """Yield *outdoor* rides (no virtual), sorted by start time."""
    def is_ride(a: dict) -> bool:
        t  = (a.get("type") or "").lower().replace(" ", "")
        st = (a.get("sport_type") or "").lower().replace(" ", "")
        return "ride" in (t or st) and "virtual" not in (t + st)

    rides = [a for a in acts if is_ride(a)]

    def _d(a: dict) -> Optional[date]:
        s = a.get("start_date_local") or a.get("start_date")
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None

    if START_DATE or END_DATE:
        r2 = []
        for a in rides:
            d = _d(a)
            if d is None: continue
            if START_DATE and d < START_DATE: continue
            if END_DATE and d > END_DATE: continue
            r2.append(a)
        rides = r2

    rides.sort(key=lambda a: a.get("start_date_local") or a.get("start_date") or "")
    return rides

def _decode_coords(activity: dict) -> List[Tuple[float, float]]:
    m = activity.get("map") or {}
    pl = m.get("summary_polyline") or m.get("polyline")
    if not pl: return []
    pts = _polyline.decode(pl)  # [(lat, lon), ...]
    return [(lon, lat) for (lat, lon) in pts]

def _resample(coords: Sequence[Tuple[float, float]], step_m: float = RESAMPLE_STEP_M) -> List[Tuple[float, float]]:
    if len(coords) < 2: return list(coords)
    line_wgs = LineString(coords)
    line_m   = _transform(_transformer_fwd, line_wgs)
    L = line_m.length
    if L <= 0: return list(coords)
    n_steps = max(1, int(L // step_m))
    pts = [line_m.interpolate(i * step_m) for i in range(n_steps + 1)]
    pts_ll = [_transform(_transformer_inv, p) for p in pts]
    return [(p.x, p.y) for p in pts_ll]

def _hex(lat: float, lon: float, res: int = H3_RES) -> int | str:
    return _geo_to_h3(lat, lon, res)

def _hex_polygon(cell) -> dict:
    """GeoJSON Polygon for one H3 cell (lon/lat). Works with v3/v4."""
    try:
        b = _cell_to_boundary(cell, geo_json=True)  # v3
        ring = [[lng, lat] for (lat, lng) in b]
    except TypeError:
        b = _cell_to_boundary(cell)  # v4 (tuples or dicts)
        ring = []
        for pt in b:
            if isinstance(pt, dict):
                lat, lng = pt["lat"], pt["lng"]
            else:
                lat, lng = pt
            ring.append([lng, lat])
    ring.append(ring[0])
    return {"type": "Polygon", "coordinates": [ring]}

def _seg_len_m(p0: Tuple[float,float], p1: Tuple[float,float]) -> float:
    lon1, lat1 = p0; lon2, lat2 = p1
    _,_,dist = _geod.inv(lon1, lat1, lon2, lat2)
    return dist

def _edge_key(c0: str, c1: str) -> Optional[Tuple[str,str]]:
    if c0 == c1: return None
    try:
        if not _are_neighbors(c0, c1):
            return None
    except Exception:
        pass
    return (c0, c1) if c0 < c1 else (c1, c0)

# ---------- build ----------
def rebuild_indexes(
    activities_path: Path | None = None,
    output_dir: Path = COV_DIR,
    h3_res: int = H3_RES,
    step_m: float = RESAMPLE_STEP_M,
) -> None:
    """
    Builds coverage artifacts and writes a rich coverage_summary.json with:
      - unique_miles_est, total_miles, repeated_miles, sum_new_miles, exploration_pct
      - repeatability_score (cap=3), repeatability_cap
      - per_ride_new_miles_est {activity_id: miles}
      - per_ride_meta {activity_id: {name, date, miles_total}}
      - by_year and by_month breakdowns (totals, new, exploration, repeatability)
      - “bests” (best month, best ride, best exploration ride)
      - streaks / zero-new summary
      - top_repeated_hexes (for curiosity/QA)
      - date_range_seen, filters, generated_at
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "activity_hexes").mkdir(exist_ok=True)
    (output_dir / "new_hexes").mkdir(exist_ok=True)

    if not activities_path:
        activities_path = _find_activities_path()

    acts  = json.loads(Path(activities_path).read_text())
    rides = list(_iter_rides(acts))

    visited_cells: Set[str] = set()
    visited_edges: Set[Tuple[str,str]] = set()

    # Rolling stats
    hex_visits: Dict[str, int] = {}
    per_year_hex_visits: Dict[int, Dict[str,int]] = {}
    per_month_hex_visits: Dict[str, Dict[str,int]] = {}   # "YYYY-MM"
    per_year_total_m: Dict[int, float] = {}
    per_year_new_mi: Dict[int, float] = {}
    per_month_total_m: Dict[str, float] = {}
    per_month_new_mi: Dict[str, float] = {}

    per_ride_new_miles_est: Dict[str, float] = {}
    per_ride_meta: Dict[str, dict] = {}

    total_edge_m = 0.0
    unique_edge_total_m = 0.0
    repeated_edge_m = 0.0

    first_ts, last_ts = None, None

    # streak helpers
    consecutive_with_new = 0
    max_streak = 0
    last_new_date: Optional[date] = None
    longest_gap_days = 0
    rides_with_zero_new = 0

    for a in rides:
        aid = str(a.get("id"))
        dt_str = a.get("start_date_local") or a.get("start_date")
        ride_name = (a.get("name") or "").strip() or f"Ride {aid}"

        if dt_str:
            first_ts = first_ts or dt_str
            last_ts  = dt_str
        ride_date: Optional[date]
        try:
            ride_date = date.fromisoformat((dt_str or "")[:10])
        except Exception:
            ride_date = None
        month_key = ride_date.strftime("%Y-%m") if ride_date else None
        year_key  = ride_date.year if ride_date else None

        coords = _decode_coords(a)
        if not coords:
            per_ride_meta[aid] = {"name": ride_name, "date": (ride_date.isoformat() if ride_date else None),
                                  "miles_total": 0.0}
            per_ride_new_miles_est[aid] = 0.0
            rides_with_zero_new += 1
            continue

        dens = _resample(coords, step_m=step_m)

        ride_hexes: Set[str] = set()
        new_m_m = 0.0
        ride_m  = 0.0

        prev = None
        prev_cell = None
        for (lon, lat) in dens:
            if REGION_BBOX and not ((REGION_BBOX[0] <= lon <= REGION_BBOX[2]) and (REGION_BBOX[1] <= lat <= REGION_BBOX[3])):
                # outside bbox, still count distance but skip privacy cut later
                pass
            if PRIVACY.inside(lon, lat):
                # skip privacy area
                prev = (lon, lat)  # move forward but don't collect hex / edge
                prev_cell = None
                continue

            cell = str(_hex(lat, lon, h3_res))
            ride_hexes.add(cell)

            if prev is not None and prev_cell is not None:
                seg_len = _seg_len_m(prev, (lon, lat))
                ride_m += seg_len
                total_edge_m += seg_len

                ek = _edge_key(prev_cell, cell)
                if ek:
                    if ek not in visited_edges:
                        unique_edge_total_m += seg_len
                        new_m_m += seg_len
                    else:
                        repeated_edge_m += seg_len

                    # visit counting (midpoint)
                    mid_lon = (prev[0] + lon) / 2.0
                    mid_lat = (prev[1] + lat) / 2.0
                    mid_cell = str(_hex(mid_lat, mid_lon, h3_res))
                    hex_visits[mid_cell] = hex_visits.get(mid_cell, 0) + 1
                    if year_key is not None:
                        bucket = per_year_hex_visits.setdefault(year_key, {})
                        bucket[mid_cell] = bucket.get(mid_cell, 0) + 1
                    if month_key is not None:
                        mb = per_month_hex_visits.setdefault(month_key, {})
                        mb[mid_cell] = mb.get(mid_cell, 0) + 1

            prev, prev_cell = (lon, lat), cell

        # Persist artifacts
        (output_dir / "activity_hexes" / f"{aid}.json").write_text(json.dumps(sorted(list(ride_hexes))))
        new_hexes = [h for h in ride_hexes if h not in visited_cells]
        (output_dir / "new_hexes" / f"{aid}.json").write_text(json.dumps(sorted(list(new_hexes))))
        visited_cells.update(ride_hexes)

        # Mark edges visited AFTER computing new
        prev = None
        prev_cell = None
        for (lon, lat) in dens:
            cell = str(_hex(lat, lon, h3_res))
            if prev is not None and prev_cell is not None:
                ek = _edge_key(prev_cell, cell)
                if ek:
                    visited_edges.add(ek)
            prev, prev_cell = (lon, lat), cell

        miles_total = meters_to_miles(ride_m)
        miles_new   = meters_to_miles(new_m_m)

        per_ride_new_miles_est[aid] = round(miles_new, 3)
        per_ride_meta[aid] = {
            "name": ride_name,
            "date": (ride_date.isoformat() if ride_date else None),
            "miles_total": round(miles_total, 3),
        }

        if year_key is not None:
            per_year_total_m[year_key] = per_year_total_m.get(year_key, 0.0) + ride_m
            per_year_new_mi[year_key]   = per_year_new_mi.get(year_key, 0.0)   + miles_new
        if month_key is not None:
            per_month_total_m[month_key] = per_month_total_m.get(month_key, 0.0) + ride_m
            per_month_new_mi[month_key]   = per_month_new_mi.get(month_key, 0.0)   + miles_new

        # streaks / gaps
        if miles_new > 0:
            consecutive_with_new += 1
            max_streak = max(max_streak, consecutive_with_new)
            # gap
            if last_new_date and ride_date:
                gap = (ride_date - last_new_date).days
                if gap > longest_gap_days:
                    longest_gap_days = gap
            if ride_date:
                last_new_date = ride_date
        else:
            rides_with_zero_new += 1
            consecutive_with_new = 0

    # ------ Summaries ------
    unique_miles  = meters_to_miles(unique_edge_total_m)
    total_miles   = meters_to_miles(total_edge_m)
    repeated_miles = meters_to_miles(repeated_edge_m)
    sum_new_miles = sum(per_ride_new_miles_est.values())
    exploration_pct = (100.0 * sum_new_miles / total_miles) if total_miles > 0 else 0.0

    # Repeatability (cap=3)
    cap = 3
    if hex_visits:
        capped_sum = sum(min(v, cap) for v in hex_visits.values())
        repeatability = 100.0 * capped_sum / (cap * len(hex_visits))
    else:
        repeatability = 0.0

    # Yearly breakdowns
    by_year = []
    for yr in sorted(per_year_total_m.keys()):
        tot_mi = meters_to_miles(per_year_total_m[yr])
        new_mi = per_year_new_mi.get(yr, 0.0)
        expl = (100.0 * new_mi / tot_mi) if tot_mi > 0 else 0.0
        yhex = per_year_hex_visits.get(yr, {})
        if yhex:
            capped_sum_y = sum(min(v, cap) for v in yhex.values())
            rep_y = 100.0 * capped_sum_y / (cap * len(yhex))
        else:
            rep_y = 0.0
        by_year.append({
            "year": yr,
            "total_miles": round(tot_mi, 2),
            "new_miles": round(new_mi, 2),
            "exploration_pct": round(expl, 1),
            "repeatability_score": round(rep_y, 1),
        })

    # Monthly breakdowns
    by_month = []
    for mk in sorted(per_month_total_m.keys()):
        tot_mi = meters_to_miles(per_month_total_m[mk])
        new_mi = per_month_new_mi.get(mk, 0.0)
        expl = (100.0 * new_mi / tot_mi) if tot_mi > 0 else 0.0
        mhex = per_month_hex_visits.get(mk, {})
        if mhex:
            capped_sum_m = sum(min(v, cap) for v in mhex.values())
            rep_m = 100.0 * capped_sum_m / (cap * len(mhex))
        else:
            rep_m = 0.0
        y, m = mk.split("-")
        by_month.append({
            "year": int(y),
            "month": int(m),
            "key": mk,
            "total_miles": round(tot_mi, 2),
            "new_miles": round(new_mi, 2),
            "exploration_pct": round(expl, 1),
            "repeatability_score": round(rep_m, 1),
        })

    # Bests / leaderboards
    def _best_month_unique():
        if not by_month: return None
        best = max(by_month, key=lambda x: x["new_miles"])
        return {"year": best["year"], "month": best["month"], "new_miles": best["new_miles"]}
    def _best_month_exploration(min_total=50.0):
        viable = [m for m in by_month if m["total_miles"] >= min_total]
        if not viable: return None
        best = max(viable, key=lambda x: x["exploration_pct"])
        return {"year": best["year"], "month": best["month"], "exploration_pct": best["exploration_pct"], "total_miles": best["total_miles"]}
    def _best_ride_unique():
        if not per_ride_new_miles_est: return None
        rid = max(per_ride_new_miles_est, key=lambda k: per_ride_new_miles_est[k])
        meta = per_ride_meta.get(rid, {"name": f"Ride {rid}", "date": None, "miles_total": 0.0})
        return {"id": rid, "name": meta["name"], "date": meta["date"], "new_miles": round(per_ride_new_miles_est[rid], 2)}
    def _best_ride_exploration(min_total=10.0):
        best_id, best_pct = None, -1.0
        for rid, meta in per_ride_meta.items():
            tot = float(meta.get("miles_total") or 0.0)
            if tot < min_total: continue
            new = float(per_ride_new_miles_est.get(rid, 0.0))
            pct = 100.0 * new / tot if tot > 0 else 0.0
            if pct > best_pct:
                best_pct, best_id = pct, rid
        if best_id is None: return None
        m = per_ride_meta[best_id]
        return {"id": best_id, "name": m["name"], "date": m["date"], "pct_new": round(best_pct, 1), "miles_total": round(float(m.get("miles_total",0)), 1)}

    rides_ranked_by_new = sorted(per_ride_new_miles_est.items(), key=lambda kv: kv[1], reverse=True)[:50]

    zero_pct = 0.0
    total_rides = len(per_ride_meta)
    if total_rides > 0:
        zero_pct = 100.0 * rides_with_zero_new / total_rides

    top_repeated_hexes = sorted(((h, v) for (h,v) in hex_visits.items()), key=lambda kv: kv[1], reverse=True)[:50]

    # Write visited cells (for map overlay)
    (output_dir / "visited_hexes.json").write_text(json.dumps(sorted(list(visited_cells))))

    # Final summary JSON
    summary = {
        "snapshot": SNAPSHOT_NAME,
        "h3_res": h3_res,
        "step_m": step_m,
        "unique_cells": len(visited_cells),
        "unique_miles_est": round(unique_miles, 2),
        "total_miles": round(total_miles, 2),
        "repeated_miles": round(repeated_miles, 2),
        "sum_new_miles": round(sum_new_miles, 2),
        "exploration_pct": round(exploration_pct, 1),
        "repeatability_score": round(repeatability, 1),
        "repeatability_cap": cap,

        "per_ride_new_miles_est": per_ride_new_miles_est,        # {id: miles}
        "per_ride_meta": per_ride_meta,                          # {id: {name,date,miles_total}}

        "by_year": by_year,
        "by_month": by_month,

        "best_month_unique": _best_month_unique(),
        "best_month_exploration": _best_month_exploration(),
        "best_ride_unique": _best_ride_unique(),
        "best_ride_exploration": _best_ride_exploration(),

        "streaks": {
            "exploration_streak_max": int(max_streak),
            "longest_gap_no_new_days": int(longest_gap_days),
            "pct_rides_zero_new": round(zero_pct, 1),
        },

        "rides_ranked_by_new": rides_ranked_by_new,              # [[id, miles]...]
        "top_repeated_hexes": top_repeated_hexes,                # [[hex, visits]...]

        "generated_at": datetime.utcnow().isoformat() + "Z",
        "filters": {
            "region_bbox": REGION_BBOX_ENV or None,
            "privacy_zones": len(PRIVACY.z),
            "start_date": START_DATE_ENV or None,
            "end_date": END_DATE_ENV or None
        },
        "date_range_seen": {"first": first_ts, "last": last_ts}
    }
    (output_dir / "coverage_summary.json").write_text(json.dumps(summary, indent=2))

# ---------- public helpers for Flask ----------
def coverage_geojson(cov_dir: Path | None = None) -> dict:
    """FeatureCollection of all visited hexes (polygons)."""
    base_dir = cov_dir or COV_DIR
    cells_path = base_dir / "visited_hexes.json"
    if not cells_path.exists():
        return {"type": "FeatureCollection", "features": []}
    cells: List[str] = json.loads(cells_path.read_text())
    feats = [{"type": "Feature", "id": c, "properties": {}, "geometry": _hex_polygon(c)} for c in cells]
    return {"type": "FeatureCollection", "features": feats}

def activity_new_geojson(activity_id: str, cov_dir: Path | None = None) -> Tuple[dict, float]:
    """(GeoJSON of new hexes for this activity, new_miles_est)."""
    base_dir = cov_dir or COV_DIR
    hp = base_dir / "new_hexes" / f"{activity_id}.json"
    sp = base_dir / "coverage_summary.json"
    if not hp.exists() or not sp.exists():
        return {"type": "FeatureCollection", "features": []}, 0.0
    cells: List[str] = json.loads(hp.read_text())
    summary = json.loads(sp.read_text())
    miles_map: Dict[str, float] = summary.get("per_ride_new_miles_est", {})
    miles = float(miles_map.get(str(activity_id), 0.0))
    feats = [{"type": "Feature", "id": c, "properties": {}, "geometry": _hex_polygon(c)} for c in cells]
    return {"type": "FeatureCollection", "features": feats}, miles

if __name__ == "__main__":
    COV_DIR.mkdir(parents=True, exist_ok=True)
    rebuild_indexes()
    print(f"✅ Coverage rebuilt into {COV_DIR}")
