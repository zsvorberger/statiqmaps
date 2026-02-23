#!/usr/bin/env python3
"""
Builds static/data/pa_unpaved.geojson from OpenStreetMap using Overpass.

What it does:
- Queries Pennsylvania for all highways that are UNPAVED (gravel/dirt/etc.) or likely unpaved
  (e.g., highway=track without a paved surface), while excluding paved surfaces.
- Retries across multiple Overpass endpoints with exponential backoff.
- Falls back to chunked bbox queries if the whole-state area query fails.
- Deduplicates features and (optionally) simplifies geometry if Shapely is installed.

Run:
  python tools/build_pa_unpaved.py

Requirements:
  pip install requests
  (optional) pip install shapely
"""

import os
import sys
import json
import time
import random
from typing import List, Dict, Any, Tuple

import requests

# ---------- CONFIG ----------

# Where to write the output
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(PROJECT_ROOT, "static", "data")
OUT_PATH = os.path.join(OUT_DIR, "pa_unpaved.geojson")

# Overpass mirrors to rotate through on retry
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

REQUEST_TIMEOUT = 240      # seconds per request
GLOBAL_TIMEOUT  = 180      # seconds in Overpass QL header

# Surface taxonomy
UNPAVED_SURFACES = [
    "unpaved", "gravel", "fine_gravel", "compacted", "dirt", "earth", "ground",
    "clay", "sand", "cinder", "pebblestone", "grass", "wood"
]
PAVED_SURFACES = [
    "asphalt", "concrete", "paved", "chipseal", "sealcoat", "cement",
    "paving_stones", "sett"
]

# Pennsylvania bbox (approx; used for tiled fallback): (S, W, N, E)
PA_BBOX = (39.72, -80.52, 42.27, -74.69)

# Optional simplification tolerance (degrees). Set via env var to enable.
# Example: SIMPLIFY_TOL=0.00005 (~5m) python tools/build_pa_unpaved.py
SIMPLIFY_TOL = float(os.environ.get("SIMPLIFY_TOL", "0"))


# ---------- QUERY BUILDERS ----------

def build_area_query() -> str:
    """Overpass QL using PA administrative area lookup (cleanest)."""
    return f"""
[out:json][timeout:{GLOBAL_TIMEOUT}];
area["name"="Pennsylvania"]["boundary"="administrative"]["admin_level"="4"]->.pa;
(
  // any highway with a surface tag that is NOT obviously paved
  way(area.pa)["highway"]["surface"]["surface"!~"^({'|'.join(PAVED_SURFACES)})$"];

  // any highway with a surface explicitly in our unpaved list
  way(area.pa)["highway"]["surface"~"^({'|'.join(UNPAVED_SURFACES)})$"];

  // typical unpaved tracks if no explicit paved surface marked
  way(area.pa)["highway"="track"]["surface"!~"^({'|'.join(PAVED_SURFACES)})$"];
);
out geom;
""".strip()


def build_bbox_query(bbox: Tuple[float, float, float, float]) -> str:
    """Overpass QL constrained to a bbox (south, west, north, east)."""
    s, w, n, e = bbox
    return f"""
[out:json][timeout:{GLOBAL_TIMEOUT}];
(
  way["highway"]["surface"]["surface"!~"^({'|'.join(PAVED_SURFACES)})$"]({s},{w},{n},{e});
  way["highway"]["surface"~"^({'|'.join(UNPAVED_SURFACES)})$"]({s},{w},{n},{e});
  way["highway"="track"]["surface"!~"^({'|'.join(PAVED_SURFACES)})$"]({s},{w},{n},{e});
);
out geom;
""".strip()


# ---------- HTTP / RETRY ----------

def fetch_overpass(query: str, max_tries: int = 6) -> Dict[str, Any]:
    """POST to Overpass with retries/backoff and rotating endpoints."""
    last_err = None
    for attempt in range(1, max_tries + 1):
        url = OVERPASS_ENDPOINTS[(attempt - 1) % len(OVERPASS_ENDPOINTS)]
        try:
            print(f"  → Overpass try {attempt}/{max_tries} @ {url}")
            r = requests.post(url, data={"data": query}, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            else:
                last_err = f"{r.status_code}: {r.text[:200]}"
        except Exception as e:
            last_err = str(e)
        # Exponential backoff with jitter
        sleep_s = (2 ** attempt) + random.uniform(0.0, 1.5)
        print(f"    Overpass failed: {last_err}. Backing off {sleep_s:.1f}s…")
        time.sleep(sleep_s)
    raise RuntimeError(f"Overpass failed after {max_tries} tries: {last_err}")


# ---------- GEOJSON CONVERSION ----------

def to_feature(way: Dict[str, Any]) -> Dict[str, Any] | None:
    tags = way.get("tags", {}) or {}
    geom = way.get("geometry") or []
    if len(geom) < 2:
        return None
    coords = [[pt["lon"], pt["lat"]] for pt in geom]

    surf = (tags.get("surface") or "").lower().strip()
    if not surf:
        # infer for tracks without paved surface
        surf = "track_unpaved" if tags.get("highway") == "track" else "unknown_unpaved"

    return {
        "type": "Feature",
        "properties": {
            "osm_id": way.get("id"),
            "name": tags.get("name", ""),
            "highway": tags.get("highway", ""),
            "surface": surf,
            "tracktype": tags.get("tracktype", "")
        },
        "geometry": {"type": "LineString", "coordinates": coords}
    }


def elements_to_features(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    feats: List[Dict[str, Any]] = []
    for el in data.get("elements", []):
        if el.get("type") != "way":
            continue
        f = to_feature(el)
        if f:
            feats.append(f)
    return feats


def dedupe_features(feats: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for f in feats:
        oid = f["properties"].get("osm_id")
        key = oid
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
    return out


def simplify_features_if_possible(feats: List[Dict[str, Any]], tol: float) -> List[Dict[str, Any]]:
    if tol <= 0:
        return feats
    try:
        from shapely.geometry import shape, mapping
        simplified = []
        for f in feats:
            geom = shape(f["geometry"])
            g2 = geom.simplify(tol, preserve_topology=False)
            f2 = dict(f)
            f2["geometry"] = mapping(g2)
            simplified.append(f2)
        print(f"  ✓ Simplified geometry with tol={tol}")
        return simplified
    except Exception as e:
        print(f"  ⚠️  Skipping simplify (Shapely not installed? {e})")
        return feats


# ---------- MAIN FLOW ----------

def write_geojson(path: str, feats: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump({"type": "FeatureCollection", "features": feats}, fp)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"  ✓ Wrote {path}  ({size_mb:.1f} MB)  features={len(feats)}")


def split_bbox(bbox: Tuple[float, float, float, float]) -> List[Tuple[float, float, float, float]]:
    """Split a bbox into 4 tiles."""
    s, w, n, e = bbox
    mid_lat = (s + n) / 2.0
    mid_lon = (w + e) / 2.0
    return [
        (s, w,      mid_lat, mid_lon),  # SW
        (s, mid_lon, mid_lat, e),       # SE
        (mid_lat, w, n,      mid_lon),  # NW
        (mid_lat, mid_lon, n, e),       # NE
    ]


def main():
    print("=== build_pa_unpaved.py ===")
    print("Target:", OUT_PATH)
    print("Fetching OpenStreetMap (Overpass)…")
    print("Step 1: Pennsylvania area query")

    # 1) Try area query first (cleanest)
    try:
        data = fetch_overpass(build_area_query())
        feats = elements_to_features(data)
        print(f"  ✓ Area query returned {len(feats)} features before dedupe")
    except Exception as e:
        print(f"  ⚠️  Area query failed: {e}")
        feats = []

    # 2) Fallback: chunked bbox queries if area was empty or failed badly
    if not feats:
        print("Step 2: Fallback to 4 chunked bbox queries over Pennsylvania")
        tiles = split_bbox(PA_BBOX)
        all_feats = []
        for i, tile in enumerate(tiles, start=1):
            print(f"  Tile {i}/4: {tile}")
            try:
                data = fetch_overpass(build_bbox_query(tile))
                tf = elements_to_features(data)
                print(f"    ✓ got {len(tf)} features")
                all_feats.extend(tf)
            except Exception as e:
                print(f"    ⚠️  tile failed: {e}")
        feats = all_feats
        print(f"  → Combined tile features: {len(feats)}")

    # 3) Deduplicate
    feats = dedupe_features(feats)
    print(f"  ✓ After dedupe: {len(feats)} features")

    # 4) Optional simplify
    if SIMPLIFY_TOL > 0:
        feats = simplify_features_if_possible(feats, SIMPLIFY_TOL)

    # 5) Write file
    write_geojson(OUT_PATH, feats)
    print("Done.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(130)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
