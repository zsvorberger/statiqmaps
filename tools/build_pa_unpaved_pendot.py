#!/usr/bin/env python3
"""
Extract unpaved roads from PennDOT State Roads dataset.
Reads a GeoJSON from static/data/pa_state_roads.geojson and writes only unpaved segments to
static/data/pa_unpaved_pendot.geojson.
Requires: geopandas
Usage:
  cp <downloaded GeoJSON> static/data/pa_state_roads.geojson
  python tools/build_pa_unpaved_pendot.py
"""

import os
import sys
import geopandas as gpd

SRC = os.path.join("static", "data", "pa_state_roads.geojson")
DST = os.path.join("static", "data", "pa_unpaved_pendot.geojson")

PAVED = {"ASPHALT","CONCRETE","PAVED","CHIP SEAL","SEAL COAT","BITUMINOUS","BRICK"}  # etc.
UNPAVED_HINTS = {"GRAVEL","DIRT","EARTH","COMPACTED","CINDER","SAND","STONE","OIL & CHIP","UNPAVED"}

if not os.path.exists(SRC):
    print(f"Source file not found: {SRC}")
    sys.exit(1)

gdf = gpd.read_file(SRC)

# Auto-detect surface-type column
surf_cols = [c for c in gdf.columns if "surface" in c.lower() or "pave" in c.lower()]
if not surf_cols:
    print("No surface column found. Available columns:", list(gdf.columns))
    sys.exit(1)
surf = surf_cols[0]

def label(u):
    v = u or ""
    vs = str(v).upper()
    if any(p in vs for p in PAVED):
        return "PAVED"
    if any(u in vs for u in UNPAVED_HINTS):
        return "UNPAVED"
    return "UNKNOWN"

gdf["SURF_LABEL"] = gdf[surf].apply(label)
unpaved = gdf[gdf["SURF_LABEL"] == "UNPAVED"].copy()
unpaved = unpaved.to_crs(epsg=4326)

cols = ["NAME","SURF_LABEL", surf, "roadtype"]  # adjust based on attributes
available = [c for c in cols if c in unpaved.columns]
unpaved = unpaved[available + ["geometry"]]

unpaved.to_file(DST, driver="GeoJSON")
print(f"Wrote {DST}, features = {len(unpaved)}")
