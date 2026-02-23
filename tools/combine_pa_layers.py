#!/usr/bin/env python3
# pip install geopandas shapely
import os
import geopandas as gpd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "static","data")
OSM = os.path.join(DATA, "pa_unpaved.geojson")
DOT = os.path.join(DATA, "pa_unpaved_dot.geojson")
OUT = os.path.join(DATA, "pa_unpaved_combined.geojson")

def main():
    osm = gpd.read_file(OSM).to_crs(epsg=4326)
    dot = gpd.read_file(DOT).to_crs(epsg=4326)

    # Prefer DOT where it overlaps OSM (spatial join to remove OSM overlaps)
    # Buffer tiny to ensure touching lines count as overlap
    dot_buf = dot.buffer(0.00002)  # ~2m
    dot_u = gpd.GeoDataFrame(geometry=dot_buf, crs=dot.crs)
    sjoin = gpd.sjoin(osm, dot_u, predicate="intersects", how="left")
    osm_only = sjoin[sjoin.index_right.isna()].drop(columns=["index_right"])

    combined = gpd.GeoDataFrame(
        pd.concat([dot, osm_only], ignore_index=True),
        crs=dot.crs
    )

    combined.to_file(OUT, driver="GeoJSON")
    print("Wrote", OUT, "features=", len(combined))

if __name__ == "__main__":
    import pandas as pd
    main()
