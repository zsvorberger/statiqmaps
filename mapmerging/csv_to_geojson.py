from pathlib import Path
import argparse
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from prune_crash_geojson import DROP_COLUMNS

def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert merged crash CSV to GeoJSON points."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Dataset year, e.g. 2024",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        help="Path to merged CSV (defaults to mapmerging/output/crash_master_<YEAR>.csv)",
    )
    parser.add_argument(
        "--output-geojson",
        type=Path,
        default=None,
        help="Output GeoJSON path (defaults to mapmerging/output/crash_master_<YEAR>.geojson)",
    )
    parser.add_argument(
        "--output-pruned-geojson",
        type=Path,
        default=None,
        help="Output pruned GeoJSON path (defaults to mapmerging/output/crash_master_<YEAR>_pruned.geojson)",
    )
    return parser.parse_args()

def main():
    args = parse_args()
    year = args.year
    script_dir = Path(__file__).resolve().parent
    output_dir = script_dir / "output"
    output_dir.mkdir(exist_ok=True)
    input_csv = args.input_csv or (output_dir / f"crash_master_{year}.csv")
    output_geojson = args.output_geojson or (output_dir / f"crash_master_{year}.geojson")
    output_pruned_geojson = args.output_pruned_geojson or (
        output_dir / f"crash_master_{year}_pruned.geojson"
    )

    print("Reading merged CSV:")
    print(input_csv)

    # ================= LOAD CSV =================
    if not input_csv.exists():
        raise FileNotFoundError(f"Missing input CSV: {input_csv}")
    df = pd.read_csv(input_csv, low_memory=False)

    # ================= PENNDOT COORDINATES =================
    # These are the OFFICIAL field names from the PennDOT data dictionary
    LAT_COL = "DEC_LATITUDE"
    LON_COL = "DEC_LONGITUDE"

    if LAT_COL not in df.columns or LON_COL not in df.columns:
        raise RuntimeError(
            "Required coordinate columns not found.\n"
            f"Expected: {LAT_COL}, {LON_COL}\n"
            f"Found columns:\n{df.columns.tolist()}"
        )

    print(f"Using latitude column: {LAT_COL}")
    print(f"Using longitude column: {LON_COL}")

    # ================= CLEAN DATA =================
    df = df.dropna(subset=[LAT_COL, LON_COL])

    # Ensure numeric
    df[LAT_COL] = pd.to_numeric(df[LAT_COL], errors="coerce")
    df[LON_COL] = pd.to_numeric(df[LON_COL], errors="coerce")

    df = df.dropna(subset=[LAT_COL, LON_COL])

    # ================= BUILD GEOMETRY =================
    geometry = [
        Point(lon, lat)
        for lon, lat in zip(df[LON_COL], df[LAT_COL])
    ]

    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # ================= WRITE GEOJSON =================
    gdf.to_file(output_geojson, driver="GeoJSON")
    if not output_geojson.exists():
        raise RuntimeError(f"Failed to write GeoJSON: {output_geojson}")

    # ================= PRUNE COLUMNS =================
    cols_to_drop = sorted([c for c in DROP_COLUMNS if c in gdf.columns])
    pruned_gdf = gdf.drop(columns=cols_to_drop)
    pruned_gdf.to_file(output_pruned_geojson, driver="GeoJSON")
    if not output_pruned_geojson.exists():
        raise RuntimeError(f"Failed to write pruned GeoJSON: {output_pruned_geojson}")

    print("========================================")
    print("GeoJSON creation complete")
    print("Features written:", len(gdf))
    print("Output file:", output_geojson)
    print("Pruned output file:", output_pruned_geojson)
    print("========================================")

if __name__ == "__main__":
    main()
