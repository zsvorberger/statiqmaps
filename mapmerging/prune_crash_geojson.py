import argparse
import geopandas as gpd
from pathlib import Path

# --------------------
# Paths
# --------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Prune unneeded columns from crash GeoJSON."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Dataset year, e.g. 2024",
    )
    parser.add_argument(
        "--input-geojson",
        type=Path,
        default=None,
        help="Path to crash_master_<YEAR>.geojson",
    )
    parser.add_argument(
        "--output-geojson",
        type=Path,
        default=None,
        help="Output GeoJSON path",
    )
    parser.add_argument(
        "--output-columns",
        type=Path,
        default=None,
        help="Output columns list path",
    )
    return parser.parse_args()

# --------------------
# DEFINITELY SAFE TO DROP
# (Tier 1 + Tier 2 + your added list)
# --------------------
DROP_COLUMNS = [
    # ---- Reporting / admin ----
    "PSP_REPORTED",
    "RDWY_SEQ_NUM",
    "UNIT_NUM",
    "SEGMENT",
    "SPEC_JURIS_CD",
    "OFFSET",
    "NTFY_HIWY_MAINT",
    "ROADWAY_CLEARED",
    "POLICE_AGCY",

    # ---- Street / routing text ----
    "STREET_NAME",
    "ROUTE",
    "RAMP_SEGMENT",
    "RAMP_TERMINAL",

    # ---- Driver age buckets (not cycling-relevant) ----
    "DRIVER_COUNT_16YR",
    "DRIVER_COUNT_17YR",
    "DRIVER_COUNT_18YR",
    "DRIVER_COUNT_19YR",
    "DRIVER_COUNT_20YR",
    "DRIVER_COUNT_50_64YR",
    "DRIVER_COUNT_65_74YR",
    "DRIVER_COUNT_75PLUS",
    "MATURE_DRIVER",

    # ---- Speed limit edge cases ----
    "LIMIT_65MPH",
    "LIMIT_70MPH",
    "SPEED_CHANGE_LANE",

    # ---- Vehicle / crash mechanics ----
    "FIRE_IN_VEHICLE",
    "OVERTURNED",
    "PHANTOM_VEHICLE",
    "TROLLEY",
    "TURNPIKE",

    # ---- Road classification clutter ----
    "FEDERAL_AID_ROUTE",
    "LOCAL_ROAD",
    "LOCAL_ROAD_ONLY",
    "OTHER_FREEWAY_EXPRESSWAY",
    "RURAL",

    # ---- Direction / orientation minutiae ----
    "HO_OPPDIR_SDSWP",
    "LN_CLOSE_DIR",
    "RDWY_ORIENT",
    "RELATION_TO_ROAD",

    # ---- Injury granularity (already covered elsewhere) ----
    "POSSIBLE_INJURY",
    "POSSIBLE_INJ_COUNT",

    # ---- Drug / impairment flags (too coarse for mapping) ----
    "MARIJUANA_RELATED",

    # ---- Clearance / blockage ----
    "NO_CLEARANCE",

    # ---- School / bus related ----
    "SCHOOL_BUS_RELATED",
    "SCHOOL_BUS_UNIT",
    "SCHOOL_ZONE",
    "SCH_BUS_IND",
    "SCH_ZONE_IND",

    # ---- Secondary / compound crashes ----
    "SECONDARY_CRASH",

    # ---- Shoulder / signal specifics ----
    "SHLDR_RELATED",
    "SIGNALIZED_INT",

    # ---- Unbelted stats (vehicle safety, not cyclist safety) ----
    "UNBELTED",
    "UNBELTED_OCC_COUNT",
    "UNB_DEATH_COUNT",
    "UNB_SUSP_SERIOUS_INJ_COUNT",

    # ---- Motorcycle helmet / clothing compliance ----
    "MC_DVR_BOOTS_IND",
    "MC_DVR_EDC_IND",
    "MC_DVR_EYEPRT_IND",
    "MC_DVR_HLMTDOT_IND",
    "MC_DVR_HLMTON_IND",
    "MC_DVR_HLMT_TYPE",
    "MC_DVR_LNGPNTS_IND",
    "MC_DVR_LNGSLV_IND",
    "MC_PAS_BOOTS_IND",
    "MC_PAS_EYEPRT_IND",
    "MC_PAS_HLMTDOT_IND",
    "MC_PAS_HLMTON_IND",
    "MC_PAS_HLMT_TYPE",
    "MC_PAS_LNGPNTS_IND",
    "MC_PAS_LNGSLV_IND",
    "MC_ENGINE_SIZE",
    "MC_PASSNGR_IND",
    "MC_TRAIL_IND",

    # ---- Work zone minutiae ----
    "WZ_FLAGGER",
    "WZ_LAW_OFFCR_IND",
    "WZ_OTHER",
    "WZ_SHLDER_MDN",
    "WZ_MOVING",
    "WZ_CLOSE_DETOUR",
    "WZ_WORKERS_INJ_KILLED",

    # ---- Vehicle mechanics ----
    "ENGINE_SIZE",
    "AXLE_COUNT",
    "TOWED_UNIT_IND",
    "TRAILER_TYPE",
    "VEH_WEIGHT_RATING",
    "VEH_CONFIGURATION",

    # ---- Post-crash response ----
    "ROADWAY_CLEARED_TIME",
    "EMS_NOTIFIED_TIME",
    "EMS_ARRIVAL_TIME",
    "TOW_ARRIVAL_TIME",
    "FIRE_DEPARTMENT_RESPONDED",

    # ---- Legal outcomes ----
    "CITATION_ISSUED",
    "CITATION_COUNT",
    "ARREST_MADE",
    "CHARGES_FILED",
    "COURT_REFERRAL",

    # ---- Hyper-granular roadway ----
    "CURB_TYPE",
    "SHOULDER_TYPE",
    "MEDIAN_TYPE",
    "LANE_MARKING_TYPE",
    "DRAINAGE_TYPE",

    # ---- Person-level detail ----
    "PERSON_ROLE",
    "PERSON_POSITION",
    "SEATING_POSITION",
    "SAFETY_EQUIPMENT",
    "EJECTION_STATUS",

    # ---- Join artifacts ----
    "RAMP_x",
    "RAMP_y",
    "INTERSECTION_RELATED_x",
    "INTERSECTION_RELATED_y",
]

def main():
    args = parse_args()
    year = args.year
    input_geojson = args.input_geojson or Path(f"mapmerging/output/crash_master_{year}.geojson")
    output_geojson = args.output_geojson or Path(f"mapmerging/output/crash_master_{year}_pruned.geojson")
    output_columns = args.output_columns or Path("mapmerging/output/kept_columns.txt")

    if not input_geojson.exists():
        raise FileNotFoundError(f"Missing input GeoJSON: {input_geojson}")

    print(f"Reading: {input_geojson}")
    gdf = gpd.read_file(input_geojson)

    before_cols = list(gdf.columns)
    print(f"Columns before: {len(before_cols)}")

    cols_to_drop = sorted([c for c in DROP_COLUMNS if c in gdf.columns])

    print(f"Dropping {len(cols_to_drop)} columns:")
    for c in cols_to_drop:
        print(f"  - {c}")

    gdf = gdf.drop(columns=cols_to_drop)

    after_cols = sorted(gdf.columns)
    print(f"Columns after: {len(after_cols)}")

    print(f"Writing pruned GeoJSON:\n{output_geojson}")
    gdf.to_file(output_geojson, driver="GeoJSON")

    print(f"Writing kept column list:\n{output_columns}")
    with open(output_columns, "w") as f:
        for c in after_cols:
            f.write(c + "\n")

    print("Done.")

if __name__ == "__main__":
    main()
