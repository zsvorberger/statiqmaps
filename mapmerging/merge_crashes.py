from pathlib import Path
import argparse
import pandas as pd

def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge PennDOT statewide crash tables into one CSV."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Dataset year, e.g. 2024",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Path to Statewide_<YEAR> folder (defaults to ~/Downloads/Statewide_<YEAR>)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "output",
        help="Output directory",
    )
    return parser.parse_args()

def main():
    args = parse_args()
    year = args.year
    data_dir = args.data_dir or (Path.home() / "Downloads" / f"Statewide_{year}")
    output_dir = args.output_dir
    output_dir.mkdir(exist_ok=True)
    output_csv = output_dir / f"crash_master_{year}.csv"

    print("Reading data from:", data_dir)
    print("Writing output to:", output_dir)

# ============ LOAD CRASH-LEVEL TABLES ============
    crash = pd.read_csv(data_dir / f"CRASH_{year}.csv")
    flags = pd.read_csv(data_dir / f"FLAGS_{year}.csv")
    road = pd.read_csv(data_dir / f"ROADWAY_{year}.csv")
    cycle = pd.read_csv(data_dir / f"CYCLE_{year}.csv")

# ============ LOAD MULTI-ROW TABLES ============
    person = pd.read_csv(data_dir / f"PERSON_{year}.csv")
    vehicle = pd.read_csv(data_dir / f"VEHICLE_{year}.csv")
    commveh = pd.read_csv(data_dir / f"COMMVEH_{year}.csv")
    trailveh = pd.read_csv(data_dir / f"TRAILVEH_{year}.csv")

# ============ AGGREGATE PERSON ============
    print("Aggregating PERSON...")
    person_agg = (
        person.groupby("CRN")
        .agg(
            person_count=("PERSON_NUM", "count"),
            injury_count=("INJ_SEVERITY", lambda x: (x != "No Injury").sum()),
            max_person_severity=("INJ_SEVERITY", "max"),
        )
        .reset_index()
    )

# ============ AGGREGATE VEHICLE ============
    print("Aggregating VEHICLE...")
    vehicle_agg = (
        vehicle.groupby("CRN")
        .agg(vehicle_count=("UNIT_NUM", "count"))
        .reset_index()
    )

# ============ AGGREGATE COMMERCIAL VEHICLES ============
    commveh_agg = (
        commveh.groupby("CRN")
        .size()
        .reset_index(name="commercial_vehicle_count")
    )

# ============ AGGREGATE TRAILER VEHICLES ============
    trailveh_agg = (
        trailveh.groupby("CRN")
        .size()
        .reset_index(name="trailer_vehicle_count")
    )

# ============ MERGE EVERYTHING ============
    print("Merging tables...")
    df = crash.merge(flags, on="CRN", how="left")
    df = df.merge(road, on="CRN", how="left")
    df = df.merge(cycle, on="CRN", how="left")
    df = df.merge(person_agg, on="CRN", how="left")
    df = df.merge(vehicle_agg, on="CRN", how="left")
    df = df.merge(commveh_agg, on="CRN", how="left")
    df = df.merge(trailveh_agg, on="CRN", how="left")

# ============ WRITE OUTPUT ============
    df.to_csv(output_csv, index=False)

    print("Done.")
    print("Rows written:", len(df))
    print("Output:", output_csv)

if __name__ == "__main__":
    main()
