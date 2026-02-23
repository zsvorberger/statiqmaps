import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run merge -> geojson -> prune for multiple years."
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2023, 2022, 2021, 2020],
        help="Years to process, e.g. 2023 2022 2021 2020",
    )
    return parser.parse_args()


def run_step(script_name, year):
    script_path = SCRIPT_DIR / script_name
    cmd = [sys.executable, str(script_path), "--year", str(year)]
    print(f"\n=== {script_name} {year} ===")
    subprocess.run(cmd, check=True)

def ensure_geojson_exists(year):
    geojson_path = SCRIPT_DIR / "output" / f"crash_master_{year}.geojson"
    if not geojson_path.exists():
        raise FileNotFoundError(
            f"Missing GeoJSON for {year}: {geojson_path}. "
            "csv_to_geojson.py did not produce the expected file."
        )


def main():
    args = parse_args()
    for year in args.years:
        run_step("merge_crashes.py", year)
        run_step("csv_to_geojson.py", year)
        ensure_geojson_exists(year)


if __name__ == "__main__":
    main()
