import subprocess
from pathlib import Path

# -------------------------
# Configuration
# -------------------------

YEARS = [2020, 2021, 2022, 2023]

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
PMTILES_DIR = BASE_DIR / "pmtiles"

LAYER_NAME = "crashes"

TIPPECANOE_FLAGS = [
    "-Z0", "-z12",
    "--drop-densest-as-needed",
    "--coalesce-densest-as-needed",
    "--extend-zooms-if-still-dropping",
    "--force",
]

# -------------------------
# Helpers
# -------------------------

def run_cmd(cmd: list[str]):
    print("\nRunning:")
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)

# -------------------------
# Main
# -------------------------

def main():
    PMTILES_DIR.mkdir(exist_ok=True)

    for year in YEARS:
        input_geojson = OUTPUT_DIR / f"crash_master_{year}_pruned.geojson"
        output_pmtiles = PMTILES_DIR / f"crash_{year}.pmtiles"

        if not input_geojson.exists():
            print(f"❌ Missing input: {input_geojson}")
            continue

        print(f"\n=== Creating PMTiles for {year} ===")

        cmd = [
            "tippecanoe",
            "-o", str(output_pmtiles),
            "-l", LAYER_NAME,
            *TIPPECANOE_FLAGS,
            str(input_geojson),
        ]

        run_cmd(cmd)

        print(f"✅ Wrote: {output_pmtiles}")

    print("\nAll done.")

if __name__ == "__main__":
    main()
