#!/usr/bin/env python3
"""
Download Geofabrik .osm.pbf files for US states into ./osm/.

Usage examples:
  python3 tools/download_osm_states.py --states "Pennsylvania, West Virginia, Ohio"
  python3 tools/download_osm_states.py --preset pa_region
  python3 tools/download_osm_states.py --all-us
  python3 tools/download_osm_states.py --states "Virginia, North Carolina" --force
"""

import argparse
import pathlib
import sys
import urllib.request

# --- Where to save .osm.pbf files (relative to repo root) ---
OSM_DIR = pathlib.Path(__file__).resolve().parents[1] / "osm"
OSM_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://download.geofabrik.de/north-america/us/{slug}-latest.osm.pbf"

# Slug map for states/territories as Geofabrik names them
SLUGS = {
    "alabama":"alabama","alaska":"alaska","arizona":"arizona","arkansas":"arkansas",
    "california":"california","colorado":"colorado","connecticut":"connecticut",
    "delaware":"delaware","district of columbia":"district-of-columbia",
    "florida":"florida","georgia":"georgia","hawaii":"hawaii","idaho":"idaho",
    "illinois":"illinois","indiana":"indiana","iowa":"iowa","kansas":"kansas",
    "kentucky":"kentucky","louisiana":"louisiana","maine":"maine","maryland":"maryland",
    "massachusetts":"massachusetts","michigan":"michigan","minnesota":"minnesota",
    "mississippi":"mississippi","missouri":"missouri","montana":"montana",
    "nebraska":"nebraska","nevada":"nevada","new hampshire":"new-hampshire",
    "new jersey":"new-jersey","new mexico":"new-mexico","new york":"new-york",
    "north carolina":"north-carolina","north dakota":"north-dakota","ohio":"ohio",
    "oklahoma":"oklahoma","oregon":"oregon","pennsylvania":"pennsylvania",
    "rhode island":"rhode-island","south carolina":"south-carolina",
    "south dakota":"south-dakota","tennessee":"tennessee","texas":"texas","utah":"utah",
    "vermont":"vermont","virginia":"virginia","washington":"washington",
    "west virginia":"west-virginia","wisconsin":"wisconsin","wyoming":"wyoming",
    # territories commonly used
    "puerto rico":"puerto-rico","united states virgin islands":"united-states-virgin-islands",
}

# Presets you’ll likely use
PRESETS = {
    # PA + surrounding + riding corridor (as you described)
    "pa_region": [
        "Pennsylvania","West Virginia","Ohio","Maryland","Delaware",
        "New Jersey","New York","Virginia","North Carolina","Kentucky","Tennessee"
    ],
    # Everything (states only; no territories)
    "all_states": [s.title() for s in SLUGS.keys()
                   if s not in ("puerto rico","united states virgin islands")],
}

def norm(name: str) -> str:
    return name.strip().lower()

def to_slug(name: str) -> str:
    key = norm(name)
    if key not in SLUGS:
        raise ValueError(f"Unknown state/territory name: {name!r}")
    return SLUGS[key]

def download(slug: str, force: bool = False) -> None:
    url = BASE.format(slug=slug)
    out = OSM_DIR / f"{slug}-latest.osm.pbf"

    if out.exists() and not force:
        print(f"✓ Skipping {out.name} (already exists)")
        return

    tmp = out.with_suffix(out.suffix + ".part")
    print(f"↓ Downloading {slug} → {out}")
    try:
        with urllib.request.urlopen(url) as r, open(tmp, "wb") as f:
            CHUNK = 1024 * 1024
            while True:
                b = r.read(CHUNK)
                if not b:
                    break
                f.write(b)
        tmp.replace(out)
        print(f"✓ Saved {out} ({out.stat().st_size/1e6:.1f} MB)")
    except Exception as e:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        print(f"✗ Failed {slug}: {e}", file=sys.stderr)

def main():
    p = argparse.ArgumentParser()
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--states", help="Comma-separated list of states/territories")
    group.add_argument("--preset", choices=sorted(PRESETS.keys()))
    group.add_argument("--all-us", action="store_true", help="All states + territories")

    p.add_argument("--force", action="store_true", help="Re-download even if file exists")
    args = p.parse_args()

    if args.all_us:
        wanted = list(SLUGS.keys())
    elif args.preset:
        wanted = [norm(s) for s in PRESETS[args.preset]]
    else:
        wanted = [norm(s) for s in args.states.split(",") if s.strip()]

    # Make unique & keep order
    seen, names = set(), []
    for w in wanted:
        if w not in seen:
            names.append(w); seen.add(w)

    print(f"Saving to: {OSM_DIR}")
    for name in names:
        slug = to_slug(name)
        download(slug, force=args.force)

if __name__ == "__main__":
    main()
