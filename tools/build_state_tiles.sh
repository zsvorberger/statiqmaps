#!/usr/bin/env bash
set -euo pipefail

# --- config (edit if you want) ---
JAR="tools/planetiler.jar"
OSMDIR="osm"
OUTDIR="tiles"
LOGDIR="logs"
MEM="12g"            # 8g works, 12g is faster if you have RAM
DL="true"            # Planetiler may download helper datasets the first time

mkdir -p "$OUTDIR" "$LOGDIR"

# Normalize args: space or comma separated; allow "all" to mean “every PBF in osm/”
if [ $# -eq 0 ]; then
  echo "Usage: bash tools/build_state_tiles.sh <state> [more...]"
  echo "   or: bash tools/build_state_tiles.sh all"
  exit 1
fi

STATES_RAW="$*"
STATES_RAW="${STATES_RAW//,/ }"

if [ "$STATES_RAW" = "all" ] || [ "$STATES_RAW" = "ALL" ]; then
  # build list from files present in osm/
  STATES=""
  for pbf in "$OSMDIR"/*-latest.osm.pbf; do
    [ -f "$pbf" ] || continue
    base="$(basename "$pbf")"
    state="${base%-latest.osm.pbf}"
    STATES="$STATES $state"
  done
else
  STATES="$STATES_RAW"
fi

echo "Building PMTiles for states:"
echo "  $STATES"
echo

for S in $STATES; do
  PBF="$OSMDIR/$S-latest.osm.pbf"
  OUT="$OUTDIR/${S}_basemap.pmtiles"
  MBT="$OUTDIR/${S}_basemap.mbtiles"
  LOG="$LOGDIR/$S.log"

  if [ ! -f "$PBF" ]; then
    echo "!! Missing PBF: $PBF  (skip)"; echo; continue
  fi

  echo "→ $S  →  $OUT  (log: $LOG)"

  # Try direct PMTiles first (fast path on current Planetiler)
  set +e
  time java -Xmx"$MEM" -jar "$JAR" \
    --osm-path "$PBF" \
    --pmtiles "$OUT" \
    --download="$DL" >>"$LOG" 2>&1
  CODE=$?
  set -e

  if [ $CODE -ne 0 ] || [ ! -f "$OUT" ]; then
    echo "   Planetiler --pmtiles failed or file missing; trying MBTiles then convert..." | tee -a "$LOG"
    rm -f "$MBT"
    time java -Xmx"$MEM" -jar "$JAR" \
      --osm-path "$PBF" \
      --mbtiles "$MBT" \
      --download="$DL" >>"$LOG" 2>&1
    # Convert MBTiles → PMTiles
    pmtiles convert "$MBT" "$OUT" >>"$LOG" 2>&1
    rm -f "$MBT" || true
  fi

  if [ -f "$OUT" ]; then
    echo "✓ Done: $OUT" | tee -a "$LOG"
    # After the first success, don’t re-download helper datasets again
    DL="false"
  else
    echo "✗ Failed: $S  (see $LOG)"
  fi
  echo
done

echo "Finished. PMTiles in $OUTDIR; logs in $LOGDIR."
