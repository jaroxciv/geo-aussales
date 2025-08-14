#!/usr/bin/env bash
set -euo pipefail

# --- Paths ---
BASE_DIR="/mnt/c/Users/gnothi/projects/geo-aussales"
PBF_SOURCE="$BASE_DIR/data/external/australia-latest.osm.pbf"
PYTHON="$BASE_DIR/.venv/Scripts/python.exe"

# --- Get city names (one per line) ---
if [[ "$1" == "--enum" ]]; then
    GROUP_NAME="$2"
    mapfile -t CITY_LIST < <("$PYTHON" scripts/get_city_list.py --enum "$GROUP_NAME")
    SLUG="$GROUP_NAME"
    shift 2
else
    mapfile -t CITY_LIST < <("$PYTHON" scripts/get_city_list.py "$@")
    SLUG=$(echo "$1" | tr '[:upper:]' '[:lower:]' | tr ' ' '_' )
fi

# --- Loop preserving spaces ---
for CITY_NAME in "${CITY_LIST[@]}"; do
    echo "📍 Extracting: $CITY_NAME"
    read -r POLYGON_FILE SLUG_OUT < <(
        "$PYTHON" scripts/get_polygon.py "$CITY_NAME"
    )

    # Strip any Windows carriage returns
    SLUG_OUT=$(echo "$SLUG_OUT" | tr -d '\r')

    OUTPUT_FILE="$BASE_DIR/data/external/${SLUG_OUT}.osm.pbf"

    osmium extract \
        --polygon "$POLYGON_FILE" \
        --overwrite \
        -o "$OUTPUT_FILE" \
        "$PBF_SOURCE"

    echo "✅ Done: $OUTPUT_FILE"
done
