#!/usr/bin/env bash
# Extract a city/region from a large .osm.pbf file using osmium-tool
# Usage: bash scripts/extract_pbf.sh "CityName, Country"

set -euo pipefail

CITY_NAME="$1"
BASE_DIR="/mnt/c/Users/gnothi/projects/geo-aussales"
PBF_SOURCE="$BASE_DIR/data/external/australia-latest.osm.pbf"

# --- Get .poly path and slug from Python ---
# Python will output: "<poly_path> <slug>"
read -r POLYGON_FILE SLUG < <(
    /mnt/c/Users/gnothi/projects/geo-aussales/.venv/Scripts/python.exe scripts/get_polygon.py "$CITY_NAME"
)

# --- Normalize SLUG to pure ASCII, lowercase, underscores ---
SLUG=$(echo "$SLUG" \
    | iconv -c -t ascii//TRANSLIT \
    | tr '[:upper:]' '[:lower:]' \
    | sed 's/[^a-z0-9]/_/g' \
    | sed 's/__\+/_/g' \
    | sed 's/^_//; s/_$//')

OUTPUT_FILE="$BASE_DIR/data/external/${SLUG}.osm.pbf"

# --- Check osmium ---
if ! command -v osmium &> /dev/null; then
    echo "Installing osmium-tool..."
    sudo apt update
    sudo apt install -y osmium-tool
fi

# --- Check source PBF ---
if [ ! -f "$PBF_SOURCE" ]; then
    echo "❌ Source PBF not found: $PBF_SOURCE"
    exit 1
fi

# --- Check polygon file ---
if [ ! -f "$POLYGON_FILE" ]; then
    echo "❌ Polygon file not found: $POLYGON_FILE"
    exit 1
fi

echo "📍 Extracting $CITY_NAME from $PBF_SOURCE"
echo "   Slug: $SLUG"
echo "   Polygon file: $POLYGON_FILE"
echo "   Output: $OUTPUT_FILE"

# --- Run osmium extract ---
osmium extract \
    --polygon "$POLYGON_FILE" \
    --overwrite \
    -o "$OUTPUT_FILE" \
    "$PBF_SOURCE"

echo "✅ Extraction complete: $OUTPUT_FILE"
