#!/usr/bin/env bash
# Extract a city region from a large .osm.pbf file using osmium-tool
# Usage: bash scripts/extract_pbf.sh "CityName, Country"

set -e

CITY_NAME="$1"

# --- Get polygon path from Python script ---
POLYGON_FILE=$(/mnt/c/Users/gnothi/projects/geo-aussales/.venv/Scripts/python.exe scripts/get_polygon.py "$CITY_NAME")

# --- Extract only the city part for filename ---
CITY_ONLY=$(echo "$CITY_NAME" | cut -d',' -f1 | tr '[:upper:]' '[:lower:]' | tr -d ' ')

# Paths
BASE_DIR="/mnt/c/Users/gnothi/projects/geo-aussales"
PBF_SOURCE="$BASE_DIR/data/external/australia-latest.osm.pbf"
OUTPUT_FILE="$BASE_DIR/data/external/${CITY_ONLY}.osm.pbf"

# Check osmium
if ! command -v osmium &> /dev/null; then
    echo "Installing osmium-tool..."
    sudo apt update
    sudo apt install -y osmium-tool
fi

# Check input file
if [ ! -f "$PBF_SOURCE" ]; then
    echo "❌ Source PBF not found: $PBF_SOURCE"
    exit 1
fi

POLYGON_FILE="outputs/$(echo "$CITY_NAME" | tr '[:upper:]' '[:lower:]' | tr -d ' ' | tr ',' '_' | tr -s '_' '_').geojson"

echo "📍 Extracting $CITY_NAME from $PBF_SOURCE"
echo "   Polygon file: $POLYGON_FILE"
echo "   Output: $OUTPUT_FILE"

osmium extract \
    --polygon "$POLYGON_FILE" \
    --overwrite \
    -o "$OUTPUT_FILE" \
    "$PBF_SOURCE"

echo "✅ Extraction complete: $OUTPUT_FILE"
