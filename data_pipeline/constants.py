import sys
from pathlib import Path


# Set project venv
VENV_PYTHON = sys.executable

# Project root (2 levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = PROJECT_ROOT / "data_pipeline"

# External data directories
EXTERNAL_DIR = PROJECT_ROOT / "data" / "external"
PBF_DIR = EXTERNAL_DIR / "pbf"  # .osm.pbf extracts
CACHE_DIR = EXTERNAL_DIR / "cache"  # Pyrosm cache files

# Processed data directories
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
GRID_DIR = PROCESSED_DIR / "grid"
MERGED_DIR = PROCESSED_DIR / "merged"
OSM_PROCESSED_DIR = PROCESSED_DIR / "osm"

# AOI metadata file
AOI_META_PATH = PIPELINE_DIR / "aoi_info.json"

# For sanitising GPKG files
SAFE_MAXLEN = 60  # keep some headroom

# POI location data
RAW_STORES_PATH = "data/raw/pois/location_stores.xlsx"
STORES_OUTPUT_PATH = PROCESSED_DIR / "osm" / "store_access.gpkg"

# Census
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CENSUS_URL = "https://www.abs.gov.au/census/find-census-data/geopackages/download"
CENSUS_DIR = RAW_DIR / "census"
