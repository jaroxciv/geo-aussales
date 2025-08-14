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
