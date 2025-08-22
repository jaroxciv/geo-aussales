"""
helpers_pois.py
---------------
Transit stop (POI) extraction and caching.

Handles:
- Loading or building per-state transit POIs
- Mode classification via TRANSIT_MODES registry
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path
from loguru import logger
from pyrosm import OSM

from data_pipeline.constants import OSM_PROCESSED_DIR
from data_pipeline.utils import relpath
from state_map import stategroup_to_pbf, StateGroups
from registry import TRANSIT_MODES


# -------------------------------
# Internal helper
# -------------------------------


def _extract_mode(osm: OSM, mode: str, custom_filter: dict) -> gpd.GeoDataFrame:
    """Extract POIs for a given transit mode using pyrosm."""
    gdf = osm.get_pois(custom_filter=custom_filter)
    if gdf is None or gdf.empty:
        logger.warning(f"No POIs found for mode={mode}")
        return gpd.GeoDataFrame(columns=["geometry", "mode"], crs="EPSG:4326")
    gdf["mode"] = mode
    return gdf


# -------------------------------
# Caching helpers
# -------------------------------


def state_transit_cache_path(state: StateGroups) -> Path:
    """Return path for cached state transit POIs."""
    return OSM_PROCESSED_DIR / f"transit_stops_{state.name}.gpkg"


def load_or_build_state_transit(state: StateGroups) -> gpd.GeoDataFrame:
    """
    Load cached transit POIs for the state if present;
    otherwise build once from the state .pbf and cache.
    Includes all modes defined in TRANSIT_MODES.
    """
    cache_path = state_transit_cache_path(state)
    if cache_path.exists():
        return gpd.read_file(cache_path)

    pbf_path = stategroup_to_pbf(state)
    if not pbf_path.exists():
        raise FileNotFoundError(
            f"Missing PBF for {state.name} at {pbf_path}. "
            f"Run: bash scripts/extract_pbf.sh --enum {state.name}"
        )

    logger.info(f"Extracting transit POIs from {pbf_path.name} for {state.name}")
    osm = OSM(str(pbf_path))

    mode_frames = []
    for mode, custom_filter in TRANSIT_MODES.items():
        logger.info(f"  {mode}: {custom_filter}")
        mode_frames.append(_extract_mode(osm, mode, custom_filter))

    gdf = gpd.GeoDataFrame(pd.concat(mode_frames, ignore_index=True), crs="EPSG:4326")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(cache_path, driver="GPKG")
    logger.success(f"Cached {len(gdf):,} transit POIs → {relpath(cache_path)}")
    return gdf
