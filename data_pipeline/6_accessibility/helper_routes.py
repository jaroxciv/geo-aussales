"""
helpers_routes.py
-----------------
Transit route (network) extraction and caching.

Handles:
- Loading or building per-state transit routes
- Classifying modes from OSM route/railway tags
"""

import geopandas as gpd
from pathlib import Path
from loguru import logger
from pyrosm import OSM

from data_pipeline.constants import OSM_PROCESSED_DIR
from data_pipeline.utils import relpath
from state_map import stategroup_to_pbf, StateGroups


# -------------------------------
# Mode classification
# -------------------------------


def classify_route_modes(transit_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Assigns a 'mode' column to route-based transit data.
    Looks at OSM tags like 'route', 'railway', 'bus', etc.
    """
    if transit_gdf is None or transit_gdf.empty:
        return gpd.GeoDataFrame(columns=["geometry", "mode"], crs="EPSG:4326")

    def detect_mode(tags):
        if tags.get("route") in ["bus", "trolleybus"]:
            return "bus"
        if tags.get("route") in ["train", "railway", "subway"]:
            return "train"
        if tags.get("route") in ["tram", "light_rail"]:
            return "tram"
        if tags.get("route") == "ferry":
            return "ferry"
        return "other"

    transit_gdf["mode"] = transit_gdf.apply(
        lambda row: detect_mode(row.to_dict()), axis=1
    )
    return transit_gdf


# -------------------------------
# Caching helpers
# -------------------------------


def state_transit_routes_cache_path(state: StateGroups) -> Path:
    """Return path for cached state transit routes."""
    return OSM_PROCESSED_DIR / f"transit_routes_{state.name}.gpkg"


def load_or_build_state_transit_routes(state: StateGroups) -> gpd.GeoDataFrame:
    """
    Load cached transit routes/rails for the state if present;
    otherwise build once from the state .pbf and cache.
    Extracts networks (bus routes, tramways, ferry lines, etc.)
    using get_data_by_custom_criteria.
    """
    cache_path = state_transit_routes_cache_path(state)
    if cache_path.exists():
        return gpd.read_file(cache_path)

    pbf_path = stategroup_to_pbf(state)
    if not pbf_path.exists():
        raise FileNotFoundError(
            f"Missing PBF for {state.name} at {pbf_path}. "
            f"Run: bash scripts/extract_pbf.sh --enum {state.name}"
        )

    logger.info(f"Extracting transit routes from {pbf_path.name} for {state.name}")
    osm = OSM(str(pbf_path))

    # Criteria from Pyrosm docs
    routes = ["bus", "ferry", "railway", "subway", "train", "tram", "trolleybus"]
    rails = ["tramway", "light_rail", "rail", "subway", "tram"]
    bus = ["yes"]

    gdf = osm.get_data_by_custom_criteria(
        custom_filter={
            "route": routes,
            "railway": rails,
            "bus": bus,
            "public_transport": True,
        },
        filter_type="keep",
        keep_nodes=False,  # exclude individual stops (nodes)
        keep_ways=True,  # keep linear features
        keep_relations=True,  # keep route relations
    )

    gdf = classify_route_modes(gdf)

    if gdf is None or gdf.empty:
        logger.warning(f"No transit routes found for {state.name}")
        return gpd.GeoDataFrame(columns=["geometry", "mode"], crs="EPSG:4326")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(cache_path, driver="GPKG")
    logger.success(f"Cached {len(gdf):,} transit routes → {relpath(cache_path)}")
    return gdf
