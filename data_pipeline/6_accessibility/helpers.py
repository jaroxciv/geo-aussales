import math
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyrosm import OSM
from loguru import logger
from pathlib import Path

from data_pipeline.constants import EXTERNAL_DIR, OSM_PROCESSED_DIR
from data_pipeline.utils import relpath
from scripts.cities import StateGroups
from state_map import stategroup_to_pbf


# -------------------------------
# Distance Calculations
# -------------------------------


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute haversine distance (in meters) between two lat/lon coordinates."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _ensure_points(gdf: gpd.GeoDataFrame, label: str) -> gpd.GeoDataFrame:
    """
    Ensure geometries are Points. Non-points are converted to centroids.
    Logs a debug message if any conversions happen.
    """
    if gdf.empty:
        return gdf

    non_points = gdf[~gdf.geometry.geom_type.eq("Point")]
    if not non_points.empty:
        logger.debug(
            f"{label}: Converting {len(non_points)} non-point geometries "
            f"({non_points.geometry.geom_type.unique().tolist()}) to centroids"
        )
        gdf = gdf.copy()
        gdf["geometry"] = gdf.geometry.apply(
            lambda g: g if g.geom_type == "Point" else g.centroid
        )
    return gdf


def nearest_distance(
    store_point: Point, features_gdf: gpd.GeoDataFrame, mode: str
) -> float:
    """Return nearest haversine distance from store_point to features of given mode."""
    subset = features_gdf[features_gdf["mode"] == mode].copy()
    if subset.empty:
        return float("inf")

    subset = _ensure_points(subset, f"nearest_distance[{mode}]")

    distances = subset.geometry.apply(
        lambda geom: haversine_distance(store_point.y, store_point.x, geom.y, geom.x)
    )
    return distances.min()


def count_within_buffer(
    store_point: Point, features_gdf: gpd.GeoDataFrame, radius_m: float, mode: str
) -> int:
    """Count features of given mode within haversine buffer (meters) around store_point."""
    subset = features_gdf[features_gdf["mode"] == mode].copy()
    if subset.empty:
        return 0

    subset = _ensure_points(subset, f"count_within_buffer[{mode}]")

    return subset.geometry.apply(
        lambda geom: haversine_distance(store_point.y, store_point.x, geom.y, geom.x)
        <= radius_m
    ).sum()


# -------------------------------
# OSM Transit Query (global load)
# -------------------------------


def load_all_transit(pbf_name: str = "australia-latest.osm.pbf") -> gpd.GeoDataFrame:
    """
    Extract all bus + train stops from the given PBF once.
    Save to processed/osm/transit_stops_raw.gpkg for reuse.
    """
    out_path = OSM_PROCESSED_DIR / "transit_stops_raw.gpkg"
    if out_path.exists():
        return gpd.read_file(out_path)

    pbf_path = str(EXTERNAL_DIR / pbf_name)
    osm = OSM(pbf_path)

    bus = osm.get_pois(custom_filter={"highway": ["bus_stop"]})
    if bus is not None and not bus.empty:
        bus["mode"] = "bus"
    else:
        bus = gpd.GeoDataFrame(columns=["geometry", "mode"], crs="EPSG:4326")

    train = osm.get_pois(custom_filter={"railway": ["station", "halt", "stop"]})
    if train is not None and not train.empty:
        train["mode"] = "train"
    else:
        train = gpd.GeoDataFrame(columns=["geometry", "mode"], crs="EPSG:4326")

    gdf = gpd.GeoDataFrame(pd.concat([bus, train], ignore_index=True), crs="EPSG:4326")
    gdf.to_file(out_path, driver="GPKG")

    return gdf


# -------------------------------
# OSM Transit Query (per state, cached)
# -------------------------------


def find_pbf_for_state(state_enum):
    """
    Resolve the exact PBF path for a StateGroups enum.
    """
    return stategroup_to_pbf(state_enum)


def state_transit_cache_path(state: StateGroups) -> Path:
    """Return path for cached state transit GPKG."""
    return OSM_PROCESSED_DIR / f"transit_stops_{state.name}.gpkg"


def load_or_build_state_transit(state: StateGroups) -> gpd.GeoDataFrame:
    """
    Load cached transit stops (bus+train) for the state if present;
    otherwise build once from the state .pbf and cache.
    """
    cache_path = state_transit_cache_path(state)
    if cache_path.exists():
        return gpd.read_file(cache_path)

    pbf_path = find_pbf_for_state(state)
    if not pbf_path.exists():
        raise FileNotFoundError(
            f"Missing PBF for {state.name} at {pbf_path}. "
            f"Run: bash scripts/extract_pbf.sh --enum {state.name}"
        )

    logger.info(f"Extracting bus/train POIs from {pbf_path.name} for {state.name}")
    osm = OSM(str(pbf_path))

    bus = osm.get_pois(custom_filter={"highway": ["bus_stop"]})
    if bus is not None and not bus.empty:
        bus["mode"] = "bus"
    else:
        bus = gpd.GeoDataFrame(columns=["geometry", "mode"], crs="EPSG:4326")

    train = osm.get_pois(custom_filter={"railway": ["station", "halt", "stop"]})
    if train is not None and not train.empty:
        train["mode"] = "train"
    else:
        train = gpd.GeoDataFrame(columns=["geometry", "mode"], crs="EPSG:4326")

    gdf = gpd.GeoDataFrame(pd.concat([bus, train], ignore_index=True), crs="EPSG:4326")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(cache_path, driver="GPKG")
    logger.success(f"Cached {len(gdf):,} transit points → {relpath(cache_path)}")
    return gdf


# -------------------------------
# Formatting
# -------------------------------


def format_meters(m: float) -> str:
    """Format meters nicely for logs (e.g. '123m' or '∞')."""
    if m == float("inf"):
        return "∞"
    return f"{m:,.0f}m"
