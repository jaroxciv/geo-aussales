"""
metrics.py
----------
Transit accessibility metrics for store analysis.

Uses vectorised haversine calculations to ensure correct
distances in meters while working in EPSG:4326 (lat/lon).
"""

import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from loguru import logger


# -------------------------------
# Vectorised Haversine
# -------------------------------


def haversine_np(lat1, lon1, lat2, lon2):
    """
    Vectorised haversine distance (in meters).
    lat/lon must be in degrees.
    """
    R = 6371000  # Earth radius in meters
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)

    a = (
        np.sin(dphi / 2.0) ** 2
        + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    )
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


# -------------------------------
# Geometry Safety
# -------------------------------


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
            f"({non_points.geometry.geom_type.unique().tolist()}) → centroids"
        )
        gdf = gdf.copy()
        gdf["geometry"] = gdf.geometry.apply(
            lambda g: g if g.geom_type == "Point" else g.representative_point()
        )
    return gdf


# -------------------------------
# Metric Functions
# -------------------------------


def nearest_distance(
    store_point: Point, features_gdf: gpd.GeoDataFrame, buffer_m: float, mode: str
) -> float:
    """
    Compute nearest haversine distance from store_point to features of a given mode.
    Returns infinity if no features available.
    """
    if features_gdf is None or features_gdf.empty:
        return float("inf")

    subset = features_gdf[features_gdf["mode"] == mode]
    if subset.empty:
        return float("inf")

    subset = _ensure_points(subset, f"nearest_distance[{mode}]")

    lat1, lon1 = store_point.y, store_point.x
    lat2 = subset.geometry.y.values
    lon2 = subset.geometry.x.values
    dists = haversine_np(lat1, lon1, lat2, lon2)
    return dists.min() if len(dists) else float("inf")


def count_within_buffer(
    store_point: Point, features_gdf: gpd.GeoDataFrame, buffer_m: float, mode: str
) -> int:
    """
    Count how many transit features of a given mode fall within buffer_m (meters).
    """
    if features_gdf is None or features_gdf.empty:
        return 0

    subset = features_gdf[features_gdf["mode"] == mode]
    if subset.empty:
        return 0

    subset = _ensure_points(subset, f"count_within_buffer[{mode}]")

    lat1, lon1 = store_point.y, store_point.x
    lat2 = subset.geometry.y.values
    lon2 = subset.geometry.x.values
    dists = haversine_np(lat1, lon1, lat2, lon2)
    return int((dists <= buffer_m).sum())


# -------------------------------
# Utilities
# -------------------------------


def format_meters(value: float) -> str:
    """
    Pretty-format meters for logs/debugging.
    """
    if value == float("inf"):
        return "∞"
    return f"{value:,.0f} m"
