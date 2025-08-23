# data_pipeline/6_accessibility/metrics_pois.py
"""
metrics_pois.py
---------------
POI-based accessibility metrics.

Includes:
- Vectorised haversine functions
- Raw metric functions (nearest_distance, count_within_buffer, format_meters)
- Class-based wrappers (NearestDistanceMetric, CountWithinBufferMetric) for engine integration
"""

from __future__ import annotations
from typing import Dict, Any, Optional
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from loguru import logger

from base_metric import AccessibilityMetric

# -------------------------------
# Vectorised Haversine
# -------------------------------


def haversine_np(lat1, lon1, lat2, lon2):
    """Vectorised haversine distance (in meters)."""
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


def _ensure_points(
    gdf: gpd.GeoDataFrame, label: str, debug: bool = False
) -> gpd.GeoDataFrame:
    """Ensure geometries are Points. Non-points are converted to centroids."""
    if gdf.empty:
        return gdf
    non_points = gdf[~gdf.geometry.geom_type.eq("Point")]
    if not non_points.empty:
        if debug:
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
    """Compute nearest haversine distance from store_point to features of a given mode."""
    if features_gdf is None or features_gdf.empty:
        return float("inf")
    subset = features_gdf[features_gdf["mode"] == mode]
    if subset.empty:
        return float("inf")
    subset = _ensure_points(subset, f"nearest_distance[{mode}]")
    lat1, lon1 = store_point.y, store_point.x
    lat2, lon2 = subset.geometry.y.values, subset.geometry.x.values
    dists = haversine_np(lat1, lon1, lat2, lon2)
    return dists.min() if len(dists) else float("inf")


def count_within_buffer(
    store_point: Point, features_gdf: gpd.GeoDataFrame, buffer_m: float, mode: str
) -> int:
    """Count how many features of a given mode fall within buffer_m (meters)."""
    if features_gdf is None or features_gdf.empty:
        return 0
    subset = features_gdf[features_gdf["mode"] == mode]
    if subset.empty:
        return 0
    subset = _ensure_points(subset, f"count_within_buffer[{mode}]")
    lat1, lon1 = store_point.y, store_point.x
    lat2, lon2 = subset.geometry.y.values, subset.geometry.x.values
    dists = haversine_np(lat1, lon1, lat2, lon2)
    return int((dists <= buffer_m).sum())


def format_meters(value: float) -> str:
    """Pretty-format meters for logs/debugging."""
    if value == float("inf"):
        return "∞"
    return f"{value:,.0f} m"


# -------------------------------
# Metric Classes (engine-compatible)
# -------------------------------


class NearestDistanceMetric(AccessibilityMetric):
    name = "nearest"

    def compute(
        self,
        store_point: Point,
        features_gdf: gpd.GeoDataFrame,
        buffer_m: float,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        val = nearest_distance(
            store_point, features_gdf, buffer_m, mode if mode else ""
        )
        return {"nearest_m": float(val)}


class CountWithinBufferMetric(AccessibilityMetric):
    name = "count"

    def compute(
        self,
        store_point: Point,
        features_gdf: gpd.GeoDataFrame,
        buffer_m: float,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        val = count_within_buffer(
            store_point, features_gdf, buffer_m, mode if mode else ""
        )
        return {f"count_{int(buffer_m)}m": int(val)}
