# data_pipeline/6_accessibility/metrics_network.py
from __future__ import annotations
from typing import Dict, Any
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import re

from base_metric import AccessibilityMetric

# ------------------------------
# Shared helpers
# ------------------------------


def clip_to_buffer(
    store_point: Point, gdf: gpd.GeoDataFrame, buffer_m: float
) -> gpd.GeoDataFrame:
    """Clip features to a circular buffer in a projected CRS (EPSG:3577)."""
    if gdf is None or gdf.empty:
        return gdf

    # Project to metric CRS
    gdf_proj = gdf.to_crs(3577)
    pt_proj = gpd.GeoSeries([store_point], crs="EPSG:4326").to_crs(3577).iloc[0]

    buf = pt_proj.buffer(buffer_m)
    return gdf_proj[gdf_proj.intersects(buf)]


def to_meters(length_series) -> pd.Series:
    """Convert degrees (lat/lon) length to meters (approx)."""
    return length_series * 111_000


def parse_maxspeed(val):
    """Parse OSM maxspeed values like '50', '50 km/h', '30 mph'."""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    match = re.search(r"(\d+)", str(val))
    return float(match.group(1)) if match else None


# ------------------------------
# Metric classes
# ------------------------------


class RoadLengthMetric(AccessibilityMetric):
    name = "road_length"

    def compute(
        self,
        store_point: Point,
        features_gdf: gpd.GeoDataFrame,
        buffer_m: float,
        mode=None,
    ) -> Dict[str, Any]:
        clipped = clip_to_buffer(store_point, features_gdf, buffer_m)
        if clipped.empty:
            return {"road_length_m": 0.0}
        total_len = to_meters(clipped.length).sum()
        return {"road_length_m": float(total_len)}


class RoadLanesMetric(AccessibilityMetric):
    name = "avg_lanes"

    def compute(
        self,
        store_point: Point,
        features_gdf: gpd.GeoDataFrame,
        buffer_m: float,
        mode=None,
    ) -> Dict[str, Any]:
        clipped = clip_to_buffer(store_point, features_gdf, buffer_m)
        if clipped.empty or "lanes" not in clipped.columns:
            return {"avg_lanes": 0.0}
        lanes = pd.to_numeric(clipped["lanes"], errors="coerce")
        weights = to_meters(clipped.length)
        valid = lanes.notna()
        if not valid.any():
            return {"avg_lanes": 0.0}
        avg = (lanes[valid] * weights[valid]).sum() / weights[valid].sum()
        return {"avg_lanes": float(avg)}


class RoadSpeedMetric(AccessibilityMetric):
    name = "avg_maxspeed"

    def compute(
        self,
        store_point: Point,
        features_gdf: gpd.GeoDataFrame,
        buffer_m: float,
        mode=None,
    ) -> Dict[str, Any]:
        clipped = clip_to_buffer(store_point, features_gdf, buffer_m)
        if clipped.empty or "maxspeed" not in clipped.columns:
            return {"avg_maxspeed_kmh": 0.0}
        speeds = clipped["maxspeed"].apply(parse_maxspeed)
        weights = to_meters(clipped.length)
        valid = speeds.notna()
        if not valid.any():
            return {"avg_maxspeed_kmh": 0.0}
        avg = (speeds[valid] * weights[valid]).sum() / weights[valid].sum()
        return {"avg_maxspeed_kmh": float(avg)}
