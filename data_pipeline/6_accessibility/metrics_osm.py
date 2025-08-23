# data_pipeline/6_accessibility/metrics_poi.py
from __future__ import annotations
from typing import Dict, Any, Optional
import geopandas as gpd
from shapely.geometry import Point

from base_metric import AccessibilityMetric
from metrics import (
    nearest_distance,
    count_within_buffer,
    format_meters,
)  # re-use your funcs


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
