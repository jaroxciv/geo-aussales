# data_pipeline/6_accessibility/base_metric.py
from __future__ import annotations
import abc
from typing import Dict, Any, Optional
from shapely.geometry import Point
import geopandas as gpd


class AccessibilityMetric(abc.ABC):
    """Abstract base for all accessibility metrics."""

    name: str  # short identifier used in output column names

    @abc.abstractmethod
    def compute(
        self,
        store_point: Point,
        features_gdf: gpd.GeoDataFrame,
        buffer_m: float,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compute metric(s) for a single store against a feature layer (optionally a specific mode).
        Return a dict like {"nearest_m": 42.0, "count_1000m": 7}.
        """
        raise NotImplementedError
