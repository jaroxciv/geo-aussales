# data_pipeline/6_accessibility/osm_loader.py
from __future__ import annotations
from pathlib import Path
from typing import Optional, Literal
import geopandas as gpd
from loguru import logger
from pyrosm import OSM

from data_pipeline.constants import OSM_PROCESSED_DIR
from state_map import StateGroups, stategroup_to_pbf
from loader import load_or_build_group  # your existing POI loader

Layer = Literal["pois", "network", "buildings", "landuse", "natural"]


class OSMLoader:
    """Unified, cached access to Pyrosm layers for a given state."""

    def __init__(self, state: StateGroups):
        self.state = state
        self.pbf_path = stategroup_to_pbf(state)
        self._osm = None  # lazy

    @property
    def osm(self) -> OSM:
        if self._osm is None:
            self._osm = OSM(str(self.pbf_path))
        return self._osm

    def _cache_path(self, layer: str) -> Path:
        return OSM_PROCESSED_DIR / f"{layer}_{self.state.name}.gpkg"

    def load_pois(self, group: str, aggregate: bool = False) -> gpd.GeoDataFrame:
        """Use the existing POI pipeline + cache (merged subcategory filters)."""
        return load_or_build_group(self.state, group, aggregate=aggregate)

    def load(
        self,
        layer: Layer,
        *,
        group: Optional[str] = None,
        aggregate: bool = False,
        network_type: str = "driving",
    ) -> gpd.GeoDataFrame:
        """
        Load a layer, building a simple per-state cache for non-POI layers.
        Note: in step 1 we ignore kwargs (like network_type) in the cache key.
        """
        if layer == "pois":
            if group is None:
                raise ValueError("For layer='pois', you must provide a 'group'.")
            return self.load_pois(group, aggregate=aggregate)

        cache = self._cache_path(layer)
        if cache.exists():
            return gpd.read_file(cache)

        if layer == "network":
            gdf = self.osm.get_network(network_type=network_type)
        elif layer == "buildings":
            gdf = self.osm.get_buildings()
        elif layer == "landuse":
            gdf = self.osm.get_landuse()
        elif layer == "natural":
            gdf = self.osm.get_natural()
        else:
            raise ValueError(f"Unknown OSM layer: {layer}")

        if gdf is None or gdf.empty:
            logger.warning(f"No features for {layer} in {self.state.name}")
            gdf = gpd.GeoDataFrame(columns=["geometry"], crs="EPSG:4326")

        cache.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(cache, driver="GPKG")
        logger.success(f"Cached {len(gdf):,} {layer} features → {cache.name}")
        return gdf
