# data_pipeline/6_accessibility/loader.py

from pathlib import Path
import geopandas as gpd
from pyrosm import OSM
from loguru import logger
import pandas as pd

from data_pipeline.constants import OSM_PROCESSED_DIR
from data_pipeline.utils import relpath
from state_map import StateGroups, stategroup_to_pbf
from urban_groups import URBAN_GROUPS


def _cache_path(state: StateGroups, group: str) -> Path:
    return OSM_PROCESSED_DIR / f"{group}_{state.name}.gpkg"


def get_group_subcategories(group: str) -> dict[str, dict]:
    """
    Expand a GroupedOsmTagsFilter entry into {subcategory: OsmTagsFilter}.
    Works for any SRAI base group.

    Example:
        "education" → {
            "school": {"amenity": ["school"]},
            "university": {"amenity": ["university"]},
            "kindergarten": {"amenity": ["kindergarten"]}
        }
    """
    if group not in URBAN_GROUPS:
        raise KeyError(f"Unknown group '{group}'")

    raw = URBAN_GROUPS[group]
    subcats: dict[str, dict] = {}

    for key, values in raw.items():
        if isinstance(values, list):
            for v in values:
                if v not in subcats:
                    subcats[v] = {key: [v]}
                else:
                    # Merge if already exists
                    if key in subcats[v]:
                        subcats[v][key].append(v)
                    else:
                        subcats[v][key] = [v]
        elif isinstance(values, str):
            v = values
            if v not in subcats:
                subcats[v] = {key: [v]}
            else:
                if key in subcats[v]:
                    subcats[v][key].append(v)
                else:
                    subcats[v][key] = [v]
        elif isinstance(values, bool) and values:
            if key not in subcats:
                subcats[key] = {key: True}
            else:
                subcats[key][key] = True

    return subcats


def load_or_build_group(
    state: StateGroups, group: str, aggregate: bool = False
) -> gpd.GeoDataFrame:
    cache_path = _cache_path(state, f"{group}{'_agg' if aggregate else ''}")
    if cache_path.exists():
        return gpd.read_file(cache_path)

    pbf_path = stategroup_to_pbf(state)
    osm = OSM(str(pbf_path))

    if aggregate:
        # Lump all subcategories together
        from srai.loaders.osm_loaders.filters import merge_osm_tags_filter

        osm_filter = merge_osm_tags_filter({group: URBAN_GROUPS[group]})
        gdf = osm.get_pois(custom_filter=osm_filter)
        if gdf is None or gdf.empty:
            return gpd.GeoDataFrame(
                columns=["geometry", "group", "mode"], crs="EPSG:4326"
            )
        gdf["group"] = group
        gdf["mode"] = group  # lumped
        out = gdf[["geometry", "group", "mode"]]
    else:
        # Per-subcategory with merged filters
        subcats = get_group_subcategories(group)

        # Detect and log merged keys
        for mode, osm_filter in subcats.items():
            if len(osm_filter) > 1:
                merged_keys = ", ".join(osm_filter.keys())
                logger.info(f"Merged filters for {group}:{mode} → [{merged_keys}]")

        frames = []
        for mode, osm_filter in subcats.items():
            gdf = osm.get_pois(custom_filter=osm_filter)
            if gdf is None or gdf.empty:
                logger.warning(f"No POIs found for {group}:{mode} in {state.name}")
                continue
            gdf["group"] = group
            gdf["mode"] = mode
            frames.append(gdf[["geometry", "group", "mode"]])

        out = (
            gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
            if frames
            else gpd.GeoDataFrame(
                columns=["geometry", "group", "mode"], crs="EPSG:4326"
            )
        )

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_file(cache_path, driver="GPKG")
    logger.success(
        f"Cached {len(out):,} POIs for group={group}{' (aggregated)' if aggregate else ''} → {relpath(cache_path)}"
    )
    return out
