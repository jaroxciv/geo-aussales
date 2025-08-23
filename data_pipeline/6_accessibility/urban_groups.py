# data_pipeline/6_accessibility/urban_groups.py
from srai.loaders.osm_loaders.filters import BASE_OSM_GROUPS_FILTER
from srai.loaders.osm_loaders.filters import merge_osm_tags_filter

URBAN_GROUPS = BASE_OSM_GROUPS_FILTER


def list_groups() -> list[str]:
    return sorted(URBAN_GROUPS.keys())


def get_group_filter(group: str):
    if group not in URBAN_GROUPS:
        raise KeyError(f"Unknown group '{group}'. Available: {list_groups()}")
    return merge_osm_tags_filter({group: URBAN_GROUPS[group]})
