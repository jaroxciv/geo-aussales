"""
geo_lookup.py
-------------
Geography-level lookup utilities for mapping ABS region codes
(SA2, SA3, …) to StateGroups enums.

Author: Geo-AusSales project
"""

import geopandas as gpd
from loguru import logger

from scripts.cities import StateGroups
from data_pipeline.constants import EXTERNAL_DIR

# --- File paths ---
SA2_PATH = EXTERNAL_DIR / "admin" / "SA2" / "SA2_2021_AUST_GDA2020.shp"
SA3_PATH = EXTERNAL_DIR / "admin" / "SA3" / "SA3_2021_AUST_GDA2020.shp"

# --- Load once ---
_sa2 = gpd.read_file(SA2_PATH)[["SA2_CODE21", "SA2_NAME21", "STE_NAME21"]]
_sa3 = gpd.read_file(SA3_PATH)[["SA3_CODE21", "SA3_NAME21", "STE_NAME21"]]

# --- Map STE_NAME21 → StateGroups ---
_STE_TO_GROUP = {
    "New South Wales": StateGroups.NSW,
    "Victoria": StateGroups.VIC,
    "Queensland": StateGroups.QLD,
    "South Australia": StateGroups.SA,
    "Western Australia": StateGroups.WA,
    "Tasmania": StateGroups.TAS,
    "Northern Territory": StateGroups.NT,
    "Australian Capital Territory": StateGroups.ACT,
}

# --- Build lookup dicts ---
_SA2_LOOKUP = {}
for _, row in _sa2.iterrows():
    if row.STE_NAME21 not in _STE_TO_GROUP:
        logger.warning(f"Skipping SA2 {row.SA2_CODE21} ({row.SA2_NAME21})")
        continue
    _SA2_LOOKUP[row.SA2_CODE21] = _STE_TO_GROUP[row.STE_NAME21]

_SA3_LOOKUP = {}
for _, row in _sa3.iterrows():
    if row.STE_NAME21 not in _STE_TO_GROUP:
        logger.warning(f"Skipping SA3 {row.SA3_CODE21} ({row.SA3_NAME21})")
        continue
    _SA3_LOOKUP[row.SA3_CODE21] = _STE_TO_GROUP[row.STE_NAME21]


def resolve_state_from_code(code: str, level: str = "SA3") -> StateGroups:
    """
    Map SA2/SA3 codes → StateGroups.
    Args:
        code: ABS code as string or int
        level: "SA2" or "SA3"
    """
    code = str(code).zfill(5)
    if level.upper() == "SA2":
        if code not in _SA2_LOOKUP:
            raise ValueError(f"SA2_CODE21='{code}' not found")
        return _SA2_LOOKUP[code]
    elif level.upper() == "SA3":
        if code not in _SA3_LOOKUP:
            raise ValueError(f"SA3_CODE21='{code}' not found")
        return _SA3_LOOKUP[code]
    else:
        raise KeyError(f"Unsupported geography level: {level}")
