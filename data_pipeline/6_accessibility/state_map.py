# state_map.py
import geopandas as gpd
from loguru import logger
from pathlib import Path

from scripts.cities import StateGroups
from data_pipeline.constants import EXTERNAL_DIR, PBF_DIR

# --- Load SA3 lookup once ---
SA3_PATH = EXTERNAL_DIR / "admin" / "SA3" / "SA3_2021_AUST_GDA2020.shp"
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
    # "Other Territories" intentionally not mapped
}

# --- Build SA3 → StateGroups lookup dict ---
_SA3_LOOKUP = {}
for _, row in _sa3.iterrows():
    if row.STE_NAME21 not in _STE_TO_GROUP:
        logger.warning(
            f"Skipping SA3_CODE21='{row.SA3_CODE21}' "
            f"({row.SA3_NAME21}) with STE_NAME21='{row.STE_NAME21}'"
        )
        continue
    _SA3_LOOKUP[row.SA3_CODE21] = _STE_TO_GROUP[row.STE_NAME21]

# --- StateGroups → canonical PBF slug ---
STATE_PBF_MAP = {
    "ACT": "australian_capital_territory",
    "NSW": "new_south_wales",
    "NT": "northern_territory",
    "QLD": "queensland",
    "SA": "south_australia",
    "TAS": "tasmania",
    "VIC": "victoria",
    "WA": "western_australia",
}


def sa3_to_stategroup(sa3_code: str) -> StateGroups:
    """Map SA3_CODE21 → StateGroups, skipping unmapped regions with warning."""
    sa3_code = str(sa3_code).zfill(5)  # normalize to 5-digit string
    if sa3_code not in _SA3_LOOKUP:
        raise ValueError(f"SA3_CODE21='{sa3_code}' not found in lookup")
    return _SA3_LOOKUP[sa3_code]


def stategroup_to_pbf(state: StateGroups) -> Path:
    """
    Resolve the .osm.pbf file for a given StateGroups enum.
    Enforces exact expected filename (no substring matching).
    """
    state_slug = STATE_PBF_MAP.get(state.name)
    if not state_slug:
        raise KeyError(f"No PBF mapping defined for {state.name}")

    expected_file = PBF_DIR / f"{state_slug}_australia.osm.pbf"

    if not expected_file.exists():
        raise FileNotFoundError(
            f"Missing PBF file for {state.name}. " f"Expected: {expected_file}"
        )

    logger.debug(f"Matched {state.name} -> {expected_file.name}")
    return expected_file
