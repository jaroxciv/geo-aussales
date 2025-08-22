"""
census_downloader.py
--------------------
Utilities for downloading, caching, and loading ABS Census GeoPackages (2021).

Handles:
- Automatic download & caching of .zip/.gpkg files
- Robust layer name matching (handles table variants like G17 vs G17A)
- Listing available layers inside each GeoPackage
- Loading across multiple states

Author: Geo-AusSales project
"""

import argparse
import requests
import zipfile
import pandas as pd
import geopandas as gpd
from pathlib import Path
from pyogrio import list_layers
from loguru import logger

from data_pipeline.constants import CENSUS_URL, CENSUS_DIR
from data_pipeline.utils import relpath

# Mapping between long names (used in your enums) and ABS state codes
STATE_CODE_MAP = {
    "New South Wales": "NSW",
    "Victoria": "VIC",
    "Queensland": "QLD",
    "South Australia": "SA",
    "Western Australia": "WA",
    "Tasmania": "TAS",
    "Northern Territory": "NT",
    "Australian Capital Territory": "ACT",
}

Path(CENSUS_DIR).mkdir(parents=True, exist_ok=True)


def download_census_table(
    table: str, state_code: str, cache_dir: str = CENSUS_DIR
) -> Path:
    """
    Download and unzip a census table for a given state.
    Returns the actual .gpkg path inside the zip.
    """
    zip_name = f"Geopackage_2021_{table}_{state_code}_GDA2020.zip"
    zip_path = Path(cache_dir) / zip_name

    # Download if missing
    if not zip_path.exists():
        url = f"{CENSUS_URL}/{zip_name}"
        logger.info(f"Downloading {url}")
        r = requests.get(url)
        r.raise_for_status()
        zip_path.write_bytes(r.content)

    # Inspect contents of the zip
    with zipfile.ZipFile(zip_path, "r") as zf:
        gpkg_files = [f for f in zf.namelist() if f.lower().endswith(".gpkg")]
        if not gpkg_files:
            raise FileNotFoundError(f"No .gpkg found inside {relpath(zip_path)}")

        gpkg_file = gpkg_files[0]  # there should only be one
        gpkg_path = Path(cache_dir) / Path(gpkg_file).name

        # Extract if missing
        if not gpkg_path.exists():
            zf.extract(gpkg_file, cache_dir)

    if not gpkg_path.exists():
        raise FileNotFoundError(
            f"Expected {relpath(gpkg_path)} not found after extraction."
        )

    return gpkg_path


def list_census_layers(
    table: str, state_code: str, cache_dir: str = CENSUS_DIR
) -> list[str]:
    """
    List all available layers in a given table/state GeoPackage.

    Args:
        table: Table ID, e.g. "G02", "G17"
        state_code: Short ABS code, e.g. "VIC", "NSW"
        cache_dir: Directory to store cached files

    Returns:
        List of layer names (strings)
    """
    gpkg_path = download_census_table(table, state_code, cache_dir)
    return [name for name, *_ in list_layers(gpkg_path)]


def load_census_layer(
    table: str, state_code: str, geography: str, cache_dir: str = CENSUS_DIR
) -> gpd.GeoDataFrame:
    """
    Load a specific geography layer from a census table for one state.

    Args:
        table: Table ID, e.g. "G02", "G17"
        state_code: Short ABS code, e.g. "VIC", "NSW"
        geography: Geography code, e.g. "SA1", "SA2", "SA3", "LGA", "POA"
        cache_dir: Directory to store cached files

    Returns:
        GeoDataFrame for that table/state/geography
    """
    gpkg_path = download_census_table(table, state_code, cache_dir)
    layers = list_census_layers(table, state_code, cache_dir)

    # Try to match exact prefix
    candidates = [
        l for l in layers if geography in l and state_code in l and l.startswith(table)
    ]
    if not candidates:
        # fallback: handle G17 vs G17A, G60 vs G60B etc
        candidates = [
            l
            for l in layers
            if geography in l and state_code in l and l.startswith(table[:3])
        ]

    if not candidates:
        raise ValueError(
            f"No matching layer for table={table}, state={state_code}, geography={geography}\n"
            f"Available layers: {layers}"
        )

    chosen_layer = candidates[0]
    logger.info(f"Loading {chosen_layer} from {relpath(gpkg_path)}")
    return gpd.read_file(
        gpkg_path, layer=chosen_layer, engine="pyogrio", use_arrow=True
    )


def load_all_states(
    table: str, geography: str, cache_dir: str = CENSUS_DIR, states=None
) -> dict[str, gpd.GeoDataFrame]:
    """
    Load a census table for all (or selected) states at a given geography level.

    Args:
        table: Table ID, e.g. "G02"
        geography: Geography code, e.g. "SA2", "SA3", "LGA"
        cache_dir: Directory to store cached files
        states: Optional list of state codes (defaults to all)

    Returns:
        Dict mapping {state_code: GeoDataFrame}
    """
    states = states or STATE_CODE_MAP.values()
    out = {}
    for state in states:
        try:
            out[state] = load_census_layer(table, state, geography, cache_dir)
        except Exception as e:
            logger.warning(f"Could not load {table}/{state}/{geography}: {e}")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and load ABS Census GeoPackages (2021)."
    )
    parser.add_argument("table", help="Census table ID, e.g. G02, G17")
    parser.add_argument(
        "geography", help="Geography level, e.g. SA1, SA2, SA3, LGA, POA"
    )
    parser.add_argument(
        "--nationwide",
        action="store_true",
        help="Concatenate all states into one nationwide dataset and save to CENSUS_DIR",
    )

    args = parser.parse_args()

    if args.nationwide:
        # Load all states
        all_states = load_all_states(args.table, args.geography)

        # Concatenate into nationwide GeoDataFrame
        nationwide = gpd.GeoDataFrame(
            pd.concat(all_states.values(), ignore_index=True),
            crs=list(all_states.values())[0].crs,
        )

        # Save nationwide file
        outfile = Path(CENSUS_DIR) / f"{args.table}_{args.geography}_AUS.gpkg"
        nationwide.to_file(outfile, driver="GPKG")
        logger.success(
            f"Saved nationwide {args.table}/{args.geography} to {relpath(outfile)}"
        )

    else:
        # Load each state separately (dict)
        all_states = load_all_states(args.table, args.geography)

        # Save each one individually (if not already cached, they're already saved by ABS .gpkg)
        for state, gdf in all_states.items():
            outfile = Path(CENSUS_DIR) / f"{args.table}_{args.geography}_{state}.gpkg"
            gdf.to_file(outfile, driver="GPKG")
            logger.success(
                f"Saved {args.table}/{args.geography} for {state} to {relpath(outfile)}"
            )
