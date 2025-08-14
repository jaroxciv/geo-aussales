#!/usr/bin/env python
import subprocess
import argparse
import json
from loguru import logger

from data_pipeline.constants import (
    VENV_PYTHON,
    PIPELINE_DIR,
    PBF_DIR,
    GRID_DIR,
    AOI_META_PATH,
)
from data_pipeline.utils import rel, slugify, resolve_grid_path


def run_step(command: list, step_name: str):
    """Run a pipeline step and log status."""
    logger.info(f"🚀 Running step: {step_name}")
    try:
        subprocess.run(command, check=True)
        logger.success(f"✅ Completed step: {step_name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Step failed: {step_name}")
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run full data pipeline")
    parser.add_argument(
        "--aoi", nargs="+", help="One or more AOIs (full form: 'City, State, Country')"
    )
    parser.add_argument(
        "--enum", help="Enum group name from cities.py (e.g., 'INNER_MELBOURNE')"
    )
    parser.add_argument(
        "--country",
        default="Australia",
        help="Country to append to AOI(s) if not already present",
    )
    parser.add_argument("--resolution", type=int, required=True, help="H3 resolution")
    args = parser.parse_args()

    # --- Resolve AOIs ---
    if args.enum:
        from scripts.cities import CityGroups

        aoi_list = CityGroups.get(args.enum)
    elif args.aoi:
        aoi_list = args.aoi
    else:
        parser.error("You must provide either --aoi or --enum")

    # Append country if provided and missing
    if args.country:
        aoi_list = [
            a if args.country.lower() in a.lower() else f"{a}, {args.country}"
            for a in aoi_list
        ]

    # Create slugs
    slugs = [slugify(a) for a in aoi_list]
    slug = (
        args.enum.lower()
        if args.enum
        else (slugs[0] if len(slugs) == 1 else "_".join(slugs))
    )

    # Save metadata
    metadata = {
        "aoi_raw": aoi_list if len(aoi_list) > 1 else aoi_list[0],
        "aoi_slug": slug,
        "aoi_slugs_individual": slugs,
        "h3_resolution": args.resolution,
    }
    AOI_META_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(AOI_META_PATH, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.success(f"📝 Metadata created at {rel(AOI_META_PATH)}")

    # --- Define file paths ---
    grid_path = resolve_grid_path(metadata, args.resolution)

    if not grid_path.exists():
        raise FileNotFoundError(f"❌ Grid file not found at {rel(grid_path)}")

    logger.info(f"📦 Using grid path: {rel(grid_path)}")

    # --- Run H3 Grid ---
    run_step(
        [VENV_PYTHON, str(PIPELINE_DIR / "1_spatial_grid" / "generate_h3_grid.py")],
        "Generate H3 Grid",
    )

    if not grid_path.exists():
        raise FileNotFoundError(f"❌ Expected grid file not found at {rel(grid_path)}")

    # --- Extract OSM Features ---
    run_step(
        [
            VENV_PYTHON,
            str(PIPELINE_DIR / "2_osm_features" / "extract_osm_features.py"),
            "--grid",
            str(grid_path),
        ],
        "Extract OSM Features",
    )

    logger.info("🎯 All pipeline steps completed successfully!")
