#!/usr/bin/env python
import subprocess
import argparse
import json
import sys
from pathlib import Path
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # geo-aussales root
PIPELINE_DIR = Path(__file__).parent
VENV_PYTHON = sys.executable  # use current environment


def rel(path: Path) -> Path:
    """Return path relative to project root."""
    return path.relative_to(PROJECT_ROOT)


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
    parser.add_argument("--resolution", type=int, required=True, help="H3 resolution")
    args = parser.parse_args()


    # TODO: FIX SINGLE AOI PARSING
    # --- Resolve AOIs ---
    if args.enum:
        # Import dynamically so cities.py is only needed if using enum
        from scripts.cities import CityGroups

        aoi_list = CityGroups.get(args.enum)
        slug = args.enum.lower()
    elif args.aoi:
        aoi_list = args.aoi
        if len(aoi_list) == 1:
            slug = aoi_list[0].split(",")[0].lower().replace(" ", "_")
        else:
            # Multiple places → create a group slug
            slug = "_".join(
                [p.split(",")[0].lower().replace(" ", "_") for p in aoi_list]
            )
    else:
        parser.error("You must provide either --aoi or --enum")

    aoi_slug_full = slug
    aoi_raw = aoi_list if len(aoi_list) > 1 else aoi_list[0]

    metadata = {
        "aoi_raw": aoi_raw,
        "aoi_slug": slug,  # short for PBF
        "aoi_slug_full": aoi_slug_full,  # full for filenames
        "h3_resolution": args.resolution,
    }

    metadata_path = PROJECT_ROOT / "data_pipeline" / "aoi_info.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.success(f"📝 Metadata created at {rel(metadata_path)}")

    # --- Define file paths ---
    pbf_path = PROJECT_ROOT / f"data/external/{aoi_slug_full}.osm.pbf"
    grid_path = (
        PROJECT_ROOT / f"data/processed/grid/{aoi_slug_full}_res{args.resolution}.gpkg"
    )

    if not pbf_path.exists():
        raise FileNotFoundError(f"❌ PBF file not found at {rel(pbf_path)}")

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
