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
    parser.add_argument("--aoi", required=True, help="Area of Interest (full name)")
    parser.add_argument("--resolution", type=int, required=True, help="H3 resolution")
    args = parser.parse_args()

    # --- Create metadata ---
    city_only = args.aoi.split(",")[0]
    aoi_slug = city_only.lower().replace(" ", "_")  # short for PBF
    aoi_slug_full = (
        args.aoi.lower().replace(",", "").replace(" ", "_")
    )  # full for processing

    metadata = {
        "aoi_raw": args.aoi,
        "aoi_slug": aoi_slug,  # short for PBF
        "aoi_slug_full": aoi_slug_full,  # full for filenames
        "h3_resolution": args.resolution,
    }

    metadata_path = PROJECT_ROOT / "data_pipeline" / "aoi_info.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.success(f"📝 Metadata created at {rel(metadata_path)}")

    # --- Define file paths from metadata ---
    pbf_path = PROJECT_ROOT / f"data/external/{aoi_slug_full}.osm.pbf"
    grid_path = (
        PROJECT_ROOT / f"data/processed/grid/{aoi_slug_full}_res{args.resolution}.gpkg"
    )

    if not pbf_path.exists():
        raise FileNotFoundError(f"❌ PBF file not found at {rel(pbf_path)}")

    # --- Run steps ---
    run_step(
        [VENV_PYTHON, str(PIPELINE_DIR / "1_spatial_grid" / "generate_h3_grid.py")],
        "Generate H3 Grid",
    )

    if not grid_path.exists():
        raise FileNotFoundError(f"❌ Expected grid file not found at {rel(grid_path)}")

    run_step(
        [
            VENV_PYTHON,
            str(PIPELINE_DIR / "2_osm_features" / "extract_osm_features.py"),
            "--pbf",
            str(pbf_path),
            "--grid",
            str(grid_path),
        ],
        "Extract OSM Features",
    )

    logger.info("🎯 All pipeline steps completed successfully!")
