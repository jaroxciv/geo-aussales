#!/usr/bin/env python
import argparse
import h3
import json
import pandas as pd
import geopandas as gpd
from pathlib import Path
from loguru import logger
from shapely.geometry import shape
from srai.regionalizers import geocode_to_region_gdf
from concurrent.futures import ProcessPoolExecutor, as_completed

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = PROJECT_ROOT / "data_pipeline"

# Import shared slugify for consistent naming
from data_pipeline.utils import slugify


# ----------------------
# Helpers
# ----------------------
def rel(path: Path) -> Path:
    return path.relative_to(PROJECT_ROOT)


def generate_h3_grid(boundary_gdf: gpd.GeoDataFrame, resolution: int, aoi_name: str):
    """Generate H3 grid for one AOI."""
    records = []
    for _, row in boundary_gdf.iterrows():
        geometry = row.geometry
        h3_ids = h3.geo_to_cells(geometry, res=resolution)
        for hid in h3_ids:
            h3_shape = h3.cells_to_h3shape([hid])
            if not isinstance(h3_shape, (list, tuple)):
                h3_shape = [h3_shape]
            for poly in h3_shape:
                geom = shape(poly.__geo_interface__)
                records.append({"aoi_name": aoi_name, "h3_id": hid, "geometry": geom})
    return gpd.GeoDataFrame(records, crs="EPSG:4326")


def process_place(place: str, resolution: int):
    """Process a single place: geocode, make grid, save per-city GPKG."""
    slug = slugify(place)
    try:
        logger.info(f"📍 Generating H3 grid for: {place} → slug: {slug}")
        gdf = geocode_to_region_gdf(place).to_crs(4326)
        grid = generate_h3_grid(gdf, resolution, place)
        output_path = (
            PROJECT_ROOT
            / "data"
            / "processed"
            / "grid"
            / f"{slug}_res{resolution}.gpkg"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        grid.to_file(output_path, driver="GPKG")
        return slug, True, output_path, grid
    except Exception as e:
        return slug, False, None, str(e)


# ----------------------
# Main
# ----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate H3 grids in parallel and merge output"
    )
    parser.add_argument(
        "places",
        nargs="*",
        help="Full city names. If omitted, reads from aoi_info.json",
    )
    parser.add_argument(
        "--resolution", type=int, help="H3 resolution (default from metadata)"
    )
    parser.add_argument(
        "--max-workers", type=int, default=4, help="Number of parallel processes"
    )
    args = parser.parse_args()

    # Load places
    if not args.places:
        metadata_path = PIPELINE_ROOT / "aoi_info.json"
        if not metadata_path.exists():
            raise FileNotFoundError(
                f"❌ Metadata file not found at {rel(metadata_path)}"
            )
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        # Ensure aoi_raw is iterable
        places = (
            metadata["aoi_raw"]
            if isinstance(metadata["aoi_raw"], list)
            else [metadata["aoi_raw"]]
        )
        resolution = args.resolution or metadata["h3_resolution"]
        merged_slug = metadata["aoi_slug"]  # unified naming from run_pipeline.py
    else:
        places = args.places
        resolution = args.resolution or 9
        merged_slug = slugify("_".join(places))

    logger.info(f"⚡ Starting parallel processing with {args.max_workers} workers")
    merged_frames = []

    with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(process_place, p, resolution): p for p in places}
        for future in as_completed(futures):
            slug, ok, path, data_or_msg = future.result()
            if ok:
                logger.success(
                    f"✅ {slug}: saved {len(data_or_msg)} hexes to {rel(path)}"
                )
                merged_frames.append(data_or_msg)
            else:
                logger.error(f"❌ {slug} failed: {data_or_msg}")

    # Merge all results
    if merged_frames:
        merged_gdf = pd.concat(merged_frames, ignore_index=True)
        merged_output_path = (
            PROJECT_ROOT
            / "data"
            / "processed"
            / "grid"
            / f"{merged_slug}_res{resolution}.gpkg"
        )
        merged_gdf.to_file(merged_output_path, driver="GPKG")
        logger.success(
            f"📦 Merged grid saved to {rel(merged_output_path)} ({len(merged_gdf)} total hexes)"
        )
    else:
        logger.warning("⚠️ No grids generated successfully — nothing to merge.")
