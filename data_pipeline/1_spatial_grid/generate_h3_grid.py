#!/usr/bin/env python
import argparse
import json
import h3
import pandas as pd
import geopandas as gpd
from pathlib import Path
from loguru import logger
from shapely.geometry import shape
from srai.regionalizers import geocode_to_region_gdf
from concurrent.futures import ProcessPoolExecutor, as_completed

from data_pipeline.constants import MERGED_DIR, GRID_DIR, AOI_META_PATH
from data_pipeline.utils import rel, slugify


# ----------------------
# Helpers
# ----------------------
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
    """Geocode AOI, generate grid, save to disk."""
    slug = slugify(place)
    try:
        logger.info(f"📍 Generating H3 grid for: {place} → slug: {slug}")
        gdf = geocode_to_region_gdf(place).to_crs(4326)
        if gdf.empty:
            logger.warning(f"⚠️ Geocoding returned empty geometry for {place}")
            return slug, False, None, f"No geometry for {place}"

        grid = generate_h3_grid(gdf, resolution, place)
        if grid.empty:
            logger.warning(
                f"⚠️ No H3 cells generated for {place} at resolution {resolution}"
            )
            return slug, False, None, "Empty grid"

        output_path = GRID_DIR / f"{slug}_res{resolution}.gpkg"
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
        description="Generate H3 grids in parallel and merge output."
    )
    parser.add_argument(
        "places",
        nargs="*",
        help="Full AOI names. If omitted, reads from aoi_info.json",
    )
    parser.add_argument(
        "--resolution", type=int, help="H3 resolution (default from metadata)"
    )
    parser.add_argument(
        "--max-workers", type=int, default=4, help="Number of parallel processes"
    )
    args = parser.parse_args()

    # Load AOIs
    if not args.places:
        if not AOI_META_PATH.exists():
            raise FileNotFoundError(
                f"❌ Metadata file not found at {rel(AOI_META_PATH)}"
            )

        with open(AOI_META_PATH, "r") as f:
            metadata = json.load(f)

        places = (
            metadata["aoi_raw"]
            if isinstance(metadata["aoi_raw"], list)
            else [metadata["aoi_raw"]]
        )
        resolution = args.resolution or metadata["h3_resolution"]
        merged_slug = metadata["aoi_slug"]
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

    # Merge all results into one file
    if merged_frames:
        merged_gdf = gpd.GeoDataFrame(
            pd.concat(merged_frames, ignore_index=True), crs=merged_frames[0].crs
        )
        merged_output_path = MERGED_DIR / f"{merged_slug}_res{resolution}.gpkg"
        merged_output_path.parent.mkdir(parents=True, exist_ok=True)
        merged_gdf.to_file(merged_output_path, driver="GPKG")
        logger.success(
            f"📦 Merged grid saved to {rel(merged_output_path)} ({len(merged_gdf)} total hexes)"
        )
    else:
        logger.warning("⚠️ No grids generated successfully — nothing to merge.")
