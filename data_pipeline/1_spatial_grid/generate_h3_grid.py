#!/usr/bin/env python
import re
import h3
import json
import argparse
import geopandas as gpd
from pathlib import Path
from loguru import logger
from shapely.geometry import shape
from srai.regionalizers import geocode_to_region_gdf
from concurrent.futures import ProcessPoolExecutor, as_completed

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = PROJECT_ROOT / "data_pipeline"

# ----------------------
# Helpers
# ----------------------
def rel(path: Path) -> Path:
    return path.relative_to(PROJECT_ROOT)

def slugify(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return re.sub(r"_+", "_", s)

def generate_h3_grid(boundary_gdf: gpd.GeoDataFrame, resolution: int, aoi_name: str):
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
    slug = slugify(place)
    try:
        logger.info(f"📍 Generating H3 grid for: {place} → slug: {slug}")
        # Geocode
        gdf = geocode_to_region_gdf(place).to_crs(4326)
        # Generate grid
        grid = generate_h3_grid(gdf, resolution, place)
        # Save
        output_path = PROJECT_ROOT / "data" / "processed" / "grid" / f"{slug}_res{resolution}.gpkg"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        grid.to_file(output_path, driver="GPKG")
        return slug, True, len(grid)
    except Exception as e:
        return slug, False, str(e)

# ----------------------
# Main
# ----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate H3 grid per city in parallel (ProcessPool)")
    parser.add_argument("places", nargs="*", help="City names (full form: 'City, Area, Country').")
    parser.add_argument("--resolution", type=int, help="H3 resolution (default from metadata)")
    parser.add_argument("--max-workers", type=int, default=6, help="Number of parallel processes")
    args = parser.parse_args()

    # Load places
    if not args.places:
        metadata_path = PIPELINE_ROOT / "aoi_info.json"
        if not metadata_path.exists():
            raise FileNotFoundError(f"❌ Metadata file not found at {rel(metadata_path)}")
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
        places = metadata["aoi_raw"]
        resolution = args.resolution or metadata["h3_resolution"]
    else:
        places = args.places
        resolution = args.resolution or 9

    logger.info(f"⚡ Starting parallel processing with {args.max_workers} workers")
    with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(process_place, p, resolution): p for p in places}
        for future in as_completed(futures):
            slug, ok, info = future.result()
            if ok:
                logger.success(f"✅ {slug}: saved {info} hexes")
            else:
                logger.error(f"❌ {slug} failed: {info}")
