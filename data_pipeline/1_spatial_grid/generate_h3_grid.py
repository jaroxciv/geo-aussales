#!/usr/bin/env python
import h3
import json
from pathlib import Path
from loguru import logger
import geopandas as gpd
from shapely.geometry import shape
from srai.regionalizers import geocode_to_region_gdf


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = PROJECT_ROOT / "data_pipeline"


def rel(path: Path) -> Path:
    """Return path relative to project root."""
    return path.relative_to(PROJECT_ROOT)


def generate_h3_grid(
    boundary_gdf: gpd.GeoDataFrame, resolution: int, aoi_slug_full: str
) -> gpd.GeoDataFrame:
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
                records.append(
                    {"aoi_name": aoi_slug_full, "h3_id": hid, "geometry": geom}
                )
    return gpd.GeoDataFrame(records, crs="EPSG:4326")


if __name__ == "__main__":
    # Load metadata
    metadata_path = PIPELINE_ROOT / "aoi_info.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"❌ Metadata file not found at {rel(metadata_path)}")

    with open(metadata_path, "r") as f:
        metadata = json.load(f)

    aoi_raw = metadata["aoi_raw"]
    aoi_slug_full = metadata["aoi_slug_full"]
    resolution = metadata["h3_resolution"]

    logger.info(f"📍 Generating H3 grid for: {aoi_raw} → slug: {aoi_slug_full}")

    # Geocode AOI
    aoi_gdf = geocode_to_region_gdf(aoi_raw).to_crs(epsg=4326)

    # Generate grid
    logger.info(f"📦 Generating H3 grid at resolution {resolution}...")
    grid = generate_h3_grid(aoi_gdf, resolution, aoi_slug_full)

    # Save grid
    output_path = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "grid"
        / f"{aoi_slug_full}_res{resolution}.gpkg"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    grid.to_file(output_path, driver="GPKG")
    logger.success(f"✅ H3 grid saved to {rel(output_path)}")
    logger.info(f"📊 Total hexagons: {len(grid)}")
