#!/usr/bin/env python
import time
import json
import argparse
from pathlib import Path
from loguru import logger
import geopandas as gpd
from pyrosm import OSM

# Import helpers
from helpers import (
    load_or_extract,
    sanitize_for_gpkg,
    aggregate_roads,
    aggregate_buildings,
    aggregate_pois,
    aggregate_landuse,
    aggregate_natural,
)

# --- Project paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
PIPELINE_ROOT = PROJECT_ROOT / "data_pipeline"


def rel(path: Path) -> Path:
    """Return path relative to project root."""
    return path.relative_to(PROJECT_ROOT)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract and aggregate OSM features per H3 hex."
    )
    parser.add_argument(
        "--pbf", help="Optional path to .pbf file. If not set, inferred from metadata."
    )
    parser.add_argument(
        "--grid",
        help="Optional path to AOI H3 grid .gpkg. If not set, inferred from metadata.",
    )
    parser.add_argument(
        "--output",
        help="Optional output path (.gpkg). If not set, inferred from metadata.",
    )
    args = parser.parse_args()

    # --- Load AOI metadata ---
    meta_path = PIPELINE_ROOT / "aoi_info.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"AOI metadata file not found: {rel(meta_path)}")
    with open(meta_path, "r") as f:
        aoi_meta = json.load(f)

    aoi_raw = aoi_meta.get("aoi_raw")
    slug = aoi_meta.get("aoi_slug")
    slug_full = aoi_meta.get("aoi_slug_full")
    resolution = aoi_meta.get("h3_resolution")

    if not slug or not aoi_raw:
        raise ValueError(f"Invalid metadata in {rel(meta_path)}: {aoi_meta}")
    logger.info(f"📍 AOI: {aoi_raw} | Slug: {slug} | Resolution: {resolution}")

    # --- Resolve PBF path ---
    if args.pbf:
        pbf_path = Path(args.pbf)
    else:
        pbf_path = DATA_DIR / "external" / f"{slug}.osm.pbf"
    if not pbf_path.exists():
        raise FileNotFoundError(f"PBF file not found: {rel(pbf_path)}")
    logger.info(f"🗺️ Using PBF: {rel(pbf_path)}")

    # --- Load H3 grid ---
    if args.grid:
        grid_path = Path(args.grid)
    else:
        grid_path = (
            DATA_DIR / "processed" / "grid" / f"{slug_full}_res{resolution}.gpkg"
        )
    if not grid_path.exists():
        raise FileNotFoundError(f"H3 grid file not found: {rel(grid_path)}")
    logger.info(f"📦 Loading H3 grid from {rel(grid_path)}")
    hex_gdf = gpd.read_file(grid_path)

    # --- Init Pyrosm ---
    logger.info("🚀 Initializing Pyrosm...")
    osm = OSM(str(pbf_path), bounding_box=hex_gdf.union_all())

    # --- Extract or load ---
    roads = load_or_extract(
        "roads",
        lambda: osm.get_network(network_type="driving"),
        pbf_path.parent,
        slug,
    )
    buildings = load_or_extract(
        "buildings", osm.get_buildings, pbf_path.parent, slug_full
    )
    pois = load_or_extract(
        "pois",
        lambda: osm.get_pois(custom_filter={"amenity": True, "shop": True}),
        pbf_path.parent,
        slug,
    )
    landuse = load_or_extract("landuse", osm.get_landuse, pbf_path.parent, slug_full)
    natural = load_or_extract("natural", osm.get_natural, pbf_path.parent, slug_full)

    # --- Aggregate ---
    for name, fn, data in [
        ("roads", aggregate_roads, roads),
        ("buildings", aggregate_buildings, buildings),
        ("pois", aggregate_pois, pois),
        ("landuse", aggregate_landuse, landuse),
        ("natural", aggregate_natural, natural),
    ]:
        start = time.time()
        logger.info(f"📊 Aggregating {name}...")
        agg_df = fn(data, hex_gdf)
        logger.success(
            f"✅ {name.capitalize()} aggregated into {len(agg_df)} hexes in {time.time() - start:.1f}s"
        )

        if name == "roads":
            aggregate_roads = agg_df
        elif name == "buildings":
            agg_buildings = agg_df
        elif name == "pois":
            agg_pois = agg_df
        elif name == "landuse":
            agg_landuse = agg_df
        elif name == "natural":
            agg_natural = agg_df

    # --- Merge ---
    logger.info("🔄 Merging all aggregated features into final hex dataset...")
    merged = hex_gdf[["h3_id", "geometry"]]
    for df in [aggregate_roads, agg_buildings, agg_pois, agg_landuse, agg_natural]:
        merged = merged.merge(df, on="h3_id", how="left")

    merged = merged.fillna(0)
    logger.success(f"✅ Final dataset ready: {len(merged)} hexes")

    # --- Save ---
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = DATA_DIR / "processed" / "osm" / f"{slug}_osm_hex_features.gpkg"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # sanitize only non-geometry columns and names
    merged_safe = sanitize_for_gpkg(merged)

    merged_safe.to_file(output_path, driver="GPKG")  # mode="w" by default
    logger.success(f"✅ Saved aggregated hex features to {rel(output_path)}")
    logger.info(f"📊 Total hexes: {len(merged)}")
