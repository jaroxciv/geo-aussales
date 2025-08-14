#!/usr/bin/env python
import time
import json
import argparse
import pandas as pd
import geopandas as gpd
from pathlib import Path
from loguru import logger
from pyrosm import OSM

# Import helpers
from helpers import (
    rel,
    load_or_extract,
    sanitize_for_gpkg,
    slugify,
    find_pbf_for_aoi,
    aggregate_roads,
    aggregate_buildings,
    aggregate_pois,
    aggregate_landuse,
    aggregate_natural,
)

# --- Project paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
PIPELINE_ROOT = PROJECT_ROOT / "data_pipeline"
EXTERNAL_DIR = DATA_DIR / "external"


def aggregate_for_aoi(
    aoi_name: str, hex_gdf: gpd.GeoDataFrame, pbf_path: Path
) -> gpd.GeoDataFrame:
    """Aggregate OSM features for a specific AOI using only that AOI's subgrid."""
    subgrid = hex_gdf[hex_gdf["aoi_name"] == aoi_name].copy()
    if subgrid.empty:
        logger.warning(f"⚠️ AOI '{aoi_name}' has no hexes in merged grid. Skipping.")
        return gpd.GeoDataFrame(
            columns=["h3_id", "geometry", "aoi_name"], crs=hex_gdf.crs
        )

    logger.info(f"🚀 Initializing Pyrosm for AOI: {aoi_name}")
    osm = OSM(str(pbf_path), bounding_box=subgrid.union_all())

    # Use per-AOI slugs for cache file names
    per_aoi_slug = slugify(aoi_name)

    # Extract or load
    roads = load_or_extract(
        "roads",
        lambda: osm.get_network(network_type="driving"),
        pbf_path.parent,
        per_aoi_slug,
    )
    buildings = load_or_extract(
        "buildings", osm.get_buildings, pbf_path.parent, per_aoi_slug
    )
    pois = load_or_extract(
        "pois",
        lambda: osm.get_pois(custom_filter={"amenity": True, "shop": True}),
        pbf_path.parent,
        per_aoi_slug,
    )
    landuse = load_or_extract("landuse", osm.get_landuse, pbf_path.parent, per_aoi_slug)
    natural = load_or_extract("natural", osm.get_natural, pbf_path.parent, per_aoi_slug)

    # Aggregate per layer
    agg_results = {}
    for name, fn, data in [
        ("roads", aggregate_roads, roads),
        ("buildings", aggregate_buildings, buildings),
        ("pois", aggregate_pois, pois),
        ("landuse", aggregate_landuse, landuse),
        ("natural", aggregate_natural, natural),
    ]:
        start = time.time()
        logger.info(f"📊 Aggregating {name} for {aoi_name}...")
        agg_df = fn(data, subgrid)
        logger.success(
            f"✅ {name.capitalize()} aggregated into "
            f"{len(agg_df)} hexes in {time.time() - start:.1f}s"
        )
        agg_results[name] = agg_df

    # Merge layer aggregates
    merged = subgrid[["h3_id", "geometry"]]
    for df in agg_results.values():
        merged = merged.merge(df, on="h3_id", how="left")

    merged["aoi_name"] = aoi_name
    return merged.fillna(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract and aggregate OSM features per H3 hex."
    )
    parser.add_argument(
        "--grid",
        help="Path to merged AOI H3 grid (.gpkg) that contains an 'aoi_name' column.",
    )
    parser.add_argument(
        "--output",
        help="Output path (.gpkg) for merged results. Defaults to metadata-driven path.",
    )
    args = parser.parse_args()

    # Load AOI metadata
    meta_path = PIPELINE_ROOT / "aoi_info.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"AOI metadata file not found: {rel(meta_path)}")
    with open(meta_path, "r") as f:
        aoi_meta = json.load(f)

    aoi_raw = aoi_meta.get("aoi_raw")
    slug_full = aoi_meta.get("aoi_slug_full")
    resolution = aoi_meta.get("h3_resolution")

    if not aoi_raw:
        raise ValueError(f"Invalid metadata in {rel(meta_path)}: {aoi_meta}")

    # Ensure list
    aoi_list = [aoi_raw] if isinstance(aoi_raw, str) else list(aoi_raw)
    logger.info(f"📍 AOIs: {aoi_list} | Resolution: {resolution}")

    # Resolve merged grid path
    if args.grid:
        grid_path = Path(args.grid)
    else:
        grid_path = (
            DATA_DIR / "processed" / "grid" / f"{slug_full}_merged_res{resolution}.gpkg"
        )
    if not grid_path.exists():
        raise FileNotFoundError(f"H3 merged grid file not found: {rel(grid_path)}")
    logger.info(f"📦 Loading merged H3 grid from {rel(grid_path)}")
    hex_gdf = gpd.read_file(grid_path)

    # Resolve each AOI's PBF by substring matching
    pbf_map = {}
    for name in aoi_list:
        pbf_map[name] = find_pbf_for_aoi(name, EXTERNAL_DIR)
        logger.info(f"🗺️ Using PBF for '{name}': {rel(pbf_map[name])}")

    # Aggregate per AOI
    results = []
    for aoi_name in aoi_list:
        logger.info(f"🧩 Processing AOI: {aoi_name}")
        out = aggregate_for_aoi(aoi_name, hex_gdf, pbf_map[aoi_name])
        if not out.empty:
            results.append(out)

    if not results:
        raise RuntimeError(
            "❌ No AOIs produced results. Check your merged grid and PBF matches."
        )

    merged_all = gpd.GeoDataFrame(
        pd.concat(results, ignore_index=True), crs=hex_gdf.crs
    )

    # Save merged output
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = (
            DATA_DIR / "processed" / "osm" / f"{slug_full}_osm_hex_features.gpkg"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged_safe = sanitize_for_gpkg(merged_all)
    merged_safe.to_file(output_path, driver="GPKG")
    logger.success(f"✅ Saved aggregated hex features to {rel(output_path)}")
    logger.info(f"📊 Total hexes (rows): {len(merged_all)}")
