#!/usr/bin/env python
import time
import json
import argparse
import pandas as pd
import geopandas as gpd
from pathlib import Path
from loguru import logger
from pyrosm import OSM

from data_pipeline.constants import (
    AOI_META_PATH,
    GRID_DIR,
    OSM_PROCESSED_DIR,
    MERGED_DIR,
)
from data_pipeline.utils import slugify
from helpers import (
    rel,
    load_or_extract,
    sanitize_for_gpkg,
    find_pbf_for_aoi,
    aggregate_roads,
    aggregate_buildings,
    aggregate_pois,
    aggregate_landuse,
    aggregate_natural,
)


def aggregate_for_aoi(
    aoi_name: str, hex_gdf: gpd.GeoDataFrame, pbf_path: Path
) -> gpd.GeoDataFrame:
    """Aggregate OSM features for a specific AOI using only that AOI's subgrid."""
    target_slug = slugify(aoi_name)
    hex_gdf = hex_gdf.copy()
    hex_gdf["aoi_slug"] = hex_gdf["aoi_name"].apply(slugify)

    subgrid = hex_gdf[hex_gdf["aoi_slug"] == target_slug]
    if subgrid.empty:
        logger.warning(
            f"⚠️ AOI '{aoi_name}' has no matching hexes in "
            f"provided grid (slug={target_slug}). Skipping."
        )
        return gpd.GeoDataFrame(
            columns=["h3_id", "geometry", "aoi_name"], crs=hex_gdf.crs
        )

    logger.info(f"🚀 Initializing Pyrosm for AOI: {aoi_name}")
    osm = OSM(str(pbf_path), bounding_box=subgrid.union_all())

    per_aoi_slug = slugify(aoi_name)

    # Extract or load layers
    roads = load_or_extract(
        "roads", lambda: osm.get_network(network_type="driving"), per_aoi_slug
    )
    buildings = load_or_extract("buildings", osm.get_buildings, per_aoi_slug)
    pois = load_or_extract(
        "pois",
        lambda: osm.get_pois(custom_filter={"amenity": True, "shop": True}),
        per_aoi_slug,
    )
    landuse = load_or_extract("landuse", osm.get_landuse, per_aoi_slug)
    natural = load_or_extract("natural", osm.get_natural, per_aoi_slug)

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
            f"✅ {name.capitalize()} aggregated into {len(agg_df)} "
            f"hexes in {time.time() - start:.1f}s"
        )
        agg_results[name] = agg_df

    # Merge all aggregates into the subgrid
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
        help="Optional: path to a grid file (.gpkg). Defaults to merged/single grid from metadata.",
    )
    parser.add_argument(
        "--output",
        help="Output path (.gpkg) for merged results. Defaults to metadata-driven path.",
    )
    args = parser.parse_args()

    # Load AOI metadata
    if not AOI_META_PATH.exists():
        raise FileNotFoundError(f"AOI metadata file not found: {rel(AOI_META_PATH)}")

    with open(AOI_META_PATH, "r") as f:
        aoi_meta = json.load(f)

    aoi_raw = aoi_meta.get("aoi_raw")
    slug_group = aoi_meta.get("aoi_slug")
    resolution = aoi_meta.get("h3_resolution")

    if not aoi_raw:
        raise ValueError(f"Invalid metadata in {rel(AOI_META_PATH)}: {aoi_meta}")

    aoi_list = [aoi_raw] if isinstance(aoi_raw, str) else list(aoi_raw)
    logger.info(f"📍 AOIs: {aoi_list} | Resolution: {resolution}")

    # Pick grid file
    if args.grid:
        grid_path = Path(args.grid)
    else:
        if len(aoi_list) > 1:
            grid_path = MERGED_DIR / f"{slug_group}_res{resolution}.gpkg"
        else:
            single_slug = slugify(aoi_list[0])
            grid_path = GRID_DIR / f"{single_slug}_res{resolution}.gpkg"

    if not grid_path.exists():
        raise FileNotFoundError(f"H3 grid file not found: {rel(grid_path)}")

    logger.info(f"📦 Loading H3 grid from {rel(grid_path)}")
    hex_gdf = gpd.read_file(grid_path)

    # Get PBF per AOI
    pbf_map = {name: find_pbf_for_aoi(name) for name in aoi_list}
    for name, pbf_path in pbf_map.items():
        logger.info(f"🗺️ Using PBF for '{name}': {rel(pbf_path)}")

    # Process AOIs
    results = []
    for aoi_name in aoi_list:
        logger.info(f"🧩 Processing AOI: {aoi_name}")
        out = aggregate_for_aoi(aoi_name, hex_gdf, pbf_map[aoi_name])
        if not out.empty:
            results.append(out)

    if not results:
        raise RuntimeError(
            "❌ No AOIs produced results. Check your grid and PBF matches."
        )

    merged_all = gpd.GeoDataFrame(
        pd.concat(results, ignore_index=True), crs=hex_gdf.crs
    )

    # Save output
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = OSM_PROCESSED_DIR / f"{slug_group}_osm_hex_features.gpkg"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged_safe = sanitize_for_gpkg(merged_all)
    merged_safe.to_file(output_path, driver="GPKG")

    logger.success(f"✅ Saved aggregated hex features to {rel(output_path)}")
    logger.info(f"📊 Total hexes (rows): {len(merged_all)}")
