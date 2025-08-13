#!/usr/bin/env python3
import argparse
import re
from pathlib import Path
from typing import Iterable, List

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from srai.regionalizers import geocode_to_region_gdf


def slugify(name: str) -> str:
    """Convert a string into a safe slug (lowercase, underscores)."""
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return re.sub(r"_+", "_", s)


def _close_ring(coords):
    # ensure first = last
    if coords[0] != coords[-1]:
        coords = list(coords) + [coords[0]]
    return coords


def save_as_poly(gdf: gpd.GeoDataFrame, poly_path: Path, header_name: str):
    """Save polygon/multipolygon GeoDataFrame to Osmosis .poly format (supports holes)."""
    poly_path.parent.mkdir(parents=True, exist_ok=True)
    geom = gdf.union_all()
    if isinstance(geom, Polygon):
        geoms: Iterable[Polygon] = [geom]
    elif isinstance(geom, MultiPolygon):
        geoms = geom.geoms
    else:
        raise ValueError("Geometry must be Polygon or MultiPolygon")

    with poly_path.open("w", encoding="utf-8") as f:
        f.write(f"{header_name}\n")
        for poly in geoms:
            # exterior
            f.write("1\n")
            for x, y in _close_ring(list(poly.exterior.coords)):
                f.write(f"  {x}  {y}\n")
            f.write("END\n")
            # holes (interiors)
            for interior in poly.interiors:
                f.write("-1\n")
                for x, y in _close_ring(list(interior.coords)):
                    f.write(f"  {x}  {y}\n")
                f.write("END\n")
        f.write("END\n")


def geocode_many(places: List[str]) -> gpd.GeoDataFrame:
    """Geocode one or more places and return a single GeoDataFrame in EPSG:4326."""
    gdfs = []
    for p in places:
        g = geocode_to_region_gdf(p).to_crs(4326)
        g["source_name"] = p
        gdfs.append(g[["source_name", "geometry"]])
    return gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs="EPSG:4326")


def main():
    parser = argparse.ArgumentParser(
        description="Geocode one or more places and write GeoJSON + .poly for osmium."
    )
    parser.add_argument(
        "place",
        nargs="+",
        help="One or more place names (e.g., 'CBD, Melbourne, Australia' 'Carlton, Melbourne, Australia').",
    )
    parser.add_argument(
        "--slug",
        help="Optional slug to use for filenames (otherwise auto-generated).",
    )
    parser.add_argument(
        "--outdir",
        default="outputs",
        help="Output directory for GeoJSON and .poly (default: outputs).",
    )
    args = parser.parse_args()

    places = args.place
    is_multi = len(places) > 1

    # Build a human-readable combined name if multiple inputs
    if args.slug:
        base_slug = slugify(args.slug)
        header_name = base_slug
    else:
        if is_multi:
            # Compact combined name for slug
            base_slug = slugify("_".join(places))
            header_name = " + ".join(places)
        else:
            base_slug = slugify(places[0])
            header_name = places[0]

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Geocode & combine
    if is_multi:
        # geocode each & concat
        frames = []
        for p in places:
            frames.append(geocode_to_region_gdf(p).to_crs(4326)[["geometry"]])
        gdf = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
    else:
        gdf = geocode_to_region_gdf(places[0]).to_crs(4326)

    # Write GeoJSON
    geojson_path = outdir / f"{base_slug}.geojson"
    gdf.to_file(geojson_path, driver="GeoJSON")

    # Write .poly (supports holes)
    poly_path = outdir / f"{base_slug}.poly"
    save_as_poly(gdf, poly_path, header_name=header_name)

    # Print for bash: "<poly_path> <slug>"
    print(poly_path.as_posix(), base_slug)


if __name__ == "__main__":
    # local import to avoid global dependency if user only runs for .poly
    import pandas as pd  # noqa: E402

    main()
