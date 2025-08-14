#!/usr/bin/env python3
import re
import argparse
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from srai.regionalizers import geocode_to_region_gdf


def slugify(name: str) -> str:
    """Convert a string into a safe slug (lowercase, underscores)."""
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return re.sub(r"_+", "_", s)


def _close_ring(coords):
    """Ensure first coordinate = last coordinate."""
    if coords[0] != coords[-1]:
        coords = list(coords) + [coords[0]]
    return coords


def save_as_poly(gdf: gpd.GeoDataFrame, poly_path: Path, header_name: str):
    """Save polygon/multipolygon GeoDataFrame to Osmosis .poly format (supports holes)."""
    poly_path.parent.mkdir(parents=True, exist_ok=True)
    geom = gdf.union_all()
    if isinstance(geom, Polygon):
        geoms = [geom]
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


def geocode_single(place: str) -> gpd.GeoDataFrame:
    """Geocode one place and return GeoDataFrame in EPSG:4326."""
    gdf = geocode_to_region_gdf(place).to_crs(4326)
    gdf["aoi_name"] = place
    return gdf[["aoi_name", "geometry"]]


def main():
    parser = argparse.ArgumentParser(
        description="Geocode one or more places and write GeoJSON + .poly for osmium."
    )
    parser.add_argument(
        "place",
        nargs="+",
        help="One or more place names (e.g., 'City of Melbourne, Victoria').",
    )
    parser.add_argument(
        "--outdir",
        default="outputs",
        help="Output directory for GeoJSON and .poly (default: outputs).",
    )
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    for place in args.place:
        slug = slugify(place)
        header_name = place

        # Geocode
        gdf = geocode_single(place)

        # Save GeoJSON
        geojson_path = outdir / f"{slug}.geojson"
        gdf.to_file(geojson_path, driver="GeoJSON")

        # Save .poly
        poly_path = outdir / f"{slug}.poly"
        save_as_poly(gdf, poly_path, header_name=header_name)

        # Print for bash pipeline
        print(poly_path.as_posix(), slug)


if __name__ == "__main__":
    main()
