#!/usr/bin/env python3
import argparse
from srai.regionalizers import geocode_to_region_gdf
from pathlib import Path


def get_polygon(place_name: str):
    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    # Geocode
    area = geocode_to_region_gdf(place_name).to_crs(epsg=4326)

    # Save polygon to GeoJSON for osmium
    safe_name = place_name.replace(",", "").replace(" ", "_").lower()
    geojson_path = output_dir / f"{safe_name}.geojson"
    area.to_file(geojson_path, driver="GeoJSON")
    print(geojson_path)  # stdout for bash


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get polygon for a place.")
    parser.add_argument("place_name", type=str, help="Place name to geocode.")
    args = parser.parse_args()
    get_polygon(args.place_name)
