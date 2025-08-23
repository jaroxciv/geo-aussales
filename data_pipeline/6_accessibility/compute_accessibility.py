"""
compute_accessibility.py
------------------------
Generalised accessibility metrics from stores to OSM-based urban features.

Architecture:
- OSMLoader: unified access to OSM layers (POIs, network, buildings, landuse, natural).
- AccessibilityMetric classes: pluggable metrics for each layer.
- AccessibilityEngine: orchestrates loading, clipping, and computing metrics.

Default usage:
    python compute_accessibility.py --buffer 1000 --geo-level SA2 --store-type trs --group transportation

Outputs:
- Store GeoPackage with metric columns (e.g., nearest distance + counts for POI modes).
"""

import argparse
from loguru import logger

from data_pipeline.utils import relpath
from data_pipeline.constants import STORES_OUTPUT_PATH
from urban_groups import list_groups
from engine import AccessibilityEngine
from metrics_osm import NearestDistanceMetric, CountWithinBufferMetric


def main(args):
    # Build metric registry
    metrics_by_layer = {
        "pois": [
            NearestDistanceMetric(),
            CountWithinBufferMetric(),
        ]
        # Future: "network": [RoadDensityMetric(), IntersectionCountMetric()],
        #         "buildings": [TotalBuildingAreaMetric()],
    }

    # Instantiate engine
    engine = AccessibilityEngine(
        buffer_m=args.buffer,
        geo_level=args.geo_level,
        store_type=args.store_type,
        group=args.group,
        layers=["pois"],  # extend later with "network","buildings", etc.
        metrics_by_layer=metrics_by_layer,
        aggregate_group=args.aggregate,
        verbose_stores=args.verbose_stores,
    )

    # Run engine
    out = engine.run()
    logger.success(f"Accessibility metrics saved to {relpath(STORES_OUTPUT_PATH)}")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute accessibility metrics from stores to OSM layers"
    )
    parser.add_argument(
        "--buffer",
        type=float,
        default=1000.0,
        help="Buffer size in meters (default: 1000)",
    )
    parser.add_argument(
        "--geo-level",
        choices=["SA2", "SA3"],
        default="SA2",
        help="Geography level (default: SA2)",
    )
    parser.add_argument(
        "--store-type", default="trs", help="Optional store type filter, e.g. 'trs'"
    )
    parser.add_argument("--group", choices=list_groups(), help="OSM POI group to use")
    parser.add_argument(
        "--list-groups", action="store_true", help="List available OSM groups and exit"
    )
    parser.add_argument(
        "--aggregate",
        action="store_true",
        help="Aggregate POI group into one lumped category instead of subcategories.",
    )
    parser.add_argument(
        "--verbose-stores",
        action="store_true",
        help="Log per-store metrics (default: only summaries).",
    )
    args = parser.parse_args()

    if args.list_groups:
        print("Available OSM POI groups:")
        for g in list_groups():
            print(f" - {g}")
        exit(0)

    if not args.group:
        parser.error(
            "the following arguments are required: --group (unless --list-groups is used)"
        )

    main(args)
