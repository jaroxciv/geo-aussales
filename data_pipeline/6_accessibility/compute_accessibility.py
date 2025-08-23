"""
compute_accessibility.py
------------------------
Generalised accessibility metrics from stores to OSM-based urban features.

Architecture:
- OSMLoader: unified access to OSM layers (POIs, network, buildings, landuse, natural).
- AccessibilityMetric classes: pluggable metrics for each layer.
- AccessibilityEngine: orchestrates loading, clipping, and computing metrics.
- metrics_registry: central mapping of layers → metrics.

Usage:
    # Only POIs (requires --group)
    uv run python data_pipeline/6_accessibility/compute_accessibility.py \
        --layers pois --group transportation

    # Only network (no group needed)
    uv run python data_pipeline/6_accessibility/compute_accessibility.py \
        --layers network

    # Both
    uv run python data_pipeline/6_accessibility/compute_accessibility.py \
        --layers pois network --group transportation
"""

import argparse
from loguru import logger

from data_pipeline.utils import relpath
from data_pipeline.constants import STORES_OUTPUT_PATH
from urban_groups import list_groups
from engine import AccessibilityEngine
from metrics_registry import METRICS_BY_LAYER


def main(args):
    engine = AccessibilityEngine(
        buffer_m=args.buffer,
        geo_level=args.geo_level,
        store_type=args.store_type,
        group=args.group if "pois" in args.layers else None,
        layers=args.layers,
        metrics_by_layer=METRICS_BY_LAYER,
        aggregate_group=args.aggregate if "pois" in args.layers else False,
        verbose_stores=args.verbose_stores,
    )

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
        "--store-type",
        default="trs",
        help="Optional store type filter, e.g. 'trs'",
    )
    parser.add_argument(
        "--layers",
        nargs="+",
        choices=list(METRICS_BY_LAYER.keys()),
        default=["pois"],
        help="Which OSM layers to include (default: pois).",
    )
    parser.add_argument(
        "--group",
        choices=list_groups(),
        help="OSM POI group to use (required if 'pois' in layers).",
    )
    parser.add_argument(
        "--list-groups",
        action="store_true",
        help="List available OSM POI groups and exit.",
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

    # Handle --list-groups
    if args.list_groups:
        print("Available OSM POI groups:")
        for g in list_groups():
            print(f" - {g}")
        exit(0)

    # Validate group requirement
    if "pois" in args.layers and not args.group:
        parser.error("--group is required when using layer 'pois'")

    main(args)
