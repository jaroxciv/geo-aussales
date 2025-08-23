"""
compute_store_access.py
-----------------------
Compute accessibility metrics from stores to OSM-based urban features.

Uses:
- SRAI's BASE_OSM_GROUPS_FILTER for group definitions
- Per-state cached OSM POIs via loader.py
- Vectorised haversine metrics from metrics.py

Outputs:
- Store GeoPackage with nearest distance + count within buffer
  for the selected OSM group (e.g., transportation, education, healthcare).
"""

import argparse
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, box
from loguru import logger
from tqdm import tqdm

from data_pipeline.utils import relpath
from data_pipeline.constants import RAW_STORES_PATH, STORES_OUTPUT_PATH
from geo_lookup import resolve_state_from_code
from loader import load_or_build_group, get_group_subcategories
from urban_groups import list_groups
from metrics import METRICS, format_meters


def main(
    buffer_m: float,
    geo_level: str,
    store_type: str,
    group: str,
    verbose_stores: bool = False,
):
    logger.info(f"Loading stores from {RAW_STORES_PATH}")
    df = pd.read_excel(RAW_STORES_PATH)

    # Optional filter
    if store_type:
        if "type" not in df.columns:
            raise ValueError(
                "Store file does not contain a 'type' column for filtering"
            )
        df = df[df["type"] == store_type]
        logger.info(f"Filtered stores to type='{store_type}' → {len(df)} rows")

    # Required fields
    code_col = f"{geo_level.upper()}_CODE21"
    name_col = f"{geo_level.upper()}_NAME21"
    required = {"Store Id", "lat", "lon", code_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in stores file: {missing}")

    # Build GeoDataFrame
    stores_gdf = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
        crs="EPSG:4326",
    )

    # Map state via geo lookup
    logger.info(f"Mapping stores → {geo_level}_CODE21 → StateGroups")
    stores_gdf["STATE_ENUM"] = stores_gdf[code_col].apply(
        lambda c: resolve_state_from_code(c, level=geo_level)
    )

    results = []
    states = sorted(stores_gdf["STATE_ENUM"].unique(), key=lambda s: s.name)
    logger.info(f"States found: {[s.name for s in states]}")

    for state in states:
        state_stores = stores_gdf[stores_gdf["STATE_ENUM"] == state].copy()
        logger.info(
            f"Processing {len(state_stores)} stores in {state.name} for group={group}"
        )

        pois = load_or_build_group(state, group, aggregate=args.aggregate)
        all_modes = (
            [group] if args.aggregate else list(get_group_subcategories(group).keys())
        )

        # Spatial index
        sidx = pois.sindex if not pois.empty else None

        state_mode_counts = {m: 0 for m in all_modes}  # track counts across all stores

        for _, store in tqdm(
            state_stores.iterrows(), total=len(state_stores), desc=state.name
        ):
            pt: Point = store.geometry
            deg = buffer_m / 111_000.0
            bbox_poly = box(pt.x - deg, pt.y - deg, pt.x + deg, pt.y + deg)
            cand_idx = list(sidx.intersection(bbox_poly.bounds)) if sidx else []
            subset = (
                pois.iloc[cand_idx]
                if cand_idx
                else gpd.GeoDataFrame(columns=pois.columns, crs=pois.crs)
            )
            subset = (
                subset[subset.geometry.within(bbox_poly)]
                if not subset.empty
                else subset
            )

            row = {
                "StoreId": store["Store Id"],
                name_col: store[name_col],
                "STATE": state.name,
            }

            store_summary = []

            for mode in all_modes:
                if subset.empty or mode not in subset["mode"].values:
                    row[f"nearest_{group}_{mode}_m"] = float("inf")
                    row[f"{group}_{mode}_count_{int(buffer_m)}m"] = 0
                    if verbose_stores:
                        logger.debug(
                            f"Store {store['Store Id']} [{store[name_col]}]: "
                            f"{group}:{mode} → no features (∞, 0)"
                        )
                    else:
                        store_summary.append(f"{mode}=(∞,0)")
                    continue

                nearest_val, count_val = None, None
                for metric_name, metric_fn in METRICS.items():
                    value = metric_fn(pt, subset, buffer_m, mode)
                    if metric_name == "nearest":
                        nearest_val = value
                        row[f"nearest_{group}_{mode}_m"] = value
                    elif metric_name == "count":
                        count_val = value
                        row[f"{group}_{mode}_count_{int(buffer_m)}m"] = value
                    else:
                        row[f"{metric_name}_{group}_{mode}"] = value

                    if verbose_stores:
                        logger.debug(
                            f"Store {store['Store Id']} [{store[name_col]}]: "
                            f"{group}:{mode} {metric_name}="
                            f"{format_meters(value) if metric_name=='nearest' else value}"
                        )

                if not verbose_stores:
                    store_summary.append(
                        f"{mode}=({format_meters(nearest_val)},{count_val})"
                    )

                # Accumulate counts for state-level summary
                if count_val is not None:
                    state_mode_counts[mode] += count_val

            results.append(row)

        # --- After finishing the state ---
        counts_str = ", ".join(f"{m}={c}" for m, c in state_mode_counts.items())
        logger.info(
            f"Finished {state.name}: {len(state_stores)} stores processed | {counts_str}"
        )

    # Merge results
    out = stores_gdf.merge(
        pd.DataFrame(results), left_on="Store Id", right_on="StoreId", how="left"
    )

    # Cleanup
    drop_cols = ["StoreId", "STATE_ENUM"]
    if f"{code_col}.1" in out.columns:
        drop_cols.append(f"{code_col}.1")
    if f"{name_col}_y" in out.columns:
        drop_cols.append(f"{name_col}_y")

    out = out.drop(columns=drop_cols, errors="ignore")
    out = out.rename(columns={f"{name_col}_x": name_col})

    STORES_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_file(STORES_OUTPUT_PATH, driver="GPKG")
    logger.success(f"Saved {len(out):,} rows → {relpath(STORES_OUTPUT_PATH)}")
    logger.info(f"Output columns: {list(out.columns)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute store accessibility to OSM groups"
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
    parser.add_argument("--group", choices=list_groups(), help="OSM group to use")
    parser.add_argument(
        "--list-groups", action="store_true", help="List available OSM groups and exit"
    )
    parser.add_argument(
        "--aggregate",
        action="store_true",
        help="Aggregate group into one lumped category instead of subcategories.",
    )
    parser.add_argument(
        "--verbose-stores",
        action="store_true",
        help="Log per-store, per-mode metrics (very verbose). Default: False",
    )
    args = parser.parse_args()

    if args.list_groups:
        print("Available OSM groups:")
        for g in list_groups():
            print(f" - {g}")
        exit(0)

    if not args.group:
        parser.error(
            "the following arguments are required: --group (unless --list-groups is used)"
        )

    main(
        buffer_m=args.buffer,
        geo_level=args.geo_level,
        store_type=args.store_type,
        group=args.group,
        verbose_stores=args.verbose_stores,
    )
