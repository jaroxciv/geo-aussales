import argparse
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, box
from loguru import logger
from tqdm import tqdm

from data_pipeline.utils import relpath
from data_pipeline.constants import RAW_STORES_PATH, STORES_OUTPUT_PATH
from registry import TRANSIT_MODES, TRANSIT_ROUTE_FILTERS, METRICS
from metrics import format_meters
from helper_pois import load_or_build_state_transit
from helper_routes import load_or_build_state_transit_routes
from geo_lookup import resolve_state_from_code


def main(
    buffer_m: float = 1000.0,
    geo_level: str = "SA3",
    store_type: str = None,
    use_routes: bool = False,
):
    logger.info(f"Loading stores from {RAW_STORES_PATH}")
    df = pd.read_excel(RAW_STORES_PATH)

    # Optional filtering
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

    # Map state
    logger.info(f"Mapping stores → {geo_level}_CODE21 → StateGroups")
    stores_gdf["STATE_ENUM"] = stores_gdf[code_col].apply(
        lambda c: resolve_state_from_code(c, level=geo_level)
    )

    results = []
    states = sorted(stores_gdf["STATE_ENUM"].unique(), key=lambda s: s.name)
    logger.info(f"States found: {[s.name for s in states]}")

    for state in states:
        state_stores = stores_gdf[stores_gdf["STATE_ENUM"] == state].copy()
        logger.info(f"Processing {len(state_stores)} stores in {state.name}")

        # Decide stops vs routes
        source_type = "routes" if use_routes else "stops"
        loader = (
            load_or_build_state_transit_routes
            if use_routes
            else load_or_build_state_transit
        )
        transit = loader(state)

        if transit.empty:
            logger.warning(f"No transit {source_type} in {state.name}. Marking as ∞/0.")
            for _, store in state_stores.iterrows():
                row = {
                    "StoreId": store["Store Id"],
                    name_col: store[name_col],
                    "STATE": state.name,
                }
                for mode in TRANSIT_MODES:
                    row[f"nearest_{mode}_m"] = float("inf")
                    row[f"{mode}_count_{int(buffer_m)}m"] = 0
                results.append(row)
            continue

        # Ensure mode column exists
        if "mode" not in transit.columns:
            logger.warning(
                f"Transit data for {state.name} missing 'mode' column → defaulting to 'unknown'"
            )
            transit["mode"] = "unknown"

        # Spatial index for pre-filtering
        sidx = transit.sindex

        for _, store in tqdm(
            state_stores.iterrows(), total=len(state_stores), desc=state.name
        ):
            pt: Point = store.geometry

            # Quick bounding box filter
            deg = buffer_m / 111_000.0
            bbox_poly = box(pt.x - deg, pt.y - deg, pt.x + deg, pt.y + deg)
            cand_idx = list(sidx.intersection(bbox_poly.bounds))
            subset = transit.iloc[cand_idx]
            subset = subset[subset.geometry.within(bbox_poly)]

            row = {
                "StoreId": store["Store Id"],
                name_col: store[name_col],
                "STATE": state.name,
            }

            if subset.empty:
                for mode in TRANSIT_MODES:
                    row[f"nearest_{mode}_m"] = float("inf")
                    row[f"{mode}_count_{int(buffer_m)}m"] = 0
                results.append(row)
                continue

            # Compute metrics per mode
            for mode in TRANSIT_MODES:
                for metric_name, metric_fn in METRICS.items():
                    value = metric_fn(pt, subset, buffer_m, mode)
                    if metric_name == "nearest":
                        row[f"nearest_{mode}_m"] = value
                    elif metric_name == "count":
                        row[f"{mode}_count_{int(buffer_m)}m"] = value
                    else:
                        row[f"{metric_name}_{mode}"] = value

                    logger.debug(
                        f"Store {store['Store Id']} [{store[name_col]}]: "
                        f"{metric_name} {mode}="
                        f"{format_meters(value) if metric_name=='nearest' else value}"
                    )

            results.append(row)

    # Merge results back
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
        description="Compute store accessibility to transit (stops or routes)"
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
    parser.add_argument(
        "--use-routes", action="store_true", help="Use transit routes instead of stops"
    )
    args = parser.parse_args()

    main(
        buffer_m=args.buffer,
        geo_level=args.geo_level,
        store_type=args.store_type,
        use_routes=args.use_routes,
    )
