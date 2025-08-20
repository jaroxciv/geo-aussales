import argparse
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, box
from loguru import logger
from tqdm import tqdm

from data_pipeline.utils import relpath
from data_pipeline.constants import RAW_STORES_PATH, STORES_OUTPUT_PATH
from helpers import (
    nearest_distance,
    count_within_buffer,
    load_or_build_state_transit,
    format_meters,
)
from state_map import sa3_to_stategroup


def main(buffer_m: float = 1000.0):
    logger.info(f"Loading stores from {RAW_STORES_PATH}")
    df = pd.read_excel(RAW_STORES_PATH)

    required = {"Store Id", "lat", "long", "SA3_CODE21"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in stores file: {missing}")

    stores_gdf = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df["long"], df["lat"]),
        crs="EPSG:4326",
    )

    # --- Resolve state via ABS shapefile lookup ---
    logger.info("Mapping stores → SA3_CODE21 → StateGroups")
    stores_gdf["STATE_ENUM"] = stores_gdf["SA3_CODE21"].apply(sa3_to_stategroup)

    results = []
    states = sorted(stores_gdf["STATE_ENUM"].unique(), key=lambda s: s.name)
    logger.info(f"States found: {[s.name for s in states]}")

    for state in states:
        state_stores = stores_gdf[stores_gdf["STATE_ENUM"] == state].copy()
        logger.info(f"Processing {len(state_stores)} stores in {state.name}")

        transit = load_or_build_state_transit(state)
        if transit.empty:
            logger.warning(f"No transit POIs in {state.name}. Marking as ∞/0.")
            for _, store in state_stores.iterrows():
                results.append(
                    {
                        "StoreId": store["Store Id"],
                        "SA3_NAME21": store["SA3_NAME21"],
                        "STATE": state.name,
                        "nearest_bus_m": float("inf"),
                        "nearest_train_m": float("inf"),
                        f"bus_count_{int(buffer_m)}m": 0,
                        f"train_count_{int(buffer_m)}m": 0,
                    }
                )
            continue

        # Spatial index for quick bbox pre-filter
        sidx = transit.sindex

        for _, store in tqdm(
            state_stores.iterrows(), total=len(state_stores), desc=state.name
        ):
            pt: Point = store.geometry

            # BBox in degrees (~ accurate enough for small buffer)
            deg = buffer_m / 111_000.0
            bbox_poly = box(pt.x - deg, pt.y - deg, pt.x + deg, pt.y + deg)
            cand_idx = list(sidx.intersection(bbox_poly.bounds))
            subset = transit.iloc[cand_idx]
            subset = subset[subset.geometry.within(bbox_poly)]

            # Metrics
            nearest_bus = nearest_distance(pt, subset, mode="bus")
            nearest_train = nearest_distance(pt, subset, mode="train")
            bus_count = count_within_buffer(pt, subset, buffer_m, mode="bus")
            train_count = count_within_buffer(pt, subset, buffer_m, mode="train")

            logger.info(
                f"Store {store['Store Id']} [{store['SA3_NAME21']}]: "
                f"bus {format_meters(nearest_bus)}, "
                f"train {format_meters(nearest_train)}, "
                f"bus≤{int(buffer_m)}m={bus_count}, train≤{int(buffer_m)}m={train_count}"
            )

            results.append(
                {
                    "StoreId": store["Store Id"],
                    "SA3_NAME21": store["SA3_NAME21"],
                    "STATE": state.name,
                    "nearest_bus_m": nearest_bus,
                    "nearest_train_m": nearest_train,
                    f"bus_count_{int(buffer_m)}m": bus_count,
                    f"train_count_{int(buffer_m)}m": train_count,
                }
            )

    # Merge results back
    out = stores_gdf.merge(
        pd.DataFrame(results), left_on="Store Id", right_on="StoreId", how="left"
    )
    STORES_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_file(STORES_OUTPUT_PATH, driver="GPKG")
    logger.success(f"Saved {len(out):,} rows → {relpath(STORES_OUTPUT_PATH)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute store accessibility to bus/train stops (state-cached)"
    )
    parser.add_argument(
        "--buffer",
        type=float,
        default=1000.0,
        help="Buffer size in meters (default: 1000)",
    )
    args = parser.parse_args()
    main(buffer_m=args.buffer)
