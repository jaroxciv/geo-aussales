from __future__ import annotations
from typing import Dict, List, Optional
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, box
from loguru import logger
from tqdm import tqdm
from data_pipeline.constants import RAW_STORES_PATH, STORES_OUTPUT_PATH
from data_pipeline.utils import relpath
from geo_lookup import resolve_state_from_code
from loader import get_group_subcategories
from osm_loader import OSMLoader
from metrics.base_metric import AccessibilityMetric
from summaries.registry import SUMMARY_REGISTRY


class AccessibilityEngine:
    def __init__(
        self,
        *,
        buffer_m: float,
        geo_level: str,
        store_type: Optional[str],
        group: Optional[str],
        layers: List[str],
        metrics_by_layer: Dict[str, List[AccessibilityMetric]],
        aggregate_group: bool = False,
        verbose_stores: bool = False,
    ):
        self.buffer_m = buffer_m
        self.geo_level = geo_level
        self.store_type = store_type
        self.group = group
        self.layers = layers
        self.metrics_by_layer = metrics_by_layer
        self.aggregate_group = aggregate_group
        self.verbose_stores = verbose_stores

    def _load_stores(self) -> gpd.GeoDataFrame:
        df = pd.read_excel(RAW_STORES_PATH)
        if self.store_type:
            if "type" not in df.columns:
                raise ValueError("Store file missing 'type' column for filtering.")
            df = df[df["type"] == self.store_type]
            logger.info(f"Filtered stores to type='{self.store_type}' → {len(df)} rows")

        code_col = f"{self.geo_level.upper()}_CODE21"
        if not {"Store Id", "lat", "lon", code_col}.issubset(df.columns):
            missing = {"Store Id", "lat", "lon", code_col} - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")

        gdf = gpd.GeoDataFrame(
            df.copy(),
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326",
        )
        logger.info(f"Mapping stores → {self.geo_level}_CODE21 → StateGroups")
        gdf["STATE_ENUM"] = gdf[code_col].apply(
            lambda c: resolve_state_from_code(c, level=self.geo_level)
        )
        return gdf

    def _subset_by_bbox(
        self, pt: Point, features: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        if features is None or features.empty:
            return features
        deg = self.buffer_m / 111_000.0
        bbox_poly = box(pt.x - deg, pt.y - deg, pt.x + deg, pt.y + deg)
        sidx = features.sindex if not features.empty else None
        cand_idx = list(sidx.intersection(bbox_poly.bounds)) if sidx else []
        out = features.iloc[cand_idx] if cand_idx else features.iloc[[]]
        return out

    def run(self) -> gpd.GeoDataFrame:
        stores = self._load_stores()
        code_col = f"{self.geo_level.upper()}_CODE21"
        name_col = f"{self.geo_level.upper()}_NAME21"

        states = sorted(stores["STATE_ENUM"].unique(), key=lambda s: s.name)
        logger.info(f"States found: {[s.name for s in states]}")

        results: List[dict] = []

        for state in states:
            state_stores = stores[stores["STATE_ENUM"] == state].copy()
            loader = OSMLoader(state)

            logger.info(
                f"Processing {len(state_stores)} stores in {state.name} "
                f"for layers={self.layers}"
                + (f", group={self.group}" if "pois" in self.layers else "")
            )

            # Preload layers once per state
            layer_data: Dict[str, gpd.GeoDataFrame] = {}
            for layer in self.layers:
                if layer == "pois":
                    layer_data[layer] = loader.load(
                        "pois", group=self.group, aggregate=self.aggregate_group
                    )
                else:
                    layer_data[layer] = loader.load(layer)

            # Modes only for POIs
            if "pois" in self.layers:
                all_modes = (
                    [self.group]
                    if self.aggregate_group
                    else list(get_group_subcategories(self.group).keys())
                )
                state_mode_counts = {m: 0 for m in all_modes}
            else:
                all_modes = []
                state_mode_counts = {}

            # --- Iterate stores with tqdm ---
            for _, store in tqdm(
                state_stores.iterrows(),
                total=len(state_stores),
                desc=state.name,
            ):
                pt: Point = store.geometry
                row = {
                    "StoreId": store["Store Id"],
                    name_col: store[name_col],
                    "STATE": state.name,
                }

                for layer in self.layers:
                    feats = layer_data[layer]
                    subset = self._subset_by_bbox(pt, feats)

                    if layer == "pois":
                        for mode in all_modes:
                            subset_mode = (
                                subset[subset["mode"] == mode]
                                if not subset.empty and "mode" in subset.columns
                                else subset.iloc[[]]
                            )
                            for metric in self.metrics_by_layer.get("pois", []):
                                values = metric.compute(
                                    pt, subset_mode, self.buffer_m, mode=mode
                                )
                                for k, v in values.items():
                                    row[f"{self.group}_{mode}_{k}"] = v
                                    if k.startswith("count") and isinstance(
                                        v, (int, float)
                                    ):
                                        state_mode_counts[mode] += v
                    else:
                        for metric in self.metrics_by_layer.get(layer, []):
                            values = metric.compute(pt, subset, self.buffer_m)
                            for k, v in values.items():
                                row[f"{layer}_{k}"] = v

                results.append(row)

            # --- Summarise state ---
            df_results = pd.DataFrame(results)

            layer_summaries = []
            for layer in self.layers:
                if layer in SUMMARY_REGISTRY:
                    summary_str = SUMMARY_REGISTRY[layer].summarise(
                        df_results, state.name
                    )
                    if summary_str:
                        layer_summaries.append(summary_str)

            if layer_summaries:
                logger.info(
                    f"Finished {state.name}: {len(state_stores)} stores processed | "
                    + " | ".join(layer_summaries)
                )
            else:
                logger.info(
                    f"Finished {state.name}: {len(state_stores)} stores processed"
                )

        # Merge with stores and save
        out = stores.merge(
            pd.DataFrame(results), left_on="Store Id", right_on="StoreId", how="left"
        )
        drop_cols = ["StoreId", "STATE_ENUM"]
        if f"{code_col}.1" in out.columns:
            drop_cols.append(f"{code_col}.1")
        if f"{name_col}_y" in out.columns:
            drop_cols.append(f"{name_col}_y")
        out = out.drop(columns=drop_cols, errors="ignore").rename(
            columns={f"{name_col}_x": name_col}
        )

        STORES_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        out.to_file(STORES_OUTPUT_PATH, driver="GPKG")
        logger.success(f"Saved {len(out):,} rows → {relpath(STORES_OUTPUT_PATH)}")
        return out
