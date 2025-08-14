import re
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from loguru import logger

from data_pipeline.constants import PBF_DIR, CACHE_DIR, SAFE_MAXLEN
from data_pipeline.utils import rel, relpath, _slug_for_match

pd.set_option("future.no_silent_downcasting", True)


def find_pbf_for_aoi(aoi_name: str) -> Path:
    """
    Resolve a .osm.pbf for an AOI by substring matching on the file stem.
    Priority:
      1) exact stem == slug
      2) stem contains slug
    If none or many match, raise with a helpful message.
    """
    slug = _slug_for_match(aoi_name)
    files = list(PBF_DIR.glob("*.osm.pbf"))
    if not files:
        raise FileNotFoundError(f"❌ No PBFs found in {rel(PBF_DIR)}")

    exact = None
    candidates = []
    for p in files:
        stem = p.stem.lower()
        if stem == slug:
            exact = p
        if slug in stem:
            candidates.append(p)

    if exact:
        return exact
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        available = ", ".join(sorted(p.stem for p in files))
        raise FileNotFoundError(
            f"❌ Missing PBF for AOI '{aoi_name}' (slug '{slug}') in {rel(PBF_DIR)}\n"
            f"   Available: {available}"
        )
    opts = "\n   - " + "\n   - ".join(str(p.name) for p in candidates)
    raise FileNotFoundError(
        f"❌ Ambiguous PBF for AOI '{aoi_name}' (slug '{slug}'); multiple matches:{opts}"
    )


def sanitize_for_gpkg(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Make column names/dtypes GDAL-friendly:
    - ASCII, [a-z0-9_], lowercase
    - truncate to SAFE_MAXLEN and ensure uniqueness
    - cast bool -> int8, nullable ints -> int32
    - object columns -> string (replace None with "")
    """
    gdf = gdf.copy()

    # 1) sanitize column names
    new_cols = []
    seen = set()
    for col in gdf.columns:
        if col == "geometry":
            new_cols.append(col)
            continue
        safe = col.lower()
        safe = re.sub(r"[^a-z0-9_]", "_", safe)
        safe = re.sub(r"__+", "_", safe).strip("_")
        safe = safe[:SAFE_MAXLEN]
        base = safe
        i = 1
        while safe in seen:
            suffix = f"_{i}"
            safe = base[: SAFE_MAXLEN - len(suffix)] + suffix
            i += 1
        seen.add(safe)
        new_cols.append(safe)
    gdf.columns = new_cols

    # 2) cast dtypes
    for c in gdf.columns:
        if c == "geometry":
            continue
        s = gdf[c]
        if pd.api.types.is_bool_dtype(s):
            gdf[c] = s.astype("int8")
        elif pd.api.types.is_integer_dtype(s):
            # downcast large pandas nullable ints to plain int32
            gdf[c] = s.fillna(0).infer_objects(copy=False).astype("int32")
        elif pd.api.types.is_float_dtype(s):
            # ok to leave as float64
            pass
        elif pd.api.types.is_object_dtype(s):
            # ensure plain strings (no lists/dicts)
            gdf[c] = s.astype(str).fillna("")
        else:
            # fallback: stringify
            gdf[c] = s.astype(str).fillna("")

    return gdf


def load_or_extract(layer_name, extractor_fn, slug):
    """Load cached layer if exists, else extract and save."""
    path = CACHE_DIR / f"{slug}_{layer_name}.gpkg"
    logger.info(f"🔍 Checking {layer_name} cache at {relpath(path)}")

    if path.exists():
        logger.info(f"[CACHE] Loading {layer_name} from {relpath(path)}")
        gdf = gpd.read_file(path)
        logger.info(f"[CACHE] {layer_name} loaded: {len(gdf):,} features")
        return gdf

    logger.info(f"[EXTRACT] Extracting {layer_name} from PBF...")
    gdf = extractor_fn()
    logger.success(f"[EXTRACT] {layer_name} extracted: {len(gdf):,} features")

    gdf.to_file(path, driver="GPKG")
    logger.success(f"[SAVE] {layer_name} saved to {relpath(path)}")
    return gdf


def aggregate_roads(
    edges_gdf: gpd.GeoDataFrame, hex_gdf: gpd.GeoDataFrame, hex_id_col="h3_id"
) -> pd.DataFrame:
    """
    Aggregate road network attributes per H3 hex.
    Ensures all hexes are included, even if they have no roads.
    """
    if edges_gdf.empty:
        # Return empty columns but preserve all hex IDs
        cols = ["roads_length_m", "roads_count", "avg_lanes", "avg_maxspeed"]
        return pd.DataFrame({hex_id_col: hex_gdf[hex_id_col], **{c: 0 for c in cols}})

    # Ensure CRS matches for spatial join
    if edges_gdf.crs != hex_gdf.crs:
        edges_gdf = edges_gdf.to_crs(hex_gdf.crs)

    # Spatial join edges to hexes
    edges_with_hex = gpd.sjoin(
        edges_gdf,
        hex_gdf[[hex_id_col, "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # Convert numeric-like columns safely (avoid SettingWithCopy warnings)
    for col in ["lanes", "maxspeed"]:
        if col in edges_with_hex.columns:
            edges_with_hex.loc[:, col] = pd.to_numeric(
                edges_with_hex[col], errors="coerce"
            )

    # Base aggregations
    agg_dict = {
        "length": "sum",
        "id": "count",
        "lanes": "mean",
        "maxspeed": "mean",
    }

    # One-hot encode selected categorical fields
    categorical_cols = [
        "highway",
        "service",
        "access",
        "motor_vehicle",
        "bicycle",
        "foot",
        "bridge",
        "tunnel",
        "sidewalk",
        "cycleway",
        "segregated",
        "surface",
        "lit",
    ]
    for col in categorical_cols:
        if col in edges_with_hex.columns:
            dummies = pd.get_dummies(edges_with_hex[col].fillna("unknown"), prefix=col)
            edges_with_hex = pd.concat([edges_with_hex, dummies], axis=1)
            for dummy_col in dummies.columns:
                agg_dict[dummy_col] = "sum"

    # Aggregate
    agg_df = edges_with_hex.groupby(hex_id_col).agg(agg_dict).reset_index()

    # Rename base columns
    rename_map = {
        "length": "roads_length_m",
        "id": "roads_count",
        "lanes": "avg_lanes",
        "maxspeed": "avg_maxspeed",
    }
    agg_df.rename(columns=rename_map, inplace=True)

    # Merge with all hexes to ensure full coverage
    agg_df = (
        hex_gdf[[hex_id_col]]
        .merge(agg_df, on=hex_id_col, how="left")
        .fillna(0)
        .infer_objects(copy=False)
        .infer_objects(copy=False)  # explicitly downcast object columns
    )

    return agg_df


def aggregate_buildings(
    buildings_gdf: gpd.GeoDataFrame, hex_gdf: gpd.GeoDataFrame, hex_id_col="h3_id"
) -> pd.DataFrame:
    """
    Aggregate building attributes per H3 hex.
    Always returns the same number of rows as hex_gdf, with 0/NaN where data is missing.
    """

    # --- Filter valid buildings ---
    if "building" in buildings_gdf.columns:
        exclude_values = {
            "bridge",
            "road",
            "footway",
            "service",
            "steps",
            "path",
            "cycleway",
            "corridor",
        }
        buildings_gdf = buildings_gdf[
            buildings_gdf["building"].notna()
            & (~buildings_gdf["building"].isin(exclude_values))
        ]
    buildings_gdf = buildings_gdf[
        ~buildings_gdf.geometry.is_empty & buildings_gdf.geometry.is_valid
    ]

    if buildings_gdf.empty:
        out_df = hex_gdf[[hex_id_col]].copy()
        out_df["buildings_count"] = 0
        out_df["total_building_area_m2"] = 0
        out_df["avg_building_levels"] = np.nan
        out_df["avg_building_height_m"] = np.nan
        out_df["avg_building_area_m2"] = np.nan
        return out_df

    # --- CRS match for join ---
    if buildings_gdf.crs != hex_gdf.crs:
        buildings_gdf = buildings_gdf.to_crs(hex_gdf.crs)

    # --- Spatial join ---
    buildings_with_hex = gpd.sjoin(
        buildings_gdf,
        hex_gdf[[hex_id_col, "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # --- Area calculation ---
    if buildings_with_hex.crs.is_geographic:
        utm_crs = buildings_with_hex.estimate_utm_crs()
        buildings_with_hex = buildings_with_hex.to_crs(utm_crs)
    buildings_with_hex["building_area_m2"] = buildings_with_hex.geometry.area

    # --- Convert numeric columns only if they exist ---
    for col in ["building:levels", "height"]:
        if col in buildings_with_hex.columns:
            buildings_with_hex[col] = pd.to_numeric(
                buildings_with_hex[col].copy(), errors="coerce"
            )

    # --- Build aggregation dict dynamically ---
    agg_dict = {
        "id": "count",
        "building_area_m2": "sum",
    }
    if "building:levels" in buildings_with_hex.columns:
        agg_dict["building:levels"] = "mean"
    if "height" in buildings_with_hex.columns:
        agg_dict["height"] = "mean"

    # --- One-hot encode building types ---
    if "building" in buildings_with_hex.columns:
        dummies = pd.get_dummies(
            buildings_with_hex["building"].fillna("unknown"), prefix="building"
        )
        buildings_with_hex = pd.concat([buildings_with_hex, dummies], axis=1)
        for dummy_col in dummies.columns:
            agg_dict[dummy_col] = "sum"

    # --- Reproject back to hex CRS ---
    buildings_with_hex = buildings_with_hex.to_crs(hex_gdf.crs)

    # --- Aggregate ---
    agg_df = buildings_with_hex.groupby(hex_id_col).agg(agg_dict).reset_index()

    # --- Rename for clarity ---
    rename_map = {
        "id": "buildings_count",
        "building_area_m2": "total_building_area_m2",
        "building:levels": "avg_building_levels",
        "height": "avg_building_height_m",
    }
    agg_df.rename(columns=rename_map, inplace=True)

    # --- Derived metrics ---
    agg_df["avg_building_area_m2"] = (
        agg_df["total_building_area_m2"] / agg_df["buildings_count"]
    ).replace([np.inf, -np.inf], np.nan)

    # --- Ensure all hexes are present ---
    agg_df = hex_gdf[[hex_id_col]].merge(agg_df, on=hex_id_col, how="left")
    agg_df["buildings_count"] = (
        agg_df["buildings_count"].fillna(0).infer_objects(copy=False)
    )
    agg_df["total_building_area_m2"] = (
        agg_df["total_building_area_m2"].fillna(0).infer_objects(copy=False)
    )

    return agg_df


def aggregate_pois(
    pois_gdf: gpd.GeoDataFrame, hex_gdf: gpd.GeoDataFrame, hex_id_col="h3_id"
) -> pd.DataFrame:
    """
    Aggregate Points of Interest (POIs) per H3 hex using only `poi_type`.

    Returns a DataFrame with:
    - pois_count: total number of POIs in the hex
    - poi_type_count: number of unique POI types in the hex
    - poi_<type>: counts of each POI type
    """

    if pois_gdf.empty:
        # Return empty stats for all hexes
        out_df = hex_gdf[[hex_id_col]].copy()
        out_df["pois_count"] = 0
        out_df["poi_type_count"] = 0
        return out_df

    # --- Prepare POIs safely ---
    amenity_col = pois_gdf["amenity"] if "amenity" in pois_gdf.columns else None
    shop_col = pois_gdf["shop"] if "shop" in pois_gdf.columns else None

    if amenity_col is not None and shop_col is not None:
        pois_gdf["poi_type"] = amenity_col.fillna(shop_col)
    elif amenity_col is not None:
        pois_gdf["poi_type"] = amenity_col
    elif shop_col is not None:
        pois_gdf["poi_type"] = shop_col
    else:
        pois_gdf["poi_type"] = "unknown"

    # Ensure CRS matches for spatial join
    if pois_gdf.crs != hex_gdf.crs:
        pois_gdf = pois_gdf.to_crs(hex_gdf.crs)

    # Keep only POIs with non-null poi_type
    pois_gdf = pois_gdf.dropna(subset=["poi_type"])

    # Spatial join POIs to hexes
    pois_with_hex = gpd.sjoin(
        pois_gdf, hex_gdf[[hex_id_col, "geometry"]], how="inner", predicate="intersects"
    )

    # One-hot encode poi_type
    dummies = pd.get_dummies(pois_with_hex["poi_type"], prefix="poi")
    pois_with_hex = pd.concat([pois_with_hex, dummies], axis=1)

    # Aggregation dictionary
    agg_dict = {"id": "count"}
    for dummy_col in dummies.columns:
        agg_dict[dummy_col] = "sum"

    # Aggregate
    agg_df = pois_with_hex.groupby(hex_id_col).agg(agg_dict).reset_index()

    # Add diversity metric
    diversity = (
        pois_with_hex.groupby(hex_id_col)["poi_type"]
        .nunique()
        .reset_index(name="poi_type_count")
    )
    agg_df = agg_df.merge(diversity, on=hex_id_col, how="left")

    # Rename total POI count
    agg_df.rename(columns={"id": "pois_count"}, inplace=True)

    # Merge with full hex grid to ensure all hexes are included
    agg_df = hex_gdf[[hex_id_col]].merge(agg_df, on=hex_id_col, how="left")

    # Fill NaNs with 0 for counts
    count_cols = [col for col in agg_df.columns if col != hex_id_col]
    agg_df[count_cols] = agg_df[count_cols].fillna(0).infer_objects(copy=False)

    return agg_df


def aggregate_landuse(
    landuse_gdf: gpd.GeoDataFrame, hex_gdf: gpd.GeoDataFrame, hex_id_col="h3_id"
) -> pd.DataFrame:
    """
    Aggregate landuse polygons per H3 hex using only the `landuse` column.

    Returns a DataFrame with:
    - landuse_count: total number of landuse polygons in the hex
    - landuse_type_count: number of unique landuse types in the hex
    - landuse_<type>: counts of each landuse type
    """
    # Ensure CRS matches for spatial join
    if landuse_gdf.crs != hex_gdf.crs:
        landuse_gdf = landuse_gdf.to_crs(hex_gdf.crs)

    # Spatial join
    landuse_with_hex = gpd.sjoin(
        landuse_gdf,
        hex_gdf[[hex_id_col, "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # One-hot encode landuse
    dummies = pd.get_dummies(
        landuse_with_hex["landuse"].fillna("unknown"), prefix="landuse"
    )
    landuse_with_hex = pd.concat([landuse_with_hex, dummies], axis=1)

    # Aggregation
    agg_dict = {"id": "count"}
    for dummy_col in dummies.columns:
        agg_dict[dummy_col] = "sum"

    agg_df = landuse_with_hex.groupby(hex_id_col).agg(agg_dict).reset_index()

    # Diversity metric
    diversity = (
        landuse_with_hex.groupby(hex_id_col)["landuse"]
        .nunique()
        .reset_index(name="landuse_type_count")
    )
    agg_df = agg_df.merge(diversity, on=hex_id_col, how="left")

    # Rename total count
    agg_df.rename(columns={"id": "landuse_count"}, inplace=True)

    # Merge with all hexes to keep empty ones
    agg_df = hex_gdf[[hex_id_col]].merge(agg_df, on=hex_id_col, how="left")

    # Fill missing with 0
    count_cols = [col for col in agg_df.columns if col != hex_id_col]
    agg_df[count_cols] = agg_df[count_cols].fillna(0).infer_objects(copy=False)

    return agg_df


def aggregate_natural(
    natural_gdf: gpd.GeoDataFrame, hex_gdf: gpd.GeoDataFrame, hex_id_col="h3_id"
) -> pd.DataFrame:
    """
    Aggregate natural features per H3 hex using only the `natural` column.

    Returns a DataFrame with:
    - natural_count: total number of natural features in the hex
    - natural_type_count: number of unique natural types in the hex
    - natural_<type>: counts of each natural type
    """
    # Ensure CRS matches
    if natural_gdf.crs != hex_gdf.crs:
        natural_gdf = natural_gdf.to_crs(hex_gdf.crs)

    # Spatial join
    natural_with_hex = gpd.sjoin(
        natural_gdf,
        hex_gdf[[hex_id_col, "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # One-hot encode 'natural'
    dummies = pd.get_dummies(
        natural_with_hex["natural"].fillna("unknown"), prefix="natural"
    )
    natural_with_hex = pd.concat([natural_with_hex, dummies], axis=1)

    # Aggregation dictionary
    agg_dict = {"id": "count"}
    for dummy_col in dummies.columns:
        agg_dict[dummy_col] = "sum"

    # Aggregate
    agg_df = natural_with_hex.groupby(hex_id_col).agg(agg_dict).reset_index()

    # Diversity metric
    diversity = (
        natural_with_hex.groupby(hex_id_col)["natural"]
        .nunique()
        .reset_index(name="natural_type_count")
    )
    agg_df = agg_df.merge(diversity, on=hex_id_col, how="left")

    # Rename
    agg_df.rename(columns={"id": "natural_count"}, inplace=True)

    # Merge with all hexes to retain those with no natural features
    agg_df = hex_gdf[[hex_id_col]].merge(agg_df, on=hex_id_col, how="left")

    # Fill NaNs with 0
    count_cols = [col for col in agg_df.columns if col != hex_id_col]
    agg_df[count_cols] = agg_df[count_cols].fillna(0).infer_objects(copy=False)

    return agg_df
