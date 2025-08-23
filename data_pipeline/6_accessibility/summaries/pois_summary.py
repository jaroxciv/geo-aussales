# data_pipeline/6_accessibility/summaries/pois_summary.py
import pandas as pd
from .base_summary import LayerSummary


class PoisSummary(LayerSummary):
    layer = "pois"

    def summarise(self, df: pd.DataFrame, state_name: str) -> str:
        subset = df[df["STATE"] == state_name]
        if subset.empty:
            return ""

        # detect group prefixes dynamically from column names (e.g. "transportation_bus_stop_count_1000m")
        poi_cols = [
            c for c in subset.columns if "_count_" in c and not c.startswith("network_")
        ]
        if not poi_cols:
            return "POIs → no features"

        counts = {col: subset[col].sum() for col in poi_cols}
        nonzero = {k: v for k, v in counts.items() if v > 0}
        if not nonzero:
            return "POIs → no features"

        # Build summary string: extract group + mode cleanly
        parts = []
        for col, val in nonzero.items():
            tokens = col.split("_")
            # format: {group}_{mode}_count_{buffer}m
            group = tokens[0]  # e.g. "transportation"
            mode = "_".join(tokens[1:-2])  # e.g. "bus_stop"
            parts.append(f"{mode}={int(val)}")
        return f"POIs[{group}] → " + ", ".join(parts)
