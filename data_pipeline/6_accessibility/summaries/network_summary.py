# data_pipeline/6_accessibility/summaries/network_summary.py
import pandas as pd
from .base_summary import LayerSummary


class NetworkSummary(LayerSummary):
    layer = "network"

    def summarise(self, df: pd.DataFrame, state_name: str) -> str:
        subset = df[df["STATE"] == state_name]
        if subset.empty:
            return ""
        cols = [c for c in subset.columns if c.startswith("network_")]
        if not cols:
            return "Network → no features"
        means = subset[cols].mean().to_dict()
        parts = [f"{k}={v:,.1f}" for k, v in means.items()]
        return "Network → " + ", ".join(parts)
