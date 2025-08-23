# data_pipeline/6_accessibility/summaries/base_summary.py
import abc
import pandas as pd


class LayerSummary(abc.ABC):
    """Abstract base for layer summaries."""

    layer: str  # e.g., "pois", "network"

    @abc.abstractmethod
    def summarise(self, df: pd.DataFrame, state_name: str) -> str:
        """Return a human-readable summary string for this layer/state."""
        raise NotImplementedError
