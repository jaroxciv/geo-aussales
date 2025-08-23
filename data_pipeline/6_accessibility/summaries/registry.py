# data_pipeline/6_accessibility/summaries/registry.py
from .pois_summary import PoisSummary
from .network_summary import NetworkSummary

SUMMARY_REGISTRY = {
    "pois": PoisSummary(),
    "network": NetworkSummary(),
}
