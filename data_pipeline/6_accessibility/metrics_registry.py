# data_pipeline/6_accessibility/metrics_registry.py
from metrics_pois import NearestDistanceMetric, CountWithinBufferMetric
from metrics_network import RoadLengthMetric, RoadLanesMetric, RoadSpeedMetric

# from metrics_buildings import BuildingAreaMetric  # future
# from metrics_landuse import GreenShareMetric      # future

METRICS_BY_LAYER = {
    "pois": [
        NearestDistanceMetric(),
        CountWithinBufferMetric(),
    ],
    "network": [
        RoadLengthMetric(),
        RoadLanesMetric(),
        RoadSpeedMetric(),
    ],
    # "buildings": [BuildingAreaMetric()],
    # "landuse": [GreenShareMetric()],
}
