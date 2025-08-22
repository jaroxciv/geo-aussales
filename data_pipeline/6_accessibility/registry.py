"""
registry.py
-----------
Central definitions for transit modes and metrics.
Keeps extraction and compute logic modular & extensible.
"""

from metrics import nearest_distance, count_within_buffer

# -------------------------------
# Transit Mode Registry (POIs)
# -------------------------------

TRANSIT_MODES = {
    "bus": {"highway": ["bus_stop"]},
    "train": {"railway": ["station", "halt", "stop"]},
    "tram": {"railway": ["tram_stop"]},
    "ferry": {"amenity": ["ferry_terminal"]},
    # Extensible: add more here in the future
}

# -------------------------------
# Transit Route Filters
# -------------------------------

TRANSIT_ROUTE_FILTERS = {
    "route": ["bus", "ferry", "railway", "subway", "train", "tram", "trolleybus"],
    "railway": ["tramway", "light_rail", "rail", "subway", "tram"],
    "bus": ["yes"],
    "public_transport": True,
}

# -------------------------------
# Metric Registry
# -------------------------------

METRICS = {
    "nearest": nearest_distance,
    "count": count_within_buffer,
    # Extensible: add more metrics here
}
