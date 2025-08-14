# cities.py
from enum import Enum


class CityGroups(Enum):
    INNER_MELBOURNE = [
        "City of Melbourne, Victoria",
        "Yarra, Victoria",
        "City of Port Phillip, Victoria",
    ]
    # More groups...

    @classmethod
    def get(cls, name):
        return getattr(cls, name).value
