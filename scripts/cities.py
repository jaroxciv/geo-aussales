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


class StateGroups(Enum):
    NSW = ["New South Wales"]
    VIC = ["Victoria"]
    QLD = ["Queensland"]
    SA = ["South Australia"]
    WA = ["Western Australia"]
    TAS = ["Tasmania"]
    NT = ["Northern Territory"]
    ACT = ["Australian Capital Territory"]

    # Special "all states" option
    ALL_STATES = [
        "New South Wales",
        "Victoria",
        "Queensland",
        "South Australia",
        "Western Australia",
        "Tasmania",
        "Northern Territory",
        "Australian Capital Territory",
    ]

    @classmethod
    def get(cls, name):
        return getattr(cls, name).value
