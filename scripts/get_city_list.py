#!/usr/bin/env python3
import argparse
from cities import CityGroups, StateGroups


def main():
    parser = argparse.ArgumentParser(
        description="Return full city or state names from enum or direct input."
    )
    parser.add_argument("--enum", help="CityGroups or StateGroups enum key.")
    parser.add_argument("--country", default="Australia", help="Country to append.")
    parser.add_argument(
        "cities", nargs="*", help="City or state names with area (skip country)."
    )
    args = parser.parse_args()

    if args.enum:
        if hasattr(CityGroups, args.enum):
            places = CityGroups.get(args.enum)
        elif hasattr(StateGroups, args.enum):
            places = StateGroups.get(args.enum)
        else:
            raise ValueError(f"Enum {args.enum} not found in CityGroups or StateGroups")
    else:
        places = args.cities

    # Append country
    for place in places:
        print(f"{place}, {args.country}")


if __name__ == "__main__":
    main()
