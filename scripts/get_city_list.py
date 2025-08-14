#!/usr/bin/env python3
import argparse
from cities import CityGroups


def main():
    parser = argparse.ArgumentParser(description="Return full city names from enum or direct input.")
    parser.add_argument("--enum", help="CityGroups enum key.")
    parser.add_argument("--country", default="Australia", help="Country to append.")
    parser.add_argument("cities", nargs="*", help="City names with area (skip country).")
    args = parser.parse_args()

    if args.enum:
        cities = CityGroups.get(args.enum)
    else:
        cities = args.cities

    # Append country
    for city in cities:
        print(f"{city}, {args.country}")

if __name__ == "__main__":
    main()
