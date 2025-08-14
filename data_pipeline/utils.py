from slugify import slugify as py_slugify


def slugify(name: str) -> str:
    """Slugify to lowercase with underscores, preserving alphanumerics."""
    return py_slugify(name, separator="_", lowercase=True)


def ensure_country(aoi_name: str, country: str = "Australia") -> str:
    """Append country if missing in the AOI name."""
    if country.lower() not in aoi_name.lower():
        return f"{aoi_name}, {country}"
    return aoi_name