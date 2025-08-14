import re
from pathlib import Path
from slugify import slugify as py_slugify
from data_pipeline.constants import PROJECT_ROOT, MERGED_DIR, GRID_DIR


def rel(path: Path) -> Path:
    """Return path relative to project root."""
    return path.relative_to(PROJECT_ROOT)


def relpath(p: Path) -> str:
    """Return path relative to project root."""
    try:
        return str(p.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(p)


def slugify(name: str) -> str:
    """Slugify to lowercase with underscores, preserving alphanumerics."""
    return py_slugify(name, separator="_", lowercase=True)


def _slug_for_match(s: str) -> str:
    """Lightweight slug only for matching/substring checks."""
    s = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    return re.sub(r"_+", "_", s)


def resolve_grid_path(metadata, resolution):
    """Pick correct H3 grid path based on AOI metadata."""
    aoi_list = metadata["aoi_raw"]
    if isinstance(aoi_list, str):
        aoi_list = [aoi_list]

    if len(aoi_list) > 1:
        # Multiple AOIs: use merged
        return MERGED_DIR / f"{metadata['aoi_slug']}_res{resolution}.gpkg"
    else:
        # Single AOI: use individual
        single_slug = metadata["aoi_slugs_individual"][0]
        return GRID_DIR / f"{single_slug}_res{resolution}.gpkg"
