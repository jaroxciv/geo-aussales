**Geo-spatial modeling of store sales in Australia**

This project aims to build a spatially-aware model to explain variation in store sales across Australia, integrating sociodemographic, urban, and environmental datasets.

## Objectives

* Consolidate multi-year datasets covering:

  * Sociodemographics (census, open data, raster data)
  * Urban data (street networks, POIs, connectivity, accessibility, land use, buildings)
  * Other relevant geospatial and environmental variables
* Build a reproducible and modular pipeline for:

  * Data acquisition and preprocessing
  * Feature engineering
  * Spatial statistical modeling and visualization

## Repository Structure

```
geo-aussales/
├── src/geo_aussales/       # Core Python package
├── data/
│   ├── raw/                # Unprocessed datasets
│   ├── processed/          # Clean datasets
│   └── external/           # Third-party data sources
├── notebooks/              # Jupyter notebooks for EDA & experiments
├── scripts/                # Standalone scripts for data tasks
├── tests/                  # Unit and integration tests
├── main.py                 # Entry point for running project code
├── pyproject.toml          # uv project config & dependencies
├── .gitignore
└── README.md
```

## Getting Started

### Prerequisites

* Python 3.12+
* [uv](https://docs.astral.sh/uv/) package manager

### Installation

```bash
uv venv
uv sync
```

### Running the Project

```bash
python main.py
```

## Development

Install development dependencies:

```bash
uv add --dev pytest black
```

Run tests:

```bash
pytest
```

## Data Sources (Planned)

Potential sources include:

* Australian Bureau of Statistics (ABS) Census Data
* OpenStreetMap for urban networks and POIs
* Geoscience Australia datasets
* Publicly available land use and building footprints
* Climate and environmental data from Bureau of Meteorology
