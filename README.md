# Geo-AUSSales

**Geospatial OSM Feature Extraction & H3 Hexagon Analysis for Australian Cities**

This project automates the workflow of:

1. Extracting **OpenStreetMap** data for specific **AOIs** (Areas of Interest) from a large `.osm.pbf` file.
2. Generating **H3 hexagonal grids** for each AOI.
3. Aggregating **OSM features** (buildings, roads, POIs, landuse, natural features) into those hexes.
4. Producing geospatial datasets and visualizations for further urban analysis.

## 🚀 Features

* **AOI-driven processing**
  AOIs are defined in `data_pipeline/aoi_info.json`, supporting multiple cities in a group (e.g., Inner Melbourne).

* **Parallel H3 grid generation**
  Uses Python’s multiprocessing to generate per-city grids quickly and save a merged grid for downstream processing.

* **OSM feature aggregation**
  Uses [Pyrosm](https://pyrosm.readthedocs.io) to extract and aggregate:

  * Building counts, areas, heights
  * Road network length
  * Points of interest
  * Landuse and natural features

* **PBF extraction per AOI**
  Large `.osm.pbf` files are split into smaller AOI-specific extracts with `osmium`.

## 📂 Project Structure

```
geo-aussales/
│
├── data/                     # Raw and processed geospatial data
│   ├── external/              # Raw PBFs per AOI
│   ├── processed/             # H3 grids and aggregated results
│   └── outputs/               # Final outputs
│
├── data_pipeline/             # Core data pipeline
│   ├── 1_spatial_grid/        # H3 grid generation
│   ├── 2_osm_features/        # OSM extraction & aggregation
│   ├── aoi_info.json          # AOI definitions
│   └── run_pipeline.py        # Pipeline orchestrator
│
├── scripts/                   # Utility scripts
│   ├── extract_pbf.sh         # Extract AOI-specific PBFs from large PBF
│   ├── get_polygon.py         # Get polygon for a city AOI
│   ├── get_city_list.py       # List AOIs for a given group or enum
│   └── cities.py              # AOI group definitions
│
└── pyproject.toml             # Python dependencies
```

## 🛠 Installation

```bash
# Clone repo
git clone https://github.com/jaroxciv/geo-aussales.git
cd geo-aussales

# Create venv
uv venv
uv .venv/bin/activate  # or .\.venv\Scripts\activate on Windows
uv sync

# Install dependencies
uv pip install -e .
```

## ⚙️ Usage

### 1️⃣ Extract AOI-specific PBFs

```bash
bash scripts/extract_pbf.sh "City of Melbourne, Victoria"
```

or for a group:

```bash
bash scripts/extract_pbf.sh --enum inner_melbourne
```

### 2️⃣ Generate H3 grids

```bash
python data_pipeline/1_spatial_grid/generate_h3_grid.py
```

### 3️⃣ Extract & aggregate OSM features

```bash
python data_pipeline/2_osm_features/extract_osm_features.py
```

## 📊 Example Outputs

The pipeline produces **H3-based geopackages** that include aggregated OSM data for:

* **Building Count**
* **Total Building Area (m²)**
* **Average Building Height (m)**

## 📌 Notes

* AOIs are matched to `.osm.pbf` files dynamically by slug substring matching.
* Large `.osm.pbf` source files (e.g., `australia-latest.osm.pbf`) should be placed in `data/external/`.

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
