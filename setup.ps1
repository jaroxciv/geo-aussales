# Create package directory
mkdir -p src/geo_aussales | Out-Null
New-Item -Path src/geo_aussales/__init__.py -ItemType File | Out-Null

# Create data folders
mkdir -p data/raw, data/processed, data/external | Out-Null

# Create top-level folders
mkdir outputs, notebooks, scripts, tests | Out-Null

# Touch README files to explain folders
"Raw, unprocessed datasets" | Out-File -Encoding utf8 data/raw/README.md
"Processed, analysis-ready datasets" | Out-File -Encoding utf8 data/processed/README.md
"External third-party datasets" | Out-File -Encoding utf8 data/external/README.md

# Create a test placeholder
"def test_placeholder():`n    assert True" | Out-File -Encoding utf8 tests/test_placeholder.py

Write-Host "✅ Project structure created."
