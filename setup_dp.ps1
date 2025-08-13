#!/usr/bin/env pwsh
# setup_dp.ps1 — Initialize spatiotemporal feature cube pipeline structure
# Usage: pwsh setup_dp.ps1

Write-Host "📦 Setting up data_pipeline directory structure..." -ForegroundColor Cyan

# --- Base pipeline directory ---
New-Item -ItemType Directory -Force -Path "data_pipeline" | Out-Null

# --- Core stages ---
$stages = @(
    "1_spatial_grid",
    "2_osm_features",
    "3_census_features",
    "4_remote_sensing",
    "5_merge"
)
foreach ($stage in $stages) {
    New-Item -ItemType Directory -Force -Path "data_pipeline/$stage" | Out-Null
}

# --- Shared modules & scripts ---
New-Item -ItemType Directory -Force -Path "data_pipeline/modules" | Out-Null
New-Item -ItemType Directory -Force -Path "data_pipeline/tests" | Out-Null

# --- Config ---
New-Item -ItemType File -Force -Path "data_pipeline/config.yaml" | Out-Null

# --- Orchestrator ---
New-Item -ItemType File -Force -Path "data_pipeline/run_pipeline.py" | Out-Null

# --- Data directories ---
$rawDirs = @("osm", "census", "remote_sensing")
$processedDirs = @("osm", "census", "remote_sensing", "merged")

foreach ($dir in $rawDirs) {
    New-Item -ItemType Directory -Force -Path "data/raw/$dir" | Out-Null
}

foreach ($dir in $processedDirs) {
    New-Item -ItemType Directory -Force -Path "data/processed/$dir" | Out-Null
}

New-Item -ItemType Directory -Force -Path "data/outputs" | Out-Null

# --- .gitkeep files ---
$allDirs = Get-ChildItem -Directory -Recurse data_pipeline, data
foreach ($dir in $allDirs) {
    New-Item -ItemType File -Force -Path (Join-Path $dir.FullName ".gitkeep") | Out-Null
}

Write-Host "✅ data_pipeline structure created successfully." -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Edit data_pipeline/config.yaml with AOI, years, H3 resolution, and paths."
Write-Host "  2. Implement stage scripts in their respective folders."
Write-Host "  3. Run: python data_pipeline/run_pipeline.py"
