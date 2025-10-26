# Brigadas-SaludMaterna — Data Pipeline (README)

## Overview

This repo contains a reproducible Python pipeline that builds ADM2-level risk tables for maternal health brigades in Mexico, ready for BI tools (Tableau) and auto-published to Google Sheets. It operationalizes the indicators and weights agreed in prior deliverables:

- **Descriptive Composite Risk (DCR100)** — current structural and spatial risk.  
- **Predictive Risk (PRS100)** — near-term risk forecast, combining recent trends, spillover, and state-level CAST forecasts.  
- **Priority Score** — weighted blend of PRS & DCR.

Daily-refresh variables: **ACLED event counts** (subject to ACLED’s public recency cap) and **ACLED CAST forecasts**.  
Static inputs (population, CLUES facilities, CONEVAL poverty) are built once and cached.

### Where to find the tables
- **Google Sheet:** `mx_brigadas_dashboard` (tabs: `adm2_risk_daily`, `acled_events_90d`, `adm2_geometry`, `sources_log`)  
- **CSVs in repo:**  
  - `out/adm2_risk_daily.csv`  
  - `out/acled_events_violent_90d.csv`  
  - `out/adm2_geometry.csv`

---

## What the Pipeline Produces (Viz-Ready Tables)

### 1) `adm2_risk_daily.csv` — main fact table (one row per ADM2)

| Column        | Meaning                                                                                           |
|:--------------|:-------------------------------------------------------------------------------------------------|
| `run_date`    | Date the pipeline ran.                                                                            |
| `data_as_of`  | Last ACLED date available (recency cap).                                                        |
| `adm1_name`, `adm2_name`, `adm2_code` | State, municipality, INEGI/COD-AB code (e.g., MX25008).                                    |
| `pop_total`, `pop_wra` | Total population & women 15–49 (WRA, proxied @25%).                                          |
| `v30`, `v3m`  | Events per 100k WRA in last 30/90 days (violent types).                                          |
| `dlt_v30_raw` | v30 minus previous 30-day rate.                                                                  |
| `spillover`   | Queen-contiguity neighbor average of v30.                                                       |
| `cast_state`  | State-level CAST forecast (0–1, winsorized 5–95%).                                              |
| `access_A`    | Inverse facility density (facilities per 100k WRA), scaled 0–1.                                 |
| `mvi`         | Municipal poverty (CONEVAL 2020 % pobreza), scaled 0–1.                                         |
| `DCR100`      | 100 × [0.35·V3m + 0.15·S + 0.30·A + 0.20·MVI].                                                 |
| `PRS100`      | 100 × [with CAST: 0.30·V30 + 0.25·dV30 + 0.10·S + 0.18·CAST + 0.12·A + 0.05·MVI; without CAST: 0.40·V30 + 0.30·dV30 + 0.10·S + 0.12·A + 0.08·MVI]. |
| `priority100` | 100 × [0.6·PRS + 0.4·DCR].                                                                      |

**Notes:** V30/V3m/dV30/S/CAST/A/MVI are all winsorized & scaled so higher = worse.  
Removed `strain_H` to avoid double-counting with `access_A` (high correlation).

---

### 2) `acled_events_90d.csv` — event points for map layers

ACLED violent events (last 90 days), with original ACLED fields (`event_id_cnty`, `event_date`, `event_type`, `latitude`, `longitude`, `source`, etc.) plus the joined `adm2_code`/`adm2_name`, and `run_date`/`data_as_of`. Ready for direct plotting.

---

### 3) `adm2_geometry.csv` — centroids helper

Minimal lookup (`adm1_name`, `adm2_name`, `adm2_code`, `lon`, `lat`) for lightweight map layers or tooltips.

---

## How It Refreshes (Automation & Caching)

- **Daily API refresh:**
  - ACLED events (last 90 days and prior 30 days)
  - ACLED CAST (state-level forecasts, scaled)
- **Caching:** Stores meta file (`out/acled_meta.json`) and last pulled CSVs.  
  If the ACLED recency cap hasn’t advanced, the API call is skipped.
- **Static builds:** Population (WorldPop raster), CLUES facilities, CONEVAL poverty (one-time unless missing).

**Important:** ACLED’s public “recency” restriction means recent events may be unavailable.  
The pipeline records `data_as_of` to reflect this and uses cached data if the cap is unchanged.

---

## Running the Pipeline

### Environment

```bash
pip install -r requirements.txt
```

or ensure the following are installed:

```bash
pandas geopandas shapely pyproj rtree libpysal rasterio rasterstats
requests python-dotenv gspread gspread-dataframe oauth2client unidecode
```

### Secrets

Create a `.env` file in the repo root:

```env
ACLED_USER=your_email@domain
ACLED_PASS=your_password
ACLED_REFRESH=true
CAST_REFRESH=true
SSL_VERIFY=true
# Optional:
# FORCE_REBUILD_POP=false
```

Add your Google service account JSON to the repo root and set in `pipeline.py`:

```python
GOOGLE_CREDS_JSON = ROOT / "brigadas-salud-materna-<id>.json"
```

Share the Google Sheet or folder with the service account email.

### Execute

```bash
python pipeline.py
```

Outputs:
- `out/adm2_risk_daily.csv`
- `out/acled_events_violent_90d.csv`
- `out/adm2_geometry.csv`

Uploaded to Google Sheets tabs (in order):  
`adm2_risk_daily`, `acled_events_90d`, `adm2_geometry`, `sources_log`.

---

## Data Sources & Transformations (Citations)

- ACLED: Armed Conflict Location & Event Data Project — events & CAST forecasts via API.  
  Filters: Mexico, violent event types, rolling 90d & 30d windows.  
  Attribution: © ACLED, access logged in `data_as_of`.
- WorldPop (R2025A): 2025 population, 100m WGS84. Aggregated via rasterstats.  
  WRA estimated as 25% of total. DOI: 10.5258/SOTON/WP00839
- CLUES (DGIS/Secretaría de Salud): ESTABLECIMIENTO_SALUD_202509.xlsx  
  Filters: public networks, active, non-mobile, valid coordinates. Spatial join to ADM2 polygons.
- CONEVAL 2020: Municipal poverty (% pobreza).  
  Extracted by municipal code, mapped to ADM2 via MX+state+municipio key.
- Boundaries: COD-AB Mexico ADM2 from HDX, used for joins, spillover, and centroids.

### Key Transforms

- Winsorize indicators (5–95%), scale to 0–1 (higher = worse).
- Spillover = neighbor average of v30 via libpysal.Queen.
- DCR100 = 100 × [0.35·V3m + 0.15·S + 0.30·A + 0.20·MVI]
- PRS100 = 100 × [with CAST: 0.30·V30 + 0.25·dV30 + 0.10·S + 0.18·CAST + 0.12·A + 0.05·MVI]
- priority100 = 100 × [0.6·PRS + 0.4·DCR]

---

## Quick Links

- `adm2_risk_daily.csv`
- `acled_events_violent_90d.csv`
- `adm2_geometry.csv`
- Google Sheet: `mx_brigadas_dashboard` (tabs auto-created by pipeline)
