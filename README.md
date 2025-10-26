# Brigadas-SaludMaterna — Data Pipeline (README)

## Overview

This repository hosts a reproducible Python data pipeline designed to generate ADM2-level risk tables that support maternal health brigades operating in Mexico under humanitarian programming. The pipeline operationalizes an evidence-based risk prioritization model, integrating multiple data sources to provide actionable insights for targeting interventions where they are most needed. By transforming complex datasets into concise, interpretable risk scores, the pipeline empowers program managers and field teams with timely, data-driven decision support.

The risk model combines structural vulnerabilities and dynamic indicators to capture both current and near-term maternal health risks. This approach aligns with evidence-based programming, ensuring resources are allocated efficiently to municipalities facing the greatest challenges.

### Input Variables and Conceptual Roles

- **Violence (ACLED event counts):** Violence adversely affects maternal health by disrupting access to care and increasing stress and insecurity. Including recent violent event counts captures acute risk factors impacting communities.

- **Access (CLUES facility density):** Facility density inversely represents healthcare access. Lower density indicates potential barriers to maternal health services, which is critical for identifying underserved areas.

- **Poverty (CONEVAL municipal poverty rates):** Socioeconomic deprivation is a key determinant of health outcomes. Poverty rates contextualize structural vulnerabilities influencing maternal health risks.

- **Spillover (Neighboring violence rates):** Violence and instability can spread geographically. The spillover metric captures the influence of violence in adjacent municipalities, acknowledging spatial contagion effects.

- **CAST Forecast (ACLED state-level forecast):** The CAST forecast provides predictive insight into likely near-term violence trends, enabling anticipatory program adjustments.

### Weighting Logic

The risk scores are constructed by weighting these inputs to reflect their relative importance based on prior validation and expert consensus. Structural factors like violence trends and access receive higher weights due to their direct impact on maternal health outcomes, while spillover and poverty contribute complementary context. The weighting scheme balances stability (structural risk) and responsiveness (predictive risk) to generate actionable priority scores.

### Where to find the tables

- **Google Sheet:** `[Insert Google Sheet URL here]` (tabs: `adm2_risk_daily`, `acled_events_90d`, `adm2_geometry`, `sources_log`)  
- **CSVs in repo:**  
  - `out/adm2_risk_daily.csv` `[Insert CSV URL here]`  
  - `out/acled_events_violent_90d.csv` `[Insert CSV URL here]`  
  - `out/adm2_geometry.csv` `[Insert CSV URL here]`

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

### Mathematical Formulations

The **Descriptive Composite Risk (DCR100)** is calculated as:

\[
\text{DCR100} = 100 \times \left( 0.35 \times V_{3m} + 0.15 \times S + 0.30 \times A + 0.20 \times MVI \right)
\]

where:  
- \( V_{3m} \) = 90-day violent event rate per 100k WRA  
- \( S \) = Spillover (neighbor average violence rate)  
- \( A \) = Inverse facility density (access)  
- \( MVI \) = Municipal poverty index

The **Predictive Risk Score (PRS100)** incorporates recent trends and forecasts:

\[
\text{PRS100} = 100 \times \left( 0.30 \times V_{30} + 0.25 \times \Delta V_{30} + 0.10 \times S + 0.18 \times CAST + 0.12 \times A + 0.05 \times MVI \right)
\]

(with CAST forecast available), or:

\[
\text{PRS100} = 100 \times \left( 0.40 \times V_{30} + 0.30 \times \Delta V_{30} + 0.10 \times S + 0.12 \times A + 0.08 \times MVI \right)
\]

(without CAST forecast), where:  
- \( V_{30} \) = 30-day violent event rate per 100k WRA  
- \( \Delta V_{30} \) = Change in 30-day violent event rate  
- \( CAST \) = State-level forecast of violence risk

The final **priority score** balances predictive and descriptive risk:

\[
\text{priority100} = 100 \times (0.6 \times PRS + 0.4 \times DCR)
\]

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

- `adm2_risk_daily.csv` `[Insert CSV URL here]`
- `acled_events_violent_90d.csv` `[Insert CSV URL here]`
- `adm2_geometry.csv` `[Insert CSV URL here]`
- Google Sheet: `mx_brigadas_dashboard` `[Insert Google Sheet URL here]` (tabs auto-created by pipeline)
