# %%
# Imports
import os
import pathlib
import datetime as dt
from typing import Dict

import requests
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point as ShpPoint
from dotenv import load_dotenv
from unidecode import unidecode

# Spatial weights (required for spillover)
from libpysal.weights import Queen

# Raster utilities for one-time population aggregation
from rasterstats import zonal_stats
import rasterio

# %%
# Configuration & environment
load_dotenv()  # expects .env in project root
ACLED_USER = os.getenv("ACLED_USER")
ACLED_PASS = os.getenv("ACLED_PASS")
assert ACLED_USER and ACLED_PASS, "Set ACLED_USER and ACLED_PASS in your .env"

SSL_VERIFY = os.getenv("SSL_VERIFY", "true").lower() == "true"

# Project folders
ROOT = pathlib.Path.cwd()
DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Population build control
FORCE_REBUILD_POP = os.getenv("FORCE_REBUILD_POP", "false").lower() == "true"

# Toggle daily API refreshes (set to "false" to use cached outputs when possible)
ACLED_REFRESH = os.getenv("ACLED_REFRESH", "true").lower() == "true"
CAST_REFRESH  = os.getenv("CAST_REFRESH",  "true").lower() == "true"

# Meta/cache files
META_JSON = OUT_DIR / "acled_meta.json"

# Administrative boundaries (with ADM1 attributes)
MX_ADM2_SHP = DATA_DIR / "mex_admbnda_govmex_20210618_SHP" / "mex_admbnda_adm2_govmex_20210618.shp"
assert MX_ADM2_SHP.exists(), f"Missing shapefile: {MX_ADM2_SHP}"

# ACLED constants
ACLED_TOKEN_URL = "https://acleddata.com/oauth/token"
ACLED_READ_URL = "https://acleddata.com/api/acled/read"
ACLED_CAST_URL = "https://acleddata.com/api/cast/read"
ACLED_FIELDS = "event_id_cnty|event_date|year|disorder_type|event_type|sub_event_type|country|region|iso|admin1|admin2|location|latitude|longitude|source|fatalities|notes"
VIOLENT_TYPES = {"Violence against civilians", "Battles", "Explosions/Remote violence"}

# Static inputs (created once, then reused)
POP_RASTER = DATA_DIR / "worldpop" / "mex_pop_2025_CN_100m_R2025A_v1.tif"
TEMP_RASTER = DATA_DIR / "worldpop" / "mex_pop_2025_clean.tif"
POP_CSV = DATA_DIR / "pop_adm2.csv"

CLUES_XLSX = DATA_DIR / "CLUES" / "ESTABLECIMIENTO_SALUD_202509.xlsx"
FAC_CSV = DATA_DIR / "clues_facility_counts_adm2.csv"

CONEVAL_XLSX = DATA_DIR / "CONEVAL" / "Concentrado_indicadores_de_pobreza_2020.xlsx"
MVI_CSV = DATA_DIR / "coneval_muni.csv"

CAST_STATE_CSV = DATA_DIR / "cast_state.csv"

# Outputs
FACT_CSV = OUT_DIR / "adm2_risk_daily.csv"
GEOM_CSV = OUT_DIR / "adm2_geometry.csv"
EVENTS_CSV = OUT_DIR / "acled_events_violent_90d.csv"
EVENTS_PREV_CSV = OUT_DIR / "acled_events_prev30.csv"

# %%
import json

# Utility helpers
def winsor01_series(s: pd.Series, lo=0.05, hi=0.95) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.notna().sum() == 0:
        return pd.Series(np.zeros(len(s)), index=s.index)
    a, b = np.nanquantile(s, lo), np.nanquantile(s, hi)
    s2 = np.clip(s, a, b)
    if not np.isfinite(a) or not np.isfinite(b) or b <= a:
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s2 - a) / (b - a)

def normalize_admin_names(df: pd.DataFrame) -> pd.DataFrame:
    rep = {
        "Michoacan de Ocampo": "Michoacán",
        "Queretaro": "Querétaro",
        "Nuevo Leon": "Nuevo León",
        "San Luis Potosi": "San Luis Potosí",
        "Yucatan": "Yucatán",
        "Mexico": "México",
    }
    d = df.copy()
    if "admin1" in d.columns:
        d["admin1"] = d["admin1"].replace(rep)
    return d

def normalize_adm1_join_name(s: pd.Series) -> pd.Series:
    """
    Canonical ADM1 key for joining across sources (CAST - ADM2).
    Handles accents and Mexico-specific aliases.
    """
    def canon(x: str) -> str:
        if pd.isna(x):
            return ""
        y = unidecode(str(x)).strip()
        # Title-case for consistent tokens
        y = " ".join(w.capitalize() for w in y.split())
        # Mexico-specific aliasing (CAST ↔ INEGI)
        rep = {
            "Distrito Federal": "Ciudad De Mexico",
            "Ciudad De Mexico": "Ciudad De Mexico",
            "Estado De Mexico": "Mexico",       # CAST uses 'Mexico'
            "Mexico": "Mexico",
            "Queretaro De Arteaga": "Queretaro",
            "Queretaro": "Queretaro",
            "Veracruz": "Veracruz De Ignacio De La Llave",
            "Yucatan": "Yucatan",
            "Nuevo Leon": "Nuevo Leon",
            "San Luis Potosi": "San Luis Potosi",
            "Michoacan De Ocampo": "Michoacan De Ocampo",
        }
        return rep.get(y, y)
    return s.apply(canon)

def to_points(df: pd.DataFrame) -> gpd.GeoDataFrame:
    if df.empty:
        return gpd.GeoDataFrame(df.copy(), geometry=[], crs="EPSG:4326")
    d = df.copy()
    for col in ("latitude", "longitude"):
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
    d = d.dropna(subset=["latitude", "longitude"]).copy()
    if d.empty:
        return gpd.GeoDataFrame(d, geometry=[], crs="EPSG:4326")
    geom = [ShpPoint(lon, lat) for lon, lat in zip(d["longitude"], d["latitude"])]
    return gpd.GeoDataFrame(d, geometry=geom, crs="EPSG:4326")

def get_acled_token(username: str, password: str, verify: bool = True) -> str:
    resp = requests.post(
        ACLED_TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        data={"username": username, "password": password, "grant_type": "password", "client_id": "acled"},
        timeout=60,
        verify=verify,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def acled_quick_probe(params: Dict, token: str, verify: bool = True) -> Dict:
    q = {"_format": "json", "page": 1, "limit": 1}
    q.update(params)
    r = requests.get(
        ACLED_READ_URL,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params=q,
        timeout=60,
        verify=verify,
    )
    r.raise_for_status()
    return r.json()

def acled_fetch(params: Dict, token: str, *, limit: int = 5000, max_pages: int = 200, verify: bool = True) -> pd.DataFrame:
    frames = []
    for page in range(1, max_pages + 1):
        q = {"_format": "json", "page": page, "limit": limit}
        q.update(params)
        r = requests.get(
            ACLED_READ_URL,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            params=q,
            timeout=120,
            verify=verify,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            break
        frames.append(pd.DataFrame(data))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def resolve_recency_cap(token: str, verify: bool = True) -> dt.date:
    js = acled_quick_probe({}, token, verify=verify)
    allowed_end_str = (js.get("data_query_restrictions") or {}).get("date_recency", {}).get("date")
    if not allowed_end_str:
        return dt.date.today()
    try:
        return dt.date.fromisoformat(allowed_end_str)
    except ValueError:
        return dt.date.today()

def anchors_from_end(end_date: dt.date) -> Dict[str, dt.date]:
    return dict(
        TODAY=end_date,
        FROM_30=end_date - dt.timedelta(days=30),
        FROM_90=end_date - dt.timedelta(days=90),
        FROM_60PREV=end_date - dt.timedelta(days=60),
        TO_30PREV=end_date - dt.timedelta(days=30),
    )

def build_params(country: str, iso: int, start: dt.date, end: dt.date) -> Dict[str, Dict]:
    iso_params = {"iso": iso, "event_date": f"{start}|{end}", "event_date_where": "BETWEEN", "fields": ACLED_FIELDS}
    ctry_params = {"country": country, "event_date": f"{start}|{end}", "event_date_where": "BETWEEN", "fields": ACLED_FIELDS}
    return {"iso": iso_params, "country": ctry_params}

def choose_live_params(params_pair: Dict[str, Dict], token: str, verify: bool = True) -> Dict:
    p_iso = acled_quick_probe(params_pair["iso"], token, verify=verify)
    if (p_iso.get("total_count") or 0) > 0:
        return params_pair["iso"]
    p_ctry = acled_quick_probe(params_pair["country"], token, verify=verify)
    if (p_ctry.get("total_count") or 0) > 0:
        return params_pair["country"]
    return params_pair["iso"]

def _read_meta(path: pathlib.Path) -> dict:
    if path.exists():
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _write_meta(path: pathlib.Path, data: dict):
    try:
        with open(path, "w") as f:
            json.dump(data, f, default=str, indent=2)
    except Exception:
        pass

# %%
# One-time static builds (Population, CLUES, CONEVAL)
def build_population_if_needed():
    if POP_CSV.exists() and not FORCE_REBUILD_POP:
        pop = pd.read_csv(POP_CSV)
        for c in ["adm2_code","adm1_name","adm2_name"]:
            if c in pop.columns:
                pop[c] = pop[c].astype(str)
        return pop

    assert POP_RASTER.exists(), f"Missing raster: {POP_RASTER}"
    print("Running population zonal statistics; this may take several minutes.")

    with rasterio.open(POP_RASTER) as src:
        profile = src.profile
        data = src.read(1).astype("float32")
        data[data < 0] = 0
        profile.update(dtype="float32")
        TEMP_RASTER.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(TEMP_RASTER, "w", **profile) as dst:
            dst.write(data, 1)

    adm2 = gpd.read_file(MX_ADM2_SHP).to_crs(4326)
    adm2 = adm2.rename(columns={"ADM1_ES": "adm1_name", "ADM2_ES": "adm2_name", "ADM2_PCODE": "adm2_code"})
    adm2 = adm2[["adm1_name", "adm2_name", "adm2_code", "geometry"]]

    # Fix invalid geometries and dissolve to single part per ADM2
    adm2["geometry"] = adm2.geometry.buffer(0)
    adm2 = adm2.dissolve(by=["adm1_name","adm2_name","adm2_code"], as_index=False)

    # Compute zonal stats; do NOT treat 0 as nodata (0 is a valid population value)
    zs = zonal_stats(
        adm2,
        TEMP_RASTER,
        stats=["sum", "count"],
        nodata=None,
        geojson_out=False,
        all_touched=True
    )
    adm2["pop_total"] = [float(z.get("sum", 0) or 0) for z in zs]
    adm2["pix_count"] = [int(z.get("count", 0) or 0) for z in zs]

    # Diagnostic: flag ADM2 with zero population but non-trivial land area
    adm2_proj = adm2.to_crs(3857)
    adm2["area_km2"] = adm2_proj.geometry.area.values / 1e6
    zero_pop_flags = adm2[(adm2["pop_total"] <= 0) & (adm2["area_km2"] > 5)]
    if not zero_pop_flags.empty:
        chk_dir = OUT_DIR / "checks"
        chk_dir.mkdir(parents=True, exist_ok=True)
        zero_pop_flags[["adm1_name","adm2_name","adm2_code","area_km2","pix_count"]].to_csv(chk_dir / "pop_zero_area_flags.csv", index=False)

    adm2["pop_total"] = np.where(adm2["pop_total"] < 0, 0, adm2["pop_total"])
    adm2["pop_wra"] = (adm2["pop_total"] * 0.25)
    adm2["pop_total"] = adm2["pop_total"].round(0).astype(int)
    adm2["pop_wra"] = adm2["pop_wra"].round(0).astype(int)

    # Ensure string keys when writing/returning
    adm2["adm2_code"] = adm2["adm2_code"].astype(str)
    adm2["adm1_name"] = adm2["adm1_name"].astype(str)
    adm2["adm2_name"] = adm2["adm2_name"].astype(str)

    pop_out = adm2.drop(columns=["geometry","pix_count","area_km2"], errors="ignore").copy()
    pop_out.to_csv(POP_CSV, index=False)
    print(f"Saved ADM2 population summary -> {POP_CSV}")

    # Optional: print probe rows for specific ADM2 codes if present
    probe_codes = {"MX02001","MX23003"}
    probe = pop_out[pop_out["adm2_code"].isin(probe_codes)]
    if not probe.empty:
        print("Probe population rows:")
        print(probe.to_string(index=False))

    return pop_out

def build_clues_if_needed():
    if FAC_CSV.exists():
        fac = pd.read_csv(FAC_CSV)
        for c in ["adm2_code","entidad","municipio"]:
            if c in fac.columns:
                fac[c] = fac[c].astype(str)
        return fac

    # Load CLUES raw
    df = pd.read_excel(CLUES_XLSX, sheet_name="CLUES_202509")
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_")
        .str.replace("á","a").str.replace("é","e").str.replace("í","i")
        .str.replace("ó","o").str.replace("ú","u").str.replace("ñ","n")
    )

    # Filters: public networks, active, non-mobile, valid lat/lon
    public_institutions = {"SSA", "IMB", "IMS", "IST", "SDN", "SMP"}
    def s(x): return "" if pd.isna(x) else str(x).upper()

    df["inst"]   = df["clave_de_la_institucion"].map(s)
    df["status"] = df["clave_estatus_de_operacion"].map(s)   # '1' = active
    df["nivel"]  = df["clave_nivel_atencion"].map(s)         # '6' = mobile

    for col in ("latitud","longitud"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["latitud","longitud"]).copy()

    filtered = df[
        df["inst"].isin(public_institutions) &
        (df["status"] == "1") &
        (df["nivel"] != "6")
    ].copy()

    # Spatially assign ADM2 via point-in-polygon
    adm2 = gpd.read_file(MX_ADM2_SHP)
    if adm2.crs is None or adm2.crs.to_epsg() != 4326:
        adm2 = adm2.to_crs(4326)
    adm2 = adm2.rename(columns={
        "ADM2_PCODE": "adm2_code",
        "ADM2_ES": "adm2_name",
        "ADM1_ES": "entidad"
    })[["adm2_code","adm2_name","entidad","geometry"]].copy()
    adm2["adm2_code"] = adm2["adm2_code"].astype(str)

    g = gpd.GeoDataFrame(
        filtered,
        geometry=gpd.points_from_xy(filtered["longitud"], filtered["latitud"]),
        crs=4326
    )

    joined = gpd.sjoin(g, adm2, how="left", predicate="within").drop(columns=["index_right"])
    # Standardize expected column names after sjoin (defensive for any suffixing)
    if "adm2_code_right" in joined.columns and "adm2_code" not in joined.columns:
        joined = joined.rename(columns={"adm2_code_right": "adm2_code"})
    if "adm2_name_right" in joined.columns and "adm2_name" not in joined.columns:
        joined = joined.rename(columns={"adm2_name_right": "adm2_name"})
    if "entidad_right" in joined.columns and "entidad" not in joined.columns:
        joined = joined.rename(columns={"entidad_right": "entidad"})

    # Some points may fall outside polygons; keep them but they won't count into any ADM2
    # Aggregate counts by ADM2 code (robust even if 'entidad' is missing)
    fac_counts = (
        joined.groupby(["adm2_code"], dropna=False)
        .size()
        .reset_index(name="facilities")
    )

    # Attach human-readable names from ADM2 lookup
    fac_counts = (
        fac_counts.merge(adm2.drop(columns="geometry"), on="adm2_code", how="left")
        [["adm2_code", "adm2_name", "entidad", "facilities"]]
    )

    # Ensure types
    fac_counts["adm2_code"] = fac_counts["adm2_code"].astype(str)
    fac_counts["adm2_name"] = fac_counts["adm2_name"].astype(str)
    fac_counts["entidad"]   = fac_counts["entidad"].astype(str)

    fac_counts.to_csv(FAC_CSV, index=False)
    print(f"Saved aggregated facility counts (spatially joined) -> {FAC_CSV}")
    return fac_counts

def build_coneval_if_needed():
    if MVI_CSV.exists():
        mvi = pd.read_csv(MVI_CSV)
        for c in ["adm2_code","entidad","municipio"]:
            if c in mvi.columns:
                mvi[c] = mvi[c].astype(str)
        return mvi

    raw = pd.read_excel(CONEVAL_XLSX, sheet_name="Concentrado municipal", header=None, engine="openpyxl")
    hdr_idx = None
    for i in range(min(50, len(raw))):
        val = str(raw.iloc[i, 1]).strip().lower() if not pd.isna(raw.iloc[i, 1]) else ""
        if val.startswith("clave de entidad"):
            hdr_idx = i
            break
    if hdr_idx is None:
        raise RuntimeError("Could not locate header row in CONEVAL sheet.")

    df = raw.iloc[hdr_idx + 1 :, 1:11].copy()
    df.columns = [
        "clave_ent","entidad","clave_mun_5d","municipio",
        "pobreza_personas_2010","pobreza_pct_2010",
        "pobreza_personas_2015","pobreza_pct_2015",
        "pobreza_personas_2020","pobreza_pct_2020",
    ]
    df = df[~(df["clave_ent"].isna() & df["clave_mun_5d"].isna())].copy()
    def to_str(x): 
        return "" if pd.isna(x) else str(x).strip()
    df["clave_ent"] = df["clave_ent"].apply(to_str).str.zfill(2)
    df["clave_mun_5d"] = df["clave_mun_5d"].apply(to_str).str.zfill(5)
    df = df[df["clave_ent"].str.match(r"^\d{2}$")]
    df = df[df["clave_mun_5d"].str.match(r"^\d{5}$")]
    df["clave_mun_3d"] = df["clave_mun_5d"].str[-3:]
    df["adm2_code"] = "MX" + df["clave_ent"] + df["clave_mun_3d"]

    def parse_pct(x):
        if pd.isna(x): 
            return np.nan
        s = str(x).strip().lower()
        if s in {"n.d.", "nd", "n.d", "na", "n/a", ""}:
            return np.nan
        s = s.replace(",", ".")
        try:
            v = float(s)
        except Exception:
            return np.nan
        return max(0.0, min(100.0, v))

    df["poverty_rate"] = df["pobreza_pct_2020"].apply(parse_pct)
    out = df[["adm2_code", "entidad", "municipio", "poverty_rate"]].drop_duplicates().reset_index(drop=True)
    out["adm2_code"] = out["adm2_code"].astype(str)
    out["entidad"] = out["entidad"].astype(str)
    out["municipio"] = out["municipio"].astype(str)
    out.to_csv(MVI_CSV, index=False, encoding="utf-8")
    print(f"Saved municipal poverty records -> {MVI_CSV}")
    return out

# %%
# Daily refresh: ACLED events and ACLED CAST
def cast_fetch_mexico(token: str, *, verify: bool = True, future_only: bool = True) -> pd.DataFrame:
    headers = {"Authorization": f"Bearer {token}"}
    dfs = []
    page, limit = 1, 5000
    while True:
        params = {"_format": "csv", "country": "Mexico", "limit": limit, "page": page}
        r = requests.get(ACLED_CAST_URL, headers=headers, params=params, timeout=60, verify=verify)
        r.raise_for_status()
        chunk = pd.read_csv(pd.io.common.StringIO(r.text))
        if chunk.empty:
            break
        dfs.append(chunk)
        if len(chunk) < limit:
            break
        page += 1

    cast_raw = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if cast_raw.empty:
        return cast_raw

    cast_raw = cast_raw.rename(columns={"admin1": "adm1_name", "total_forecast": "cast_raw"})
    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
        "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12
    }
    cast_raw["month_num"] = cast_raw["month"].map(month_map)
    cast_raw["forecast_date"] = pd.to_datetime(
        dict(year=cast_raw["year"], month=cast_raw["month_num"], day=1), errors="coerce"
    )

    if future_only:
        today_anchor = pd.Timestamp(dt.date.today().replace(day=1))
        future = cast_raw[cast_raw["forecast_date"] >= today_anchor].copy()
        if not future.empty:
            cast_raw = future
        else:
            # fallback: use the latest forecast month present
            cast_raw = cast_raw.dropna(subset=["forecast_date"])
            latest = cast_raw["forecast_date"].max()
            cast_raw = cast_raw[cast_raw["forecast_date"] == latest].copy()

    # Aggregate across sub-rows per state for chosen month(s)
    cast_state = cast_raw.groupby("adm1_name", as_index=False)["cast_raw"].sum()
    cast_state["cast_raw"] = cast_state["cast_raw"].fillna(0).astype(float)
    return cast_state

# %%
# Load static inputs (build if missing)
pop = build_population_if_needed()
fac = build_clues_if_needed()
mvi = build_coneval_if_needed()

# Geometry once
adm2_raw = gpd.read_file(MX_ADM2_SHP)
if adm2_raw.crs is None or adm2_raw.crs.to_epsg() != 4326:
    adm2_raw = adm2_raw.to_crs(4326)
ADM2 = adm2_raw.rename(columns={
    "ADM2_PCODE": "adm2_code",
    "ADM2_ES": "adm2_name",
    "ADM1_PCODE": "adm1_code",
    "ADM1_ES": "adm1_name",
}).loc[:, ["adm1_name","adm1_code","adm2_name","adm2_code","geometry"]].copy()
# Harmonize key types (avoid object/float mismatches)
ADM2["adm2_code"] = ADM2["adm2_code"].astype(str)
ADM2["adm1_code"] = ADM2["adm1_code"].astype(str)
ADM2["adm1_name"] = ADM2["adm1_name"].astype(str)
ADM2["adm2_name"] = ADM2["adm2_name"].astype(str)

# Canonical ADM1 join key on ADM2
ADM2["adm1_join"] = normalize_adm1_join_name(ADM2["adm1_name"])

# %%
# %%
# Authenticate and compute event windows (with caching to avoid unnecessary API calls)
token = get_acled_token(ACLED_USER, ACLED_PASS, verify=SSL_VERIFY)
end_allowed = resolve_recency_cap(token, verify=SSL_VERIFY)
if end_allowed < dt.date.today():
    print(f"ACLED recency cap in effect; ending at {end_allowed}")
A = anchors_from_end(end_allowed)

# Determine whether to refresh ACLED calls
meta = _read_meta(META_JSON)
last_end_allowed = dt.date.fromisoformat(meta["end_allowed"]) if meta.get("end_allowed") else None
can_use_cache = (
    (EVENTS_CSV.exists() and EVENTS_PREV_CSV.exists()) and
    (last_end_allowed == end_allowed)
)
do_refresh_acled = ACLED_REFRESH and not can_use_cache

if do_refresh_acled:
    # Build parameter sets and fetch fresh data
    p_90   = build_params("Mexico", 484, A["FROM_90"],   A["TODAY"])
    p_prev = build_params("Mexico", 484, A["FROM_60PREV"], A["TO_30PREV"])
    active_90   = choose_live_params(p_90,   token, verify=SSL_VERIFY)
    active_prev = choose_live_params(p_prev, token, verify=SSL_VERIFY)

    acled_90 = acled_fetch(active_90, token, verify=SSL_VERIFY)
    acled_prev = acled_fetch(active_prev, token, verify=SSL_VERIFY)

    def _clean(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        d = df.copy()
        if "country" in d.columns:
            d = d[d["country"] == "Mexico"]
        if "event_id_cnty" in d.columns:
            d = d.drop_duplicates(subset=["event_id_cnty"]).reset_index(drop=True)
        return d

    acled_90 = _clean(acled_90)
    acled_prev = _clean(acled_prev)

    acled_90v = normalize_admin_names(acled_90[acled_90["event_type"].isin(VIOLENT_TYPES)].copy()) if not acled_90.empty else pd.DataFrame(columns=acled_90.columns)
    acled_prevv = normalize_admin_names(acled_prev[acled_prev["event_type"].isin(VIOLENT_TYPES)].copy()) if not acled_prev.empty else pd.DataFrame(columns=acled_prev.columns)

    # Points & spatial joins to ADM2
    acled_90v_g = to_points(acled_90v)
    acled_prevv_g = to_points(acled_prevv)

    if not acled_90v_g.empty:
        j_90 = gpd.sjoin(acled_90v_g, ADM2[["adm2_code","adm2_name","geometry"]], how="left", predicate="intersects")
        events_out = j_90.drop(columns=["geometry","index_right"], errors="ignore").copy()
        events_out["data_as_of"] = end_allowed
        events_out["run_date"] = dt.date.today()
        if "adm2_code" in events_out.columns:
            events_out["adm2_code"] = events_out["adm2_code"].astype(str)
        events_out.to_csv(EVENTS_CSV, index=False)
        print(f"Wrote violent events (last 90d) -> {EVENTS_CSV}")
    else:
        events_out = pd.DataFrame()

    if not acled_prevv_g.empty:
        j_prev = gpd.sjoin(acled_prevv_g, ADM2[["adm2_code","adm2_name","geometry"]], how="left", predicate="intersects")
        events_prev_out = j_prev.drop(columns=["geometry","index_right"], errors="ignore").copy()
        events_prev_out["data_as_of"] = end_allowed
        events_prev_out["run_date"] = dt.date.today()
        if "adm2_code" in events_prev_out.columns:
            events_prev_out["adm2_code"] = events_prev_out["adm2_code"].astype(str)
        events_prev_out.to_csv(EVENTS_PREV_CSV, index=False)
    else:
        events_prev_out = pd.DataFrame()

    # Update meta
    meta.update({
        "end_allowed": str(end_allowed),
        "last_run": str(dt.datetime.now())
    })
    _write_meta(META_JSON, meta)

else:
    # Use cached ACLED outputs
    print("ACLED refresh skipped (using cached EVENTS CSVs).")
    events_out = pd.read_csv(EVENTS_CSV) if EVENTS_CSV.exists() else pd.DataFrame()
    events_prev_out = pd.read_csv(EVENTS_PREV_CSV) if EVENTS_PREV_CSV.exists() else pd.DataFrame()
    for _df in (events_out, events_prev_out):
        if isinstance(_df, pd.DataFrame) and not _df.empty:
            if "adm2_code" in _df.columns:
                _df["adm2_code"] = _df["adm2_code"].astype(str)
            if "adm2_name" in _df.columns:
                _df["adm2_name"] = _df["adm2_name"].astype(str)
                
# Build muni_counts from cached or freshly computed outputs
if not events_out.empty:
    muni_counts_90v = (
        events_out.groupby(["adm2_code","adm2_name"], dropna=False)["event_id_cnty"]
        .count().reset_index(name="events_90v")
    )
else:
    muni_counts_90v = pd.DataFrame(columns=["adm2_code","adm2_name","events_90v"])

if not events_prev_out.empty:
    muni_counts_prevv = (
        events_prev_out.groupby(["adm2_code","adm2_name"], dropna=False)["event_id_cnty"]
        .count().reset_index(name="events_prevv")
    )
else:
    muni_counts_prevv = pd.DataFrame(columns=["adm2_code","adm2_name","events_prevv"])

for df_ in (muni_counts_90v, muni_counts_prevv):
    if not df_.empty:
        df_["adm2_code"] = df_["adm2_code"].astype(str)

muni_counts = pd.merge(
    muni_counts_90v, muni_counts_prevv,
    on=["adm2_code","adm2_name"], how="outer"
)
for c in ["events_90v", "events_prevv"]:
    if c in muni_counts.columns:
        muni_counts[c] = pd.to_numeric(muni_counts[c], errors="coerce").fillna(0).astype(int)

# %%
# Attach population and compute rates
def _ensure_str(df, cols):
    d = df.copy()
    for c in cols:
        if c in d.columns:
            d[c] = d[c].astype(str)
    return d

pop = _ensure_str(pop, ["adm2_code","adm1_name","adm2_name"])
fac = _ensure_str(fac, ["adm2_code"])
mvi = _ensure_str(mvi, ["adm2_code"])

adm2_lite = ADM2.drop(columns="geometry").copy()
acled_metrics = (
    adm2_lite
    .merge(pop[["adm1_name","adm2_name","adm2_code","pop_wra"]], on=["adm1_name","adm2_name","adm2_code"], how="left")
    .merge(muni_counts, on=["adm2_code","adm2_name"], how="left")
    .fillna({"pop_wra": 0, "events_90v": 0, "events_prevv": 0})
)

# 30-day window counts (events30) derived from events_out (works for fresh or cached paths)
def _parse_event_dates(df_events: pd.DataFrame, col: str = "event_date") -> pd.DataFrame:
    if df_events is None or len(df_events) == 0:
        return pd.DataFrame()
    d = df_events.copy()
    if col in d.columns:
        d[col] = pd.to_datetime(d[col], errors="coerce").dt.date
    for k in ["adm2_code", "adm2_name"]:
        if k in d.columns:
            d[k] = d[k].astype(str)
    return d

events_out_parsed = _parse_event_dates(events_out, "event_date") if isinstance(events_out, pd.DataFrame) else pd.DataFrame()
if not events_out_parsed.empty:
    events30_df = (
        events_out_parsed[events_out_parsed["event_date"] > A["FROM_30"]]
        .groupby(["adm2_code", "adm2_name"], dropna=False)
        .size()
        .reset_index(name="events30")
    )
    events30_df["adm2_code"] = events30_df["adm2_code"].astype(str)
    events30_df["adm2_name"] = events30_df["adm2_name"].astype(str)
else:
    events30_df = pd.DataFrame(columns=["adm2_code", "adm2_name", "events30"])

acled_metrics = acled_metrics.merge(
    events30_df, on=["adm2_code","adm2_name"], how="left"
)
acled_metrics["events30"] = pd.to_numeric(acled_metrics["events30"], errors="coerce").fillna(0).astype(int)

#
# Defensive denominators: if pop_wra == 0, force rates to 0 instead of dividing by 1
den = acled_metrics["pop_wra"].astype(float)

acled_metrics["v30"] = np.where(
    den > 0, 1e5 * acled_metrics["events30"].astype(float) / den, 0.0
)
acled_metrics["v3m"] = np.where(
    den > 0, 1e5 * acled_metrics["events_90v"].astype(float) / den, 0.0
)
acled_metrics["v30_prev"] = np.where(
    den > 0, 1e5 * acled_metrics["events_prevv"].astype(float) / den, 0.0
)
acled_metrics["dlt_v30_raw"] = acled_metrics["v30"] - acled_metrics["v30_prev"]

# Flag ADM2 with zero population but non-zero event counts (can skew spillovers)
zero_pop_anom = acled_metrics[
    (den <= 0) & (
        (acled_metrics["events30"] > 0) |
        (acled_metrics["events_90v"] > 0) |
        (acled_metrics["events_prevv"] > 0)
    )
][["adm1_name","adm2_name","adm2_code","events30","events_90v","events_prevv"]].copy()

if not zero_pop_anom.empty:
    anom_dir = OUT_DIR / "checks"
    anom_dir.mkdir(parents=True, exist_ok=True)
    zero_pop_anom.to_csv(anom_dir / "zero_population_anomalies.csv", index=False)

acled_metrics = acled_metrics[["adm1_name","adm2_name","adm2_code","pop_wra","v30","v3m","dlt_v30_raw"]].copy()

# %%
# Spillover (queen contiguity on ADM2 polygons)
g = ADM2.merge(acled_metrics, on=["adm1_name","adm2_name","adm2_code"], how="left")
g["v30"] = g["v30"].fillna(0)
# Build Queen contiguity weights; use ADM2 codes as IDs to avoid index warnings
wq = Queen.from_dataframe(g, ids=g["adm2_code"].tolist())
# Row-standardize so the matrix multiplication yields neighbor averages
wq.transform = "R"
S = wq.sparse.dot(g["v30"].to_numpy())
spill = pd.DataFrame({"adm2_code": g["adm2_code"].astype(str), "spillover": S})

# %%
# Access (A), Strain (H), Vulnerability (MVI), CAST (state-level)
adm2_base = (
    ADM2.drop(columns="geometry")
    .merge(pop, on=["adm1_name","adm2_name","adm2_code"], how="left")
    .merge(fac, on="adm2_code", how="left")
)

# Ensure required join keys exist; backfill from ADM2 lookup if needed
adm2_lu = ADM2.drop(columns="geometry").copy()
for c in ["adm1_name", "adm2_name", "adm2_code"]:
    if c in adm2_lu.columns:
        adm2_lu[c] = adm2_lu[c].astype(str)

# If adm2_name/adm1_name are missing or contain nulls, reattach from lookup by code
if ("adm2_name" not in adm2_base.columns) or adm2_base["adm2_name"].isna().any() or ("adm1_name" not in adm2_base.columns) or adm2_base["adm1_name"].isna().any():
    cols_to_drop = [c for c in ["adm1_name", "adm2_name"] if c in adm2_base.columns]
    adm2_base = adm2_base.drop(columns=cols_to_drop, errors="ignore").merge(
        adm2_lu[["adm2_code", "adm1_name", "adm2_name"]], on="adm2_code", how="left"
    )

# Normalize types on keys to avoid object/float mismatches
for c in ["adm1_name", "adm2_name", "adm2_code"]:
    if c in adm2_base.columns:
        adm2_base[c] = adm2_base[c].astype(str)

adm2_base["adm1_join"] = ADM2["adm1_join"]

adm2_base["facilities"] = adm2_base.get("facilities", pd.Series(dtype=float)).fillna(0)
adm2_base["pop_wra"]    = adm2_base.get("pop_wra", pd.Series(dtype=float)).fillna(0)
adm2_base["fac_per_100k"] = (1e5 * adm2_base["facilities"] / adm2_base["pop_wra"].clip(lower=1)).replace([np.inf, -np.inf], 0.0).fillna(0.0)
inv_fac_density = pd.Series(np.where(adm2_base["fac_per_100k"] > 0, 1.0 / adm2_base["fac_per_100k"], np.nan), index=adm2_base.index)


mvi = mvi.rename(columns={"poverty_rate":"MVI_raw"}) if "poverty_rate" in mvi.columns else mvi.rename(columns={"rezago_social":"MVI_raw"})
if "MVI_raw" not in mvi.columns:
    mvi["MVI_raw"] = 0.0
mvi["MVI_raw"] = pd.to_numeric(mvi["MVI_raw"], errors="coerce").fillna(0.0)

# CAST (always refreshed unless cache is valid for current month and CAST_REFRESH is false)
# Determine the current anchor month (first of month)
current_month_anchor = dt.date.today().replace(day=1)
use_cast_cache = False
if CAST_STATE_CSV.exists():
    _cast_tmp = pd.read_csv(CAST_STATE_CSV)
    if "forecast_date" in _cast_tmp.columns:
        try:
            # If saved forecast_date is >= current month, we can reuse unless forced
            saved_date = pd.to_datetime(_cast_tmp["forecast_date"].iloc[0]).date()
            if (not CAST_REFRESH) and (saved_date >= current_month_anchor):
                use_cast_cache = True
                cast = _cast_tmp[["adm1_join","cast_state"]].copy()
        except Exception:
            pass

if not use_cast_cache:
    token_cast = get_acled_token(ACLED_USER, ACLED_PASS, verify=SSL_VERIFY)
    cast_state = cast_fetch_mexico(token_cast, verify=SSL_VERIFY, future_only=True)
    if cast_state.empty:
        cast = pd.DataFrame({"adm1_join": [], "cast_state": []})
    else:
        a, b = np.nanquantile(cast_state["cast_raw"], 0.05), np.nanquantile(cast_state["cast_raw"], 0.95)
        x = np.clip(cast_state["cast_raw"].astype(float), a, b)
        cast_state["cast_state"] = (x - a) / (b - a) if b > a else 0.5
        cast_state["adm1_join"] = normalize_adm1_join_name(cast_state["adm1_name"])
        # Persist with the chosen forecast month for cache validation
        if "forecast_date" not in cast_state.columns:
            # Add a single forecast_date (use min/first of selected set)
            if "month_num" in cast_state.columns and "year" in cast_state.columns:
                cast_state["forecast_date"] = pd.to_datetime(
                    dict(year=cast_state["year"], month=cast_state["month_num"], day=1), errors="coerce"
                )
            else:
                cast_state["forecast_date"] = pd.Timestamp(current_month_anchor)
        cast_state.to_csv(CAST_STATE_CSV, index=False)
        cast = cast_state[["adm1_join","cast_state"]].drop_duplicates().copy()

# %%
# Defensive: ensure both sides of the merge have the expected keys
required_keys = ["adm1_name", "adm2_name", "adm2_code", "pop_wra"]
for k in required_keys:
    if k not in adm2_base.columns:
        raise KeyError(f"adm2_base missing required key: {k}")
    if k not in acled_metrics.columns:
        raise KeyError(f"acled_metrics missing required key: {k}")

# If any key columns drifted to non-string types, coerce
for df_ in (adm2_base, acled_metrics):
    for k in ["adm1_name", "adm2_name", "adm2_code"]:
        df_[k] = df_[k].astype(str)

# Final table, normalization, indices
final = (
    adm2_base
    .merge(acled_metrics, on=["adm1_name","adm2_name","adm2_code","pop_wra"], how="left")
    .merge(spill, on="adm2_code", how="left")
    .merge(cast, left_on="adm1_join", right_on="adm1_join", how="left")
    .merge(mvi[["adm2_code","MVI_raw"]], on="adm2_code", how="left")
)

for col in ["v30","v3m","dlt_v30_raw","spillover","cast_state","fac_per_100k","MVI_raw"]:
    final[col] = pd.to_numeric(final[col], errors="coerce").fillna(0.0)

final["V30"] = winsor01_series(final["v30"])
final["V3m"] = winsor01_series(final["v3m"])

d = final["dlt_v30_raw"].astype(float)
p1, p9 = np.nanpercentile(d, 10), np.nanpercentile(d, 90)
d_clip = np.clip(d, p1, p9)
d_unit = -1 + 2 * (d_clip - p1) / (p9 - p1) if p9 > p1 else np.zeros_like(d_clip)
final["dV30"] = winsor01_series(pd.Series(d_unit, index=final.index))

final["S"]    = winsor01_series(final["spillover"])
final["CAST"] = final["cast_state"]

final["A"] = winsor01_series(inv_fac_density)
final["MVI"] = winsor01_series(final["MVI_raw"])

med_wra = final["pop_wra"].replace(0, np.nan).median()
w = np.sqrt(final["pop_wra"].replace(0, 1) / (med_wra if med_wra and med_wra > 0 else 1))
final["w_exposure"] = np.clip(w, 0.5, 2.0)
final["w_exposure"] = final["w_exposure"].replace([np.inf, -np.inf], np.nan).fillna(1.0)

# Note: Dropped strain_H (system load) to avoid redundancy with access_A (facility coverage),
# which showed very high correlation. Access_A remains the single proxy for health-system fragility.
# Revised weights (removed H/strain to avoid redundancy with access_A)
w_dcr = {"V3m":0.35, "S":0.15, "A":0.30, "MVI":0.20}
w_prs_cast = {"V30":0.30, "dV30":0.25, "S":0.10, "CAST":0.18, "A":0.12, "MVI":0.05}
w_prs_nocast = {"V30":0.40, "dV30":0.30, "S":0.10, "A":0.12, "MVI":0.08}

def compute_prs_row(r):
    if r.get("CAST", 0) > 0:
        w = w_prs_cast
        return (r["V30"]*w["V30"] + r["dV30"]*w["dV30"] + r["S"]*w["S"] +
                r["CAST"]*w["CAST"] + r["A"]*w["A"] + r["MVI"]*w["MVI"])
    else:
        w = w_prs_nocast
        return (r["V30"]*w["V30"] + r["dV30"]*w["dV30"] + r["S"]*w["S"] +
                r["A"]*w["A"] + r["MVI"]*w["MVI"])

final["DCR"] = (final["V3m"]*w_dcr["V3m"] + final["S"]*w_dcr["S"] +
                final["A"]*w_dcr["A"] + final["MVI"]*w_dcr["MVI"])
final["PRS"] = final.apply(compute_prs_row, axis=1)

final["DCR100"] = 100*final["DCR"]
final["PRS100"] = 100*final["PRS"]
final["priority100"] = 100*(0.6*final["PRS"] + 0.4*final["DCR"])
run_date_today = dt.date.today()
final["run_date"] = run_date_today

for c in ["DCR","PRS","DCR100","PRS100","priority100"]:
    final[c] = pd.to_numeric(final[c], errors="coerce").fillna(0.0)

fact = final[[
    "run_date","adm1_name","adm2_name","adm2_code","pop_total","pop_wra","w_exposure",
    "v30","v3m","dlt_v30_raw","spillover","CAST","A","MVI",
    "DCR100","PRS100","priority100"
]].copy().rename(columns={"CAST":"cast_state","A":"access_A","MVI":"mvi"})

# Add data_as_of column to capture last ACLED date available (end_allowed)
fact["data_as_of"] = end_allowed

numeric_cols = ["pop_total","pop_wra","w_exposure","v30","v3m","dlt_v30_raw","spillover","cast_state","access_A","mvi","DCR100","PRS100","priority100"]
for c in numeric_cols:
    fact[c] = pd.to_numeric(fact[c], errors="coerce").fillna(0.0)

print("Fact rows:", len(fact))
n_zero_pop = int((final["pop_wra"] <= 0).sum())
print(f"ADM2 with zero pop_wra: {n_zero_pop}")

# %%
# Geometry lookup (centroids) for BI
# Compute centroids in a projected CRS to avoid distortion, then convert back to lon/lat
adm2_proj = ADM2.to_crs(3857).copy()
centroids_proj = adm2_proj.geometry.centroid
centroids_ll = gpd.GeoSeries(centroids_proj, crs=adm2_proj.crs).to_crs(4326)

geom_lu = pd.DataFrame({
    "adm1_name": ADM2["adm1_name"].astype(str),
    "adm2_name": ADM2["adm2_name"].astype(str),
    "adm2_code": ADM2["adm2_code"].astype(str),
    "lon": centroids_ll.x,
    "lat": centroids_ll.y,
})

# %%
# Outputs — CSVs (Tableau-ready)
# Ensure data_as_of is included in the CSV export
fact.to_csv(FACT_CSV, index=False)
geom_lu.to_csv(GEOM_CSV, index=False)
print(f"Written:\n  {FACT_CSV}\n  {GEOM_CSV}")

# %%
# Optional: Google Sheets upload (enabled)
ENABLE_SHEETS = True
SHEET_NAME = "mx_brigadas_dashboard"
GOOGLE_CREDS_JSON = ROOT / "brigadas-salud-materna-f3613cd11d81.json"

if ENABLE_SHEETS:
    print(f"ACLED_REFRESH={ACLED_REFRESH}, CAST_REFRESH={CAST_REFRESH}")
    import gspread
    from gspread_dataframe import set_with_dataframe
    from oauth2client.service_account import ServiceAccountCredentials

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(GOOGLE_CREDS_JSON), scope)
    gc = gspread.authorize(creds)

    # Open or create spreadsheet
    try:
        sh = gc.open(SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SHEET_NAME)

    def write_or_replace(df: pd.DataFrame, ws_name: str):
        try:
            ws = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=ws_name, rows="2", cols="2")
        ws.clear()
        set_with_dataframe(ws, df)
        return ws

    # 1) Main fact table
    ws_risk = write_or_replace(fact, "adm2_risk_daily")

    # 2) ACLED violent events (last 90d) — load from CSV if present
    if EVENTS_CSV.exists():
        events_df = pd.read_csv(EVENTS_CSV)
        ws_events = write_or_replace(events_df, "acled_events_90d")
    else:
        ws_events = None

    # 3) Geometry lookup (centroids)
    ws_geom = write_or_replace(geom_lu, "adm2_geometry")

    # Optional: Provenance / sources log
    prov = pd.DataFrame({
        "run_date": [dt.date.today()] * 3,
        "source": ["ACLED", "CONEVAL", "CLUES/Population"],
        "details": ["MX violent events last 90d/prev30", "Municipal poverty", "Facility counts + WRA"],
        "url": ["https://acleddata.com", "https://www.coneval.org.mx", "http://www.dgis.salud.gob.mx"]
    })
    ws_sources = write_or_replace(prov, "sources_log")

    # Reorder worksheets so risk_daily is first, then events, then geometry, then sources
    # Only include worksheets that actually exist
    desired_titles = ["adm2_risk_daily", "acled_events_90d", "adm2_geometry", "sources_log"]
    current = {ws.title: ws for ws in sh.worksheets()}
    ordered = [current[t] for t in desired_titles if t in current]
    if ordered:
        sh.reorder_worksheets(ordered)

    # Summary
    counts = {
        "adm2_risk_daily": len(fact),
        "acled_events_90d": (len(events_df) if EVENTS_CSV.exists() else 0),
        "adm2_geometry": len(geom_lu),
        "sources_log": len(prov),
    }
    print(f"Wrote to Google Sheet: {SHEET_NAME}")
    print("Tabs and row counts:", counts)
# %%
