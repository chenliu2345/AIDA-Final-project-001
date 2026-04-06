import os
import re
import sys
import hashlib
import logging
import datetime
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError
import requests
import time
from functools import lru_cache
from dotenv import load_dotenv

# Load ORS API key from .env at project root (two levels up from this file)
load_dotenv(Path(__file__).parent.parent / ".env")

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION  ← edit before running
# ─────────────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "server":      ".", 
    "database":    "AB_CarSale_DB",
    "driver":      "ODBC+Driver+17+for+SQL+Server",
    "use_trusted": True,                      # set False and add uid/pwd for SQL auth
}

BASE_DIR = Path(__file__).parent.parent
CSV_PATH = BASE_DIR / "Data_Raw" / "Optimized_Alberta_owner_sales_car_clean.csv" # NOTICE THE FILE PATH!
LOG_FILE   = "etl_run.log"
CHUNK_SIZE = 1000          # rows per bulk-insert batch

# ── Table name map (matches tbl_ naming convention in SQL schema) ─────────────
TABLE = {
    "stg":           "tbl_stg_Raw",        # retained in DB for architecture
    "models":        "tbl_Models",
    "years":         "tbl_Years",
    "trims":         "tbl_Trims",
    "locations":     "tbl_Locations",
    "statuses":      "tbl_Statuses",
    "conditions":    "tbl_Conditions",
    "transmissions": "tbl_Transmissions",
    "drivetrains":   "tbl_Drivetrains",
    "body_styles":   "tbl_Body_Styles",
    "colours":       "tbl_Colours",
    "seats":         "tbl_Seats",
    "vehicles":      "tbl_Vehicles",
    "listings":      "tbl_Listings",
    "status_log":    "tbl_Listing_Status",
    "rejected":      "tbl_Rejected_Rows",
}

# ─────────────────────────────────────────────────────────────────────────────
#  VALIDATION RULES
# ─────────────────────────────────────────────────────────────────────────────
VALID_STATUSES   = {"SOLD", "ACTIVE", "ACTIVE_REPOST", "RESHELVED"}
VALID_CONDITIONS = {"USED", "DAMAGED", "SALVAGE", "LEASE TAKEOVER", "UNKNOWN"}
VALID_TRANS      = {"AUTOMATIC", "MANUAL", "SEMI-AUTOMATIC", "OTHER", "UNKNOWN"}
PRICE_MIN,  PRICE_MAX = 1,        10_000_000
KMS_MIN,    KMS_MAX   = 0,         2_000_000
YEAR_MIN,   YEAR_MAX  = 1900,           2027
KMS_BLACKLIST = {1, 99, 123, 1234, 12345, 123456, 999999, 111111}


# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING — persistent .log file + console output
# ─────────────────────────────────────────────────────────────────────────────
def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("etl_ab_carsales")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

log = _setup_logging()


# ─────────────────────────────────────────────────────────────────────────────
#  ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def build_engine() -> Engine:
    cfg = DB_CONFIG
    try:
        if cfg.get("use_trusted"):
            url = (
                f"mssql+pyodbc://{cfg['server']}/{cfg['database']}"
                f"?driver={cfg['driver']}&trusted_connection=yes"
            )
        else:
            url = (
                f"mssql+pyodbc://{cfg['uid']}:{cfg['pwd']}"
                f"@{cfg['server']}/{cfg['database']}"
                f"?driver={cfg['driver']}"
            )
        engine = create_engine(url, connect_args={"fast_executemany": True})
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        log.info(f"Connected → {cfg['server']} / {cfg['database']}")
        return engine
    except Exception as exc:
        log.error(f"[CONNECTION ERROR] {exc}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 1 — EXTRACT
# ─────────────────────────────────────────────────────────────────────────────
def extract(csv_path: str) -> pd.DataFrame:
    try:
        log.info(f"[EXTRACT] Reading: {csv_path}")
        df = pd.read_csv(csv_path, encoding="latin-1")
        log.info(f"[EXTRACT] {len(df):,} rows × {len(df.columns)} columns loaded.")
        return df
    except FileNotFoundError:
        log.error(f"[EXTRACT] File not found: {csv_path}")
        raise
    except Exception as exc:
        log.error(f"[EXTRACT] Unexpected error: {exc}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 2 — TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────
_DOOR_SUFFIX_RE = re.compile(r",\s*(?:\d+|OTHER)\s+DOORS?$", re.IGNORECASE)

def _normalize_body_style(value) -> str:
    if pd.isna(value):
        return "UNKNOWN"
    cleaned = _DOOR_SUFFIX_RE.sub("", str(value)).strip()
    return cleaned if cleaned else "UNKNOWN"

def _clean_kilometres(value) -> int:
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0

def _hash_url(url: str) -> str:
    """SHA-256 hash of URL — PII masking."""
    return hashlib.sha256(str(url).encode("utf-8")).hexdigest()

def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("[TRANSFORM] Starting ...")
    df = df.rename(columns={
        "Listing title": "Listing_Title",
        "Price(CA$)":    "Price_CAD",
        "Link":          "Link_URL",
        "Location":      "City_Name",
        "Body Style":    "Body_Style",
    })

    # Numeric cleaning
    df["Kilometres"] = df["Kilometres"].apply(_clean_kilometres)
    df["Price_CAD"]  = pd.to_numeric(df["Price_CAD"], errors="coerce").fillna(0)

    # Year: coerce to numeric before validation to handle any string/NaN values
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").fillna(0).astype(int)

    # Date parsing
    df["Scrape_Date"] = pd.to_datetime(df["Scrape_Date"], errors="coerce").dt.date
    df["Sold_Date"]   = pd.to_datetime(df["Sold_Date"],   errors="coerce").dt.date

    # Categorical normalization
    df["Body_Style"] = df["Body_Style"].apply(_normalize_body_style)
    for col in ["Transmission", "Drivetrain", "Seats", "Colour",
                "Condition", "Status", "Trim", "Base_Model", "City_Name"]:
        df[col] = df[col].fillna("UNKNOWN").str.strip().str.upper()

    # PII masking — store hash instead of raw URL
    df["Link_URL_Hash"] = df["Link_URL"].apply(_hash_url)

    # Truncate title to DB column limit
    df["Listing_Title"] = df["Listing_Title"].str[:500]

    log.info(f"[TRANSFORM] Complete. {len(df):,} rows ready for validation.")
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 3 — VALIDATE
#  Bad rows are written to tbl_Rejected_Rows for inspection,
#  then dropped from the load. Audit of that table is handled
#  by the database trigger trg_Audit_Rejected_Rows (if added),
#  or simply tracked via the ETL log file.
# ─────────────────────────────────────────────────────────────────────────────
def validate(df: pd.DataFrame, engine: Engine, source_file: str) -> pd.DataFrame:
    """
    Validate data ranges using vectorized operations.
    Rejected rows → tbl_Rejected_Rows.
    Clean rows    → returned for loading.
    """
    log.info("[VALIDATE] Running validation ...")

    # One boolean mask per rule (True = row passes)
    price_ok  = df["Price_CAD"].between(PRICE_MIN, PRICE_MAX)
    kms_range = df["Kilometres"].between(KMS_MIN, KMS_MAX)
    kms_real  = ~df["Kilometres"].isin(KMS_BLACKLIST)
    year_ok   = df["Year"].between(YEAR_MIN, YEAR_MAX)
    status_ok = df["Status"].isin(VALID_STATUSES)
    cond_ok   = df["Condition"].isin(VALID_CONDITIONS)
    trans_ok  = df["Transmission"].isin(VALID_TRANS)

    all_ok   = price_ok & kms_range & kms_real & year_ok & status_ok & cond_ok & trans_ok
    clean_df = df[all_ok].reset_index(drop=True)

    # ── Log per-rule rejection counts ────────────────────────────────────────
    rules = {
        "Price_CAD out of range":        ~price_ok,
        "Kilometres out of range":        ~kms_range,
        "Kilometres is a placeholder":    ~kms_real,
        "Year out of range":              ~year_ok,
        "Status invalid":                 ~status_ok,
        "Condition invalid":              ~cond_ok,
        "Transmission invalid":           ~trans_ok,
    }
    for reason, bad_mask in rules.items():
        count = int(bad_mask.sum())
        if count:
            log.warning(f"[VALIDATE] {count} row(s) failed — {reason}")

    # ── Write rejected rows to DB ─────────────────────────────────────────────
    rejected_df = df[~all_ok].copy()
    if not rejected_df.empty:
        # Tag each row with its first failing reason (priority order)
        priority = [
            (~price_ok,  "Price_CAD out of range"),
            (~kms_range, "Kilometres out of range"),
            (~kms_real,  "Kilometres is a placeholder"),
            (~year_ok,   "Year out of range"),
            (~status_ok, "Status invalid"),
            (~cond_ok,   "Condition invalid"),
            (~trans_ok,  "Transmission invalid"),
        ]
        reason_series = pd.Series("Unknown", index=df.index)
        for mask, label in reversed(priority):   # reversed so highest priority wins
            reason_series[mask] = label
        rejected_df["Reject_Reason"] = reason_series[~all_ok].values

        _write_rejected(rejected_df, engine, source_file)
        log.info(f"[VALIDATE] {len(rejected_df):,} rejected row(s) written to {TABLE['rejected']}.")

    log.info(f"[VALIDATE] {len(clean_df):,} passed | {len(rejected_df):,} rejected.")
    return clean_df


def _write_rejected(df: pd.DataFrame, engine: Engine, source_file: str) -> None:
    """Insert rejected rows into tbl_Rejected_Rows as raw strings."""
    rows = [
        {
            "source": str(source_file),
            "reason": str(r.get("Reject_Reason", "Unknown")),
            "title":  str(r.get("Listing_Title", ""))[:500],
            "price":  str(r.get("Price_CAD", "")),
            "kms":    str(r.get("Kilometres", "")),
            "year":   str(r.get("Year", "")),
            "status": str(r.get("Status", "")),
            "cond":   str(r.get("Condition", "")),
            "trans":  str(r.get("Transmission", "")),
            "city":   str(r.get("City_Name", "")),
            "scrape": str(r.get("Scrape_Date", "")),
            "sold":   str(r.get("Sold_Date", "")),
        }
        for r in df.to_dict("records")
    ]
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {TABLE['rejected']}
                    (Source_File, Reject_Reason,
                     Raw_Title, Raw_Price, Raw_Kilometres, Raw_Year,
                     Raw_Status, Raw_Condition, Raw_Transmission,
                     Raw_City, Raw_Scrape_Date, Raw_Sold_Date)
                VALUES
                    (:source, :reason,
                     :title, :price, :kms, :year,
                     :status, :cond, :trans,
                     :city, :scrape, :sold)
            """),
            rows,
        )


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 4a — DIMENSION HELPERS  Idempotency / Incremental
# ─────────────────────────────────────────────────────────────────────────────
def _upsert_dimension(conn, table: str, pk_col: str, value_col: str,
                      values: list) -> dict:
    """
    Idempotent dimension upsert — safe to re-run.
    Only inserts values that don't already exist (incremental).
    Returns a {value: id} lookup dict for FK mapping.
    """
    result = conn.execute(text(f"SELECT {pk_col}, {value_col} FROM {table}"))
    lookup = {row[1]: row[0] for row in result}
    next_id = max(lookup.values(), default=0) + 1
    new_vals = [v for v in values if v not in lookup]
    if new_vals:
        rows = [{"pk": next_id + i, "val": v} for i, v in enumerate(new_vals)]
        conn.execute(
            text(f"INSERT INTO {table} ({pk_col}, {value_col}) VALUES (:pk, :val)"),
            rows,
        )
        for i, v in enumerate(new_vals):
            lookup[v] = next_id + i
        log.info(f"  {table}: +{len(new_vals)} new value(s)")
    return lookup


def _next_id(conn, table: str, pk_col: str) -> int:
    row = conn.execute(text(f"SELECT MAX({pk_col}) FROM {table}")).fetchone()
    return (row[0] or 0) + 1


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 4b — LOAD NORMALIZED TABLES Transaction Management
# ─────────────────────────────────────────────────────────────────────────────
def load_normalized(df: pd.DataFrame, engine: Engine) -> None:
    """
    Distribute validated data into normalized tables inside a single transaction.
    engine.begin() auto-COMMITs on success, auto-ROLLBACKs on any exception.
    Audit of each INSERT is handled automatically by database triggers.
    """
    with engine.begin() as conn:

        # ── Dimensions ───────────────────────────────────────────────────────
        log.info("[LOAD] Upserting dimension tables ...")
        T = TABLE
        models_lkp        = _upsert_dimension(conn, T["models"],        "Model_ID",        "Base_Model",        df["Base_Model"].unique().tolist())
        years_lkp         = _upsert_dimension(conn, T["years"],         "Year_ID",         "Year",              df["Year"].unique().tolist())
        trims_lkp         = _upsert_dimension(conn, T["trims"],         "Trim_ID",         "Trim",              df["Trim"].unique().tolist())
        locations_lkp     = _upsert_dimension(conn, T["locations"],     "Location_ID",     "City_Name",         df["City_Name"].unique().tolist())
        statuses_lkp      = _upsert_dimension(conn, T["statuses"],      "Status_ID",       "Status_Label",      df["Status"].unique().tolist())
        conditions_lkp    = _upsert_dimension(conn, T["conditions"],    "Condition_ID",    "Condition_Label",   df["Condition"].unique().tolist())
        transmissions_lkp = _upsert_dimension(conn, T["transmissions"], "Transmission_ID", "Transmission_Type", df["Transmission"].unique().tolist())
        drivetrains_lkp   = _upsert_dimension(conn, T["drivetrains"],   "Drivetrain_ID",   "Drivetrain_Type",   df["Drivetrain"].unique().tolist())
        body_styles_lkp   = _upsert_dimension(conn, T["body_styles"],   "Body_Style_ID",   "Body_Style",        df["Body_Style"].unique().tolist())
        colours_lkp       = _upsert_dimension(conn, T["colours"],       "Colour_ID",       "Colour",            df["Colour"].unique().tolist())
        seats_lkp         = _upsert_dimension(conn, T["seats"],         "Seats_ID",        "Seats_Label",       df["Seats"].unique().tolist())
        log.info("[LOAD] Dimension tables done.")

        records = df.to_dict("records")

        # ── tbl_Vehicles ─────────────────────────────────────────────────────
        log.info("[LOAD] Inserting tbl_Vehicles ...")
        base_vid = _next_id(conn, T["vehicles"], "Vehicle_ID")
        vehicle_rows = [
            {
                "vid":  base_vid + i,
                "mid":  models_lkp[r["Base_Model"]],
                "yid":  years_lkp[r["Year"]],
                "tid":  trims_lkp[r["Trim"]],
                "bsid": body_styles_lkp[r["Body_Style"]],
                "trid": transmissions_lkp[r["Transmission"]],
                "did":  drivetrains_lkp[r["Drivetrain"]],
                "cid":  colours_lkp[r["Colour"]],
                "sid":  seats_lkp[r["Seats"]],
                "title": r["Listing_Title"],
                "hash":  r["Link_URL_Hash"],   # hashed URL only
            }
            for i, r in enumerate(records)
        ]
        for start in range(0, len(vehicle_rows), CHUNK_SIZE):
            conn.execute(
                text(f"""
                    INSERT INTO {T['vehicles']}
                        (Vehicle_ID, Model_ID, Year_ID, Trim_ID, Body_Style_ID,
                         Transmission_ID, Drivetrain_ID, Colour_ID, Seats_ID,
                         Listing_Title, Link_URL_Hash)
                    VALUES (:vid, :mid, :yid, :tid, :bsid,
                            :trid, :did, :cid, :sid, :title, :hash)
                """),
                vehicle_rows[start: start + CHUNK_SIZE],
            )
        log.info(f"[LOAD] tbl_Vehicles: {len(vehicle_rows):,} row(s) inserted.")

        # ── tbl_Listings ─────────────────────────────────────────────────────
        log.info("[LOAD] Inserting tbl_Listings ...")
        base_lid = _next_id(conn, T["listings"], "Listing_ID")
        listing_rows = [
            {
                "lid":   base_lid + i,
                "vid":   base_vid + i,
                "locid": locations_lkp[r["City_Name"]],
                "price": float(r["Price_CAD"]),
                "kms":   int(r["Kilometres"]),
                "conid": conditions_lkp[r["Condition"]],
            }
            for i, r in enumerate(records)
        ]
        for start in range(0, len(listing_rows), CHUNK_SIZE):
            conn.execute(
                text(f"""
                    INSERT INTO {T['listings']}
                        (Listing_ID, Vehicle_ID, Location_ID,
                         Price_CAD, Kilometres, Condition_ID)
                    VALUES (:lid, :vid, :locid, :price, :kms, :conid)
                """),
                listing_rows[start: start + CHUNK_SIZE],
            )
        log.info(f"[LOAD] tbl_Listings: {len(listing_rows):,} row(s) inserted.")

        # ── tbl_Listing_Status ────────────────────────────────────────────────
        log.info("[LOAD] Inserting tbl_Listing_Status ...")
        base_srid = _next_id(conn, T["status_log"], "Status_Record_ID")
        status_rows = [
            {
                "srid":      base_srid + i,
                "lid":       base_lid + i,
                "stid":      statuses_lkp[r["Status"]],
                "scrape_dt": r["Scrape_Date"],
                "sold_dt":   r["Sold_Date"] if pd.notna(r["Sold_Date"]) else None,
            }
            for i, r in enumerate(records)
        ]
        for start in range(0, len(status_rows), CHUNK_SIZE):
            conn.execute(
                text(f"""
                    INSERT INTO {T['status_log']}
                        (Status_Record_ID, Listing_ID, Status_ID,
                         Scrape_Date, Sold_Date)
                    VALUES (:srid, :lid, :stid, :scrape_dt, :sold_dt)
                """),
                status_rows[start: start + CHUNK_SIZE],
            )
        log.info(f"[LOAD] tbl_Listing_Status: {len(status_rows):,} row(s) inserted.")

    # engine.begin() auto-COMMITs here
    log.info("[LOAD] Transaction committed successfully.")


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT  Single-click execution
# ─────────────────────────────────────────────────────────────────────────────
def run_etl(csv_path: str = CSV_PATH) -> None:
    run_start = datetime.datetime.now()
    log.info("=" * 65)
    log.info(f"ETL START  {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 65)

    engine = None
    try:
        engine   = build_engine()
        df_raw   = extract(csv_path)
        df_clean = transform(df_raw)

        # validate() now needs engine to write rejected rows,
        # and source_file for traceability in tbl_Rejected_Rows
        df_valid = validate(df_clean, engine, source_file=csv_path)

        if df_valid.empty:
            log.warning("[ETL] All rows failed validation. Nothing loaded.")
            return

        # Staging table exists in DB for architecture demonstration
        # but is not written to during ETL — CSV is the source of truth.
        # load_staging(df_valid, engine)   ← intentionally skipped
        # truncate_staging(engine)         ← intentionally skipped

        load_normalized(df_valid, engine)    # Transactional load

        elapsed = (datetime.datetime.now() - run_start).total_seconds()
        log.info("=" * 65)
        log.info(f"ETL SUCCESS | {len(df_valid):,} rows loaded | {elapsed:.1f}s elapsed")
        log.info("=" * 65)

    except SQLAlchemyError as exc:
        log.error(f"[ETL FAILED] DB error — transaction rolled back: {exc}")
        raise
    except Exception as exc:
        log.error(f"[ETL FAILED] {exc}")
        raise
    finally:
        if engine:
            engine.dispose()
            log.info("Engine disposed.")

# ─────────────────────────────────────────────────────────────────────────────
#  LOAD DISTANCE API 
# ─────────────────────────────────────────────────────────────────────────────

ORS_API_KEY   = os.getenv("ORS_API_KEY")
ORS_URL       = "https://api.openrouteservice.org/v2/directions/driving-car"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"

# ── City name aliases ─────────────────────────────────────────────────────────
#  Some Kijiji location labels are not directly geocodable — map them to the
#  nearest real city name before sending to the geocoding API.
_CITY_ALIASES = {
    "STRATHCONA COUNTY":      "Sherwood Park",
    "ROCKY VIEW COUNTY":      "Airdrie",
    "FOOTHILLS COUNTY":       "Okotoks",
    "TSUU T'INA":             "Calgary",
    "BANFF / CANMORE":        "Canmore",
    "SOUTH LETHBRIDGE":       "Lethbridge",
    "ALBERTA":                "Edmonton",
    "OTHER":                  "Red Deer",
    "LANGDON":                "Chestermere",
    "ACADIA":                 "Calgary",
    "SWEET GRASS":            "Edmonton",
    "FOOTHILLS":              "Calgary",
    "NORTHEAST CALGARY":      "Calgary",
    "NORTHWEST CALGARY":      "Calgary",
    "SOUTHEAST CALGARY":      "Calgary",
    "SOUTHWEST CALGARY":      "Calgary",
    "NORTH CENTRAL EDMONTON": "Edmonton",
    "SOUTHEAST EDMONTON":     "Edmonton",
    "NORTHWEST EDMONTON":     "Edmonton",
    "WEST EDMONTON":          "Edmonton",
    "NORTHEAST EDMONTON":     "Edmonton",
    "SOUTHWEST EDMONTON":     "Edmonton",
}
@lru_cache(maxsize=None)
def get_lat_lon(city_name: str):
    """Return (longitude, latitude) for a city name, preferring Alberta results."""
    search_query = _CITY_ALIASES.get(city_name.upper().strip(), city_name.strip())
    try:
        res = requests.get(
            GEOCODING_URL,
            params={"name": search_query, "count": 20},
            timeout=15,
        ).json()
        if "results" in res:
            for place in res["results"]:
                if place.get("admin1") == "Alberta":
                    return place["longitude"], place["latitude"]
            for place in res["results"]:
                if place.get("country") == "Canada":
                    return place["longitude"], place["latitude"]
            return res["results"][0]["longitude"], res["results"][0]["latitude"]
    except Exception as e:
        log.warning(f"[GEOCODING ERROR] {search_query}: {e}")
    log.warning(f"[GEOCODING] Could not find: {search_query}")
    return None, None


def calculate_road_distance(lon1: float, lat1: float, lon2: float, lat2: float):
    if not ORS_API_KEY:
        log.error("[ORS] ORS_API_KEY not set — cannot calculate distance.")
        return None
    if (lon1, lat1) == (lon2, lat2):
        return 0.0
    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type":  "application/json",
    }
    body = {"coordinates": [[lon1, lat1], [lon2, lat2]]}
    try:
        res = requests.post(ORS_URL, json=body, headers=headers, timeout=15).json()
        if "routes" in res:
            return round(res["routes"][0]["summary"]["distance"] / 1000, 2)
        log.warning(f"[ORS] Unexpected response: {res.get('error', res)}")
    except Exception as e:
        log.warning(f"[ORS ROUTING ERROR] {e}")
    return None


def road_distance_pipeline():
    """
    Fetches driving distances (via OpenRouteService) for every tbl_Locations row
    that is still missing Distance_from_Edmonton_KM or Distance_from_Calgary_KM,
    then writes the values back to the DB.

    Run this once after the main ETL load — or any time new cities appear.
    ORS free tier limit: 40 requests/min → 1.5 s sleep keeps us safely under.
    """
    if not ORS_API_KEY:
        log.error("[DISTANCE] ORS_API_KEY not found in .env — skipping distance pipeline.")
        return

    edm_lon, edm_lat = get_lat_lon("Edmonton")
    cal_lon, cal_lat = get_lat_lon("Calgary")
    if not all([edm_lon, edm_lat, cal_lon, cal_lat]):
        log.error("[DISTANCE] Could not geocode Edmonton or Calgary — aborting.")
        return

    engine = build_engine()
    try:
        with engine.begin() as conn:
            targets = conn.execute(text("""
                SELECT Location_ID, City_Name
                FROM tbl_Locations
                WHERE Distance_from_Edmonton_KM IS NULL
                   OR Distance_from_Calgary_KM  IS NULL
            """)).fetchall()

            total = len(targets)
            log.info(f"[DISTANCE] {total} location(s) need distance calculation.")

            for count, (loc_id, city_name) in enumerate(targets, 1):
                tgt_lon, tgt_lat = get_lat_lon(city_name)
                if tgt_lon is None:
                    log.warning(f"[DISTANCE] ({count}/{total}) {city_name} — could not geocode, skipped.")
                    continue

                dist_edm = calculate_road_distance(edm_lon, edm_lat, tgt_lon, tgt_lat)
                dist_cal = calculate_road_distance(cal_lon, cal_lat, tgt_lon, tgt_lat)

                if dist_edm is not None and dist_cal is not None:
                    conn.execute(
                        text("""
                            UPDATE tbl_Locations
                            SET Distance_from_Edmonton_KM = :edm,
                                Distance_from_Calgary_KM  = :cal
                            WHERE Location_ID = :id
                        """),
                        {"edm": dist_edm, "cal": dist_cal, "id": loc_id},
                    )
                    log.info(f"[DISTANCE] ({count}/{total}) {city_name} → YEG: {dist_edm} km | YYC: {dist_cal} km")
                else:
                    log.warning(f"[DISTANCE] ({count}/{total}) {city_name} — routing failed, skipped.")

                time.sleep(1.5)   # ORS free tier: 40 req/min

        log.info("[DISTANCE] All distances updated and committed.")
    except Exception as e:
        log.error(f"[DISTANCE] Database error: {e}")
    finally:
        engine.dispose()
if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else CSV_PATH
    run_etl(csv_file)
    road_distance_pipeline()