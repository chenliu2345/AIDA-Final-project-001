"""
================================================================================
 ETL Script — AB_CarSale_DB  |  AIDA 1145 Phase 2
================================================================================
 Table names match the CURRENT SQL schema (no tbl_ prefix yet).
 When your teammate updates the SQL naming convention to tbl_, update the
 TABLE_NAMES dict at the top of this file to match.

 Requires: pip install sqlalchemy pyodbc pandas
================================================================================
"""

import re
import sys
import hashlib
import logging
import datetime
import pandas as pd
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  ← edit before running
# ─────────────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "server":       ".",           # e.g. "localhost\\SQLEXPRESS"
    "database":     "AB_CarSale_DB",
    "driver":       "ODBC+Driver+17+for+SQL+Server",
    "use_trusted":  True,
}
# CSV PATH
CSV_PATH   = r"C:\Users\chenl\GitHub\AIDA-Final-project-001\Data_Raw\Optimized_Alberta_owner_sales_car_clean.csv"
LOG_FILE   = "etl_run.log"     # persistent log file written next to this script
CHUNK_SIZE = 500               # rows per bulk insert batch

# ── Table name map — update here if teammate renames tables to tbl_ prefix ──
TABLE = {
    "models":        "Models",
    "years":         "Years",
    "trims":         "Trims",
    "locations":     "Locations",
    "statuses":      "Statuses",
    "conditions":    "Conditions",
    "transmissions": "Transmissions",
    "drivetrains":   "Drivetrains",
    "body_styles":   "Body_Styles",
    "colours":       "Colours",
    "seats":         "Seats",
    "vehicles":      "Vehicles_table",
    "listings":      "Listings_table",
    "status_log":    "Listing_Status_table",
}

# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION RULES  (Python validation layer — Req 9)
# ─────────────────────────────────────────────────────────────────────────────
VALID_STATUSES   = {"SOLD", "ACTIVE", "ACTIVE_REPOST", "RESHELVED"}
VALID_CONDITIONS = {"USED", "DAMAGED", "SALVAGE", "LEASE TAKEOVER", "UNKNOWN"}
VALID_TRANS      = {"AUTOMATIC", "MANUAL", "SEMI-AUTOMATIC", "OTHER", "UNKNOWN"}
PRICE_MIN,  PRICE_MAX = 1,   10_000_000
KMS_MIN,    KMS_MAX   = 0,    2_000_000
YEAR_MIN,   YEAR_MAX  = 1900,      2027  # 2028+ rejected as clearly invalid
KMS_BLACKLIST = {1, 99, 123, 1234, 12345, 123456, 999999, 111111}  # placeholder values


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING  (persistent .log file — Req 7)
# ─────────────────────────────────────────────────────────────────────────────
def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("etl_ab_carsales")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
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
# ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def build_engine() -> Engine:
    cfg = DB_CONFIG
    try:
        if cfg.get("use_trusted"):
            url = (f"mssql+pyodbc://{cfg['server']}/{cfg['database']}"
                   f"?driver={cfg['driver']}&trusted_connection=yes")
        else:
            url = (f"mssql+pyodbc://{cfg['uid']}:{cfg['pwd']}"
                   f"@{cfg['server']}/{cfg['database']}"
                   f"?driver={cfg['driver']}")
        engine = create_engine(url, connect_args={"fast_executemany": True})
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        log.info(f"Connected → {cfg['server']} / {cfg['database']}")
        return engine
    except Exception as exc:
        log.error(f"[CONNECTION ERROR] {exc}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — EXTRACT
# ─────────────────────────────────────────────────────────────────────────────
def extract(csv_path: str) -> pd.DataFrame:
    try:
        log.info(f"[EXTRACT] Reading: {csv_path}")
        df = pd.read_csv(csv_path, encoding="latin-1")
        log.info(f"[EXTRACT] {len(df):,} rows x {len(df.columns)} columns loaded.")
        return df
    except FileNotFoundError:
        log.error(f"[EXTRACT] File not found: {csv_path}")
        raise
    except Exception as exc:
        log.error(f"[EXTRACT] Unexpected error: {exc}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — TRANSFORM
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
    """SHA-256 hash of URL for PII masking (stored in DB instead of raw URL)."""
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
    df["Kilometres"]  = df["Kilometres"].apply(_clean_kilometres)
    df["Scrape_Date"] = pd.to_datetime(df["Scrape_Date"], errors="coerce").dt.date
    df["Sold_Date"]   = pd.to_datetime(df["Sold_Date"],   errors="coerce").dt.date
    df["Body_Style"]  = df["Body_Style"].apply(_normalize_body_style)
    df["Link_Hash"]   = df["Link_URL"].apply(_hash_url)   # PII masking
    for col in ["Transmission", "Drivetrain", "Seats", "Colour",
                "Condition", "Status", "Trim", "Base_Model"]:
        df[col] = df[col].str.strip().str.upper().fillna("UNKNOWN")
    df["Listing_Title"] = df["Listing_Title"].str[:500]
    before = len(df)
    df = df[df["Price_CAD"] > 0].copy().reset_index(drop=True)
    log.info(f"[TRANSFORM] Dropped {before - len(df)} rows (Price <= 0). "
             f"{len(df):,} rows remain.")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3a — PYTHON VALIDATION LAYER  (Req 9)
# ─────────────────────────────────────────────────────────────────────────────
def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data ranges in Python before SQL INSERT. Log and drop bad rows."""
    log.info("[VALIDATE] Running validation ...")
    clean_idx = []

    for i, r in enumerate(df.to_dict("records")):
        errors = []
        if not (PRICE_MIN <= float(r["Price_CAD"]) <= PRICE_MAX):
            errors.append(f"Price_CAD={r['Price_CAD']} out of range")
        kms = int(r["Kilometres"])
        if not (KMS_MIN <= kms <= KMS_MAX):
            errors.append(f"Kilometres={kms} out of range")
        if kms in KMS_BLACKLIST:
            errors.append(f"Kilometres={kms} is a placeholder value")
        if not (YEAR_MIN <= int(r["Year"]) <= YEAR_MAX):
            errors.append(f"Year={r['Year']} out of range (2028+ rejected)")
        if r["Status"] not in VALID_STATUSES:
            errors.append(f"Status='{r['Status']}' invalid")
        if r["Condition"] not in VALID_CONDITIONS:
            errors.append(f"Condition='{r['Condition']}' invalid")
        if r["Transmission"] not in VALID_TRANS:
            errors.append(f"Transmission='{r['Transmission']}' invalid")

        if errors:
            log.warning(f"[VALIDATE] Row {i} REJECTED — {'; '.join(errors)}")
        else:
            clean_idx.append(i)

    clean_df = df.iloc[clean_idx].reset_index(drop=True)
    rejected = len(df) - len(clean_df)
    log.info(f"[VALIDATE] {len(clean_df):,} passed | {rejected} rejected.")
    return clean_df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3b — LOAD HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _upsert_dimension(conn, table: str, pk_col: str, value_col: str,
                      values: list) -> dict:
    """Idempotent dimension upsert — safe to re-run (Req 8)."""
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
        log.info(f"  {table}: +{len(new_vals)} new row(s)")
    return lookup

def _upsert_years(conn, years: list) -> dict:
    t = TABLE["years"]
    result = conn.execute(text(f"SELECT Year_ID, Year FROM {t}"))
    lookup = {row[1]: row[0] for row in result}
    next_id = max(lookup.values(), default=0) + 1
    new_yrs = [y for y in years if y not in lookup]
    if new_yrs:
        rows = [{"pk": next_id + i, "val": y} for i, y in enumerate(new_yrs)]
        conn.execute(text(f"INSERT INTO {t} (Year_ID, Year) VALUES (:pk, :val)"), rows)
        for i, y in enumerate(new_yrs):
            lookup[y] = next_id + i
        log.info(f"  {t}: +{len(new_yrs)} new row(s)")
    return lookup

def _next_id(conn, table: str, pk_col: str) -> int:
    row = conn.execute(text(f"SELECT MAX({pk_col}) FROM {table}")).fetchone()
    return (row[0] or 0) + 1


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3c — LOAD  (Req 11 Transaction, Req 16 Bulk Insert)
# ─────────────────────────────────────────────────────────────────────────────
def load(df: pd.DataFrame, engine: Engine) -> None:
    """
    Load all dimension + fact tables in a single transaction.
    engine.begin() auto-COMMITs on success, auto-ROLLBACKs on any exception.
    """
    with engine.begin() as conn:

        # ── Dimensions ───────────────────────────────────────────────────────
        log.info("[LOAD] Upserting dimension tables ...")
        T = TABLE
        models_lkp        = _upsert_dimension(conn, T["models"],        "Model_ID",        "Base_Model",        df["Base_Model"].unique().tolist())
        years_lkp         = _upsert_years(conn,                                                                  df["Year"].unique().tolist())
        trims_lkp         = _upsert_dimension(conn, T["trims"],          "Trim_ID",         "Trim",              df["Trim"].unique().tolist())
        locations_lkp     = _upsert_dimension(conn, T["locations"],      "Location_ID",     "City_Name",         df["City_Name"].unique().tolist())
        statuses_lkp      = _upsert_dimension(conn, T["statuses"],       "Status_ID",       "Status_Label",      df["Status"].unique().tolist())
        conditions_lkp    = _upsert_dimension(conn, T["conditions"],     "Condition_ID",    "Condition_Label",   df["Condition"].unique().tolist())
        transmissions_lkp = _upsert_dimension(conn, T["transmissions"],  "Transmission_ID", "Transmission_Type", df["Transmission"].unique().tolist())
        drivetrains_lkp   = _upsert_dimension(conn, T["drivetrains"],    "Drivetrain_ID",   "Drivetrain_Type",   df["Drivetrain"].unique().tolist())
        body_styles_lkp   = _upsert_dimension(conn, T["body_styles"],    "Body_Style_ID",   "Body_Style",        df["Body_Style"].unique().tolist())
        colours_lkp       = _upsert_dimension(conn, T["colours"],        "Colour_ID",       "Colour",            df["Colour"].unique().tolist())
        seats_lkp         = _upsert_dimension(conn, T["seats"],          "Seats_ID",        "Seats_Label",       df["Seats"].unique().tolist())
        log.info("[LOAD] Dimension tables done.")

        records  = df.to_dict("records")

        # ── Vehicles_table ───────────────────────────────────────────────────
        log.info("[LOAD] Inserting Vehicles_table ...")
        base_vid = _next_id(conn, T["vehicles"], "Vehicle_ID")
        vehicle_rows = [
            {
                "vid":   base_vid + i,
                "mid":   models_lkp[r["Base_Model"]],
                "yid":   years_lkp[r["Year"]],
                "tid":   trims_lkp[r["Trim"]],
                "bsid":  body_styles_lkp[r["Body_Style"]],
                "trid":  transmissions_lkp[r["Transmission"]],
                "did":   drivetrains_lkp[r["Drivetrain"]],
                "cid":   colours_lkp[r["Colour"]],
                "sid":   seats_lkp[r["Seats"]],
                "title": r["Listing_Title"],
                "url":   r["Link_URL"],         # NOTE: swap to r["Link_Hash"] once
                                                # teammate adds Link_URL_Hash column
            }
            for i, r in enumerate(records)
        ]
        for start in range(0, len(vehicle_rows), CHUNK_SIZE):
            conn.execute(
                text(f"""
                    INSERT INTO {T['vehicles']}
                        (Vehicle_ID, Model_ID, Year_ID, Trim_ID, Body_Style_ID,
                         Transmission_ID, Drivetrain_ID, Colour_ID, Seats_ID,
                         Listing_Title, Link_URL)
                    VALUES (:vid, :mid, :yid, :tid, :bsid,
                            :trid, :did, :cid, :sid, :title, :url)
                """),
                vehicle_rows[start: start + CHUNK_SIZE],
            )
        log.info(f"[LOAD] Vehicles_table: {len(vehicle_rows):,} row(s) inserted.")

        # ── Listings_table ───────────────────────────────────────────────────
        log.info("[LOAD] Inserting Listings_table ...")
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
        log.info(f"[LOAD] Listings_table: {len(listing_rows):,} row(s) inserted.")

        # ── Listing_Status_table ─────────────────────────────────────────────
        log.info("[LOAD] Inserting Listing_Status_table ...")
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
        log.info(f"[LOAD] Listing_Status_table: {len(status_rows):,} row(s) inserted.")

    # engine.begin() auto-COMMITs here (Req 11)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
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
        df_valid = validate(df_clean)

        if df_valid.empty:
            log.warning("[ETL] All rows failed validation. Nothing loaded.")
            return

        load(df_valid, engine)

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


if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else CSV_PATH
    run_etl(csv_file)
