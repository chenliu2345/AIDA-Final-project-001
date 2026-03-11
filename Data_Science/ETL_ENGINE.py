import re
import sys
import hashlib
import logging
import datetime
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION  ← edit before running
# ─────────────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "server":      ".",                       # e.g. "localhost\\SQLEXPRESS"
    "database":    "AB_CarSale_DB",
    "driver":      "ODBC+Driver+17+for+SQL+Server",
    "use_trusted": True,                      # set False and add uid/pwd for SQL auth
}

BASE_DIR = Path(__file__).parent.parent
CSV_PATH = BASE_DIR / "Data_Raw" / "Optimized_Alberta_owner_sales_car_clean.csv"
LOG_FILE   = "etl_run.log"
CHUNK_SIZE = 1000          # rows per bulk-insert batch (raised from 500)

# ── Table name map (matches tbl_ naming convention in SQL schema) ─────────────
TABLE = {
    "stg":           "tbl_stg_Raw",
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
}

# ─────────────────────────────────────────────────────────────────────────────
#  VALIDATION RULES  [Req 9]
# ─────────────────────────────────────────────────────────────────────────────
VALID_STATUSES   = {"SOLD", "ACTIVE", "ACTIVE_REPOST", "RESHELVED"}
VALID_CONDITIONS = {"USED", "DAMAGED", "SALVAGE", "LEASE TAKEOVER", "UNKNOWN"}
VALID_TRANS      = {"AUTOMATIC", "MANUAL", "SEMI-AUTOMATIC", "OTHER", "UNKNOWN"}
PRICE_MIN,  PRICE_MAX = 1,           10_000_000
KMS_MIN,    KMS_MAX   = 0,          2_000_000
YEAR_MIN,   YEAR_MAX  = 1900,            2027
KMS_BLACKLIST = {1, 99, 123, 1234, 12345, 123456, 999999, 111111}


# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING  [Req 7] — persistent .log file + console output
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
        engine = create_engine(url, connect_args={"fast_executemany": True})  # [Req 16]
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
    """SHA-256 hash of URL — PII masking [Req 15]."""
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
    df["Kilometres"]  = df["Kilometres"].apply(_clean_kilometres)
    df["Price_CAD"]   = pd.to_numeric(df["Price_CAD"], errors="coerce").fillna(0)

    # Date parsing
    df["Scrape_Date"] = pd.to_datetime(df["Scrape_Date"], errors="coerce").dt.date
    df["Sold_Date"]   = pd.to_datetime(df["Sold_Date"],   errors="coerce").dt.date

    # Categorical normalization
    df["Body_Style"]  = df["Body_Style"].apply(_normalize_body_style)
    for col in ["Transmission", "Drivetrain", "Seats", "Colour",
                "Condition", "Status", "Trim", "Base_Model", "City_Name"]:
        df[col] = df[col].str.strip().str.upper().fillna("UNKNOWN")

    # PII masking [Req 15] — store hash instead of raw URL
    df["Link_URL_Hash"] = df["Link_URL"].apply(_hash_url)

    # Truncate title to DB column limit
    df["Listing_Title"] = df["Listing_Title"].str[:500]

    log.info(f"[TRANSFORM] Complete. {len(df):,} rows ready for validation.")
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 3 — VALIDATE 
# ─────────────────────────────────────────────────────────────────────────────
def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data ranges using vectorized operations. Log and drop bad rows."""
    log.info("[VALIDATE] Running validation ...")

    # One boolean mask per rule (True = row passes)
    price_ok  = df["Price_CAD"].between(PRICE_MIN, PRICE_MAX)
    kms_range = df["Kilometres"].between(KMS_MIN, KMS_MAX)
    kms_real  = ~df["Kilometres"].isin(KMS_BLACKLIST)
    year_ok   = df["Year"].between(YEAR_MIN, YEAR_MAX)
    status_ok = df["Status"].isin(VALID_STATUSES)
    cond_ok   = df["Condition"].isin(VALID_CONDITIONS)
    trans_ok  = df["Transmission"].isin(VALID_TRANS)

    # Log per-rule rejection counts
    rules = {
        "Price_CAD out of range":           ~price_ok,
        "Kilometres out of range":          ~kms_range,
        "Kilometres is a placeholder":      ~kms_real,
        "Year out of range (2028+ reject)": ~year_ok,
        "Status invalid":                   ~status_ok,
        "Condition invalid":                ~cond_ok,
        "Transmission invalid":             ~trans_ok,
    }
    for reason, bad_mask in rules.items():
        count = int(bad_mask.sum())
        if count:
            log.warning(f"[VALIDATE] {count} row(s) failed — {reason}")

    for _, row in df[df["Price_CAD"] < 500].iterrows():
        log.info(f"[AFFORDABLE VEHICLE DETECTED] 价格: ${row['Price_CAD']:.2f} CAD | 车型: {row['Base_Model']} | 城市: {row['City_Name']}")

    all_ok   = price_ok & kms_range & kms_real & year_ok & status_ok & cond_ok & trans_ok
    clean_df = df[all_ok].reset_index(drop=True)
    rejected = len(df) - len(clean_df)
    log.info(f"[VALIDATE] {len(clean_df):,} passed | {rejected} rejected.")
    return clean_df


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 4a — LOAD STAGING  
#  Raw validated data → tbl_stg_Raw first
# ─────────────────────────────────────────────────────────────────────────────
def load_staging(df: pd.DataFrame, engine: Engine) -> None:
    """
    Insert validated rows into tbl_stg_Raw using chunked bulk insert.
    Uses the same fast_executemany approach as normalized tables [Req 16].
    Load_Timestamp is filled automatically by DEFAULT GETDATE() in SQL.
    """
    log.info("[STAGING] Loading into tbl_stg_Raw ...")

    stg_rows = [
        {
            "title":  r["Listing_Title"],
            "price":  float(r["Price_CAD"]),
            "hash":   r["Link_URL_Hash"],
            "city":   r["City_Name"],
            "scrape": r["Scrape_Date"],
            "status": r["Status"],
            "sold":   r["Sold_Date"] if pd.notna(r["Sold_Date"]) else None,
            "cond":   r["Condition"],
            "kms":    int(r["Kilometres"]),
            "trans":  r["Transmission"],
            "drive":  r["Drivetrain"],
            "seats":  r["Seats"],
            "body":   r["Body_Style"],
            "colour": r["Colour"],
            "year":   int(r["Year"]),
            "trim":   r["Trim"],
            "model":  r["Base_Model"],
        }
        for r in df.to_dict("records")
    ]

    with engine.begin() as conn:
        for start in range(0, len(stg_rows), CHUNK_SIZE):
            conn.execute(
                text(f"""
                    INSERT INTO {TABLE['stg']}
                        (Listing_Title, Price_CAD, Link_URL_Hash, City_Name,
                         Scrape_Date, Status, Sold_Date, Condition_Label,
                         Kilometres, Transmission, Drivetrain, Seats,
                         Body_Style, Colour, Year, Trim, Base_Model)
                    VALUES
                        (:title, :price, :hash, :city,
                         :scrape, :status, :sold, :cond,
                         :kms, :trans, :drive, :seats,
                         :body, :colour, :year, :trim, :model)
                """),
                stg_rows[start: start + CHUNK_SIZE],
            )
            log.debug(f"[STAGING] Inserted rows {start}–{min(start+CHUNK_SIZE, len(stg_rows))}")

    log.info(f"[STAGING] {len(stg_rows):,} rows inserted into {TABLE['stg']}.")


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 4b — DIMENSION HELPERS  Idempotency / Incremental
# ─────────────────────────────────────────────────────────────────────────────
def _upsert_dimension(conn, table: str, pk_col: str, value_col: str,
                      values: list) -> dict:
    """
    Idempotent dimension upsert — safe to re-run [Req 8].
    Only inserts values that don't already exist (incremental) [Req 13].
    """
    result  = conn.execute(text(f"SELECT {pk_col}, {value_col} FROM {table}"))
    lookup  = {row[1]: row[0] for row in result}
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



def _next_id(conn, table: str, pk_col: str) -> int:
    row = conn.execute(text(f"SELECT MAX({pk_col}) FROM {table}")).fetchone()
    return (row[0] or 0) + 1


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 4c — LOAD NORMALIZED TABLES  [Req 11] Transaction Management
# ─────────────────────────────────────────────────────────────────────────────
def load_normalized(df: pd.DataFrame, engine: Engine) -> None:
    """
    Distribute staging data into normalized tables inside a single transaction.
    engine.begin() auto-COMMITs on success, auto-ROLLBACKs on any exception [Req 11].
    """
    with engine.begin() as conn:

        # ── Dimensions ───────────────────────────────────────────────────────
        log.info("[LOAD] Upserting dimension tables ...")
        T = TABLE
        models_lkp        = _upsert_dimension(conn, T["models"],        "Model_ID",        "Base_Model",        df["Base_Model"].unique().tolist())
        years_lkp         = _upsert_dimension(conn, TABLE["years"],        "Year_ID",         "Year",              df["Year"].unique().tolist())
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

        records   = df.to_dict("records")

        # ── tbl_Vehicles ─────────────────────────────────────────────────────
        log.info("[LOAD] Inserting tbl_Vehicles ...")
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
                "hash":  r["Link_URL_Hash"],   # [Req 15] hashed URL only
            }
            for i, r in enumerate(records)
        ]
        for start in range(0, len(vehicle_rows), CHUNK_SIZE):   # [Req 16]
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

    # engine.begin() auto-COMMITs here [Req 11]
    log.info("[LOAD] Transaction committed successfully.")


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 4d — TRUNCATE STAGING  
#  Clean up staging table after successful normalized load
# ─────────────────────────────────────────────────────────────────────────────
def truncate_staging(engine: Engine) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {TABLE['stg']}"))
        log.info(f"[STAGING] {TABLE['stg']} truncated after successful load.")
    except Exception as exc:
        log.warning(f"[STAGING] Could not truncate staging table: {exc}")


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
        engine     = build_engine()
        df_raw     = extract(csv_path)
        df_clean   = transform(df_raw)
        df_valid   = validate(df_clean)

        if df_valid.empty:
            log.warning("[ETL] All rows failed validation. Nothing loaded.")
            return

        load_staging(df_valid, engine)       # [Req 12] Stage first
        load_normalized(df_valid, engine)    # [Req 11] Transactional load
        truncate_staging(engine)             # [Req 12] Clean up staging

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