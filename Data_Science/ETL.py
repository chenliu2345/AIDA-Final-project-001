import pandas as pd
import pyodbc
import logging
import os

# 1. System Logging
logging.basicConfig(
    filename='etl_fortress.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_db_connection():
    """Establishes connection to local SQL instance."""
    try:
        conn_str = (
            'DRIVER={SQL Server};'
            'SERVER=.;'
            'DATABASE=AB_CarSale_DB;'
            'Trusted_Connection=yes;'
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise RuntimeError(f"Database connection failed: {e}")

def transform_data(df):
    """Cleans data and handles privacy masking."""
    if 'Link' in df.columns:
        df = df.drop(columns=['Link'])

    df['Kilometres'] = (
        df['Kilometres']
        .astype(str)
        .str.replace(',', '')
        .str.replace(' ', '')
    )
    df['Kilometres'] = pd.to_numeric(df['Kilometres'], errors='coerce').fillna(0).astype(int)
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce').fillna(0).astype(int)
    df = df[(df['Price(CA$)'] >= 500) & (df['Year'] <= 2026)]
    df = df.fillna('UNKNOWN')
    df = df.reset_index(drop=True)
    return df

def populate_lookup(cursor, table_name, col_name, id_col, unique_vals):
    """Maps values to IDs for dimension table normalization."""
    mapping = {}
    for i, val in enumerate(unique_vals, start=1):
        native_val = val.item() if hasattr(val, 'item') else val
        try:
            cursor.execute(
                f"INSERT INTO {table_name} ({id_col}, {col_name}) VALUES (?, ?)",
                (i, native_val)
            )
            mapping[native_val] = i
        except Exception as e:
            logging.error(f"INSERT FAILED [{table_name}] val={native_val} error={e}")
            raise RuntimeError(f"INSERT FAILED [{table_name}] val={native_val} error={e}")
    return mapping

def run_etl_pipeline(csv_path):
    """Main orchestrator for data migration."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        df = pd.read_csv(csv_path)
        df = transform_data(df)

        # Dimension Table Population
        m_map  = populate_lookup(cursor, "Models",        "Base_Model",        "Model_ID",        df['Base_Model'].unique())
        y_map  = populate_lookup(cursor, "Years",         "Year",              "Year_ID",         df['Year'].astype(int).unique())
        t_map  = populate_lookup(cursor, "Trims",         "Trim",              "Trim_ID",         df['Trim'].unique())
        l_map  = populate_lookup(cursor, "Locations",     "City_Name",         "Location_ID",     df['Location'].unique())
        s_map  = populate_lookup(cursor, "Statuses",      "Status_Label",      "Status_ID",       df['Status'].unique())
        c_map  = populate_lookup(cursor, "Conditions",    "Condition_Label",   "Condition_ID",    df['Condition'].unique())
        tr_map = populate_lookup(cursor, "Transmissions", "Transmission_Type", "Transmission_ID", df['Transmission'].unique())
        d_map  = populate_lookup(cursor, "Drivetrains",   "Drivetrain_Type",   "Drivetrain_ID",   df['Drivetrain'].unique())
        b_map  = populate_lookup(cursor, "Body_Styles",   "Body_Style",        "Body_Style_ID",   df['Body Style'].unique())
        co_map = populate_lookup(cursor, "Colours",       "Colour",            "Colour_ID",       df['Colour'].unique())
        st_map = populate_lookup(cursor, "Seats",         "Seats_Label",       "Seats_ID",        df['Seats'].unique())

        # Build both payloads in a single pass
        vehicle_payload = []
        listing_payload = []

        for idx, row in df.iterrows():
            vehicle_payload.append((
                idx,
                m_map[row['Base_Model']],
                y_map[int(row['Year'])],
                t_map[row['Trim']],
                b_map[row['Body Style']],
                tr_map[row['Transmission']],
                d_map[row['Drivetrain']],
                co_map[row['Colour']],
                st_map[row['Seats']],
                row['Listing title'],
                'REDACTED'
            ))
            listing_payload.append((
                idx, idx,
                l_map[row['Location']],
                row['Price(CA$)'],
                row['Kilometres'],
                c_map[row['Condition']]
            ))

        # Fact Table 1: Vehicles
        veh_sql = """
            INSERT INTO Vehicles_table (
                Vehicle_ID, Model_ID, Year_ID, Trim_ID, Body_Style_ID,
                Transmission_ID, Drivetrain_ID, Colour_ID, Seats_ID,
                Listing_Title, Link_URL
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(veh_sql, vehicle_payload)

        # Fact Table 2: Listings
        list_sql = """
            INSERT INTO Listings_table (
                Listing_ID, Vehicle_ID, Location_ID,
                Price_CAD, Kilometres, Condition_ID
            ) VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(list_sql, listing_payload)

        conn.commit()
        print(f"Success: {len(df)} records migrated.")
        logging.info(f"ETL complete: {len(df)} records migrated.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Pipeline failed, transaction rolled back: {e}")
        print(f"Pipeline Failed: {type(e).__name__} - {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    TARGET_FILE = r"C:\Users\chenl\GitHub\AIDA-Final-project-001\Data_Raw\Optimized_Alberta_owner_sales_car_clean.csv"

    if os.path.exists(TARGET_FILE):
        run_etl_pipeline(TARGET_FILE)
    else:
        print(f"File Not Found: {TARGET_FILE}")