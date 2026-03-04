import pandas as pd
import pyodbc
import logging

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
        return None

def transform_data(df):
    """Cleans data and handles PII masking."""
    # Physical removal of Link for privacy compliance
    if 'Link' in df.columns:
        df = df.drop(columns=['Link'])
    
    # Cleaning Kilometres and filtering outliers
    df['Kilometres'] = (
        df['Kilometres']
        .astype(str)
        .str.replace(',', '')
        .str.replace(' ', '')
    )
    df['Kilometres'] = pd.to_numeric(df['Kilometres'], errors='coerce').fillna(0).astype(int)
    
    # Business logic filters
    df = df[(df['Price(CA$)'] >= 500) & (df['Year'] <= 2026)]
    df = df.fillna('UNKNOWN')
    return df

def populate_lookup(cursor, table_name, col_name, id_col, unique_vals):
    """Explicitly maps strings to IDs using a provided ID column name."""
    mapping = {}
    for i, val in enumerate(unique_vals, start=1):
        try:
            cursor.execute(
                f"INSERT INTO {table_name} ({id_col}, {col_name}) VALUES (?, ?)", 
                (i, val)
            )
            mapping[val] = i
        except Exception as e:
            logging.warning(f"Lookup error in {table_name}: {e}")
    return mapping

def run_etl_pipeline(csv_path):
    """Main Orchestrator for high-performance ETL."""
    conn = get_db_connection()
    if not conn: return
    cursor = conn.cursor()
    
    try:
        df = pd.read_csv(csv_path)
        df = transform_data(df)

        # Step 1: Mapping all 11 required Lookup tables
        m_map = populate_lookup(cursor, "Models", "Base_Model", "Model_ID", df['Base_Model'].unique())
        y_map = populate_lookup(cursor, "Years", "Year", "Year_ID", df['Year'].unique())
        t_map = populate_lookup(cursor, "Trims", "Trim", "Trim_ID", df['Trim'].unique())
        l_map = populate_lookup(cursor, "Locations", "City_Name", "City_ID", df['Location'].unique())
        s_map = populate_lookup(cursor, "Statuses", "Status_Label", "Status_ID", df['Status'].unique())
        c_map = populate_lookup(cursor, "Conditions", "Condition_Label", "Condition_ID", df['Condition'].unique())
        tr_map = populate_lookup(cursor, "Transmissions", "Transmission_Type", "Transmission_ID", df['Transmission'].unique())
        d_map = populate_lookup(cursor, "Drivetrains", "Drivetrain_Type", "Drivetrain_ID", df['Drivetrain'].unique())
        b_map = populate_lookup(cursor, "Body_Styles", "Body_Style", "Body_Style_ID", df['Body Style'].unique())
        co_map = populate_lookup(cursor, "Colours", "Colour", "Colour_ID", df['Colour'].unique())
        st_map = populate_lookup(cursor, "Seats", "Seats_Label", "Seats_ID", df['Seats'].unique())

        # Step 2: High-Performance Bulk Insertion for Vehicles_table
        cursor.fast_executemany = True 

        vehicle_payload = []
        for idx, row in df.iterrows():
            vehicle_payload.append((
                idx, m_map[row['Base_Model']], y_map[row['Year']], 
                t_map[row['Trim']], b_map[row['Body Style']], 
                tr_map[row['Transmission']], d_map[row['Drivetrain']], 
                co_map[row['Colour']], st_map[row['Seats']], 
                s_map[row['Status']], # FIXED: Included Status_ID
                row['Listing title']
            ))

        veh_sql = """
            INSERT INTO Vehicles_table (
                Vehicle_ID, Model_ID, Year_ID, Trim_ID, Body_Style_ID, 
                Transmission_ID, Drivetrain_ID, Colour_ID, Seats_ID, 
                Status_ID, Listing_Title
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(veh_sql, vehicle_payload)

        # Step 3: Populate Listings_table (Fact table 2)
        listing_payload = []
        for idx, row in df.iterrows():
            listing_payload.append((
                idx, idx, l_map[row['Location']], 
                row['Price(CA$)'], row['Kilometres'], 
                c_map[row['Condition']]
            ))

        list_sql = """
            INSERT INTO Listings_table (
                Listing_ID, Vehicle_ID, Location_ID, 
                Price_CAD, Kilometres, Condition_ID
            ) VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(list_sql, listing_payload)

        conn.commit()
        print(f"Success: Processed {len(df)} records into 13 tables.")

    except Exception as e:
        conn.rollback()
        print(f"Pipeline failure: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_etl_pipeline('Optimized_Alberta_owner_sales_car_clean.csv')