import requests
import pyodbc
from datetime import datetime
import time
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"
DB_CONFIG = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=MilesZhang;DATABASE=AB_CarSale_DB;Trusted_Connection=yes;"
def get_lat_lon(city_name):
    res = requests.get(GEOCODING_URL, params={'name': city_name, 'count': 1},timeout=15).json()
    if 'results' in res:
        return res['results'][0]['latitude'], res['results'][0]['longitude']
    return None, None
def open_meteo_pipeline():
    conn = pyodbc.connect(DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT L.Location_ID, L.City_Name, S.Sold_Date
            FROM tbl_Listing_Status S
            JOIN tbl_Listings LS ON S.Listing_ID = LS.Listing_ID
            JOIN tbl_Locations L ON LS.Location_ID = L.Location_ID
            WHERE S.Sold_Date IS NOT NULL 
            AND NOT EXISTS (
                SELECT 1 FROM tbl_Weather W 
                WHERE W.Location_ID = L.Location_ID 
                AND W.Weather_Date = S.Sold_Date
                )
            """)
        targets = cursor.fetchall()
        for loc_id, city_name, sold_date in targets:
            lat, lon = get_lat_lon(city_name)
            if lat is None: continue
            date_str = sold_date.strftime('%Y-%m-%d')
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": date_str,
                "end_date": date_str,
                "daily": "temperature_2m_mean,weathercode",
                "timezone": "auto"
            }
            response = requests.get(HISTORICAL_URL, params=params,timeout=30).json()
            if 'daily' not in response: continue
            temp = response['daily']['temperature_2m_mean'][0]
            wmo_code = response['daily']['weathercode'][0]
            status_label = f"WMO_CODE_{wmo_code}" 
            if temp is None: continue
            cursor.execute("SELECT Condition_ID FROM tbl_Weather_Conditions WHERE Condition_Label = ?", status_label)
            row = cursor.fetchone()
            if not row:
                cursor.execute("INSERT INTO tbl_Weather_Conditions (Condition_Label) VALUES (?)", status_label)
                cursor.execute("SELECT @@IDENTITY")
                cond_id = cursor.fetchone()[0]
            else:
                cond_id = row[0]
            upsert_sql = """
            MERGE INTO tbl_Weather AS target
            USING (SELECT ? AS lid, ? AS wdate) AS source
            ON (target.Location_ID = source.lid AND target.Weather_Date = source.wdate)
            WHEN MATCHED THEN
                UPDATE SET Temperature_C = ?, Condition_ID = ?
            WHEN NOT MATCHED THEN
                INSERT (Location_ID, Weather_Date, Temperature_C, Condition_ID)
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(upsert_sql, loc_id, date_str, temp, cond_id, loc_id, date_str, temp, cond_id)
            print(f"Captured: {city_name} on {date_str} -> {temp}°C")
            time.sleep(1)
        conn.commit()
        print("API DATA GET！")
    finally:
        conn.close()
if __name__ == "__main__":
    open_meteo_pipeline()