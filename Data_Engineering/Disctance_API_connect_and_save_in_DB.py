import requests
import pyodbc
import time
from functools import lru_cache
ROUTING_URL = "http://router.project-osrm.org/route/v1/driving/"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
DB_CONFIG = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=AB_CarSale_DB;Trusted_Connection=yes;"
@lru_cache(maxsize=None)
def get_lat_lon(city_name):
    name = city_name.upper().strip()
    mapping = {
        "STRATHCONA COUNTY": "Sherwood Park",
        "ROCKY VIEW COUNTY": "Airdrie",
        "FOOTHILLS COUNTY": "Okotoks",
        "TSUU T'INA": "Calgary",
        "BANFF / CANMORE": "Canmore",
        "SOUTH LETHBRIDGE": "Lethbridge",
        "ALBERTA": "Edmonton",
        "OTHER": "Red Deer",
        "LANGDON": "Chestermere",
        "ACADIA": "Calgary",
        "SWEET GRASS": "Edmonton",
        "FOOTHILLS": "Calgary",
        "NORTHEAST CALGARY": "Calgary",
        "NORTHWEST CALGARY": "Calgary",
        "SOUTHEAST CALGARY": "Calgary",
        "SOUTHWEST CALGARY": "Calgary",
        "NORTH CENTRAL EDMONTON": "Edmonton",
        "SOUTHEAST EDMONTON": "Edmonton",
        "NORTHWEST EDMONTON": "Edmonton",
        "WEST EDMONTON": "Edmonton",
        "NORTHEAST EDMONTON": "Edmonton",
        "SOUTHWEST EDMONTON": "Edmonton",
    }
    search_query = mapping.get(name, city_name.strip())
    try:
        res = requests.get(GEOCODING_URL, params={'name': search_query, 'count': 20}, timeout=15).json()
        if 'results' in res:
            for place in res['results']:
                if place.get('admin1') == 'Alberta':
                    return place['longitude'], place['latitude']
            for place in res['results']:
                if place.get('country') == 'Canada':
                    return place['longitude'], place['latitude']
            return res['results'][0]['longitude'], res['results'][0]['latitude']
    except Exception as e:
        print(f"Error finding {search_query}: {e}")
    print(f"!!! API completely can not find: {search_query}")
    return None, None
def calculate_road_distance(lon1, lat1, lon2, lat2):
    try:
        url = f"{ROUTING_URL}{lon1},{lat1};{lon2},{lat2}?overview=false"
        response = requests.get(url, timeout=10).json()
        if response.get('code') == 'Ok':
            distance_km = response['routes'][0]['distance'] / 1000
            return round(distance_km, 2)
    except Exception as e:
        print(f"Routing Error: {e}")
    return None
def road_distance_pipeline():
    conn = pyodbc.connect(DB_CONFIG)
    cursor = conn.cursor()
    edm_lon, edm_lat = get_lat_lon("Edmonton")
    cal_lon, cal_lat = get_lat_lon("Calgary")
    if not all([edm_lon, edm_lat, cal_lon, cal_lat]):
        print("Can not get Edmonton or Calgary coordinate, something wrong!")
        return
    try:
        cursor.execute("""
            SELECT Location_ID, City_Name 
            FROM tbl_Locations 
            WHERE Distance_from_Edmonton_KM IS NULL OR Distance_from_Calgary_KM IS NULL
        """)
        targets = cursor.fetchall()
        count = 0
        for loc_id, city_name in targets:
            count += 1
            target_lon, target_lat = get_lat_lon(city_name)
            if target_lon is None: 
                continue
            dist_from_edm = calculate_road_distance(edm_lon, edm_lat, target_lon, target_lat)
            dist_from_cal = calculate_road_distance(cal_lon, cal_lat, target_lon, target_lat)
            if dist_from_edm is not None and dist_from_cal is not None:
                update_sql = """
                UPDATE tbl_Locations 
                SET Distance_from_Edmonton_KM = ?, 
                    Distance_from_Calgary_KM = ?
                WHERE Location_ID = ?
                """
                cursor.execute(update_sql, dist_from_edm, dist_from_cal, loc_id)
                print(f"[{count}/{len(targets)}] {city_name} -> YEG: {dist_from_edm}km, YYC: {dist_from_cal}km")
            time.sleep(1)      
        conn.commit()
        print("All distance get!")
    except Exception as e:
        print(f"Database Error: {e}")
        conn.rollback()
    finally:
        conn.close()
if __name__ == "__main__":
    road_distance_pipeline()