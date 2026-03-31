import os
from pathlib import Path
import requests
import pyodbc
import time
from functools import lru_cache
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
#  API key is loaded from .env file — never hard-code it here.
#  Create a .env file in the project root with:
#      ORS_API_KEY=your_key_here
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent.parent / ".env")
ORS_API_KEY   = os.getenv("ORS_API_KEY")
ORS_URL       = "https://api.openrouteservice.org/v2/directions/driving-car"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
DB_CONFIG     = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=AB_CarSale_DB;Trusted_Connection=yes;"

# ─────────────────────────────────────────────────────────────────────────────
#  CITY NAME ALIASES
#  Some Kijiji location labels are not directly geocodable — map them to the
#  nearest real city name before sending to the geocoding API.
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
#  GEOCODING  (open-meteo — no API key required)
#  lru_cache avoids repeat calls for the same city name in one run.
# ─────────────────────────────────────────────────────────────────────────────
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
        print(f"[GEOCODING ERROR] {search_query}: {e}")
    print(f"[GEOCODING] Could not find: {search_query}")
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
#  ROAD DISTANCE  (OpenRouteService)
#  ORS expects coordinates as [longitude, latitude] in the request body.
#  Response distance is in metres — divide by 1000 for km.
# ─────────────────────────────────────────────────────────────────────────────
def calculate_road_distance(lon1: float, lat1: float, lon2: float, lat2: float):
    """Return driving road distance in km between two coordinates, or None on failure."""
    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type":  "application/json",
    }
    body = {
        "coordinates": [
            [lon1, lat1],
            [lon2, lat2],
        ]
    }
    try:
        res = requests.post(ORS_URL, json=body, headers=headers, timeout=15).json()
        distance_m = res["routes"][0]["summary"]["distance"]
        return round(distance_m / 1000, 2)
    except Exception as e:
        print(f"[ORS ROUTING ERROR] {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN PIPELINE
#  Fetches road distances for all locations in tbl_Locations that are
#  still missing Distance_from_Edmonton_KM or Distance_from_Calgary_KM,
#  then updates them in the database.
# ─────────────────────────────────────────────────────────────────────────────
def road_distance_pipeline():
    if not ORS_API_KEY:
        print("[ERROR] ORS_API_KEY not found. Add it to your .env file.")
        return

    conn   = pyodbc.connect(DB_CONFIG)
    cursor = conn.cursor()

    # Geocode the two reference cities once
    edm_lon, edm_lat = get_lat_lon("Edmonton")
    cal_lon, cal_lat = get_lat_lon("Calgary")
    if not all([edm_lon, edm_lat, cal_lon, cal_lat]):
        print("[ERROR] Could not geocode Edmonton or Calgary.")
        conn.close()
        return

    try:
        cursor.execute("""
            SELECT Location_ID, City_Name
            FROM tbl_Locations
            WHERE Distance_from_Edmonton_KM IS NULL
               OR Distance_from_Calgary_KM  IS NULL
        """)
        targets = cursor.fetchall()
        total   = len(targets)
        print(f"[INFO] {total} location(s) need distance calculation.")

        for count, (loc_id, city_name) in enumerate(targets, 1):
            tgt_lon, tgt_lat = get_lat_lon(city_name)
            if tgt_lon is None:
                print(f"[SKIP] ({count}/{total}) {city_name} — could not geocode")
                continue

            dist_edm = calculate_road_distance(edm_lon, edm_lat, tgt_lon, tgt_lat)
            dist_cal = calculate_road_distance(cal_lon, cal_lat, tgt_lon, tgt_lat)

            if dist_edm is not None and dist_cal is not None:
                cursor.execute(
                    """
                    UPDATE tbl_Locations
                    SET Distance_from_Edmonton_KM = ?,
                        Distance_from_Calgary_KM  = ?
                    WHERE Location_ID = ?
                    """,
                    dist_edm, dist_cal, loc_id,
                )
                print(f"[{count}/{total}] {city_name} → YEG: {dist_edm} km  |  YYC: {dist_cal} km")
            else:
                print(f"[FAIL] ({count}/{total}) {city_name} — routing failed, skipped")

            # ORS free tier: 40 requests/minute → 1.5s gap keeps us safely under
            time.sleep(1.5)

        conn.commit()
        print("[DONE] All distances updated.")

    except Exception as e:
        print(f"[DB ERROR] {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    road_distance_pipeline()