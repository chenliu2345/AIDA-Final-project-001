"""
Model 1 — Inference: Price Prediction with Real-Time Distances
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Loads the trained CatBoost pipeline and predicts the optimal listing
price for a vehicle, given its attributes and city name.

Distance pipeline at inference time:
  1. City name  →  geocoding API (open-meteo)      →  (lon, lat)
  2. (lon, lat) →  OpenRouteService routing API    →  road distance in KM
  This mirrors how distances were computed before being stored in
  tbl_Locations during the ETL — the same logic, run in real time.

Setup:
  Create a .env file in the project root containing:
      ORS_API_KEY=your_key_here

Usage example:
  from inference.predict import predict

  result = predict({
      "Year":              2019,
      "Kilometres":        65000,
      "Base_Model":        "RAV4",
      "Trim":              "XLE",
      "Condition_Label":   "USED",
      "Transmission_Type": "AUTOMATIC",
      "Drivetrain_Type":   "AWD",
      "Body_Style":        "SUV",
      "Colour":            "WHITE",
      "Seats_Count":       "5 SEATS",
      "City_Name":         "RED DEER",
  })
  print(result)
"""

import os
import joblib
import requests
import pandas as pd
from pathlib import Path
from functools import lru_cache
from dotenv import load_dotenv

from catboost import CatBoostRegressor
from sklearn.base import BaseEstimator, RegressorMixin

# ─────────────────────────────────────────────────────────────────────────────
#  CATBOOST WRAPPER
#  Must be defined here so joblib can deserialize the saved .pkl correctly.
#  This class must match the one used during training exactly.
# ─────────────────────────────────────────────────────────────────────────────
class CatBoostWrapper(BaseEstimator, RegressorMixin):
    def __init__(
        self,
        iterations=2000,
        learning_rate=0.05,
        depth=8,
        l2_leaf_reg=3.0,
        subsample=0.8,
        early_stopping_rounds=50,
        cat_features=None,
        loss_function="RMSE",
        random_seed=42,
        verbose=False,
    ):
        self.iterations            = iterations
        self.learning_rate         = learning_rate
        self.depth                 = depth
        self.l2_leaf_reg           = l2_leaf_reg
        self.subsample             = subsample
        self.early_stopping_rounds = early_stopping_rounds
        self.cat_features          = cat_features
        self.loss_function         = loss_function
        self.random_seed           = random_seed
        self.verbose               = verbose

    def fit(self, X, y, eval_set=None):
        self.model_ = CatBoostRegressor(
            iterations=self.iterations,
            learning_rate=self.learning_rate,
            depth=self.depth,
            l2_leaf_reg=self.l2_leaf_reg,
            subsample=self.subsample,
            early_stopping_rounds=self.early_stopping_rounds,
            cat_features=self.cat_features,
            loss_function=self.loss_function,
            random_seed=self.random_seed,
            verbose=self.verbose,
        )
        self.model_.fit(X, y)
        return self

    def predict(self, X):
        return self.model_.predict(X)

    def get_feature_importance(self):
        return self.model_.get_feature_importance()


# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent.parent / ".env")
ORS_API_KEY   = os.getenv("ORS_API_KEY")
ORS_URL       = "https://api.openrouteservice.org/v2/directions/driving-car"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"

MODEL_PATH = Path(__file__).parent.parent / "models" / "catboost_price_model.pkl"

# Features the model expects — must match Y1_model_catboost.py exactly
MODEL_FEATURES = [
    "Year", "Kilometres",
    "Distance_from_Edmonton_KM", "Distance_from_Calgary_KM",
    "Condition_Label", "Transmission_Type", "Drivetrain_Type",
    "Body_Style", "Colour", "Seats_Count", "City_Name", "Base_Model", "Trim",
]

# City name aliases — some Kijiji location labels are not directly
# geocodable, so we map them to the nearest real city name first.
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
#  DISTANCE API HELPERS
#  lru_cache avoids repeated API calls for the same city within one session.
# ─────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=None)
def _get_lat_lon(city_name: str):
    """Geocode a city name to (longitude, latitude) using open-meteo."""
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
    return None, None


def _road_distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float | None:
    """
    Call OpenRouteService to get the driving road distance in km.
    ORS expects coordinates as [longitude, latitude] pairs.
    Response distance field is in metres.
    """
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


def get_distances(city_name: str) -> tuple[float, float]:
    """
    Returns (distance_from_edmonton_km, distance_from_calgary_km)
    for the given city, calculated via real-time road routing.

    This is the same logic used to pre-populate tbl_Locations during
    the ETL — here it runs on demand at inference time.
    """
    if not ORS_API_KEY:
        raise ValueError("ORS_API_KEY not set. Add it to your .env file.")

    edm_lon, edm_lat = _get_lat_lon("Edmonton")
    cal_lon, cal_lat = _get_lat_lon("Calgary")
    tgt_lon, tgt_lat = _get_lat_lon(city_name)

    if None in (tgt_lon, tgt_lat):
        raise ValueError(f"Could not geocode city: '{city_name}'")

    dist_edm = _road_distance_km(edm_lon, edm_lat, tgt_lon, tgt_lat)
    dist_cal = _road_distance_km(cal_lon, cal_lat, tgt_lon, tgt_lat)

    if None in (dist_edm, dist_cal):
        raise ValueError(f"Could not calculate road distance for: '{city_name}'")

    return dist_edm, dist_cal


# ─────────────────────────────────────────────────────────────────────────────
#  PREDICT
# ─────────────────────────────────────────────────────────────────────────────
def predict(vehicle_info: dict) -> dict:
    """
    Predict the optimal listing price for a vehicle.

    Parameters
    ----------
    vehicle_info : dict
        Required keys:
          Year, Kilometres, Base_Model, Trim, Condition_Label,
          Transmission_Type, Drivetrain_Type, Body_Style,
          Colour, Seats_Count, City_Name

    Returns
    -------
    dict with:
      predicted_price_CAD          — recommended listing price
      distance_from_edmonton_km    — road distance fetched via ORS
      distance_from_calgary_km     — road distance fetched via ORS
    """
    model = joblib.load(MODEL_PATH)

    # Step 1: fetch real-time road distances from the seller's city
    print(f"[API] Fetching road distances for: {vehicle_info['City_Name']} ...")
    dist_edm, dist_cal = get_distances(vehicle_info["City_Name"])
    print(f"[API] Edmonton: {dist_edm} km  |  Calgary: {dist_cal} km")

    # Step 2: assemble the feature row the model expects
    row = {
        **vehicle_info,
        "Distance_from_Edmonton_KM": dist_edm,
        "Distance_from_Calgary_KM":  dist_cal,
    }
    df = pd.DataFrame([row])[MODEL_FEATURES]

    # Step 3: predict
    predicted_price = round(float(model.predict(df)[0]), 2)

    return {
        "predicted_price_CAD":       predicted_price,
        "distance_from_edmonton_km": dist_edm,
        "distance_from_calgary_km":  dist_cal,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT  (demo / quick test)
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    example = {
        "Year":              2019,
        "Kilometres":        65000,
        "Base_Model":        "RAV4",
        "Trim":              "XLE",
        "Condition_Label":   "USED",
        "Transmission_Type": "AUTOMATIC",
        "Drivetrain_Type":   "AWD",
        "Body_Style":        "SUV",
        "Colour":            "WHITE",
        "Seats_Count":       "5 SEATS",
        "City_Name":         "RED DEER",
    }

    result = predict(example)

    print()
    print("─" * 45)
    print(f"  Predicted Price       : ${result['predicted_price_CAD']:>10,.2f} CAD")
    print(f"  Distance from Edmonton: {result['distance_from_edmonton_km']:>10.1f} km")
    print(f"  Distance from Calgary : {result['distance_from_calgary_km']:>10.1f} km")
    print("─" * 45)