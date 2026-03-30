"""
Alberta Used Car — Optimal Price Advisor
Streamlit UI for Model 1 (CatBoost price prediction).

Run:
  streamlit run app.py

Requirements:
  pip install streamlit requests joblib pandas catboost scikit-learn sqlalchemy
"""

import time
import requests
import streamlit as st
from pathlib import Path

# ── import predict from inference/ (sibling folder) ──────────────────────────
import sys
sys.path.append(str(Path(__file__).parent))
from inference.predict import CatBoostWrapper, predict, get_distances, GEOCODING_URL

# ─────────────────────────────────────────────────────────────────────────────
#  PRICING STRATEGY CONFIG
# ─────────────────────────────────────────────────────────────────────────────
STRATEGIES = {
    "Quick Sale  (−10%)": {
        "multiplier": 0.90,
        "description": "Price 10% below market to attract buyers fast. "
                       "Best when you need cash quickly or want to avoid "
                       "a long listing period.",
        "color": "#E24B4A",
    },
    "Balanced  (market price)": {
        "multiplier": 1.00,
        "description": "List at the predicted market price. Balances sale "
                       "speed and profit — the best starting point for "
                       "most sellers.",
        "color": "#1D9E75",
    },
    "Max Profit  (+8%)": {
        "multiplier": 1.08,
        "description": "Price 8% above market to maximize return. Expect "
                       "a longer listing period. Works best for rare or "
                       "low-mileage vehicles in high demand.",
        "color": "#378ADD",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
#  DROPDOWN OPTIONS  (sourced from training data)
# ─────────────────────────────────────────────────────────────────────────────
BASE_MODELS = [
    'ACURA ILX','ACURA MDX','ACURA RDX','ACURA TL','ACURA TSX','AUDI A3',
    'AUDI A4','AUDI A5','AUDI A6','AUDI Q3','AUDI Q5','AUDI Q7','AUDI S4',
    'AUDI S5','AUDI SQ5','BMW 3-SERIES','BMW 4-SERIES','BMW 5-SERIES',
    'BMW 6-SERIES','BMW 7-SERIES','BMW X1','BMW X3','BMW X3 M','BMW X5',
    'BMW X5 M','BMW X6','BUICK ENCLAVE','BUICK ENCORE','BUICK LUCERNE',
    'BUICK REGAL','BUICK VERANO','CADILLAC ATS','CADILLAC CTS',
    'CADILLAC ESCALADE','CADILLAC SRX','CHEVROLET AVALANCHE','CHEVROLET AVEO',
    'CHEVROLET BLAZER','CHEVROLET C/K PICKUP 1500','CHEVROLET C/K PICKUP 2500',
    'CHEVROLET C/K PICKUP 3500','CHEVROLET CAMARO','CHEVROLET COBALT',
    'CHEVROLET COLORADO','CHEVROLET CORVETTE','CHEVROLET CRUZE',
    'CHEVROLET EQUINOX','CHEVROLET EXPRESS','CHEVROLET IMPALA',
    'CHEVROLET MALIBU','CHEVROLET OTHER','CHEVROLET S-10',
    'CHEVROLET SILVERADO 1500','CHEVROLET SILVERADO 2500',
    'CHEVROLET SILVERADO 3500','CHEVROLET SILVERADO 3500HD','CHEVROLET SONIC',
    'CHEVROLET SPARK','CHEVROLET SUBURBAN','CHEVROLET TAHOE',
    'CHEVROLET TRAILBLAZER','CHEVROLET TRAVERSE','CHEVROLET TRAX',
    'CHEVROLET UPLANDER','CHRYSLER 200-SERIES','CHRYSLER 300-SERIES',
    'CHRYSLER CONCORDE','CHRYSLER GRAND CARAVAN','CHRYSLER OTHER',
    'CHRYSLER PACIFICA','CHRYSLER PT CRUISER','CHRYSLER SEBRING',
    'CHRYSLER TOWN & COUNTRY','DODGE AVENGER','DODGE CALIBER','DODGE CARAVAN',
    'DODGE CHALLENGER','DODGE CHARGER','DODGE DAKOTA','DODGE DART',
    'DODGE DURANGO','DODGE GRAND CARAVAN','DODGE JOURNEY','DODGE NITRO',
    'DODGE OTHER','DODGE OTHER PICKUPS','DODGE POWER RAM 1500',
    'DODGE POWER RAM 2500','DODGE POWER RAM 3500','FIAT 500','FORD BRONCO',
    'FORD E-150','FORD E-250','FORD E-350','FORD E-450','FORD EDGE',
    'FORD ESCAPE','FORD EXPEDITION','FORD EXPLORER','FORD EXPLORER SPORT',
    'FORD EXPLORER SPORT TRAC','FORD F-150','FORD F-250','FORD F-350',
    'FORD F-450','FORD F-550','FORD FIESTA','FORD FLEX','FORD FOCUS',
    'FORD FREESTAR','FORD FUSION','FORD MODEL T','FORD MUSTANG','FORD OTHER',
    'FORD RANGER','FORD TAURUS','FORD TRANSIT','FORD TRANSIT CARGO',
    'FORD TRANSIT CONNECT','FORD WINDSTAR','GMC ACADIA','GMC C/K 1500',
    'GMC C/K 2500','GMC C/K 3500','GMC CANYON','GMC ENVOY','GMC OTHER',
    'GMC SAVANA','GMC SIERRA','GMC SIERRA 1500','GMC SIERRA 2500',
    'GMC SIERRA 2500HD','GMC SIERRA 3500','GMC SIERRA 3500HD','GMC TERRAIN',
    'GMC YUKON','GMC YUKON XL','HONDA ACCORD','HONDA CIVIC','HONDA CR-V',
    'HONDA ELEMENT','HONDA FIT','HONDA ODYSSEY','HONDA OTHER','HONDA PILOT',
    'HONDA RIDGELINE','HUMMER H2','HYUNDAI ACCENT','HYUNDAI ELANTRA',
    'HYUNDAI GENESIS COUPE','HYUNDAI KONA','HYUNDAI OTHER','HYUNDAI PALISADE',
    'HYUNDAI SANTA FE','HYUNDAI SONATA','HYUNDAI TUCSON','HYUNDAI VELOSTER',
    'HYUNDAI VERACRUZ','INFINITI EX35','INFINITI FX','INFINITI G35',
    'INFINITI G35X','INFINITI G37X','INFINITI OTHER','INFINITI Q50',
    'INFINITI QX4','INFINITI QX60','JAGUAR F-PACE','JAGUAR XF',
    'JEEP CHEROKEE','JEEP COMPASS','JEEP GLADIATOR','JEEP GRAND CHEROKEE',
    'JEEP GRAND CHEROKEE L','JEEP LIBERTY','JEEP PATRIOT','JEEP RENEGADE',
    'JEEP TJ','JEEP WRANGLER','JEEP WRANGLER UNLIMITED','KIA FORTE',
    'KIA OPTIMA','KIA RIO','KIA RIO 5-DOOR','KIA RONDO','KIA SEDONA',
    'KIA SORENTO','KIA SOUL','KIA SPORTAGE','LAND ROVER RANGE ROVER',
    'LAND ROVER RANGE ROVER EVOQUE','LAND ROVER RANGE ROVER SPORT',
    'LEXUS IS 250','LEXUS IS 350','LEXUS LS','LEXUS OTHER','LEXUS RX',
    'LINCOLN MKX','LINCOLN MKZ','LINCOLN NAVIGATOR','MAZDA 2','MAZDA 3',
    'MAZDA 5','MAZDA 6','MAZDA CX-5','MAZDA CX-7','MAZDA CX-9','MAZDA OTHER',
    'MERCEDES-AMG C-CLASS','MERCEDES-BENZ B-CLASS','MERCEDES-BENZ C-CLASS',
    'MERCEDES-BENZ CLA','MERCEDES-BENZ E-CLASS','MERCEDES-BENZ GL-CLASS',
    'MERCEDES-BENZ GLA','MERCEDES-BENZ GLC','MERCEDES-BENZ GLE',
    'MERCEDES-BENZ GLK-CLASS','MERCEDES-BENZ M-CLASS','MERCEDES-BENZ S-CLASS',
    'MERCEDES-BENZ SPRINTER VAN','MINI MINI COOPER','MITSUBISHI ECLIPSE',
    'MITSUBISHI ECLIPSE CROSS','MITSUBISHI LANCER','MITSUBISHI OTHER',
    'MITSUBISHI OUTLANDER','MITSUBISHI RVR','NISSAN ALTIMA','NISSAN ARMADA',
    'NISSAN FRONTIER','NISSAN JUKE','NISSAN KICKS','NISSAN MAXIMA',
    'NISSAN MICRA','NISSAN MURANO','NISSAN OTHER','NISSAN PATHFINDER',
    'NISSAN QASHQAI','NISSAN QUEST','NISSAN ROGUE','NISSAN SENTRA',
    'NISSAN TITAN','NISSAN VERSA','NISSAN X-TRAIL','NISSAN XTERRA','OTHER',
    'OTHER OTHER','PONTIAC G5','PONTIAC G6','PONTIAC GRAND PRIX',
    'PONTIAC MONTANA','PONTIAC SUNFIRE','PONTIAC TORRENT','PORSCHE CAYENNE',
    'PORSCHE MACAN','RAM 1500','RAM 1500 CLASSIC','RAM 2500','RAM 3500',
    'RAM CARGO','SATURN ION','SATURN VUE','SMART FORTWO','SUBARU FORESTER',
    'SUBARU IMPREZA','SUBARU IMPREZA WRX STI','SUBARU LEGACY','SUBARU OTHER',
    'SUBARU OUTBACK','SUBARU WRX','SUZUKI OTHER','SUZUKI SX4','TESLA MODEL 3',
    'TESLA MODEL Y','TOYOTA 4RUNNER','TOYOTA CAMRY','TOYOTA COROLLA',
    'TOYOTA ECHO','TOYOTA FJ CRUISER','TOYOTA HIGHLANDER','TOYOTA MATRIX',
    'TOYOTA OTHER','TOYOTA PRIUS','TOYOTA RAV4','TOYOTA SEQUOIA',
    'TOYOTA SIENNA','TOYOTA TACOMA','TOYOTA TUNDRA','TOYOTA VENZA',
    'TOYOTA YARIS','VOLKSWAGEN ATLAS','VOLKSWAGEN BEETLE','VOLKSWAGEN GOLF',
    'VOLKSWAGEN GOLF R','VOLKSWAGEN GTI','VOLKSWAGEN JETTA','VOLKSWAGEN OTHER',
    'VOLKSWAGEN PASSAT','VOLKSWAGEN TIGUAN','VOLKSWAGEN TOUAREG','VOLVO S60',
    'VOLVO XC60','VOLVO XC70','VOLVO XC90',
]

TRIMS = [
    'UNKNOWN','1LT','2.0T','2LT','350','A-SPEC','ALL TERRAIN','ALTITUDE','AT4',
    'AUTOBAHN','AWD','BASE','BIG HORN','BIGHORN','BLACK',
    'BLACK OPTIC TECHNIK PREMIUM PLUS','CARGO VAN','CE','COMFORTLINE','CREW',
    'CROSSROAD','CUSTOM','DENALI','DX','E430','ECONOLINE','ELEVATION','ES',
    'EX','EX-L','EXL','EXPRESS','EXPRESS NITE EDITION','FULLY LOADED SL 4X4',
    'FX4','GFX','GL','GLS','GS','GT','GTS','GX','HIGH ALTITUDE','HIGH COUNTRY',
    'HIGHLINE','HSE','HYBRID','HYBRID LIMITED','KING RANCH','L','LARAMIE',
    'LAREDO','LARIAT','LE','LE AWD','LEATHER','LIMITED','LONGHORN','LS','LT',
    'LTZ','LUXURY','LX','LX PREMIUM','M SPORT','ML 350 BLUETEC','NORTH',
    'NORTH EDITION','OTHER','OVERLAND','PLATINUM','PREFERRED','PREMIER',
    'PREMIUM','PREMIUM PLUS','PRESTIGE','PROGRESSIV','R/T','RESERVE','RS',
    'RST','RT','RUBICON','S','S-LINE','SAHARA','SE','SEL','SI','SL','SL AWD',
    'SLE','SLT','SPORT','SPORT S','SPORTS','SR','SR5','SRT8','SS','ST',
    'ST LINE','SUMMIT','SUPER DUTY','SUPERDUTY','SV','SX','SXT','TECHNIK',
    'TECHNOLOGY','TITANIUM','TOURING','TRADESMAN','TRAILHAWK','TRD SPORT',
    'TRENDLINE','ULTIMATE','WRX','WT','X','XL','XLE','XLT','XSE','XTR',
    'Z71','ZR2',
]

CONDITIONS    = ['USED', 'DAMAGED', 'LEASE TAKEOVER', 'SALVAGE']
TRANSMISSIONS = ['AUTOMATIC', 'MANUAL', 'SEMI-AUTOMATIC', 'OTHER', 'UNKNOWN']
DRIVETRAINS   = [
    'ALL-WHEEL DRIVE (AWD)', '4 X 4', 'FRONT-WHEEL DRIVE (FWD)',
    'REAR-WHEEL DRIVE (RWD)', 'OTHER', 'UNKNOWN',
]
BODY_STYLES   = [
    'SUV, CROSSOVER', 'SEDAN', 'PICKUP TRUCK', 'HATCHBACK',
    'MINIVAN, VAN', 'COUPE (2 DOOR)', 'WAGON', 'CONVERTIBLE', 'OTHER',
]
COLOURS = [
    'WHITE EXTERIOR', 'BLACK EXTERIOR', 'SILVER EXTERIOR', 'GREY EXTERIOR',
    'BLUE EXTERIOR', 'RED EXTERIOR', 'BROWN EXTERIOR', 'BEIGE EXTERIOR',
    'BURGUNDY EXTERIOR', 'GOLD EXTERIOR', 'GREEN EXTERIOR', 'OFF-WHITE EXTERIOR',
    'ORANGE EXTERIOR', 'OTHER EXTERIOR', 'PURPLE EXTERIOR', 'TAN EXTERIOR',
    'TEAL EXTERIOR', 'YELLOW EXTERIOR', 'UNKNOWN',
]
SEATS = ['5 SEATS', '7 SEATS', '4 SEATS', '6 SEATS', '8 SEATS',
         '2 SEATS', '3 SEATS', 'OTHER SEATS', 'UNKNOWN']


# ─────────────────────────────────────────────────────────────────────────────
#  GEOCODING HELPER  (for city search autocomplete)
# ─────────────────────────────────────────────────────────────────────────────
def search_cities(query: str) -> list[dict]:
    """Return up to 8 Alberta city suggestions for the given query string."""
    if len(query) < 2:
        return []
    try:
        res = requests.get(
            GEOCODING_URL,
            params={"name": query, "count": 20, "language": "en"},
            timeout=8,
        ).json()
        results = []
        for place in res.get("results", []):
            if place.get("admin1") == "Alberta" and place.get("country") == "Canada":
                results.append({
                    "name":      place["name"],
                    "display":   f"{place['name']}, Alberta",
                    "longitude": place["longitude"],
                    "latitude":  place["latitude"],
                })
        return results[:8]
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Alberta Used Car — Price Advisor",
    page_icon="🚗",
    layout="wide",
)

st.title("🚗 Alberta Used Car — Optimal Price Advisor")
st.caption(
    "Enter your vehicle details below. The model will predict the optimal "
    "market price and suggest three pricing strategies."
)
st.divider()

# ─────────────────────────────────────────────────────────────────────────────
#  FORM — LEFT COLUMN (vehicle details) / RIGHT COLUMN (city search)
# ─────────────────────────────────────────────────────────────────────────────
col_left, col_right = st.columns([3, 2], gap="large")

with col_left:
    st.subheader("Vehicle details")

    base_model = st.selectbox("Make & Model", BASE_MODELS, index=BASE_MODELS.index("TOYOTA RAV4"))
    trim       = st.selectbox("Trim", TRIMS, index=TRIMS.index("XLE"))

    c1, c2 = st.columns(2)
    with c1:
        year       = st.number_input("Year", min_value=1990, max_value=2025, value=2019, step=1)
    with c2:
        kilometres = st.number_input("Kilometres", min_value=0, max_value=500_000, value=65_000, step=1000)

    condition    = st.selectbox("Condition",    CONDITIONS,    index=0)
    transmission = st.selectbox("Transmission", TRANSMISSIONS, index=0)
    drivetrain   = st.selectbox("Drivetrain",   DRIVETRAINS,   index=0)
    body_style   = st.selectbox("Body Style",   BODY_STYLES,   index=0)
    colour       = st.selectbox("Colour",       COLOURS,       index=0)
    seats        = st.selectbox("Seats",        SEATS,         index=0)

with col_right:
    st.subheader("Seller location")
    st.caption(
        "Type your city name. Select from the suggestions to confirm "
        "a valid Alberta location."
    )

    city_query = st.text_input("Search city", placeholder="e.g. Red Deer")

    # Session state to hold the confirmed city
    if "confirmed_city" not in st.session_state:
        st.session_state.confirmed_city = None

    if city_query:
        suggestions = search_cities(city_query)
        if suggestions:
            st.write("Select your city:")
            for s in suggestions:
                if st.button(s["display"], key=s["display"]):
                    st.session_state.confirmed_city = s["name"].upper()
                    st.rerun()
        else:
            st.warning("No Alberta cities found. Try a different spelling.")

    if st.session_state.confirmed_city:
        st.success(f"Location confirmed: **{st.session_state.confirmed_city}**")
    else:
        st.info("No location selected yet.")

    # Reset location button
    if st.session_state.confirmed_city:
        if st.button("Change location"):
            st.session_state.confirmed_city = None
            st.rerun()

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
#  PREDICT BUTTON
# ─────────────────────────────────────────────────────────────────────────────
ready = st.session_state.confirmed_city is not None

if not ready:
    st.warning("Please select a city before predicting.")

if st.button("Predict optimal price", disabled=not ready, type="primary"):

    vehicle_info = {
        "Year":              int(year),
        "Kilometres":        int(kilometres),
        "Base_Model":        base_model,
        "Trim":              trim,
        "Condition_Label":   condition,
        "Transmission_Type": transmission,
        "Drivetrain_Type":   drivetrain,
        "Body_Style":        body_style,
        "Colour":            colour,
        "Seats_Count":       seats,
        "City_Name":         st.session_state.confirmed_city,
    }

    with st.spinner("Fetching road distances via OSRM API..."):
        try:
            result = predict(vehicle_info)
        except Exception as e:
            st.error(f"Prediction failed: {e}")
            st.stop()

    base_price = result["predicted_price_CAD"]
    dist_edm   = result["distance_from_edmonton_km"]
    dist_cal   = result["distance_from_calgary_km"]

    # ── Distance info ─────────────────────────────────────────────────────────
    st.subheader("Location distances (fetched via OSRM API)")
    dc1, dc2 = st.columns(2)
    dc1.metric("Distance from Edmonton", f"{dist_edm:,.1f} km")
    dc2.metric("Distance from Calgary",  f"{dist_cal:,.1f} km")

    st.divider()

    # ── Pricing strategies ────────────────────────────────────────────────────
    st.subheader("Pricing strategies")
    st.caption(f"Predicted market price: **${base_price:,.0f} CAD**")

    sc1, sc2, sc3 = st.columns(3)
    cols = [sc1, sc2, sc3]

    for col, (name, cfg) in zip(cols, STRATEGIES.items()):
        price = base_price * cfg["multiplier"]
        delta = price - base_price
        with col:
            st.markdown(
                f"""
                <div style="
                    border: 1.5px solid {cfg['color']};
                    border-radius: 10px;
                    padding: 20px 18px;
                    text-align: center;
                ">
                  <div style="font-size:14px;font-weight:500;
                              color:{cfg['color']};margin-bottom:8px;">
                    {name}
                  </div>
                  <div style="font-size:32px;font-weight:600;
                              color:{cfg['color']};margin-bottom:4px;">
                    ${price:,.0f}
                  </div>
                  <div style="font-size:13px;color:#888;margin-bottom:12px;">
                    {'+' if delta >= 0 else ''}{delta:,.0f} CAD vs market
                  </div>
                  <div style="font-size:13px;line-height:1.5;color:#555;">
                    {cfg['description']}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.divider()

    # ── Summary table ─────────────────────────────────────────────────────────
    st.subheader("Summary")
    summary_data = {
        "Strategy": list(STRATEGIES.keys()),
        "Listing Price (CAD)": [
            f"${base_price * cfg['multiplier']:,.0f}"
            for cfg in STRATEGIES.values()
        ],
        "vs Market": [
            f"{'+' if (base_price * cfg['multiplier'] - base_price) >= 0 else ''}"
            f"${abs(base_price * cfg['multiplier'] - base_price):,.0f}"
            for cfg in STRATEGIES.values()
        ],
    }
    st.table(summary_data)