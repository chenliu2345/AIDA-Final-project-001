import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import re
import datetime
import random
import os
from pathlib import Path
BASE_DIR = Path(__file__).parent.parent
RAW_DATA_DIR = BASE_DIR / "Data_Raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
MASTER_FILENAME = str(RAW_DATA_DIR / "Alberta_owner_sales_car.csv")
def get_owner_car_price():
    all_car_data = []
    catch_link = set()
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"}
    CITY_URLS = {
        "Calgary": "https://www.kijiji.ca/b-cars-trucks/calgary/c174l1700199?for-sale-by=ownr&view=list",
        "Edmonton (City)": "https://www.kijiji.ca/b-cars-trucks/edmonton/c174l1700203?for-sale-by=ownr&view=list",
        "St. Albert": "https://www.kijiji.ca/b-cars-trucks/st-albert/c174l1700205?for-sale-by=ownr&view=list",
        "Strathcona County": "https://www.kijiji.ca/b-cars-trucks/strathcona-county/c174l1700204?for-sale-by=ownr&view=list",
        "Red Deer": "https://www.kijiji.ca/b-cars-trucks/red-deer/c174l1700136?for-sale-by=ownr&view=list",
        "Lethbridge": "https://www.kijiji.ca/b-cars-trucks/lethbridge/c174l1700230?for-sale-by=ownr&view=list",
        "Grande Prairie": "https://www.kijiji.ca/b-cars-trucks/grande-prairie/c174l1700233?for-sale-by=ownr&view=list",
        "Medicine Hat": "https://www.kijiji.ca/b-cars-trucks/medicine-hat/c174l1700231?for-sale-by=ownr&view=list",
        "Lloydminster": "https://www.kijiji.ca/b-cars-trucks/lloydminster-ab/c174l1700095?for-sale-by=ownr&view=list",
        "Fort McMurray": "https://www.kijiji.ca/b-cars-trucks/fort-mcmurray/c174l1700232?for-sale-by=ownr&view=list",
        "Banff / Canmore": "https://www.kijiji.ca/b-cars-trucks/banff-canmore/c174l1700234?for-sale-by=ownr&view=list"
    }
    print(f"Starting Multi-Region Scan... Targets: {len(CITY_URLS)} regions.")
    for city_name, base_url in CITY_URLS.items():
        print(f"\n------ Switching to: {city_name} ------")
        page = 1
        city_data_count = 0
        while True:
            if page == 1:
                url = base_url
            else:
                try:
                    parts = base_url.split("/c174")
                    url = f"{parts[0]}/page-{page}/c174{parts[1]}"
                except:
                    print(f"URL format error for {city_name}")
                    break
            try:
                req = requests.get(url, headers=headers, timeout=15)
                if req.status_code != 200: break
            except: break
            soup = BeautifulSoup(req.text, "html.parser")
            car_card = soup.find_all("section", attrs={"data-testid": re.compile("listing-card")})
            if len(car_card) <= 0:
                print(f"{city_name} Finished! (Reached Page {page-1})")
                break
            new_in_page = 0
            for car in car_card:
                try:
                    car_name = car.find("a", attrs={"data-testid": re.compile("listing-link")})
                    if not car_name: continue
                    raw_link = car_name['href']
                    if not raw_link.startswith("http"):
                        car_link = "https://www.kijiji.ca" + raw_link.strip()
                    else:
                        car_link = raw_link.strip()
                    if car_link in catch_link: continue
                    catch_link.add(car_link)
                    new_in_page += 1
                    city_data_count += 1
                    car_title = car_name.text.strip()
                    loc_elem = car.find("p", attrs={"data-testid": re.compile("listing-location")})
                    car_location = loc_elem.text.strip() if loc_elem else city_name 
                    car_price = car.find("p", attrs={"data-testid": re.compile("listing-price")})
                    if car_price:
                        p_price = car_price.text
                        if any(i.isdigit() for i in p_price):
                            price = float(p_price.replace("$", "").replace(",", "").strip())
                            all_car_data.append({
                                "Listing title": car_title,
                                "Price(CA$)": price,
                                "Link": car_link,
                                "Location": car_location
                            })
                except: continue
            print(f"  -> {city_name} Page {page}: Found {len(car_card)} cars")
            if new_in_page == 0:
                break
            time.sleep(random.uniform(3, 5))
            page += 1
            if page > 200:
                break
    print(f"All Regions Scanned! Total unique listings: {len(all_car_data)}")
    return all_car_data
def clean_car_data(df):
    valid_brands = ["ford", "chev", "gmc", "dodge", "ram", "toyota", "honda", "nissan", "mazda", 
        "vw", "volkswagen", "bmw", "mercedes", "audi", "kia", "hyundai", "hyandai", 
        "jeep", "subaru", "lexus", "acura", "infiniti", "infinity", "cadillac", 
        "buick", "lincoln", "chrysler", "pontiac", "saturn", "volvo", "mitsubishi", 
        "suzuki", "mini", "tesla", "porsche", "land rover", "range rover", "alfa", 
        "jaguar", "bentley", "genesis", "hummer", "fiat", "smart", "freightliner",
        "hino", "peterbilt", "kenworth", "isuzu", "international", "sterling",
        "f150", "f250", "f350", "silverado", "sierra", "tahoe", "yukon", "suburban", 
        "equinox", "traverse", "explorer", "expedition", "escape", "edge", "rav4", 
        "crv", "highlander", "4runner", "tacoma", "tundra", "civic", "accord", 
        "corolla", "camry", "elantra", "santa fe", "tucson", "wrangler", "cherokee", 
        "caravan", "odyssey", "sienna", "mustang", "camaro", "corvette", "challenger", 
        "charger", "denali", "ducato", "sprinter", "savana", "savanna", "express",
        "f-150", "f-250", "f-350", "f-450", "cr-v", "pt cruiser", "golf r", 
        "trailblazer", "enclave", "malibu", "cobalt", "cruze", "focus", "fusion", 
        "ridgeline", "pathfinder", "dakota", "beetle", "passat", "outlander", "gla"]
    blacklist = ["part out", "parting out", "parts only", "wrecking", "scrap", 
        "tires", "rims", "wheels", "engine", "transmission", "box only", 
        "canopy", "topper", "tonneau", "tailgate", "bumper", "mirror",
        "wanted", "buying", "looking for", "wtb", "cash for cars", 
        "junk cars", "we buy", "towing", "detailing", "mechanic", 
        "container", "tank", "trailer", "plow", "parts car"]
    print(f"Cleaning {len(df)} data...")
    cleaned_data = []
    for index, row in df.iterrows():
        title = str(row['Listing title']).strip()
        title_lower = title.lower()
        price = row['Price(CA$)']
        if any(fake in title_lower for fake in blacklist):
            continue
        if price < 500:
            continue
        has_year = re.search(r'\b(19|20)\d{2}\b', title)
        has_brand = any(brand in title_lower for brand in valid_brands)
        if has_year or has_brand:
            cleaned_data.append(row)
        else:
            print(f"the {title} is not a car")
    return pd.DataFrame(cleaned_data)
def update_database(current_df):
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(MASTER_FILENAME):
        print("Creating new master database...")
        current_df["Scrape_Date"] = today_str
        current_df["Status"] = "Active"   
        current_df["Sold_Date"] = ""       
        current_df.to_csv(MASTER_FILENAME, index=False, encoding="utf-8-sig")
        print(f"Created {MASTER_FILENAME} with {len(current_df)} active cars.")
        return
    print("Reading existing database to check status...")
    try:
        df_master = pd.read_csv(MASTER_FILENAME,dtype=str)
        if "Status" not in df_master.columns: df_master["Status"] = "Active"
        if "Sold_Date" not in df_master.columns: df_master["Sold_Date"] = ""
    except Exception as e:
        print(f"Error reading master file: {e}")
        return
    total_active = len(df_master[df_master["Status"] == "Active"])
    scraped_count = len(current_df)
    is_scan_complete = True
    if total_active > 0 and scraped_count < (total_active * 0.8):
        print(f"Warning! ({scraped_count}) less than ({total_active})to much!")
        is_scan_complete = False
    current_links_set = set(current_df["Link"].astype(str).tolist())
    master_links_set = set(df_master["Link"].astype(str).tolist())
    new_cars_df = current_df[~current_df["Link"].isin(master_links_set)].copy()
    if not new_cars_df.empty:
        new_cars_df["Scrape_Date"] = today_str
        new_cars_df["Status"] = "Active"
        new_cars_df["Sold_Date"] = ""
        print(f"Found {len(new_cars_df)} NEW listings!")
        df_master = pd.concat([df_master, new_cars_df], ignore_index=True)
    else:
        print("No newly listed cars today.")
    if is_scan_complete:
        active_mask = df_master["Status"] == "Active"
        sold_mask = active_mask & (~df_master["Link"].isin(current_links_set))
        sold_count = sold_mask.sum()
        if sold_count > 0:
            print(f"Detected {sold_count} cars SOLD (removed from site) today!")
            df_master.loc[sold_mask, "Status"] = "Sold"
            df_master.loc[sold_mask, "Sold_Date"] = today_str
        else:
            print("No active cars were removed/sold today.")
    df_master.to_csv(MASTER_FILENAME, index=False, encoding="utf-8-sig")
    print(f"Database updated successfully: {len(df_master)} total records.")
if __name__ == "__main__":
    print(f"Car Price & Sales Tracker ON!") 
    data = get_owner_car_price()
    if data:
        df_raw = pd.DataFrame(data)
        df_current = clean_car_data(df_raw)
        if not df_current.empty:
            update_database(df_current)
        else:
            print(f"Current scan result is empty after cleaning.")
    else:
        print(f"No data scraped.")