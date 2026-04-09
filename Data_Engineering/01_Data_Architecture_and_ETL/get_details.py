import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import glob
import os
import re
from pathlib import Path
BASE_DIR = Path(__file__).parent.parent
RAW_DATA_DIR = BASE_DIR / "Data_Raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
FILE_PATTERN = str(RAW_DATA_DIR / "Alberta_owner_sales_car.csv")
SLEEP_MIN = 2
SLEEP_MAX = 5
SAVE_INTERVAL = 5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
def get_latest_file():
    if os.path.exists(FILE_PATTERN):
        return FILE_PATTERN
    search_path = str(RAW_DATA_DIR / "Alberta_*_car*.csv")
    files = glob.glob(search_path)
    if not files: 
        return None
    return max(files, key=os.path.getmtime)
def extract_page_data(soup):
    data = {}
    target_keys = ["Condition", "Kilometres", "Transmission", "Drivetrain", "Seats", "Body Style", "Colour", "Model"]
    for key in target_keys:
        data[key] = "N/A"
        pattern = re.compile(rf"^\s*{key}\s*:?\s*$", re.IGNORECASE)
        label = soup.find("p", string=pattern)
        if label:
            values = label.find_next_siblings("p")
            if values:
                data[key] = ", ".join([v.get_text(strip=True) for v in values])  
    return data
def scrape_details(filename):
    print(f"========== Start: Details Scraper ==========")
    print(f"Target File: {filename}")
    try: 
        df = pd.read_csv(filename, dtype=str)
    except Exception as e: 
        print(f"Error reading file: {e}"); return
    new_columns = ["Condition", "Kilometres", "Transmission", "Drivetrain", "Seats", "Body Style", "Colour", "Model"]
    for col in new_columns:
        if col not in df.columns: df[col] = "Pending"
    total_count = len(df)
    print(f"Database loaded: {total_count} records. Checking for missing details...")
    processed_count = 0
    for index, row in df.iterrows():
        if "Status" in df.columns and str(row["Status"]) == "Sold":
            continue
        current_val = str(row['Body Style'])
        if current_val != "Pending" and current_val != "nan" and current_val != "":
            continue
        raw_link = str(row['Link'])
        if raw_link == "nan" or raw_link == "": continue
        full_link = raw_link if raw_link.startswith("http") else "https://www.kijiji.ca" + raw_link.strip()
        print(f"[{index}/{total_count}] Processing ID {index}...", end="")
        try:
            req = requests.get(full_link, headers=HEADERS, timeout=15)
            if req.status_code == 200:
                soup = BeautifulSoup(req.text, "html.parser")
                page_data = extract_page_data(soup)
                for key, val in page_data.items(): 
                    df.at[index, key] = val
                print(f"Got: {page_data.get('Model', 'N/A')}")
                processed_count += 1
            elif req.status_code == 404:
                print("404 (Page Gone)")
                if "Status" in df.columns: df.at[index, "Status"] = "Sold"
            else:
                print(f"Status Code: {req.status_code}")  
        except Exception as e:
            print(f"Error: {e}")
        if processed_count > 0 and processed_count % SAVE_INTERVAL == 0:
            try: df.to_csv(filename, index=False, encoding="utf-8-sig")
            except: pass
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
    try: df.to_csv(filename, index=False, encoding="utf-8-sig")
    except: pass
    print(f"Finished! Processed {processed_count} new entries.")

if __name__ == "__main__":
    target_file = get_latest_file()
    if target_file:
        print(f"Found Database: {target_file}")
        scrape_details(target_file)
    else:
        print("Master database not found. Please run the list scraper first!")
        user = input("Or enter filename manually: ")
        if user.strip(): scrape_details(user.strip().strip('"'))