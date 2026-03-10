import pandas as pd
import re
import datetime
import os
from difflib import SequenceMatcher
MASTER_FILENAME = "Alberta_owner_sales_car.csv"
def clean_km(km_str):
    if pd.isna(km_str) or str(km_str).lower() in ["n/a", "pending", "nan", ""]:
        return None
    try:
        clean = re.sub(r'[^\d]', '', str(km_str))
        return int(clean)
    except:
        return None
def clean_price(val):
    try:
        return float(val)
    except:
        return 0.0
def clean_str(val):
    if pd.isna(val) or str(val).lower() in ["n/a", "pending", "nan"]:
        return None
    return str(val).strip().lower()
def extract_year_brand(title):
    title = str(title).lower()
    year_match = re.search(r'\b(19|20)\d{2}\b', title)
    year = year_match.group(0) if year_match else "0000"
    valid_brands = ["ford", "chev", "gmc", "dodge", "ram", "toyota", "honda", "nissan", "mazda", 
                    "vw", "volkswagen", "bmw", "mercedes", "audi", "kia", "hyundai", "jeep", 
                    "subaru", "lexus", "acura", "infiniti", "tesla", "porsche", "land rover"]
    brand = "unknown"
    for b in valid_brands:
        if b in title:
            brand = b
            break
    return year, brand
def get_title_similarity(t1, t2):
    return SequenceMatcher(None, str(t1).lower(), str(t2).lower()).ratio()
def process_reposts():
    print(f"========== Start: Anti-False-Positive Logic (Time-Locked) ==========")
    if not os.path.exists(MASTER_FILENAME):
        print(f"Error: {MASTER_FILENAME} not found.")
        return
    try:
        df = pd.read_csv(MASTER_FILENAME, dtype=str)
    except Exception as e:
        print(f"Error reading file: {e}")
        return
    if "Status" not in df.columns or "Sold_Date" not in df.columns:
        print("Error: Required columns missing.")
        return
    df['Scrape_Date_DT'] = pd.to_datetime(df['Scrape_Date'], errors='coerce')
    df['Sold_Date_DT'] = pd.to_datetime(df['Sold_Date'], errors='coerce')
    if df['Scrape_Date_DT'].isnull().all():
        print("Error: No valid dates found in Scrape_Date.")
        return
    today_dt = df['Scrape_Date_DT'].max() 
    yesterday_dt = today_dt - pd.Timedelta(days=1)
    print(f"Reference Date (Latest Scrape): {today_dt.strftime('%Y-%m-%d')}")
    print(f"Comparing against: {yesterday_dt.strftime('%Y-%m-%d')}")
    mask_new = (df["Status"] == "Active") & (df["Scrape_Date_DT"] == today_dt)
    new_candidates = df[mask_new].copy()
    mask_sold = (df["Status"] == "Sold") & (df["Sold_Date_DT"] >= yesterday_dt)
    sold_candidates = df[mask_sold].copy()
    print(f"Comparing: {len(sold_candidates)} recently sold (<= 1 day ago) vs {len(new_candidates)} new arrivals.")
    if sold_candidates.empty or new_candidates.empty:
        print("No candidates match the 24h time window.")
        return
    repost_count = 0
    for idx_new, row_new in new_candidates.iterrows():
        n_year, n_brand = extract_year_brand(row_new['Listing title'])
        n_price = clean_price(row_new['Price(CA$)'])
        n_km = clean_km(row_new.get('Kilometres'))
        n_title = str(row_new['Listing title'])
        n_date = row_new['Scrape_Date_DT']
        match_found = False
        match_old_index = -1
        for idx_old, row_old in sold_candidates.iterrows():
            o_date = row_old['Sold_Date_DT']
            if pd.isna(n_date) or pd.isna(o_date):
                continue 
            days_diff = (n_date - o_date).days
            if days_diff > 1 or days_diff < 0:
                continue 
            score = 0
            o_year, o_brand = extract_year_brand(row_old['Listing title'])
            if n_year != o_year: continue
            if n_brand != "unknown" and n_brand != o_brand: continue
            o_km = clean_km(row_old.get('Kilometres'))
            km_match_type = "none"
            if n_km is not None and o_km is not None:
                diff = abs(n_km - o_km)
                if diff == 0:
                    if n_km % 1000 == 0:
                        score += 30 
                        km_match_type = "rounded"
                    else:
                        score += 60 
                        km_match_type = "exact"
                elif diff < 1000:
                    score += 40
                    km_match_type = "close"
                elif diff > 5000:
                    score -= 100 
            o_title = str(row_old['Listing title'])
            sim = get_title_similarity(n_title, o_title)
            if sim > 0.8: score += 20
            elif sim < 0.3:
                if km_match_type != "exact": score -= 50 
            o_price = clean_price(row_old['Price(CA$)'])
            if o_price > 0:
                ratio = n_price / o_price
                if 0.8 < ratio < 1.1: score += 15
                elif ratio < 0.6 or ratio > 1.4: score -= 20
            n_color = clean_str(row_new.get('Colour'))
            o_color = clean_str(row_old.get('Colour'))
            if n_color and o_color:
                if n_color == o_color: score += 10
                else: score -= 20
            if score >= 60:
                match_found = True
                match_old_index = idx_old
                print(f"[REPOST FOUND] Score:{score} | Gap:{days_diff} days")
                print(f"   Old: {o_title}")
                print(f"   New: {n_title}")
                break
        if match_found:
            df.at[idx_new, "Status"] = "Active_Repost"
            df.at[match_old_index, "Status"] = "Reshelved"
            repost_count += 1
    df.drop(columns=['Scrape_Date_DT', 'Sold_Date_DT'], inplace=True)
    if repost_count > 0:
        df.to_csv(MASTER_FILENAME, index=False, encoding="utf-8-sig")
        print(f"\nSuccess! Marked {repost_count} cars as Repost/Reshelved.")
    else:
        print("\nNo reposts found matching criteria.")
if __name__ == "__main__":
    process_reposts()