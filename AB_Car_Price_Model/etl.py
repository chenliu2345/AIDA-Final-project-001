import hashlib
import json
import os
import re
import shutil
import tempfile
from difflib import SequenceMatcher
from pathlib import Path
import pandas as pd
ETL_RULE_VERSION = 'final-submit-aligned-v2'
VALID_STATUSES = {'SOLD', 'ACTIVE', 'ACTIVE_REPOST', 'RESHELVED'}
VALID_CONDITIONS = {'USED', 'DAMAGED', 'SALVAGE', 'LEASE TAKEOVER', 'UNKNOWN'}
VALID_TRANS = {'AUTOMATIC', 'MANUAL', 'SEMI-AUTOMATIC', 'OTHER', 'UNKNOWN'}
PRICE_MIN, PRICE_MAX = (1, 10000000)
KMS_MIN, KMS_MAX = (0, 2000000)
YEAR_MIN, YEAR_MAX = (1900, 2027)
KMS_BLACKLIST = {1, 99, 123, 1234, 12345, 123456, 999999, 111111}
_DOOR_SUFFIX_RE = re.compile(',\\s*(?:\\d+|OTHER)\\s+DOORS?$', re.IGNORECASE)

def clean_km(km_str):
    """按原版规则清洗重发匹配用的里程，去非数字，转换失败返回 None。
    Clean matching mileage with the original nondigit-removal rule; return None on failure."""
    if pd.isna(km_str) or str(km_str).lower() in ['n/a', 'pending', 'nan', '']:
        return None
    try:
        clean = re.sub('[^\\d]', '', str(km_str))
        return int(clean)
    except:
        return None

def clean_price(val):
    """按原版规则转换匹配用价格，转换失败返回零。
    Convert matching prices by the original rule, returning zero on failure."""
    try:
        return float(val)
    except:
        return 0.0

def clean_str(val):
    """按原版规则规范化匹配用颜色与缺失值。
    Normalize matching colours and missing values by the original rules."""
    if pd.isna(val) or str(val).lower() in ['n/a', 'pending', 'nan']:
        return None
    return str(val).strip().lower()

def extract_year_brand(title):
    """按原品牌列表顺序提取标题年份和品牌，仅用于重发匹配，不补齐训练年份。
    Extract title year and make in the original make-list order for matching only, without filling training years."""
    title = str(title).lower()
    year_match = re.search('\\b(19|20)\\d{2}\\b', title)
    year = year_match.group(0) if year_match else '0000'
    valid_brands = ['ford', 'chev', 'gmc', 'dodge', 'ram', 'toyota', 'honda', 'nissan', 'mazda', 'vw', 'volkswagen', 'bmw', 'mercedes', 'audi', 'kia', 'hyundai', 'jeep', 'subaru', 'lexus', 'acura', 'infiniti', 'tesla', 'porsche', 'land rover']
    brand = 'unknown'
    for b in valid_brands:
        if b in title:
            brand = b
            break
    return (year, brand)

def get_title_similarity(t1, t2):
    """使用 SequenceMatcher 计算小写标题相似度，保持原版评分行为。
    Compute lowercase title similarity with SequenceMatcher, preserving original scores."""
    return SequenceMatcher(None, str(t1).lower(), str(t2).lower()).ratio()

def process_reposts(master_filename):
    """按最新采集日的 0–1 天窗口与原评分规则识别重发，仅修改传入的临时工作副本。
    Detect reposts within the latest scrape date's 0–1 day window using original scores, modifying only a temporary copy."""
    if not os.path.exists(master_filename):
        return
    try:
        df = pd.read_csv(master_filename, dtype=str)
    except Exception as e:
        return
    if 'Status' not in df.columns or 'Sold_Date' not in df.columns:
        return
    df['Scrape_Date_DT'] = pd.to_datetime(df['Scrape_Date'], errors='coerce')
    df['Sold_Date_DT'] = pd.to_datetime(df['Sold_Date'], errors='coerce')
    if df['Scrape_Date_DT'].isnull().all():
        return
    today_dt = df['Scrape_Date_DT'].max()
    yesterday_dt = today_dt - pd.Timedelta(days=1)
    mask_new = (df['Status'] == 'Active') & (df['Scrape_Date_DT'] == today_dt)
    new_candidates = df[mask_new].copy()
    mask_sold = (df['Status'] == 'Sold') & (df['Sold_Date_DT'] >= yesterday_dt)
    sold_candidates = df[mask_sold].copy()
    if sold_candidates.empty or new_candidates.empty:
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
            if n_year != o_year:
                continue
            if n_brand != 'unknown' and n_brand != o_brand:
                continue
            o_km = clean_km(row_old.get('Kilometres'))
            km_match_type = 'none'
            if n_km is not None and o_km is not None:
                diff = abs(n_km - o_km)
                if diff == 0:
                    if n_km % 1000 == 0:
                        score += 30
                        km_match_type = 'rounded'
                    else:
                        score += 60
                        km_match_type = 'exact'
                elif diff < 1000:
                    score += 40
                    km_match_type = 'close'
                elif diff > 5000:
                    score -= 100
            o_title = str(row_old['Listing title'])
            sim = get_title_similarity(n_title, o_title)
            if sim > 0.8:
                score += 20
            elif sim < 0.3:
                if km_match_type != 'exact':
                    score -= 50
            o_price = clean_price(row_old['Price(CA$)'])
            if o_price > 0:
                ratio = n_price / o_price
                if 0.8 < ratio < 1.1:
                    score += 15
                elif ratio < 0.6 or ratio > 1.4:
                    score -= 20
            n_color = clean_str(row_new.get('Colour'))
            o_color = clean_str(row_old.get('Colour'))
            if n_color and o_color:
                if n_color == o_color:
                    score += 10
                else:
                    score -= 20
            if score >= 60:
                match_found = True
                match_old_index = idx_old
                break
        if match_found:
            df.at[idx_new, 'Status'] = 'Active_Repost'
            df.at[match_old_index, 'Status'] = 'Reshelved'
            repost_count += 1
    df.drop(columns=['Scrape_Date_DT', 'Sold_Date_DT'], inplace=True)
    if repost_count > 0:
        df.to_csv(master_filename, index=False, encoding='utf-8-sig')
    else:
        pass

def clean_car_sales_data(input_file='Alberta_owner_sales_car.csv', output_file='Alberta_owner_sales_car_clean.csv', columns_to_fill=None):
    """只对最终提交版指定的七个属性列填充 Unknown，保存清洗中间文件。
    Fill Unknown only in the seven specified attributes and save intermediate cleaning output."""
    if columns_to_fill is None:
        columns_to_fill = ['Condition', 'Transmission', 'Drivetrain', 'Seats', 'Body Style', 'Colour', 'Model']
    df = pd.read_csv(input_file)
    cols_to_fix = [col for col in columns_to_fill if col in df.columns]
    for col in cols_to_fix:
        df[col] = df[col].fillna('Unknown')
    df.to_csv(output_file, index=False)
    return df

def optimize_car_data(input_file='Alberta_owner_sales_car_clean.csv', output_file='Optimized_Alberta_owner_sales_car_clean.csv'):
    """严格从 Model 拆分年份、配置及车型；三类频数小于 10 时归入 OTHER，地点在分箱后才转大写。
    Split year, trim and model strictly from Model; bucket frequencies below 10 as OTHER, then uppercase locations."""
    df_raw = pd.read_csv(input_file)
    df_cleaned = df_raw.copy()
    if 'Model' in df_cleaned.columns:
        df_cleaned['Year'] = df_cleaned['Model'].str.extract('^(\\d{4})')
        df_cleaned['Year'] = pd.to_numeric(df_cleaned['Year'], errors='coerce')
        df_cleaned['Trim'] = df_cleaned['Model'].str.extract(',\\s*(.*)$')
        df_cleaned['Trim'] = df_cleaned['Trim'].fillna('UNKNOWN').str.strip().str.upper()
        df_cleaned['Base_Model'] = df_cleaned['Model'].str.extract('^\\d{4}\\s+(.*?)(?:,|$)')
        df_cleaned['Base_Model'] = df_cleaned['Base_Model'].str.strip().str.upper()
        df_cleaned = df_cleaned.drop(columns=['Model'])
    binning_config = {'Base_Model': 10, 'Trim': 10, 'Location': 10}
    for col, threshold in binning_config.items():
        if col in df_cleaned.columns:
            counts = df_cleaned[col].value_counts()
            to_keep = counts[counts >= threshold].index
            df_cleaned[col] = df_cleaned[col].where(df_cleaned[col].isin(to_keep), 'OTHER')
    categorical_cols = ['Transmission', 'Drivetrain', 'Seats', 'Body Style', 'Colour', 'Condition']
    for col in categorical_cols:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].fillna('UNKNOWN').str.strip().str.upper()
    df_cleaned.to_csv(output_file, index=False)
    return df_cleaned

def _normalize_body_style(value) -> str:
    """按原版正则移除车门数量后缀，处理空值和空字符串。
    Remove door-count suffixes with the original regex and handle null or empty strings."""
    if pd.isna(value):
        return 'UNKNOWN'
    cleaned = _DOOR_SUFFIX_RE.sub('', str(value)).strip()
    return cleaned if cleaned else 'UNKNOWN'

def _clean_kilometres(value) -> int:
    """复现入库前的里程转换：仅去逗号和首尾空格，转换失败置零。
    Convert pre-import mileage by removing commas and outer whitespace only; return zero on failure."""
    try:
        return int(str(value).replace(',', '').strip())
    except (ValueError, AttributeError):
        return 0

def _hash_url(url: str) -> str:
    """对完整原始 URL 计算 SHA-256，不删查询参数，不额外按链接去重。
    Hash complete raw URLs with SHA-256, preserving query parameters and adding no URL deduplication."""
    return hashlib.sha256(str(url).encode('utf-8')).hexdigest()

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """复现原版字段重命名、数值日期转换、类别统一、URL 哈希及标题截断。
    Reproduce field renaming, numeric/date conversion, category normalization, URL hashes and title truncation."""
    df = df.rename(columns={'Listing title': 'Listing_Title', 'Price(CA$)': 'Price_CAD', 'Link': 'Link_URL', 'Location': 'City_Name', 'Body Style': 'Body_Style'})
    df['Kilometres'] = df['Kilometres'].apply(_clean_kilometres)
    df['Price_CAD'] = pd.to_numeric(df['Price_CAD'], errors='coerce').fillna(0)
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce').fillna(0).astype(int)
    df['Scrape_Date'] = pd.to_datetime(df['Scrape_Date'], errors='coerce').dt.date
    df['Sold_Date'] = pd.to_datetime(df['Sold_Date'], errors='coerce').dt.date
    df['Body_Style'] = df['Body_Style'].apply(_normalize_body_style)
    for col in ['Transmission', 'Drivetrain', 'Seats', 'Colour', 'Condition', 'Status', 'Trim', 'Base_Model', 'City_Name']:
        df[col] = df[col].fillna('UNKNOWN').str.strip().str.upper()
    df['Link_URL_Hash'] = df['Link_URL'].apply(_hash_url)
    df['Listing_Title'] = df['Listing_Title'].str[:500]
    return df

def validate(df: pd.DataFrame):
    """按原版七条规则和相同的失败优先级返回合格行及拒绝行，数据库写入由调用方负责。
    Apply the original seven rules in the same rejection order; let the caller write accepted and rejected rows."""
    price_ok = df['Price_CAD'].between(PRICE_MIN, PRICE_MAX)
    kms_range = df['Kilometres'].between(KMS_MIN, KMS_MAX)
    kms_real = ~df['Kilometres'].isin(KMS_BLACKLIST)
    year_ok = df['Year'].between(YEAR_MIN, YEAR_MAX)
    status_ok = df['Status'].isin(VALID_STATUSES)
    cond_ok = df['Condition'].isin(VALID_CONDITIONS)
    trans_ok = df['Transmission'].isin(VALID_TRANS)
    all_ok = price_ok & kms_range & kms_real & year_ok & status_ok & cond_ok & trans_ok
    clean_df = df[all_ok].reset_index(drop=True)
    rejected_df = df[~all_ok].copy()
    if not rejected_df.empty:
        priority = [(~price_ok, 'Price_CAD out of range'), (~kms_range, 'Kilometres out of range'), (~kms_real, 'Kilometres is a placeholder'), (~year_ok, 'Year out of range'), (~status_ok, 'Status invalid'), (~cond_ok, 'Condition invalid'), (~trans_ok, 'Transmission invalid')]
        reason_series = pd.Series('Unknown', index=df.index)
        for mask, label in reversed(priority):
            reason_series[mask] = label
        rejected_df['Reject_Reason'] = reason_series[~all_ok].values
    return (clean_df, rejected_df)


def normalize(value):
    """按原 transform 的规则处理单个预测类别，不擅自改写类别或内部空格。
    Normalize a prediction category without changing the original categories or internal whitespace."""
    return "UNKNOWN" if pd.isna(value) else str(value).strip().upper()


def assign_groups(frame, raw):
    """联合完整 URL 与未分箱车辆指纹生成分组，用于审计随机划分中的同车重叠情况。
    Group full URLs and unbucketed vehicle fingerprints to audit same-car overlap across random splits."""
    parents = list(range(len(frame)))
    seen = {}

    def find(index):
        """取得并压缩车辆分组的代表索引。
        Find and path-compress the representative vehicle-group index."""
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    for index, row in enumerate(frame.to_dict("records")):
        original = raw.iloc[int(row["Source_Row"]) - 1]
        base = re.sub(r"^\d{4}\s+", "", normalize(original["Model"])).split(",", 1)[0]
        for key in ["url:" + row["Link_URL_Hash"], f"car:{base}|{row['Year']}|{row['Kilometres']}"]:
            if key in seen:
                parents[find(index)] = find(seen[key])
            else:
                seen[key] = index
    return [hashlib.sha256(f"group:{find(i)}".encode()).hexdigest() for i in range(len(frame))]


def clean_csv(path):
    """按最终提交版全部清洗步骤及 CSV 读写顺序执行，保留合法状态和重复行，不修改输入文件。
    Run all final-submission cleaning steps in the original CSV read/write order, preserving valid statuses and duplicates without changing input."""
    raw = pd.read_csv(path, encoding="utf-8-sig")
    required = {"Listing title", "Price(CA$)", "Link", "Location", "Scrape_Date", "Status", "Sold_Date",
                "Condition", "Kilometres", "Transmission", "Drivetrain", "Seats", "Body Style", "Colour", "Model"}
    missing = required - set(raw.columns)
    if missing:
        raise ValueError("CSV 缺少原始字段：" + ", ".join(sorted(missing)))
    with tempfile.TemporaryDirectory(prefix="ab_car_etl_") as directory:
        work = Path(directory)
        master, filled_path, optimized_path = work / "raw.csv", work / "clean.csv", work / "optimized.csv"
        shutil.copy2(path, master)
        process_reposts(str(master))
        reposts = pd.read_csv(master)
        clean_car_sales_data(str(master), str(filled_path))
        optimize_car_data(str(filled_path), str(optimized_path))
        transformed = transform(pd.read_csv(optimized_path, encoding="utf-8"))
    transformed["Source_Row"] = range(1, len(transformed) + 1)
    accepted, rejected = validate(transformed)
    if accepted.empty:
        raise ValueError("CSV 没有通过最终提交版清洗规则的记录。")
    rejected_by_row = dict(zip(rejected["Source_Row"], rejected.get("Reject_Reason", [])))
    audit = []
    for index, row in raw.iterrows():
        payload = row.astype(object).where(pd.notna(row), None).to_dict()
        payload.pop("Link", None)
        payload["Link_SHA256"] = transformed.at[index, "Link_URL_Hash"]
        audit.append({"Source_Row": index + 1, "Raw_JSON": json.dumps(payload, ensure_ascii=False),
                      "Reject_Reason": rejected_by_row.get(index + 1, "")})
    accepted["Group_Key"] = assign_groups(accepted, raw)
    accepted["Listing_Key"] = accepted["Link_URL_Hash"]
    accepted = accepted.rename(columns={"Condition": "Condition_Label", "Transmission": "Transmission_Type",
                                        "Drivetrain": "Drivetrain_Type", "Seats": "Seats_Count"}).drop(columns=["Link_URL"])
    accepted.attrs["etl_rule_version"] = ETL_RULE_VERSION
    accepted.attrs["status_counts"] = accepted["Status"].value_counts().to_dict()
    accepted.attrs["reposts_changed"] = int((reposts["Status"].fillna("") != raw["Status"].fillna("")).sum())
    return accepted, audit
