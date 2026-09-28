import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
import requests

from settings import application_dir, resource_dir

ALIASES = {
    "STRATHCONA COUNTY": "Sherwood Park", "ROCKY VIEW COUNTY": "Airdrie", "FOOTHILLS COUNTY": "Okotoks",
    "TSUU T'INA": "Calgary", "BANFF / CANMORE": "Canmore", "SOUTH LETHBRIDGE": "Lethbridge",
    "ALBERTA": "Edmonton", "OTHER": "Red Deer", "LANGDON": "Chestermere", "ACADIA": "Calgary",
    "SWEET GRASS": "Edmonton", "FOOTHILLS": "Calgary", "NORTHEAST CALGARY": "Calgary",
    "NORTHWEST CALGARY": "Calgary", "SOUTHEAST CALGARY": "Calgary", "SOUTHWEST CALGARY": "Calgary",
    "NORTH CENTRAL EDMONTON": "Edmonton", "SOUTHEAST EDMONTON": "Edmonton", "NORTHWEST EDMONTON": "Edmonton",
    "WEST EDMONTON": "Edmonton", "NORTHEAST EDMONTON": "Edmonton", "SOUTHWEST EDMONTON": "Edmonton",
}
# 新数据中的社区和县使用代表城市道路距离，沿用原版地区别名规则；不代表每条街道的精确距离。
# Communities and counties use representative-city distances under the original aliases, not exact street-level distances.
# Lethbridge 官方社区地图：https://www.lethbridge.ca/community-services-supports/neighbourhood-maps/
# Official Lethbridge neighbourhood maps: https://www.lethbridge.ca/community-services-supports/neighbourhood-maps/
# Sturgeon County 县政中心：https://www.sturgeoncounty.ca/contact-sturgeon-county/
# Sturgeon County administration centre: https://www.sturgeoncounty.ca/contact-sturgeon-county/
# Parkland County 地址：https://www.parklandcounty.com/county-government/contact-us/
# Parkland County address: https://www.parklandcounty.com/county-government/contact-us/
SUPPLEMENTAL_ALIASES = {
    "INDIAN BATTLE HEIGHTS": "Lethbridge", "NORTH LETHBRIDGE": "Lethbridge",
    "WEST LETHBRIDGE": "Lethbridge", "RED DEER COUNTY": "Red Deer",
    "PARKLAND COUNTY": "Stony Plain", "STURGEON COUNTY": "Morinville",
    "SAINT PAUL": "St. Paul", "TIMBERLEA": "Fort McMurray", "UNIVERSITY AREA": "Edmonton",
}
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
ORS_URL = "https://api.openrouteservice.org/v2/directions/driving-car"


def api_key():
    """仅从环境变量或项目本地 .env 读取密钥，禁止把密钥写入模型、缓存或日志。
    Read keys only from environment variables or local .env; never save them in models, caches or logs."""
    base = application_dir()
    if base.name.lower() == "dist":
        load_dotenv(base.parent / ".env", override=True, encoding="utf-8-sig")
    load_dotenv(base / ".env", override=True, encoding="utf-8-sig")
    return os.environ.get("ORS_API_KEY", "").strip()


def get_lat_lon(city, session):
    """保留原版城市别名，限定加拿大并扩大候选集，避免同名城市被定位到其他省份。
    Preserve city aliases and restrict candidates to Canada, broadening search to avoid wrong-province matches."""
    response = session.get(GEOCODING_URL, params={"name": city, "count": 100, "countryCode": "CA"}, timeout=20)
    response.raise_for_status()
    results = response.json().get("results", [])
    ordered = [place for place in results if place.get("admin1") == "Alberta" and place.get("country_code") == "CA"]
    # Lloydminster 跨 Alberta / Saskatchewan 省界，允许其加拿大同名城市坐标。
    # Lloydminster straddles Alberta and Saskatchewan, so its Canadian city coordinates are allowed.
    if not ordered and city.upper() == "LLOYDMINSTER":
        ordered = [place for place in results if place.get("country_code") == "CA"
                   and place.get("name", "").upper() == "LLOYDMINSTER"
                   and abs(place["longitude"] + 110) < 0.2 and abs(place["latitude"] - 53.28) < 0.2]
    if not ordered:
        raise ValueError(f"无法取得 {city} 的坐标，距离补全未完成。")
    return ordered[0]["longitude"], ordered[0]["latitude"]


def road_distance(origin, destination, key, session):
    """调用原版 ORS driving-car 路线端点，米转公里并保留两位小数，相同坐标返回零。
    Call ORS driving-car, convert metres to kilometres with two decimals, and return zero for identical coordinates."""
    if origin == destination:
        return 0.0
    time.sleep(2)
    response = session.post(ORS_URL, json={"coordinates": [origin, destination]},
                            headers={"Authorization": key, "Content-Type": "application/json"}, timeout=30)
    # 地名中心点可能偏离道路；仅在明确的 2010 定位错误时扩大至 1 公里，不填造距离。
    # Expand the radius to 1 km only for explicit positioning error 2010; never fabricate distances.
    if response.status_code == 404 and response.json().get("error", {}).get("code") == 2010:
        time.sleep(2)
        response = session.post(ORS_URL, json={"coordinates": [origin, destination], "radiuses": [1000, 1000]},
                                headers={"Authorization": key, "Content-Type": "application/json"}, timeout=30)
    if response.status_code in (401, 403):
        raise ValueError("ORS 密钥无效或没有路线权限，请检查项目 .env 中的 ORS_API_KEY。")
    if response.status_code == 429:
        raise ValueError("ORS 调用达到限额；已成功取得的城市距离已缓存，稍后重新训练可继续。")
    response.raise_for_status()
    return round(response.json()["routes"][0]["summary"]["distance"] / 1000, 2)


def ensure_distances(cities, cache_path, log=print):
    """为每个清洗后城市补全真实道路距离，逐城市保存缓存；缺失时明确失败，不使用直线距离或假数据。
    Complete real road distances and cache each city; fail rather than fabricate missing distances or use straight lines."""
    cache_path = Path(cache_path)
    cached = {}
    for path in [resource_dir() / "distance_cache.json", cache_path]:
        if path.is_file():
            cached.update(json.loads(path.read_text(encoding="utf-8"))["cities"])
    cities = sorted(set(cities))
    missing = [city for city in cities if city not in cached]
    if not missing:
        return {city: cached[city] for city in cities}
    key = api_key()
    if not key:
        raise ValueError("原版距离特征需要 ORS_API_KEY。请在 AB_Car_Price_Model/.env 配置后重新训练。")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.Session() as session:
        edmonton, calgary = get_lat_lon("Edmonton", session), get_lat_lon("Calgary", session)
        aliases = {**ALIASES, **SUPPLEMENTAL_ALIASES}
        canonical = {aliases.get(name, name).upper(): value for name, value in cached.items()}
        for index, city in enumerate(missing, 1):
            lookup = aliases.get(city, city)
            if lookup.upper() in canonical:
                distances = canonical[lookup.upper()]
            else:
                point = get_lat_lon(lookup, session)
                distances = [road_distance(edmonton, point, key, session), road_distance(calgary, point, key, session)]
                canonical[lookup.upper()] = distances
            cached[city] = distances
            temporary = cache_path.with_suffix(".tmp")
            temporary.write_text(json.dumps({"source": "OpenRouteService driving-car; original and recorded supplemental city aliases", "supplemental_aliases": SUPPLEMENTAL_ALIASES, "cities": cached}, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temporary, cache_path)
            log(f"距离 {index}/{len(missing)}：{city} → Edmonton {distances[0]} km / Calgary {distances[1]} km")
    return {city: cached[city] for city in cities}
