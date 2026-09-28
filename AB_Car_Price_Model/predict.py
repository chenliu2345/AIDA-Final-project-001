import hashlib
import json
from pathlib import Path

from model_quality import price_range
from catboost import CatBoostRegressor
import numpy as np
import pandas as pd

from etl import normalize, YEAR_MIN, YEAR_MAX, KMS_MAX
from settings import CAT_FEATURES, FEATURES, application_dir, resource_dir


def resolve_model_directory(root):
    """解析模型根目录的版本指针，同时支持直接打开单个模型版本目录。
    Resolve the model version pointer or accept an individual version folder directly."""
    root = Path(root).resolve()
    pointer = root / "current.json"
    if pointer.is_file():
        version = str(json.loads(pointer.read_text(encoding="utf-8"))["version"])
        candidate = (root / version).resolve()
        if candidate.parent != root:
            raise ValueError("模型版本路径无效。")
        return candidate
    return root


def load_model(root=None):
    """优先加载外置模型，否则使用 EXE 内置模型，并校验文件摘要与特征定义。
    Prefer an external model, otherwise load the bundled model; verify hashes and features."""
    roots = [Path(root)] if root else [application_dir() / "models", resource_dir() / "bundled_model"]
    for candidate in roots:
        directory = resolve_model_directory(candidate)
        if not (directory / "metadata.json").is_file():
            continue
        metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
        if metadata.get("features") != FEATURES or metadata.get("schema_version") != 4:
            raise ValueError("模型版本或特征定义与程序不兼容。")
        if not metadata.get("price_band_metrics") or not metadata.get("prediction_intervals"):
            raise ValueError("模型缺少误差报告或区间校准数据。")
        path = directory / "price_model.cbm"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != metadata.get("model_sha256"):
            raise ValueError("模型文件校验失败，请重新选择完整的模型目录。")
        model = CatBoostRegressor()
        model.load_model(str(path))
        if list(model.feature_names_) != FEATURES:
            raise ValueError("模型实际特征与清单不一致。")
        return model, metadata, directory
    raise FileNotFoundError("尚无可用模型。请先在训练页选择原始 CSV 并完成训练。")


def predict_price(model, metadata, values):
    """验证车辆参数并预测挂牌参考价，返回超出训练范围等提示，不调用网络服务。
    Validate vehicle inputs and estimate listing prices with training-range warnings, without network calls."""
    from datetime import date
    missing = set(CAT_FEATURES + ["Year", "Kilometres"]) - set(values)
    if missing:
        raise ValueError("缺少字段：" + ", ".join(sorted(missing)))
    try:
        year, km = float(values["Year"]), float(values["Kilometres"])
    except (ValueError, TypeError):
        raise ValueError("年份与里程必须是数字。") from None
    if not np.isfinite(year) or not year.is_integer() or not YEAR_MIN <= year <= YEAR_MAX:
        raise ValueError(f"年份必须在 {YEAR_MIN}–{YEAR_MAX} 之间。")
    if not np.isfinite(km) or not 0 <= km <= KMS_MAX:
        raise ValueError(f"里程必须在 0–{KMS_MAX} 公里之间。")
    row = {c: normalize(values[c]) for c in CAT_FEATURES}
    row.update(Year=int(year), Kilometres=km)
    warnings = []
    for column in ["Base_Model", "Trim", "City_Name"]:
        if row[column] not in metadata["catalog"][column] and "OTHER" in metadata["catalog"][column]:
            warnings.append(f"{column} 未见于训练类别，按原版低频类别规则使用 OTHER。")
            row[column] = "OTHER"
    if row["City_Name"] not in metadata["city_distances"]:
        raise ValueError("该城市没有已验证的道路距离，请在训练页更新模型及距离缓存。")
    row["Distance_from_Edmonton_KM"], row["Distance_from_Calgary_KM"] = metadata["city_distances"][row["City_Name"]]
    for column in CAT_FEATURES:
        if row[column] not in metadata["catalog"][column]:
            warnings.append(f"训练数据未出现类别：{column}={row[column]}")
    for column in ["Year", "Kilometres"]:
        low, high = metadata["numeric_ranges"][column]
        if not low <= row[column] <= high:
            warnings.append(f"{column} 超出训练数据范围 {low}–{high}")
    price = float(model.predict(pd.DataFrame([row])[FEATURES])[0])
    if not np.isfinite(price):
        raise ValueError("模型产生无效预测。")
    result = price_range(price, metadata["prediction_intervals"])
    result["listing_price_cad"] = round(price, 2)
    if price < 0:
        warnings.append("模型原始预测为负值，该车辆估价不稳定。")
    if result["lower_cad"] == 0:
        warnings.append("区间下限触及 $0，低价估值不稳定。")
    if result["pooled_fallback"]:
        warnings.append("该预测价档样本较少，区间采用整体残差参考。")
    result["warnings"] = warnings
    return result
