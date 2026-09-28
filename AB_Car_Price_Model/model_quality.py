import math
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

EDGES = [0,3000,6000,9000,12000,15000,18000,21000,24000,27000,30000,35000,40000,50000,75000]
LABELS = [f"${a//1000}–{b//1000}k" for a,b in zip(EDGES[:-1],EDGES[1:])] + ["$75k+"]


def band_index(price):
    """按预测价分档，部署时不依赖未知的实际价格。
    Select a predicted-price band without requiring the unknown actual price."""
    return int(np.searchsorted(EDGES, max(0, price), side="right") - 1)


def band_metrics(actual, predicted):
    """按实际价格输出十五档和总体误差；相对 MAE 为 MAE 除以平均实际价格。
    Report fifteen actual-price bands and overall metrics; relative MAE is MAE divided by mean actual price."""
    actual, predicted = np.asarray(actual, float), np.asarray(predicted, float)
    rows = []
    for i, label in enumerate(LABELS + ["Overall"]):
        mask = np.ones(len(actual), bool) if label == "Overall" else (actual >= EDGES[i]) & (actual < (EDGES[i+1] if i+1 < len(EDGES) else np.inf))
        y, p = actual[mask], predicted[mask]
        error = np.abs(y-p)
        variance = float(np.sum((y-y.mean())**2)) if len(y) else 0
        rows.append({"Actual Price": label, "N": int(len(y)),
                     "MAE": float(error.mean()) if len(y) else None,
                     "MedAE": float(np.median(error)) if len(y) else None,
                     "R²": float(1-np.sum((y-p)**2)/variance) if len(y)>1 and variance>0 else None,
                     "Relative MAE": float(error.mean()/y.mean()) if len(y) and y.mean()>0 else None})
    return rows


def price_range(prediction, reference):
    """采用预测价档残差分位数构造非负区间，并向外取整到百元。
    Build nonnegative intervals from predicted-band residual quantiles and round outward to hundreds of dollars."""
    if not np.isfinite(prediction):
        raise ValueError("预测价格无效。")
    item = reference["bands"][band_index(prediction)]
    lower = max(0, math.floor((prediction + item["q10"])/100)*100)
    upper = max(lower+100, math.ceil((prediction + item["q90"])/100)*100)
    return {"lower_cad": lower, "upper_cad": upper, "reference_band": item["label"],
            "calibration_n": item["reference_n"], "pooled_fallback": item["pooled_fallback"]}


def quality_report(actual, predicted):
    """将留出集再分为残差校准和覆盖率验证两部分，验证标签不参与区间拟合。
    Separate held-out calibration and coverage validation; do not fit intervals with validation labels."""
    actual, predicted = np.asarray(actual, float), np.asarray(predicted, float)
    if len(actual) != len(predicted) or not np.isfinite(actual).all() or not np.isfinite(predicted).all():
        raise ValueError("评估输入长度不一致或包含无效数字。")
    cal, val = train_test_split(np.arange(len(actual)), test_size=0.5, random_state=73)
    residual = actual[cal]-predicted[cal]
    buckets = np.array([band_index(p) for p in predicted[cal]])
    reference = {"method": "signed_residual_q10_q90_by_predicted_price", "nominal_coverage": 0.8,
                 "split_seed": 73, "calibration_n": len(cal), "validation_n": len(val), "minimum_band_n": 100,
                 "policy": "Coverage is measured on held-out evaluation-model rows, separate from residual calibration. Full-data deployment-model coverage is not independently validated; ranges are historical listing-price references, not per-car guarantees.", "bands": []}
    for i, label in enumerate(LABELS):
        local = residual[buckets == i]
        fallback = len(local) < 100
        sample = residual if fallback else local
        q10, q90 = np.quantile(sample, [0.1,0.9])
        reference["bands"].append({"label":label, "q10":float(q10), "q90":float(q90), "band_n":len(local), "reference_n":len(sample), "pooled_fallback":fallback})
    audit = pd.DataFrame({"Actual_Price_CAD":actual, "Predicted_Price_CAD":predicted, "Interval_Split":"calibration"})
    audit.loc[val,"Interval_Split"] = "validation"
    bounds = [price_range(p, reference) for p in predicted]
    audit["Lower_CAD"] = [b["lower_cad"] for b in bounds]
    audit["Upper_CAD"] = [b["upper_cad"] for b in bounds]
    checked = audit.iloc[val]
    reference["validation_coverage"] = float(((checked.Actual_Price_CAD >= checked.Lower_CAD) & (checked.Actual_Price_CAD <= checked.Upper_CAD)).mean())
    reference["validation_mean_width_cad"] = float((checked.Upper_CAD-checked.Lower_CAD).mean())
    return {"price_band_metrics":band_metrics(actual,predicted), "prediction_intervals":reference}, audit
