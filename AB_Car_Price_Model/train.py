import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from database import import_batch, initialize_database, read_training_batch, save_distances
from distances import ensure_distances
from etl import clean_csv, ETL_RULE_VERSION
from settings import CAT_FEATURES, FEATURES
from model_quality import quality_report


def split_holdout(frame):
    """复刻 Notebook 的随机 80/20 划分和种子 42，测试行仅用于评估模型评分。
    Reproduce the Notebook random 80/20 split with seed 42, reserving test rows for evaluation."""
    if len(frame) < 100:
        raise ValueError("训练至少需要 100 条合格 SOLD 记录。")
    return train_test_split(np.arange(len(frame)), test_size=0.20, random_state=42)


def make_model(iterations, threads=4):
    """复刻原版 CatBoost 参数；与原版实际执行一致，不传验证集而训练完整轮数。
    Reproduce CatBoost settings and full-iteration fitting without a validation set."""
    return CatBoostRegressor(iterations=iterations, depth=8, learning_rate=0.05,
                             l2_leaf_reg=3, subsample=0.8, loss_function="RMSE", random_seed=42,
                             cat_features=CAT_FEATURES, early_stopping_rounds=50, thread_count=threads,
                             allow_writing_files=False, verbose=False)


def evaluate_prices(actual, predicted):
    """以加元计算 MAE、RMSE 及 R²，保持评估与界面显示的价格尺度一致。
    Compute MAE, RMSE and R² in CAD, matching displayed price units."""
    return {"r2": float(r2_score(actual, predicted)),
            "mae_cad": float(mean_absolute_error(actual, predicted)),
            "rmse_cad": float(np.sqrt(mean_squared_error(actual, predicted)))}


def train_from_csv(csv_path, output_dir, server=".", iterations=2000, log=print):
    """串联清洗、SQL 入库、独立评估及全量训练，以原子目录切换方式发布可用模型。
    Chain ETL, SQL import, held-out evaluation and full-data fitting, then publish via an atomic version switch."""
    if iterations != 2000:
        raise ValueError("当前方案固定训练 2000 轮，不使用 Early Stopping。")
    csv_path, output_dir = Path(csv_path).resolve(), Path(output_dir).resolve()
    log("[1/6] 读取并清洗原始 CSV…")
    clean, audit = clean_csv(csv_path)
    log(f"原始 {len(audit):,} 行；接受 {len(clean):,} 行；拒绝 {len(audit)-len(clean):,} 行。")
    # 在产生数据库写入前，先确认清洗结果满足最低训练要求。
    # Check minimum training requirements before writing to the database.
    split_holdout(clean.loc[clean["Status"] == "SOLD"])
    log("[2/6] 初始化独立 SQL 数据库并保存导入审计…")
    initialize_database(server)
    batch_id = import_batch(csv_path, clean, audit, server)
    log("按最终提交版补全 Edmonton / Calgary 道路距离…")
    distances = ensure_distances(clean["City_Name"], output_dir / "distance_cache.json", log)
    save_distances(distances, server)
    frame = read_training_batch(batch_id, server)
    frame["Price_CAD"] = frame["Price_CAD"].astype(float)
    for column in CAT_FEATURES:
        frame[column] = frame[column].astype(str)
    frame = frame.sort_values("Source_Row").reset_index(drop=True)
    train_idx, test_idx = split_holdout(frame)
    X, y = frame[FEATURES], frame["Price_CAD"]
    log(f"[3/6] 原版随机 80/20 划分：训练 {len(train_idx):,} / 测试 {len(test_idx):,}；seed=42。")
    evaluation_model = make_model(iterations)
    evaluation_model.fit(X.iloc[train_idx], y.iloc[train_idx])
    best_iterations = int(evaluation_model.tree_count_)
    log(f"[4/6] 按原版完整训练 {best_iterations} 棵树；评估从未参与该模型训练的测试行…")
    predictions = evaluation_model.predict(X.iloc[test_idx])
    metrics = evaluate_prices(y.iloc[test_idx], predictions)
    quality, interval_audit = quality_report(y.iloc[test_idx], predictions)
    interval_audit.insert(0, "Source_Row", frame.iloc[test_idx]["Source_Row"].to_numpy())
    baseline = evaluate_prices(y.iloc[test_idx], np.full(len(test_idx), y.iloc[train_idx].mean()))
    log(f"测试 R²={metrics['r2']:.4f}；MAE=${metrics['mae_cad']:,.0f}；RMSE=${metrics['rmse_cad']:,.0f} CAD。")
    log("[5/6] 按原版脚本在全部合格 SOLD 记录上完整训练部署模型…")
    final_model = make_model(best_iterations)
    final_model.fit(X, y)
    # 原 Notebook 用全量拟合模型回评其中 20% 行；单独保留该口径，不能当作独立测试。
    # The original Notebook rescored 20% of rows with the full-data model; retain this diagnostic separately from held-out tests.
    fit_subset_predictions = final_model.predict(X.iloc[test_idx])
    fit_subset_metrics = evaluate_prices(y.iloc[test_idx], fit_subset_predictions)
    log(f"全量部署模型回评已见样本子集（非独立测试）：R²={fit_subset_metrics['r2']:.4f}；MAE=${fit_subset_metrics['mae_cad']:,.0f} CAD。")
    catalog = {column: sorted(frame[column].unique().tolist()) for column in CAT_FEATURES}
    trim_catalog = {name: sorted(group["Trim"].unique().tolist()) for name, group in frame.groupby("Base_Model")}
    metadata = {
        "schema_version": 4, "created_utc": datetime.now(timezone.utc).isoformat(),
        "target": "Observed listing price in CAD; not verified transaction price",
        "batch_id": batch_id, "source_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "raw_rows": len(audit), "accepted_rows": len(clean), "training_rows": len(frame), "rejected_rows": len(audit)-len(clean),
        "etl_rule_version": ETL_RULE_VERSION, "etl_status_counts": clean.attrs["status_counts"],
        "reposts_changed": clean.attrs["reposts_changed"], "training_filter": "Status = SOLD",
        "city_distances": distances,
        "features": FEATURES, "categorical_features": CAT_FEATURES,
        "best_iterations": best_iterations, "test_metrics": metrics, "baseline_metrics": baseline,
        "training_rule_version": "final-submit-catboost-full-iterations-v1",
        "model_parameters": final_model.get_params(),
        "split_counts": {"train": len(train_idx), "test": len(test_idx)},
        "split_seed": 42, "split_method": "random_rows_80_20",
        "groups_shared_across_row_split": len(set(frame.iloc[train_idx].Group_Key) & set(frame.iloc[test_idx].Group_Key)),
        "evaluation_policy": "Original Notebook random 80/20 row split, seed 42. evaluation_model.cbm fits only the 80% training rows for the full configured iterations; test_metrics are computed on the other 20%. price_model.cbm then refits all SOLD rows. No validation-based early stopping is applied, matching the original fit call.",
        "deployment_training_subset_metrics": fit_subset_metrics,
        "deployment_training_subset_policy": "Diagnostic reproduction of the original saved-model scoring path: deployment model has trained on these rows. These are NOT held-out test metrics.",
        "catalog": catalog, "trims_by_model": trim_catalog,
        "numeric_ranges": {column: [int(frame[column].min()), int(frame[column].max())] for column in ["Year", "Kilometres"]},
        "feature_importance": dict(zip(FEATURES, map(float, final_model.get_feature_importance()))),
    }
    metadata.update(quality)
    # 模型文件先写入独立版本目录，完整验证后再更新 current.json。
    # Write a separate version and validate it fully before updating current.json.
    output_dir.mkdir(parents=True, exist_ok=True)
    version_dir = output_dir / batch_id
    version_dir.mkdir()
    try:
        final_model.save_model(str(version_dir / "price_model.cbm"))
        evaluation_model.save_model(str(version_dir / "evaluation_model.cbm"))
        metadata["model_sha256"] = hashlib.sha256((version_dir / "price_model.cbm").read_bytes()).hexdigest()
        (version_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame(metadata["price_band_metrics"]).to_csv(version_dir / "price_band_metrics.csv", index=False, encoding="utf-8-sig")
        interval_audit.to_csv(version_dir / "interval_audit.csv", index=False, encoding="utf-8-sig")
        clean.to_csv(version_dir / "etl_cleaned.csv", index=False, encoding="utf-8-sig")
        heldout = frame.iloc[test_idx][["Source_Row", "Listing_Key", "Group_Key", "Price_CAD"]].copy()
        heldout["Predicted_Price_CAD"] = predictions
        heldout.to_csv(version_dir / "test_predictions.csv", index=False, encoding="utf-8-sig")
        fitted_subset = heldout.copy()
        fitted_subset["Predicted_Price_CAD"] = fit_subset_predictions
        fitted_subset["Seen_During_Training"] = True
        fitted_subset.to_csv(version_dir / "deployment_training_subset_predictions.csv", index=False, encoding="utf-8-sig")
        split_manifest = frame[["Source_Row", "Listing_Key", "Group_Key"]].copy()
        split_manifest["Split"] = "train"
        split_manifest.loc[test_idx, "Split"] = "test"
        split_manifest.to_csv(version_dir / "split_manifest.csv", index=False, encoding="utf-8-sig")
        rejected = [entry for entry in audit if entry["Reject_Reason"]]
        pd.DataFrame(rejected, columns=["Source_Row", "Raw_JSON", "Reject_Reason"]).to_csv(version_dir / "rejected_rows.csv", index=False, encoding="utf-8-sig")
        check = CatBoostRegressor()
        check.load_model(str(version_dir / "price_model.cbm"))
        if not np.allclose(check.predict(X.iloc[:5]), final_model.predict(X.iloc[:5])):
            raise RuntimeError("模型保存与重新加载后的预测不一致。")
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output_dir, suffix=".tmp", delete=False) as stream:
            json.dump({"version": batch_id}, stream)
            pointer = stream.name
        os.replace(pointer, output_dir / "current.json")
    except Exception:
        # 保留失败版本供排查，旧 current.json 和旧模型仍然可用。
        # Retain failed versions for diagnosis; preserve the previous current.json and model.
        raise
    log(f"[6/6] 训练完成，模型已发布：{version_dir}")
    return metadata


def main():
    """提供原始 CSV 到模型的一条命令入口，供批处理及自动化调用。
    Provide one-command raw-CSV-to-model training for batch and automation use."""
    parser = argparse.ArgumentParser(description="原始 Kijiji CSV → SQL Server → CatBoost 模型")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--output", default=str(Path(__file__).resolve().parent / "models"))
    parser.add_argument("--server", default=".")
    parser.add_argument("--iterations", type=int, default=2000)
    args = parser.parse_args()
    train_from_csv(args.csv, args.output, args.server, args.iterations)


if __name__ == "__main__":
    main()
