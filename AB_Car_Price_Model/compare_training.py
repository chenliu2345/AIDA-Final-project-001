import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import pickle
import time

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.model_selection import GroupShuffleSplit, train_test_split

from database import initialize_database, import_batch, save_distances, read_training_batch
from distances import ensure_distances, SUPPLEMENTAL_ALIASES
from etl import clean_csv, ETL_RULE_VERSION
from settings import NUM_FEATURES, CAT_FEATURES
from train import evaluate_prices

MY_CATEGORIES = ['Base_Model', 'Trim', 'City_Name', 'Condition_Label', 'Transmission_Type',
                 'Drivetrain_Type', 'Body_Style', 'Colour', 'Seats_Count']


def write_json(path, value):
    """以可读 JSON 保存训练配置和真实测量结果，不输出或保存 API 密钥。
    Save settings and measured results as readable JSON without exposing API keys."""
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def prepare_data(csv_path, output, server):
    """只清洗并入库一次，给四个方案提供完全相同的 SOLD 数据和道路距离。
    Clean and import once so all four schemes share identical SOLD data and distances."""
    digest = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    prepared_path = output / 'dataset.json'
    if prepared_path.is_file():
        info = json.loads(prepared_path.read_text(encoding='utf-8'))
        if info['source_sha256'] != digest:
            raise ValueError('输出目录已有另一份数据的对比，请换一个输出目录。')
        return pd.read_pickle(output / 'training_frame.pkl'), info
    checkpoint = output / 'etl_checkpoint.pkl'
    if checkpoint.is_file():
        with checkpoint.open('rb') as stream:
            saved_hash, clean, audit = pickle.load(stream)
        if saved_hash != digest:
            raise ValueError('清洗检查点与当前 CSV 不一致。')
    else:
        print('[DATA] 按最终提交版清洗新 CSV…', flush=True)
        clean, audit = clean_csv(csv_path)
        with checkpoint.open('wb') as stream:
            pickle.dump((digest, clean, audit), stream)
        clean.to_csv(output / 'etl_cleaned.csv', index=False, encoding='utf-8-sig')
        pd.DataFrame([row for row in audit if row['Reject_Reason']]).to_csv(output / 'rejected_rows.csv', index=False, encoding='utf-8-sig')
    print(f'[DATA] 原始 {len(audit)}；合格 {len(clean)}；拒绝 {len(audit)-len(clean)}；SOLD {(clean.Status == "SOLD").sum()}；城市 {clean.City_Name.nunique()}。', flush=True)
    distances = ensure_distances(clean.City_Name, Path(__file__).parent / 'models' / 'distance_cache.json')
    initialize_database(server)
    batch_id = import_batch(csv_path, clean, audit, server)
    save_distances(distances, server)
    frame = read_training_batch(batch_id, server).sort_values('Source_Row').reset_index(drop=True)
    frame.Price_CAD = frame.Price_CAD.astype(float)
    for column in CAT_FEATURES:
        frame[column] = frame[column].astype(str)
    if frame[NUM_FEATURES + CAT_FEATURES + ['Price_CAD']].isna().any().any():
        raise ValueError('训练数据仍有缺失特征。')
    frame.to_pickle(output / 'training_frame.pkl')
    info = {'source': str(csv_path), 'source_sha256': digest, 'batch_id': batch_id,
            'etl_rule_version': ETL_RULE_VERSION, 'raw_rows': len(audit), 'accepted_rows': len(clean),
            'rejected_rows': len(audit)-len(clean), 'sold_rows': len(frame),
            'status_counts': clean.attrs['status_counts'], 'reposts_changed': clean.attrs['reposts_changed'],
            'cities': len(distances), 'city_distances': distances, 'supplemental_location_aliases': SUPPLEMENTAL_ALIASES}
    write_json(prepared_path, info)
    return frame, info


def define_splits(frame):
    """拟合前固定四套划分；随机方案共用外层测试集，早停方案仅从训练部分划出验证集。
    Fix all splits before fitting; random schemes share an outer test set and early stopping uses validation rows from training only."""
    if len(frame) < 100 or frame.Group_Key.nunique() < 50:
        raise ValueError('合格 SOLD 样本或车辆分组不足。')
    schemes = {}
    for name, test_fraction, validation_fraction in [('my_scheme', .20, .20), ('grouped_70_15_15_es', .15, .15)]:
        development, test = next(GroupShuffleSplit(n_splits=1, test_size=test_fraction, random_state=42).split(frame, groups=frame.Group_Key))
        subset = frame.iloc[development]
        train_relative, validation_relative = next(GroupShuffleSplit(n_splits=1, test_size=validation_fraction/(1-test_fraction), random_state=43).split(subset, groups=subset.Group_Key))
        train, validation = development[train_relative], development[validation_relative]
        groups = [set(frame.iloc[indices].Group_Key) for indices in [train, validation, test]]
        assert not (groups[0] & groups[1] or groups[0] & groups[2] or groups[1] & groups[2])
        schemes[name] = (train, validation, test)
    user_train, user_test = train_test_split(np.arange(len(frame)), test_size=.2, random_state=42)
    es_train, es_validation = train_test_split(user_train, test_size=.2, random_state=43)
    schemes['original_scheme'] = (user_train, np.array([], dtype=int), user_test)
    schemes['random_80_20_es'] = (es_train, es_validation, user_test)
    common = np.arange(len(frame))
    for train, validation, test in schemes.values():
        assert not (set(train) & set(validation) or set(train) & set(test) or set(validation) & set(test))
        assert len(train)+len(validation)+len(test) == len(frame)
        common = np.intersect1d(common, test)
    if len(common) < 2:
        raise ValueError('共同测试集不足两行，无法计算 R²。')
    return schemes, common


def make_comparison_model(categories, iterations, original):
    """保留两套历史方案各自的类别顺序及参数，统一线程数和随机种子。
    Preserve historical category orders and parameters while standardizing threads and seed."""
    params = dict(iterations=iterations, depth=8, learning_rate=.05, l2_leaf_reg=3,
                  subsample=.8, loss_function='RMSE', random_seed=42, cat_features=categories,
                  thread_count=4, allow_writing_files=False, verbose=False)
    if original:
        params['early_stopping_rounds'] = 50
    return CatBoostRegressor(**params)


def prediction_values(model, frame, features, indices, clip):
    """按方案自己的特征顺序预测；原助手方案保留其评估时的非负截断规则。
    Predict in each scheme's feature order; retain nonnegative clipping in the original assistant scheme."""
    values = model.predict(frame.iloc[indices][features])
    return np.maximum(values, 0) if clip else values


def run_scheme(name, frame, indices, common, output, iterations):
    """训练并保存该方案的独立评估模型及全量部署模型，严格区分未见样本与训练回评。
    Save evaluation and full-data models separately, distinguishing held-out scores from training-data rescoring."""
    folder = output / name
    folder.mkdir(exist_ok=True)
    original = name in {'original_scheme', 'random_80_20_es'}
    early_stopping = name != 'original_scheme'
    categories = CAT_FEATURES if original else MY_CATEGORIES
    features = NUM_FEATURES + categories
    train_indices, validation_indices, test_indices = indices
    manifest = frame[['Source_Row', 'Listing_Key', 'Group_Key']].copy()
    manifest['Split'] = 'train'
    manifest.loc[validation_indices, 'Split'] = 'validation'
    manifest.loc[test_indices, 'Split'] = 'test'
    manifest['Common_Test'] = manifest.index.isin(common)
    manifest.to_csv(folder / 'split_manifest.csv', index=False, encoding='utf-8-sig')
    frame.iloc[test_indices][['Source_Row', 'Group_Key'] + features + ['Price_CAD']].to_csv(folder / 'test_dataset.csv', index=False, encoding='utf-8-sig')
    started = time.monotonic()
    print(f'[{name}] 开始评估模型：训练 {len(train_indices)}，验证 {len(validation_indices)}，测试 {len(test_indices)}。', flush=True)
    model = make_comparison_model(categories, iterations, original)
    fit_args = {} if not early_stopping else {'eval_set': (frame.iloc[validation_indices][features], frame.iloc[validation_indices].Price_CAD), 'early_stopping_rounds': 50, 'use_best_model': True}
    model.fit(frame.iloc[train_indices][features], frame.iloc[train_indices].Price_CAD, **fit_args)
    model.save_model(str(folder / 'evaluation_model.cbm'))
    own_predictions = prediction_values(model, frame, features, test_indices, not original)
    common_predictions = prediction_values(model, frame, features, common, not original)
    own_metrics = evaluate_prices(frame.iloc[test_indices].Price_CAD, own_predictions)
    common_metrics = evaluate_prices(frame.iloc[common].Price_CAD, common_predictions)
    for label, selected, predictions in [('test', test_indices, own_predictions), ('common_test', common, common_predictions)]:
        rows = frame.iloc[selected][['Source_Row', 'Listing_Key', 'Group_Key', 'Price_CAD']].copy()
        rows['Predicted_Price_CAD'] = predictions
        rows.to_csv(folder / f'{label}_predictions.csv', index=False, encoding='utf-8-sig')
    result = {'scheme': name, 'early_stopping': early_stopping, 'early_stopping_patience': 50 if early_stopping else None, 'clip_negative_predictions': not original, 'features': features, 'model_parameters': model.get_params(),
              'evaluation_trees': int(model.tree_count_), 'split_counts': {'train': len(train_indices), 'validation': len(validation_indices), 'test': len(test_indices)},
              'test_metrics': own_metrics, 'common_test_rows': len(common), 'common_test_metrics': common_metrics,
              'test_groups_shared_with_training': len(set(frame.iloc[train_indices].Group_Key) & set(frame.iloc[test_indices].Group_Key)),
              'common_test_groups_shared_with_training': len(set(frame.iloc[train_indices].Group_Key) & set(frame.iloc[common].Group_Key)),
              'evaluation_seconds': time.monotonic()-started}
    write_json(folder / 'evaluation.json', result)
    print(f'[{name}] 测试 R²={own_metrics["r2"]:.6f} MAE={own_metrics["mae_cad"]:.2f} CAD；树数 {model.tree_count_}。开始全量拟合…', flush=True)
    final = make_comparison_model(categories, int(model.tree_count_), original)
    final.fit(frame[features], frame.Price_CAD)
    final.save_model(str(folder / 'price_model.cbm'))
    seen = prediction_values(final, frame, features, test_indices, not original)
    result['seen_subset_metrics_NOT_a_test'] = evaluate_prices(frame.iloc[test_indices].Price_CAD, seen)
    result['seen_subset_policy'] = 'This deployment model was fitted on every SOLD row, including all rows scored here. These are training reuse metrics, not held-out test performance.'
    result['elapsed_seconds'] = time.monotonic()-started
    result['evaluation_model_sha256'] = hashlib.sha256((folder / 'evaluation_model.cbm').read_bytes()).hexdigest()
    result['deployment_model_sha256'] = hashlib.sha256((folder / 'price_model.cbm').read_bytes()).hexdigest()
    saved = CatBoostRegressor()
    saved.load_model(str(folder / 'price_model.cbm'))
    np.testing.assert_allclose(saved.predict(frame.iloc[common[:10]][features]), final.predict(frame.iloc[common[:10]][features]))
    write_json(folder / 'result.json', result)
    print(f'[{name}] 全部完成；训练回评 R²={result["seen_subset_metrics_NOT_a_test"]["r2"]:.6f}（非独立测试）。', flush=True)
    return result


def main():
    """从目标项目原始 CSV 一键执行四方案比较，保存结果和模型，不改动当前 EXE 的模型指针。
    Run all four comparisons from raw CSV without changing the EXE model pointer."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', default=str(Path(__file__).parent / 'data' / 'Alberta_owner_sales_car.csv'))
    parser.add_argument('--output', required=True)
    parser.add_argument('--server', default='.')
    parser.add_argument('--iterations', type=int, default=2000)
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    frame, info = prepare_data(Path(args.csv).resolve(), output, args.server)
    splits, common = define_splits(frame)
    print(f'[COMPARE] 四套方案共同未训练的测试行：{len(common)}。', flush=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {name: pool.submit(run_scheme, name, frame, indices, common, output, args.iterations) for name, indices in splits.items()}
        results = {name: future.result() for name, future in futures.items()}
    report = {'created_utc': datetime.now(timezone.utc).isoformat(), 'dataset': info, 'iterations_limit': args.iterations,
              'comparison_policy': 'Same cleaned SOLD data and 13 feature meanings; original per-scheme category order, split and early-stopping policy are retained. Grouped fractions refer to groups, so row fractions may differ. Own test sets differ. Common test is the intersection of all four test sets, fixed before training. No metrics are used to tune parameters.',
              'random_early_stopping_policy': 'Outer random 80/20 split is identical to original_scheme; 20% of its 80% development portion becomes validation (64/16/20 overall), seed 43. Test is never used for early stopping.', 'results': results}
    write_json(output / 'comparison.json', report)
    summary = []
    for name, result in results.items():
        for scope, metrics in [('own_holdout', result['test_metrics']), ('common_holdout', result['common_test_metrics']), ('seen_subset_NOT_a_test', result['seen_subset_metrics_NOT_a_test'])]:
            summary.append({'scheme': name, 'evaluation': scope, 'r2': metrics['r2'], 'mae_cad': metrics['mae_cad'], 'rmse_cad': metrics['rmse_cad'], 'trees': result['evaluation_trees']})
    pd.DataFrame(summary).to_csv(output / 'comparison.csv', index=False, encoding='utf-8-sig')
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    print('COMPARISON_SAVED', output, flush=True)


if __name__ == '__main__':
    main()
