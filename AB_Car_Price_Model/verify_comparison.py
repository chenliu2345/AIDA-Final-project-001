import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor

from database import read_training_batch
from train import evaluate_prices


def verify_comparison(output, server='.'):
    """从保存模型重新计算四套指标，并核对数据来源、SQL 行数和测试样本隔离。
    Recompute four sets of metrics from saved models and verify provenance, SQL counts and test isolation."""
    output = Path(output).resolve()
    report = json.loads((output / 'comparison.json').read_text(encoding='utf-8'))
    info = report['dataset']
    assert hashlib.sha256(Path(info['source']).read_bytes()).hexdigest() == info['source_sha256']
    frame = pd.read_pickle(output / 'training_frame.pkl')
    sql_frame = read_training_batch(info['batch_id'], server).sort_values('Source_Row').reset_index(drop=True)
    assert len(frame) == len(sql_frame) == info['sold_rows']
    assert frame.Source_Row.tolist() == sql_frame.Source_Row.tolist()
    np.testing.assert_allclose(frame.Price_CAD, sql_frame.Price_CAD.astype(float))
    outcomes = {}
    common_rows = None
    for name, result in report['results'].items():
        folder = output / name
        features = result['features']
        manifest = pd.read_csv(folder / 'split_manifest.csv')
        assert manifest.Source_Row.tolist() == frame.Source_Row.tolist()
        training = manifest.index[manifest.Split == 'train']
        testing = manifest.index[manifest.Split == 'test']
        validation = manifest.index[manifest.Split == 'validation']
        assert len(training) + len(validation) + len(testing) == len(frame)
        assert set(validation).isdisjoint(training) and set(validation).isdisjoint(testing)
        common = manifest.index[manifest.Common_Test]
        assert set(training).isdisjoint(testing)
        assert set(common).issubset(testing)
        if common_rows is None:
            common_rows = set(frame.iloc[common].Source_Row)
        else:
            assert common_rows == set(frame.iloc[common].Source_Row)
        if name in {'my_scheme', 'grouped_70_15_15_es'}:
            groups = [set(manifest.loc[manifest.Split == part, 'Group_Key']) for part in ['train', 'validation', 'test']]
            assert not (groups[0] & groups[1] or groups[0] & groups[2] or groups[1] & groups[2])
        model = CatBoostRegressor()
        model.load_model(str(folder / 'evaluation_model.cbm'))
        assert model.tree_count_ == result['evaluation_trees']
        assert hashlib.sha256((folder / 'evaluation_model.cbm').read_bytes()).hexdigest() == result['evaluation_model_sha256']
        for label, indices in [('test', testing), ('common_test', common)]:
            predictions = model.predict(frame.iloc[indices][features])
            if name in {'my_scheme', 'grouped_70_15_15_es'}:
                predictions = np.maximum(predictions, 0)
            metrics = evaluate_prices(frame.iloc[indices].Price_CAD, predictions)
            for key, value in metrics.items():
                assert np.isclose(value, result[label + '_metrics'][key], rtol=1e-10, atol=1e-8)
            saved = pd.read_csv(folder / (label + '_predictions.csv')).sort_values('Source_Row')
            assert saved.Source_Row.tolist() == frame.iloc[indices].Source_Row.tolist()
            np.testing.assert_allclose(predictions, saved.Predicted_Price_CAD)
        model.load_model(str(folder / 'price_model.cbm'))
        assert hashlib.sha256((folder / 'price_model.cbm').read_bytes()).hexdigest() == result['deployment_model_sha256']
        predictions = model.predict(frame.iloc[testing][features])
        if name in {'my_scheme', 'grouped_70_15_15_es'}:
            predictions = np.maximum(predictions, 0)
        seen_metrics = evaluate_prices(frame.iloc[testing].Price_CAD, predictions)
        for key, value in seen_metrics.items():
            assert np.isclose(value, result['seen_subset_metrics_NOT_a_test'][key], rtol=1e-10, atol=1e-8)
        outcomes[name] = {'ok': True, 'train_rows': len(training), 'validation_rows': len(validation), 'test_rows': len(testing), 'common_test_rows': len(common), 'all_metrics_recomputed': True,
                          'common_rows_with_group_seen_in_training': int(manifest.loc[common, 'Group_Key'].isin(set(manifest.loc[training, 'Group_Key'])).sum()),
                          'test_target_max_cad': float(frame.iloc[testing].Price_CAD.max()),
                          'test_target_mean_cad': float(frame.iloc[testing].Price_CAD.mean())}
    verification = {'ok': True, 'source_sha256': info['source_sha256'], 'sold_rows': len(frame), 'schemes': outcomes}
    (output / 'verification.json').write_text(json.dumps(verification, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(verification, ensure_ascii=False, indent=2))


def main():
    """通过命令行指定对比结果目录并执行模型、数据和指标验收。
    Accept a comparison folder and verify its models, data and metrics."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--server', default='.')
    args = parser.parse_args()
    verify_comparison(args.output, args.server)


if __name__ == '__main__':
    main()
