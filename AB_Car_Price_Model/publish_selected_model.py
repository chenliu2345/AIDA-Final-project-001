import hashlib
import json
import os
from pathlib import Path
import shutil
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from model_quality import quality_report
from settings import FEATURES, CAT_FEATURES
from train import split_holdout, evaluate_prices
from predict import load_model


def publish():
    """校验已选模型及来源，补充误差区间报告后原子发布固定两千轮版本。
    Validate the selected model and data source, add error and interval reports, and atomically publish the fixed-2000-iteration model."""
    root = Path(__file__).resolve().parent
    source = root / 'models/comparisons/self_project_63377'
    selected = source / 'original_scheme'
    dataset = json.loads((source/'dataset.json').read_text(encoding='utf-8'))
    result = json.loads((selected/'result.json').read_text(encoding='utf-8'))
    if hashlib.sha256((root / 'data' / Path(dataset['source']).name).read_bytes()).hexdigest() != dataset['source_sha256']:
        raise ValueError('原始 CSV 摘要不一致。')
    frame = pd.read_pickle(source/'training_frame.pkl').sort_values('Source_Row').reset_index(drop=True)
    train_idx, test_idx = split_holdout(frame)
    evaluation, final = CatBoostRegressor(), CatBoostRegressor()
    for name, model, key in [('evaluation_model.cbm',evaluation,'evaluation_model_sha256'), ('price_model.cbm',final,'deployment_model_sha256')]:
        assert hashlib.sha256((selected/name).read_bytes()).hexdigest() == result[key]
        model.load_model(str(selected/name))
        assert model.tree_count_ == 2000 and list(model.feature_names_) == FEATURES
    predictions = evaluation.predict(frame.iloc[test_idx][FEATURES])
    saved = pd.read_csv(selected/'test_predictions.csv')
    assert np.array_equal(saved.Source_Row.to_numpy(),frame.iloc[test_idx].Source_Row.to_numpy())
    assert np.allclose(saved.Predicted_Price_CAD, predictions)
    quality, audit = quality_report(frame.iloc[test_idx].Price_CAD, predictions)
    audit.insert(0,'Source_Row',frame.iloc[test_idx].Source_Row.to_numpy())
    metadata = dict(dataset)
    metadata.update(schema_version=4,created_utc=datetime.now(timezone.utc).isoformat(),training_rows=len(frame),
        target='Observed listing price in CAD; not verified transaction price',training_filter='Status = SOLD',
        features=FEATURES,categorical_features=CAT_FEATURES,best_iterations=2000,model_parameters=result['model_parameters'],
        training_rule_version='random80-fixed2000-ranges-v1',split_counts=result['split_counts'],split_seed=42,split_method='random_rows_80_20',
        test_metrics=evaluate_prices(frame.iloc[test_idx].Price_CAD,predictions),
        groups_shared_across_row_split=result['test_groups_shared_with_training'],
        evaluation_policy='Random 80/20 seed42; evaluation model fits train only for 2000 trees without eval_set. Deployment refits all SOLD. No anomaly-V2 exclusions.',
        model_sha256=result['deployment_model_sha256'], evaluation_model_sha256=result['evaluation_model_sha256'],
        catalog={c:sorted(frame[c].astype(str).unique().tolist()) for c in CAT_FEATURES},
        trims_by_model={str(k):sorted(g.Trim.astype(str).unique().tolist()) for k,g in frame.groupby('Base_Model')},
        numeric_ranges={c:[int(frame[c].min()),int(frame[c].max())] for c in ['Year','Kilometres']},
        feature_importance=dict(zip(FEATURES,map(float,final.get_feature_importance()))), **quality)
    version = 'random2000_63377_ranges_v1'
    destination = root/'models'/version
    destination.mkdir(exist_ok=True)
    for name in ['price_model.cbm','evaluation_model.cbm','test_predictions.csv','split_manifest.csv']:
        shutil.copy2(selected/name,destination/name)
    (destination/'metadata.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    pd.DataFrame(quality['price_band_metrics']).to_csv(destination/'price_band_metrics.csv',index=False,encoding='utf-8-sig')
    audit.to_csv(destination/'interval_audit.csv',index=False,encoding='utf-8-sig')
    check, meta, directory = load_model(destination)
    assert np.allclose(check.predict(frame.iloc[:5][FEATURES]), final.predict(frame.iloc[:5][FEATURES]))
    pointer = root/'models/current.pending.json'
    pointer.write_text(json.dumps({'version':version}),encoding='utf-8')
    os.replace(pointer,root/'models/current.json')
    print(json.dumps({'version':version,'test':metadata['test_metrics'],'coverage':quality['prediction_intervals']['validation_coverage'],'training_rows':len(frame)}))


if __name__ == '__main__':
    publish()
