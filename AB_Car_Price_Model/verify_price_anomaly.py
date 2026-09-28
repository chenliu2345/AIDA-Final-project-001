import ast
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from compare_training import define_splits
from price_anomaly import ROOT, SCHEMES, EDGES, PARTS, digest


def verify():
    """独立重算输出指标、排除条件和数据保留情况，并抽查可比参考严格来自训练分区。
    Independently recompute metrics, exclusions and retained data, and check that comparables come strictly from training."""
    folder=ROOT/'models/price_anomaly_v2'
    snapshot=pd.read_csv(folder/'database_clean_snapshot.csv')
    sold=snapshot.loc[snapshot.Status.eq('SOLD')].reset_index(drop=True)
    provenance=json.loads((folder/'provenance.json').read_text(encoding='utf-8'))
    assert digest(ROOT/'data/Alberta_owner_sales_car.csv') == provenance['source_sha256']
    assert digest(ROOT/'price_anomaly.py') == provenance['script_sha256']
    results={}
    ordered_splits,_=define_splits(sold)
    for name in SCHEMES:
        current=folder/name
        audit=pd.read_csv(current/'price_anomaly_decisions.csv')
        manifest=pd.read_csv(current/'split_manifest.csv')
        candidate=pd.read_csv(current/'database_clean_v2.csv')
        report=json.loads((current/'result.json').read_text(encoding='utf-8'))
        np.testing.assert_array_equal(audit.Source_Row,sold.Source_Row)
        np.testing.assert_array_equal(audit.Actual_Price,sold.Price_CAD)
        excluded=audit.Decision.eq('AUTO_EXCLUDE')
        rows=set(audit.loc[excluded,'Source_Row'])
        expected=snapshot.loc[~snapshot.Source_Row.isin(rows)].reset_index(drop=True)
        pd.testing.assert_frame_equal(candidate,expected,check_dtype=False)
        assert set(candidate.loc[candidate.Status.ne('SOLD'),'Source_Row']) == set(snapshot.loc[snapshot.Status.ne('SOLD'),'Source_Row'])
        assert set(audit.Decision) <= {'AUTO_KEEP','REVIEW','AUTO_EXCLUDE'}
        assert audit.loc[excluded,'Evidence_Count'].ge(2).all()
        assert audit.loc[excluded,'Comparable_Count'].ge(5).all()
        assert audit.loc[excluded,'Decision_Reasons'].str.len().gt(100).all()
        for row in audit.loc[excluded].itertuples():
            pr,cr=row.Price_Ratio,row.Price_vs_Comparable_Median
            allowed=(pr > 4 and cr > 4 and (7.5 <= pr <= 12.5 or 7.5 <= cr <= 12.5)) or (pr < .25 and cr < .25 and (.075 <= pr <= .125 or .075 <= cr <= .125)) or (row.Likely_Not_Full_Vehicle_Price and pr < .25 and cr < .25) or ('parts_project' in row.Exclusion_Categories and ((pr < .1 and cr < .1) or (pr > 8 and cr > 8)))
            assert allowed
            if set(str(row.Detected_Keywords).split('|')) & set(PARTS) or str(row.Condition_Label).upper() in {'DAMAGED','SALVAGE'}:
                assert (pr < .1 and cr < .1) or (pr > 8 and cr > 8) or row.Likely_Not_Full_Vehicle_Price
            assert not row.Rare_or_Special_Vehicle or row.Comparable_Exact_Trim_Body_Drive
            assert row.Comparable_P75/max(row.Comparable_P25,1) <= 3
        ordered_train,ordered_validation,ordered_test=ordered_splits[name]
        for label,indices in [('train',ordered_train),('validation',ordered_validation),('test',ordered_test)]:
            actual_order=manifest.loc[manifest.Split.eq(label)].sort_values('Original_Partition_Order').Source_Row
            np.testing.assert_array_equal(actual_order,sold.iloc[indices].Source_Row)
        kept_training=ordered_train[~excluded.to_numpy()[ordered_train]]
        order_hash=hashlib.sha256(sold.iloc[kept_training].Source_Row.to_numpy(dtype=np.int64).tobytes()).hexdigest()
        assert order_hash == report['training_source_row_order_sha256']
        train=sold.loc[manifest.Split.eq('train')]
        checks=list(np.random.default_rng(42).choice(len(sold),size=30,replace=False))+audit.index[excluded].tolist()
        for i in checks:
            row=sold.iloc[i]
            recorded=audit.iloc[i]
            if row.Base_Model in {'OTHER','UNKNOWN','NAN',''}:
                assert recorded.Comparable_Count == 0
                continue
            pool=train.loc[train.Base_Model.eq(row.Base_Model)&train.Group_Key.ne(row.Group_Key)]
            selected=pool.loc[(pool.Year-row.Year).abs().le(int(recorded.Comparable_Level))]
            if recorded.Comparable_Level < 3:
                selected=selected.loc[(selected.Kilometres-row.Kilometres).abs().le(30000*int(recorded.Comparable_Level))]
            if recorded.Comparable_Exact_Trim_Body_Drive:
                selected=selected.loc[selected.Trim.eq(row.Trim)&selected.Body_Style.eq(row.Body_Style)&selected.Drivetrain_Type.eq(row.Drivetrain_Type)]
            values=selected.groupby('Group_Key').Price_CAD.median()
            assert len(values) == recorded.Comparable_Count
            if len(values):
                np.testing.assert_allclose(values.median(),recorded.Comparable_Median)
        predictions=pd.read_csv(current/'before_after_test_predictions.csv')
        assert set(predictions.Source_Row) == set(manifest.loc[manifest.Split.eq('test'),'Source_Row'])
        for scope in ['original_test','retained_test']:
            part=predictions if scope == 'original_test' else predictions.loc[predictions.Decision.ne('AUTO_EXCLUDE')]
            for variant,column in [('before','Before_Prediction'),('v2','V2_Prediction')]:
                metrics=report['metrics'][scope][variant]
                actual,predicted=part.Price_CAD,part[column]
                measured={'r2':r2_score(actual,predicted),'mae_cad':mean_absolute_error(actual,predicted),'rmse_cad':float(np.sqrt(mean_squared_error(actual,predicted)))}
                for key,value in measured.items():
                    np.testing.assert_allclose(value,metrics[key],rtol=1e-10)
                bands=pd.read_csv(current/f'price_band_{scope}_{variant}.csv')
                assert int(bands.iloc[:-1].N.sum()) == len(part) == int(bands.iloc[-1].N)
                np.testing.assert_allclose(bands.iloc[-1].MAE,measured['mae_cad'])
                for j,band in bands.iloc[:-1].iterrows():
                    mask=actual.ge(EDGES[j]) & actual.lt(EDGES[j+1])
                    av,pv=actual.loc[mask],predicted.loc[mask]
                    assert len(av) == int(band.N)
                    if len(av):
                        ev=(av-pv).abs()
                        for column,value in [('MAE',ev.mean()),('MedAE',ev.median()),('Mean Actual Price',av.mean()),('Mean Predicted Price',pv.mean()),('Bias',(pv-av).mean()),('Relative MAE',ev.sum()/av.sum())]:
                            np.testing.assert_allclose(band[column],value,rtol=1e-10,atol=1e-8)
                        if len(av)>1 and av.var()>0:
                            np.testing.assert_allclose(band['R²'],r2_score(av,pv),rtol=1e-10,atol=1e-8)
        model=CatBoostRegressor().load_model(str(current/'evaluation_model.cbm'))
        assert model.feature_names_ == report['features']
        for key,value in report['model_parameters'].items():
            if key != 'cat_features':
                stored_key = 'od_wait' if key == 'early_stopping_rounds' else key
                assert model.get_params()[stored_key] == value
        assert model.get_cat_feature_indices() == [report['features'].index(c) for c in report['model_parameters']['cat_features']]
        records=sold.set_index('Source_Row').loc[predictions.Source_Row]
        calculated=model.predict(records[report['features']])
        if name in {'my_scheme','grouped_70_15_15_es'}:
            calculated=np.maximum(calculated,0)
            assert not set(train.Group_Key)&set(records.Group_Key)
        np.testing.assert_allclose(calculated,predictions.V2_Prediction,rtol=1e-10)
        results[name]={'status':'passed','excluded':len(rows),'comparable_reference_checks':len(checks),'test_rows':len(predictions),'retained_test_rows':report['metrics']['retained_test']['N']}
    for script in ['price_anomaly.py','test_price_anomaly.py','verify_price_anomaly.py','summarize_price_anomaly.py']:
        tree=ast.parse((ROOT/script).read_text(encoding='utf-8-sig'))
        assert all(ast.get_docstring(node) for node in ast.walk(tree) if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)))
    output={'status':'passed','source_and_script_hashes_verified':True,'schemes':results}
    (folder/'verification.json').write_text(json.dumps(output,indent=2),encoding='utf-8')
    print(json.dumps(output,indent=2))


if __name__ == '__main__':
    verify()
