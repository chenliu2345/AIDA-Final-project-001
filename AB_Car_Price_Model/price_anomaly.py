import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor

from compare_training import define_splits, make_comparison_model, write_json
from database import connect
from settings import CAT_FEATURES, NUM_FEATURES, DATABASE
from train import evaluate_prices

ROOT = Path(__file__).resolve().parent
SCHEMES = ['original_scheme', 'my_scheme', 'grouped_70_15_15_es', 'random_80_20_es']
PAYMENT = ['lease takeover', 'lease transfer', 'takeover lease', 'take over lease', 'monthly', 'per month', '/month', 'biweekly', 'bi-weekly', 'weekly payment', 'payment', 'payments', 'finance takeover', 'financing takeover', 'down payment', 'deposit', 'cash down']
PARTS = ['parts', 'parting out', 'part out', 'for parts', 'parts car', 'mechanic special', 'project', 'project car', 'not running', 'non running', "doesn't run", 'does not run', 'blown engine', 'engine issue', 'transmission issue', 'salvage', 'wrecked', 'damaged', 'collision', 'blown motor', 'partout', 'rusted out', 'needs engine', 'needs transmission', 'as is', 'as-is']
SPECIAL = ['rare', 'collector', 'classic', 'custom', 'modified', 'supercharged', 'turbo build', 'race', 'track', 'restored', 'restoration', 'porsche', 'ferrari', 'lamborghini', 'mclaren', 'hellcat', 'trx', 'gt500', 'amg', 'service truck', 'tow truck', 'flat deck', 'dump truck', 'commercial', 'wheelchair', 'accessible', 'bentley', 'rolls royce', 'rolls-royce', 'built', 'tuned', 'stroker', 'srt', 'srt8', 'srt10', 'camper', 'motorhome', 'rv', 'freightliner', 'peterbilt', 'kenworth', 'steam truck', 'hooklift', 'flatbed', 'bucket truck', 'limited edition', 'boyd coddington']
KEYWORD_PATTERNS = {word: re.compile(r'(?<!\w)' + re.escape(word) + r'(?!\w)') for word in set(PAYMENT + PARTS + SPECIAL)}
EDGES = [0,3000,6000,9000,12000,15000,18000,21000,24000,27000,30000,35000,40000,50000,75000,np.inf]
LABELS = ['$0–3k','$3–6k','$6–9k','$9–12k','$12–15k','$15–18k','$18–21k','$21–24k','$24–27k','$27–30k','$30–35k','$35–40k','$40–50k','$50–75k','$75k+']


def csv(frame, path):
    """输出供审计和后续训练使用的 UTF-8 CSV，不覆盖原始输入。
    Write UTF-8 audit and training CSV outputs without overwriting raw inputs."""
    frame.to_csv(path, index=False, encoding='utf-8-sig')


def digest(path):
    """计算文件摘要，用于确认原始数据和模型保持不变。
    Hash files to verify that original data and models remain unchanged."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_database(batch, server):
    """只读指定 SQL 清洗批次，并从数据库原始行补充标题和可用链接。
    Read cleaned SQL rows without writing, supplementing titles and links from raw database rows."""
    connection = connect(server)
    try:
        cursor = connection.cursor().execute('SELECT V.*, L.Listing_Title, R.Raw_JSON FROM dbo.V_Training V JOIN dbo.Listings_Final_ETL L ON V.Batch_ID=L.Batch_ID AND V.Source_Row=L.Source_Row JOIN dbo.Raw_Rows R ON V.Batch_ID=R.Batch_ID AND V.Source_Row=R.Source_Row WHERE V.Batch_ID=? ORDER BY V.Source_Row', batch)
        frame = pd.DataFrame.from_records([tuple(r) for r in cursor.fetchall()], columns=[c[0] for c in cursor.description])
        metadata = connection.cursor().execute('SELECT Source_SHA256, Raw_Count, Accepted_Count FROM dbo.Import_Batches WHERE Batch_ID=?', batch).fetchone()
    finally:
        connection.close()
    raw = frame.pop('Raw_JSON').map(json.loads)
    frame['Link'] = raw.map(lambda r: r.get('Link', r.get('URL', r.get('url', ''))))
    frame['Link_SHA256'] = raw.map(lambda r: r.get('Link_SHA256', ''))
    frame.Price_CAD = frame.Price_CAD.astype(float)
    for column in CAT_FEATURES:
        frame[column] = frame[column].astype(str)
    assert len(frame) == metadata[2] and not frame.Source_Row.duplicated().any()
    assert not frame[NUM_FEATURES + CAT_FEATURES + ['Price_CAD']].isna().any().any()
    return frame, {'database': DATABASE, 'batch_id': batch, 'source_sha256': metadata[0], 'raw_rows': metadata[1], 'accepted_rows': metadata[2], 'link_policy': 'Only Link_SHA256 exists in this import. Link remains empty; no URL is invented.'}


def keyword_hits(text, words):
    """按词边界匹配标题，忽略明确否定或表示无事故、已修复的短语。
    Match title terms at word boundaries, excluding negations, accident-free descriptions and repaired-damage phrases."""
    text = str(text).lower()
    hits = []
    for word in words:
        for match in KEYWORD_PATTERNS[word].finditer(text):
            before = text[max(0, match.start()-28):match.start()]
            after = text[match.end():match.end()+18]
            if re.search(r'(?:no|not|without|new|replaced|repaired)\s+(?:\w+\s+){0,2}$', before) or re.match(r'[- ]free\b', after):
                continue
            hits.append(word)
            break
    return hits


def text_evidence(row):
    """分别提取付款、故障及特殊车辆证据，特殊保护同时检查车型和配置。
    Extract payment, fault and special-vehicle evidence separately; also inspect model and trim for protection."""
    title = str(row.Listing_Title)
    full = ' '.join([title, str(row.Base_Model), str(row.Trim)])
    payment, parts = keyword_hits(title, PAYMENT), keyword_hits(title, PARTS)
    special = keyword_hits(full, SPECIAL)
    for pattern, label in [(r'\bcorvette\b.*\b(?:zr1|z06)\b', 'Corvette ZR1/Z06'), (r'\bbmw\s+m(?:\d|\b)', 'BMW M'), (r'\bf[- ]?(?:450|550)\b', 'F450/F550'), (r'\b(?:ram|dodge|gmc|chevrolet|chevy)\s*(?:ram\s+)?(?:4500|5500|6500|7500|8500|9500)\b', 'Heavy commercial truck')]:
        if re.search(pattern, full, re.I):
            special.append(label)
    if re.search(r'\b(?:e[- ]?(?:150|250|350|450)|econoline)\b',full,re.I) and re.search(r'\b4x4\b',full,re.I):
        special.append('possible 4x4 van conversion')
    if any(int(value) >= 600 for value in re.findall(r'\b(\d{3,4})\s*(?:hp|horsepower)\b',full,re.I)):
        special.append('high horsepower build')
    return payment, parts, special


def comparable_stats(frame, reference_indices):
    """只用原训练分区逐级匹配可比车辆，排除自身和同组，按独立车辆组统计价格。
    Use training-only comparables, excluding self and same-group rows and aggregating independent vehicle groups."""
    reference = frame.iloc[reference_indices]
    pools = {name: part.index.to_numpy() for name, part in reference.groupby('Base_Model', sort=False)}
    years, kms = frame.Year.to_numpy(), frame.Kilometres.to_numpy()
    groups = pd.factorize(frame.Group_Key)[0]
    prices_all = frame.Price_CAD.to_numpy()
    trims, bodies, drives = (frame[c].to_numpy() for c in ['Trim','Body_Style','Drivetrain_Type'])
    output = []
    for i, base in enumerate(frame.Base_Model):
        pool = pools.get(base, np.array([],dtype=int)) if str(base).upper() not in {'OTHER','UNKNOWN','NAN',''} else np.array([],dtype=int)
        pool = pool[groups[pool] != groups[i]]
        selected = pool
        level, matched = 3, False
        if len(pool):
            for level, year_range, km_range in [(1,1,30000),(2,2,60000),(3,3,None)]:
                mask = np.abs(years[pool]-years[i]) <= year_range
                if km_range is not None:
                    mask &= np.abs(kms[pool]-kms[i]) <= km_range
                selected = pool[mask]
                if np.unique(groups[selected]).size >= 5 or level == 3:
                    break
            refined = selected[(trims[selected] == trims[i]) & (bodies[selected] == bodies[i]) & (drives[selected] == drives[i])]
            if np.unique(groups[refined]).size >= 5 and str(trims[i]).upper() not in {'UNKNOWN','BASE','OTHER','NAN',''}:
                selected, matched = refined, True
        selected_groups = groups[selected]
        count = np.unique(selected_groups).size
        prices = prices_all[selected]
        if count < len(selected):
            prices = pd.Series(prices).groupby(selected_groups).median().to_numpy()
        quantiles = np.quantile(prices,[.25,.5,.75]) if count else [np.nan]*3
        output.append({'Comparable_Count':int(count),'Comparable_Raw_Row_Count':len(selected),'Comparable_Level':level,'Comparable_Exact_Trim_Body_Drive':matched,'Comparable_Reliability':'HIGH' if count >= 20 else 'MEDIUM' if count >= 5 else 'LOW','Comparable_Median':float(quantiles[1]),'Comparable_Mean':float(np.mean(prices)) if count else np.nan,'Comparable_P25':float(quantiles[0]),'Comparable_P75':float(quantiles[2])})
    return pd.DataFrame(output)


def severity(ratio):
    """按用户指定倍数区间划分异常等级，边界归入较保守等级。
    Apply the requested ratio thresholds, assigning boundary values to the more conservative level."""
    if not np.isfinite(ratio):
        return 'UNKNOWN', 0
    if ratio < .10 or ratio > 8:
        return 'EXTREME', 3
    if ratio < .25 or ratio > 4:
        return 'HIGHLY_SUSPICIOUS', 2
    if ratio < .40 or ratio > 2.5:
        return 'SUSPICIOUS', 1
    return 'NORMAL', 0


def decide(row, prediction, comp):
    """要求模型与可靠可比价格同时支持明确异常规则；评分本身不能触发排除。
    Require agreement between model and reliable comparables for exclusion; a score alone cannot trigger it."""
    price = float(row.Price_CAD)
    ratio = price/max(float(prediction),1)
    cr = price/comp['Comparable_Median'] if comp['Comparable_Count'] else np.nan
    ml, ms = severity(ratio)
    cl, cs = severity(cr)
    payment, parts, special = text_evidence(row)
    if getattr(row,'Year',2026) <= 2000 and ratio > 4 and cr > 4:
        special.append('older vehicle: possible collector value requires review')
    condition_risk = str(getattr(row,'Condition_Label','')).upper() in {'DAMAGED','SALVAGE'}
    extra_model, extra_comp = 7.5 <= ratio <= 12.5, 7.5 <= cr <= 12.5
    missing_model, missing_comp = .075 <= ratio <= .125, .075 <= cr <= .125
    extra, missing = extra_model or extra_comp, missing_model or missing_comp
    enough = comp['Comparable_Count'] >= 5
    dispersed = enough and comp['Comparable_P75']/max(comp['Comparable_P25'],1) > 3
    reliable = enough and not dispersed
    special_safe = not special or (enough and comp['Comparable_Exact_Trim_Body_Drive'])
    invalid_prediction = prediction <= 0 or not np.isfinite(prediction)
    not_full = bool(payment) and ratio < .25 and cr < .25
    parts_extreme = bool(parts) and ((ratio < .1 and cr < .1) or (ratio > 8 and cr > 8))
    categories = []
    if ratio > 4 and cr > 4 and extra:
        categories.append('possible_extra_zero')
    if ratio < .25 and cr < .25 and missing:
        categories.append('possible_missing_zero')
    if not_full:
        categories.append('lease_payment')
    if parts_extreme:
        categories.append('parts_project')
    excluded = bool(categories) and reliable and special_safe and not invalid_prediction and (not parts or parts_extreme or not_full) and (not condition_risk or parts_extreme or not_full)
    flagged = ms > 0 or cs > 0 or bool(payment or parts or special) or not enough or dispersed or invalid_prediction or condition_risk
    decision = 'AUTO_EXCLUDE' if excluded else 'REVIEW' if flagged else 'AUTO_KEEP'
    sources = []
    if ms:
        sources.append('model')
    if cs and enough:
        sources.append('comparables')
    if payment or parts:
        sources.append('listing_title')
    score = int([0,10,20,25][ms] + [0,12,24,30][cs]*(1 if enough else .2) + (20 if extra or missing else 0) + (20 if payment or parts else 0) + (5 if invalid_prediction else 0))
    score_level = 'NORMAL' if score < 30 else 'LOW_SUSPICION' if score < 50 else 'SUSPICIOUS' if score < 70 else 'HIGHLY_SUSPICIOUS' if score < 85 else 'EXTREME'
    reasons = [f'Actual CAD {price:.2f}; prediction CAD {prediction:.2f}; actual/model={ratio:.4f}', f'comparable independent groups={comp["Comparable_Count"]}; median CAD {comp["Comparable_Median"]:.2f}; actual/median={cr:.4f}']
    if categories:
        reasons.append('supported patterns=' + ','.join(categories))
    if extra or missing:
        reasons.append(f'zero-pattern support: model(extra={extra_model},missing={missing_model}); comparables(extra={extra_comp},missing={missing_comp})')
    if payment or parts:
        reasons.append('title evidence=' + '|'.join(payment+parts))
    if (parts or condition_risk) and not parts_extreme and not not_full:
        reasons.append('parts/damaged/project price may be valid; dual EXTREME deviations required for exclusion')
    if special:
        reasons.append('special vehicle protection=' + '|'.join(special))
    if not enough:
        reasons.append('fewer than five comparable independent groups: automatic exclusion prohibited')
    if dispersed:
        reasons.append('comparable P75/P25 > 3: automatic exclusion prohibited')
    if not special_safe:
        reasons.append('special vehicle lacks five matching trim/body/drivetrain comparables: automatic exclusion prohibited')
    if decision == 'REVIEW':
        reasons.append('uncertain or unsupported exclusion rule; retained in V2')
    if excluded:
        assert len(sources) >= 2
        reasons.append('two evidence methods support exclusion; original price preserved')
    return {'Actual_Price':price,'Predicted_Price':float(prediction),'Residual':price-prediction,'Absolute_Error':abs(price-prediction),'Price_Ratio':ratio,'Log_Ratio_Error':abs(np.log(max(price,1))-np.log(max(prediction,1))),'Price_vs_Comparable_Median':cr,'Detected_Keywords':'|'.join(payment+parts+special),'Possible_Extra_Zero':extra,'Possible_Missing_Zero':missing,'Likely_Not_Full_Vehicle_Price':not_full,'Rare_or_Special_Vehicle':bool(special),'Model_Anomaly_Level':'MODEL_'+ml,'Comparable_Anomaly_Level':'COMPARABLE_'+cl,'Anomaly_Score':score,'Anomaly_Level':score_level,'Evidence_Sources':'|'.join(sources),'Evidence_Count':len(sources),'Exclusion_Categories':'|'.join(categories) if excluded else '', 'Decision':decision,'Decision_Reasons':'; '.join(reasons)}


def audit(frame, baseline, reference_indices, features, clip, folder):
    """使用既有评估模型和训练区可比记录审计全部 SOLD，并输出完整原因与候选排除清单。
    Audit all SOLD rows with the existing model and training comparables, saving reasons and candidate exclusions."""
    predictions = baseline.predict(frame[features])
    if clip:
        predictions = np.maximum(predictions, 0)
    comps = comparable_stats(frame, reference_indices)
    decisions = pd.DataFrame([decide(row, pred, comp) for row,pred,comp in zip(frame.itertuples(index=False), predictions, comps.to_dict('records'))])
    output = pd.concat([frame.reset_index(drop=True), comps, decisions], axis=1)
    output['Original_Index'] = output.Source_Row
    output['Drivetrain'] = output.Drivetrain_Type
    output['Baseline_Seen_During_Training'] = output.index.isin(reference_indices)
    csv(output, folder/'price_anomaly_decisions.csv')
    for decision, filename in [('AUTO_EXCLUDE','price_auto_excluded.csv'),('REVIEW','price_manual_review.csv')]:
        subset = output.loc[output.Decision == decision].sort_values(['Anomaly_Score','Absolute_Error'], ascending=False)
        csv(subset, folder/filename)
        csv(subset.head(30), folder/filename.replace('.csv','_top30.csv'))
    return output


def band_metrics(actual, predicted):
    """按原价格区间计算误差、偏差及分段 R²；总体指标从所有样本直接计算。
    Compute band errors, bias and R² using original price bins; compute overall metrics directly from all rows."""
    actual, predicted = np.asarray(actual), np.asarray(predicted)
    rows = []
    for label, low, high in list(zip(LABELS,EDGES[:-1],EDGES[1:]))+[('Overall',0,np.inf)]:
        mask = (actual >= low) & (actual < high)
        a,p = actual[mask],predicted[mask]
        n = len(a)
        e = np.abs(a-p)
        rows.append({'Actual Price':label,'N':n,'MAE':float(e.mean()) if n else None,'MedAE':float(np.median(e)) if n else None,'R²':float(1-np.sum((a-p)**2)/np.sum((a-a.mean())**2)) if n > 1 and np.var(a)>0 else None,'Mean Actual Price':float(a.mean()) if n else None,'Mean Predicted Price':float(p.mean()) if n else None,'Bias':float((p-a).mean()) if n else None,'Relative MAE':float(e.sum()/a.sum()) if n else None})
    return pd.DataFrame(rows)


def run_scheme(name, all_rows, frame, source, output, audit_only=False, reuse_grouped=False):
    """固定原方案划分，先冻结异常判断，再训练 V2；同时比较完整与保留测试集。
    Keep original splits and freeze anomaly decisions before fitting V2, comparing full and retained test sets."""
    folder = output/name
    folder.mkdir(exist_ok=True)
    old = source/name
    config = json.loads((old/'evaluation.json').read_text(encoding='utf-8'))
    manifest = pd.read_csv(old/'split_manifest.csv')
    assert np.array_equal(manifest.Source_Row, frame.Source_Row)
    split = manifest.Split.to_numpy()
    ordered_splits, _ = define_splits(frame)
    train, validation, test = ordered_splits[name]
    for label, indices in [('train',train),('validation',validation),('test',test)]:
        np.testing.assert_array_equal(np.sort(indices),np.flatnonzero(split == label))
    reuse = reuse_grouped and name in {'my_scheme','grouped_70_15_15_es'}
    previous_manifest = pd.read_csv(folder/'split_manifest.csv') if reuse else None
    previous_result = json.loads((folder/'result.json').read_text(encoding='utf-8')) if reuse else None
    if reuse:
        assert np.array_equal(train,np.sort(train)) and np.array_equal(validation,np.sort(validation))
        assert digest(folder/'evaluation_model.cbm') == previous_result['evaluation_model_sha256']
    features, clip = config['features'], config['clip_negative_predictions']
    model_path = old/'evaluation_model.cbm'
    baseline = CatBoostRegressor().load_model(str(model_path))
    print(f'[{name}] database audit starts, train-only comparables={len(train)}', flush=True)
    decisions = audit(frame, baseline, train, features, clip, folder)
    excluded = decisions.Decision.eq('AUTO_EXCLUDE').to_numpy()
    keep_train, keep_validation = train[~excluded[train]], validation[~excluded[validation]]
    keep_test = test[~excluded[test]]
    candidates = all_rows.loc[~all_rows.Source_Row.isin(frame.loc[excluded,'Source_Row'])].copy()
    csv(candidates, folder/'database_clean_v2.csv')
    csv(frame.loc[~excluded], folder/'sold_training_v2.csv')
    if reuse:
        np.testing.assert_array_equal(previous_manifest.Source_Row,manifest.Source_Row)
        np.testing.assert_array_equal(previous_manifest.Split,manifest.Split)
        np.testing.assert_array_equal(previous_manifest.Decision,decisions.Decision)
    manifest['Original_Partition_Order'] = -1
    for indices in [train,validation,test]:
        manifest.loc[indices,'Original_Partition_Order'] = np.arange(len(indices))
    manifest['Decision'] = decisions.Decision
    manifest['Retained_V2'] = ~excluded
    csv(manifest, folder/'split_manifest.csv')
    total_error = float(decisions.Absolute_Error.sum())
    counts = decisions.Decision.value_counts().to_dict()
    summary = {'scheme':name,'counts':counts,'sold_before':len(frame),'sold_v2':int((~excluded).sum()),'all_rows_v2':len(candidates),'non_sold_preserved':int((candidates.Status != 'SOLD').sum()),'excluded_fraction':float(excluded.mean()),'excluded_absolute_error':float(decisions.loc[excluded,'Absolute_Error'].sum()),'excluded_error_fraction':float(decisions.loc[excluded,'Absolute_Error'].sum()/total_error),'review_error_fraction':float(decisions.loc[decisions.Decision.eq('REVIEW'),'Absolute_Error'].sum()/total_error),'error_contribution_policy':'Existing evaluation model over all SOLD: training rows are in-sample, validation and test are held out. This is an audit diagnostic, not a test score.','baseline_model_sha256':digest(model_path),'split_counts_before':config['split_counts'],'split_counts_v2':{'train':len(keep_train),'validation':len(keep_validation),'test_retained':len(keep_test),'test_original':len(test)},'exclusion_category_counts':{category:int(decisions.Exclusion_Categories.str.contains(category,regex=False).sum()) for category in ['possible_extra_zero','possible_missing_zero','lease_payment','parts_project']}}
    write_json(folder/'audit_summary.json',summary)
    if audit_only:
        print(f'[{name}] AUDIT ONLY {counts}',flush=True)
        return summary
    print(f'[{name}] {counts}; training V2 on {len(keep_train)} rows',flush=True)
    original = name in {'original_scheme','random_80_20_es'}
    categories = [f for f in features if f in CAT_FEATURES]
    clean_model = make_comparison_model(categories, config['model_parameters']['iterations'], original)
    requested_params = clean_model.get_params()
    kwargs = {} if not config['early_stopping'] else {'eval_set':(frame.iloc[keep_validation][features],frame.iloc[keep_validation].Price_CAD),'early_stopping_rounds':50,'use_best_model':True}
    if reuse:
        assert previous_result['features'] == features
        assert previous_result['baseline_model_sha256'] == digest(model_path)
        clean_model = CatBoostRegressor().load_model(str(folder/'evaluation_model.cbm'))
        for key,value in requested_params.items():
            if key != 'cat_features':
                assert clean_model.get_params()[key] == value
        assert clean_model.get_cat_feature_indices() == [features.index(c) for c in categories]
        assert clean_model.get_params().get('od_wait') == 50
        assert clean_model.get_params().get('use_best_model') is True
        print(f'[{name}] reuse validated grouped model: identical source, decisions, parameters and ordered partitions',flush=True)
    else:
        clean_model.fit(frame.iloc[keep_train][features],frame.iloc[keep_train].Price_CAD,**kwargs)
        clean_model.save_model(str(folder/'evaluation_model.cbm'))
    before = baseline.predict(frame.iloc[test][features])
    after = clean_model.predict(frame.iloc[test][features])
    if clip:
        before,after = np.maximum(before,0),np.maximum(after,0)
    actual = frame.iloc[test].Price_CAD.to_numpy()
    retained = ~excluded[test]
    prediction_rows = frame.iloc[test][['Source_Row','Group_Key','Price_CAD']].copy()
    prediction_rows['Before_Prediction'] = before
    prediction_rows['V2_Prediction'] = after
    prediction_rows['Decision'] = decisions.iloc[test].Decision.to_numpy()
    csv(prediction_rows,folder/'before_after_test_predictions.csv')
    metrics = {}
    for scope, mask in [('original_test',np.ones(len(test),dtype=bool)),('retained_test',retained)]:
        metrics[scope] = {'N':int(mask.sum()),'before':evaluate_prices(actual[mask],before[mask]),'v2':evaluate_prices(actual[mask],after[mask])}
        for variant,pred in [('before',before),('v2',after)]:
            csv(band_metrics(actual[mask],pred[mask]),folder/f'price_band_{scope}_{variant}.csv')
    b = band_metrics(actual[retained],before[retained])
    a = band_metrics(actual[retained],after[retained])
    comparison = b.merge(a,on='Actual Price',suffixes=('_Before','_V2'))
    comparison['MAE_Improvement_CAD'] = comparison.MAE_Before-comparison.MAE_V2
    csv(comparison,folder/'price_band_comparison.csv')
    summary.update({'ordered_partition_policy':'Preserve original define_splits order, including the random train permutation; remove excluded rows without reordering.', 'training_source_row_order_sha256':hashlib.sha256(frame.iloc[keep_train].Source_Row.to_numpy(dtype=np.int64).tobytes()).hexdigest(),'grouped_fit_reused_after_validation':reuse,'early_stopping':config['early_stopping'],'early_stopping_patience':50 if config['early_stopping'] else None,'metrics':metrics,'trees_before':config['evaluation_trees'],'trees_v2':int(clean_model.tree_count_),'features':features,'model_parameters':requested_params,'evaluation_model_sha256':digest(folder/'evaluation_model.cbm'),'policy':'Original split membership frozen before filtering; ratios change slightly after exclusions. No test row used in fitting or comparable reference. Detector uses existing evaluation model and ORIGINAL TRAIN reference only. V2 test filtering is label-dependent: retained-test scores are conditional and not an unbiased new production benchmark. Both models are also scored on the unchanged full test. Per-scheme decisions differ because training references differ. No parameter tuning or price correction.'})
    # 重新加载模型验证预测，并核对原结果，确保基线没有换模型或换测试样本。
    # Reload models and check predictions against original results to keep baseline models and test samples unchanged.
    restored = CatBoostRegressor().load_model(str(folder/'evaluation_model.cbm'))
    np.testing.assert_allclose(restored.predict(frame.iloc[test[:20]][features]),clean_model.predict(frame.iloc[test[:20]][features]))
    for key,value in config['test_metrics'].items():
        np.testing.assert_allclose(metrics['original_test']['before'][key],value,rtol=1e-10)
    assert not decisions.loc[excluded,'Evidence_Count'].lt(2).any()
    assert not decisions.loc[excluded,'Comparable_Count'].lt(5).any()
    assert set(candidates.loc[candidates.Status != 'SOLD','Source_Row']) == set(all_rows.loc[all_rows.Status != 'SOLD','Source_Row'])
    assert candidates.set_index('Source_Row').Price_CAD.equals(all_rows.set_index('Source_Row').loc[candidates.Source_Row,'Price_CAD'])
    if not original:
        assert not set(frame.iloc[keep_train].Group_Key) & set(frame.iloc[test].Group_Key)
    summary['verification'] = 'passed: baseline metrics, saved predictions, evidence guards, unchanged prices, non-SOLD preservation, grouped isolation'
    write_json(folder/'result.json',summary)
    print(f'[{name}] DONE {json.dumps(metrics)}',flush=True)
    return summary


def main():
    """从数据库清洗结果执行价格异常审计及原四方案对照，不修改源 CSV、SQL 历史或 EXE。
    Audit cleaned database records and compare all four schemes without modifying source CSV, SQL history or EXE."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,default=ROOT/'models/comparisons/self_project_63377')
    parser.add_argument('--output',type=Path,default=ROOT/'models/price_anomaly_v2')
    parser.add_argument('--server',default='.')
    parser.add_argument('--audit-only',action='store_true')
    parser.add_argument('--reuse-grouped',action='store_true')
    parser.add_argument('--schemes',nargs='+',choices=SCHEMES,default=SCHEMES)
    args = parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    info = json.loads((args.source/'dataset.json').read_text(encoding='utf-8'))
    raw_path = ROOT/'data/Alberta_owner_sales_car.csv'
    raw_before = digest(raw_path)
    all_rows,provenance = read_database(info['batch_id'],args.server)
    assert provenance['source_sha256'] == raw_before == info['source_sha256']
    frame = all_rows.loc[all_rows.Status == 'SOLD'].reset_index(drop=True)
    saved = pd.read_pickle(args.source/'training_frame.pkl')
    pd.testing.assert_frame_equal(frame[saved.columns].reset_index(drop=True),saved.reset_index(drop=True),check_dtype=False)
    csv(all_rows,args.output/'database_clean_snapshot.csv')
    provenance.update({'created_utc':datetime.now(timezone.utc).isoformat(),'sold_rows':len(frame),'primary_scheme':'original_scheme','reference_policy':'For each original scheme, comparable reference and baseline model use only its original training partition. Same-group listings excluded; each comparable group contributes one median. Prefer exact trim/body/drivetrain when >=5 rows; special vehicles require five matching groups. AUTO_EXCLUDE requires same-direction >4x model AND comparable deviations plus a factor-ten pattern from either reference, or explicit title+dual extreme price evidence. P75/P25 > 3 blocks exclusion. Parts/damaged vehicles require dual EXTREME deviations; commercial, modified, bundled and potentially collectible older vehicles receive special protection. REVIEW retained. No SQL writes.','script_sha256':digest(__file__)})
    write_json(args.output/'provenance.json',provenance)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {name:pool.submit(run_scheme,name,all_rows,frame,args.source,args.output,args.audit_only,args.reuse_grouped) for name in args.schemes}
        results = {name:future.result() for name,future in futures.items()}
    assert digest(raw_path) == raw_before
    if args.audit_only:
        write_json(args.output/'audit_only.json',results)
        print('AUDIT COMPLETE',args.output,flush=True)
        return
    report = {'provenance':provenance,'raw_unchanged':True,'results':results}
    write_json(args.output/'comparison.json',report)
    rows = []
    for name,result in results.items():
        for scope,m in result['metrics'].items():
            for variant in ['before','v2']:
                rows.append({'Scheme':name,'Test':scope,'Model':variant,'N':m['N'],**m[variant]})
    csv(pd.DataFrame(rows),args.output/'comparison.csv')
    print('COMPLETE',args.output,flush=True)


if __name__ == '__main__':
    main()
