import json
from pathlib import Path
import pandas as pd

from price_anomaly import ROOT, SCHEMES, csv, write_json


def summarize():
    """汇总四方案排除数量、已知案例、分价位变化及全部前三十条审计记录。
    Summarize exclusions, known cases, price-band changes and all top-thirty audit records across four schemes."""
    root=ROOT/'models/price_anomaly_v2'
    report=json.loads((root/'comparison.json').read_text(encoding='utf-8'))
    summaries,known,band_tables=[],[],[]
    known_rows=[54428,37525,54271,29854,4514,27101,22029,55926,4000,30887]
    for name in SCHEMES:
        result=report['results'][name]
        folder=root/name
        audit=pd.read_csv(folder/'price_anomaly_decisions.csv')
        cases=audit.loc[audit.Source_Row.isin(known_rows)].copy()
        cases.insert(0,'Scheme',name)
        known.append(cases)
        counts=result['counts']
        summaries.append({'Scheme':name,'SOLD':result['sold_before'],**counts,'Excluded_Percent':100*result['excluded_fraction'],'Excluded_Absolute_Error_Contribution_Percent':100*result['excluded_error_fraction'],'Review_Absolute_Error_Contribution_Percent':100*result['review_error_fraction'],'Sold_V2':result['sold_v2'],'Non_Sold_Preserved':result['non_sold_preserved']})
        bands=pd.read_csv(folder/'price_band_comparison.csv')
        bands.insert(0,'Scheme',name)
        band_tables.append(bands)
        extreme=audit.loc[audit.Decision.eq('REVIEW')].sort_values('Absolute_Error',ascending=False).head(30)
        csv(extreme,folder/'price_manual_review_extreme_top30.csv')
    csv(pd.DataFrame(summaries),root/'anomaly_summary.csv')
    csv(pd.concat(known,ignore_index=True),root/'known_cases.csv')
    csv(pd.concat(band_tables,ignore_index=True),root/'price_band_comparison_all_schemes.csv')
    primary=report['results']['original_scheme']
    full=primary['metrics']['original_test']
    kept=primary['metrics']['retained_test']
    error=full['before']['mae_cad']-kept['before']['mae_cad']
    change=kept['before']['mae_cad']-kept['v2']['mae_cad']
    final={'primary_scheme':'original_scheme','units':{'prices_and_errors':'CAD','Relative MAE':'MAE / mean actual price, stored as a fraction; multiply by 100 for percent','Bias':'mean(predicted - actual), CAD'},'primary_counts':primary['counts'],'exclude_categories_may_overlap':primary['exclusion_category_counts'],'other_price_error_exclusions':0,'primary_selection_only_MAE_reduction_CAD':error,'primary_retraining_only_MAE_reduction_on_same_retained_test_CAD':change,'interpretation':'Positive values mean lower MAE. Selection-only measures dropping test labels with the OLD model. Retraining-only compares OLD and V2 models on exactly the same retained test rows. Per-scheme clean datasets differ, so rank each before/after pair rather than comparing different test sets.','outputs':{'primary_v2':'original_scheme/database_clean_v2.csv','primary_sold_v2':'original_scheme/sold_training_v2.csv','excluded_top30':'original_scheme/price_auto_excluded_top30.csv','review_top30':'original_scheme/price_manual_review_top30.csv','review_largest_error_top30':'original_scheme/price_manual_review_extreme_top30.csv','known_cases':'known_cases.csv','all_metrics':'comparison.csv','all_bands':'price_band_comparison_all_schemes.csv'}}
    write_json(root/'final_summary.json',final)
    print(pd.DataFrame(summaries).to_string(index=False))
    print(json.dumps(final,indent=2))


if __name__ == '__main__':
    summarize()
