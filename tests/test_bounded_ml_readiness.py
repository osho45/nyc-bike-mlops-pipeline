import numpy as np
import pandas as pd
import pytest
from tools.bounded_ml_readiness import split_name,select_cohort,metrics,features,historical_fit,baseline_predictions


def test_calendar_roles_use_new_york_and_exclude_october_test():
    times=pd.Series(pd.to_datetime(['2025-01-01T04:00Z','2025-01-01T05:00Z','2026-01-01T05:00Z','2026-10-01T03:00Z','2026-10-01T04:00Z']))
    assert list(split_name(times))==['train','validation','test','test','outside']


def test_count_metrics_and_existing_asymmetric_penalty():
    f=pd.DataFrame({'station_id':['a']*3,'ride_count':[0,4,10]})
    m=metrics(f,[10,4,0],{'a':[2,8]})
    assert m['mae']==pytest.approx(20/3)
    assert m['rmse']==pytest.approx(np.sqrt(200/3))
    assert m['wape']==pytest.approx(20/14)
    assert m['mean_operational_penalty']==pytest.approx(50/3)
    assert metrics(f.assign(ride_count=0),[0]*3,{'a':[0,0]})['wape'] is None
    with pytest.raises(ValueError):metrics(f,[np.nan]*3,{'a':[2,8]})


def test_cohort_uses_training_totals_and_excludes_low_coverage():
    totals=pd.DataFrame({'station_id':[str(i) for i in range(25)],'rides':np.arange(25)+1,'hours':[8784]*24+[100]})
    cohort=select_cohort(totals,12)
    assert len(cohort)==12 and all(c['station_id']!='24' for c in cohort)
    assert {c['training_volume_band'] for c in cohort}=={'low','medium_low','medium_high','high'}
    assert select_cohort(totals.sample(frac=1,random_state=3),12)==cohort


def test_gap_reset_no_cross_station_history_and_mutation_oracle():
    ts=pd.date_range('2024-01-01',periods=400,freq='h',tz='UTC')
    raw=pd.concat([pd.DataFrame({'station_id':s,'station_hour_ts':ts,'ride_count':np.arange(400)%17+offset}) for s,offset in [('a',0),('b',50)]],ignore_index=True)
    raw=raw.loc[~raw.station_hour_ts.isin(ts[200:202])]
    f,audit=features(raw,{'a':0,'b':1})
    assert all(r['target_mutation_and_lag_oracle_passed'] for r in audit)
    assert len(f)==2*((200-168)+(198-168))
    assert (f.loc[f.station_id=='b','lag_1h']>=50).all()


def test_historical_baseline_is_fitted_on_training_only():
    t=pd.DataFrame({'station_id':['a']*2,'station_hour_ts':pd.to_datetime(['2024-01-01T05:00Z','2024-01-08T05:00Z']),'ride_count':[2,4]})
    fit=historical_fit(t)
    v=pd.DataFrame({'station_id':['a'],'station_hour_ts':pd.to_datetime(['2025-01-06T05:00Z']),'ride_count':[999],'lag_1h':[1],'lag_24h':[2],'lag_168h':[3]})
    assert baseline_predictions(v,fit)['training_weekly_mean'][0]==3
    assert baseline_predictions(v.assign(ride_count=0),fit)['training_weekly_mean'][0]==3


def test_existing_year_interface_excludes_later_test_months():
    from jobs.year_features import prepare_year_model_frames
    raw=pd.DataFrame({'station_id':'a','station_hour_ts':pd.date_range('2024-12-15','2026-10-02',freq='h',tz='UTC')})
    raw['ride_count']=np.arange(len(raw))%9
    train,val,test,_,_=prepare_year_model_frames(raw)
    local=pd.to_datetime(test.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
    assert local.dt.year.eq(2026).all() and local.dt.month.le(9).all()
    assert pd.to_datetime(train.station_hour_ts,utc=True).dt.tz_convert('America/New_York').dt.year.eq(2024).all()


def test_default_training_config_uses_calendar_year_roles():
    import yaml
    from pathlib import Path
    cfg=yaml.safe_load((Path(__file__).parents[1]/'config/local.yml').read_text())
    assert cfg['ml']['split_policy']=='calendar_year'


def test_progress_reports_completed_bounded_training_separately(tmp_path):
    import json
    from tools.ingestion_status import progress
    root=tmp_path/'monthly';root.mkdir()
    bounded=tmp_path/'ml-readiness-01';bounded.mkdir()
    (bounded/'metrics.json').write_text(json.dumps({'status':'complete','rows':{'train':1},'validation_selected_model':'fixture'}))
    result=progress(root)
    assert result['model_training_performed'] and not result['ingestion_trains_models']
    assert result['bounded_ml_experiment']['validation_selected_model']=='fixture'
