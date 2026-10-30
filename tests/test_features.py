import numpy as np
import pandas as pd
import pytest
from jobs.ml_features import *

def test_chronological_train_only_thresholds(history):
    train,val,test,t,features=prepare_model_frames(history)
    assert len(features)==13 and train.station_hour_ts.max()<val.station_hour_ts.min()<test.station_hour_ts.min()
    assert t.q25==train.ride_count.quantile(.25) and t.q75==train.ride_count.quantile(.75)
    changed=history.copy();changed.loc[changed.station_hour_ts.isin(test.station_hour_ts),'ride_count']=9999
    assert prepare_model_frames(changed)[3]==t

def test_online_offline_feature_parity_and_no_target_leak(history):
    prepared=build_training_features(history)
    target=history.station_hour_ts.iloc[300];t=DemandThresholds(3,12)
    online,meta=build_one_prediction_feature_row(history,'001.20',target-pd.Timedelta(hours=1),t)
    expected=prepared.loc[prepared.station_hour_ts==target,REQUIRED_FEATURES]
    np.testing.assert_allclose(online.values,expected.values)
    changed=history.copy();changed.loc[changed.station_hour_ts>=target,'ride_count']=9999
    altered,_=build_one_prediction_feature_row(changed,'001.20',target-pd.Timedelta(hours=1),t)
    np.testing.assert_allclose(online.values,altered.values)
    assert meta['prediction_hour_ts']==target

@pytest.mark.parametrize('kind',['gap','duplicate','mixed'])
def test_invalid_station_history_rejected(history,kind):
    if kind=='gap':history=history.drop(index=100)
    elif kind=='duplicate':history=pd.concat([history,history.iloc[[100]]])
    else:history.loc[100,'station_id']='other'
    with pytest.raises(ValueError):prepare_model_frames(history)

def test_penalty_contract_and_timezone():
    assert calculate_penalty(2,0)==30 and calculate_penalty(0,2)==20
    assert calculate_penalty(None,1) is None
    assert shifted_simulation_time('2024-04-15T10:00:00-04:00',3)==pd.Timestamp('2024-01-15 14:00')

@pytest.mark.parametrize('family',['LogisticRegression','RandomForest','LightGBM'])
def test_small_model_families_and_genuine_metrics(history,family,tmp_path):
    from jobs.ml_experiments import build_model,evaluate_classifier,cost_sensitive_predict
    train,val,_,_,features=prepare_model_frames(history)
    params={'n_estimators':5} if family!='LogisticRegression' else {}
    model=build_model(family,params);model.fit(train[features],train.demand_class)
    pred=cost_sensitive_predict(model,val[features]);metrics=evaluate_classifier(model,val[features],val.demand_class,'val')
    assert np.isfinite(list(metrics.values())).all()
    assert metrics['val_mean_penalty']==mean_operational_penalty(val.demand_class,pred)
    # Functional synthetic test only; these are not Citi Bike accuracy claims.
