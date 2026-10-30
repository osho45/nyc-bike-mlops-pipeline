import json
import numpy as np
import pandas as pd
import pytest
from jobs.scaling import station_feature_stream,FEATURE_COLUMNS,DevelopmentArtifactSink,MetricAccumulator,write_development_memmap,policy_support_diagnostics
from jobs.ml_features import build_training_features


def history(start='2024-12-15',periods=1600):
    return pd.DataFrame({'station_id':'dev','station_hour_ts':pd.date_range(start,periods=periods,freq='h',tz='America/New_York').tz_convert('UTC'),'ride_count':np.arange(periods)%11})


def stream(raw,size=137):return pd.concat(station_feature_stream((raw.iloc[i:i+size] for i in range(0,len(raw),size)),7,max_batch_rows=size),ignore_index=True)


def test_chunk_parity_cross_month_year_and_future_target_isolation():
    raw=history();actual=stream(raw);expected=build_training_features(raw);expected['station_code']=7
    np.testing.assert_allclose(actual[FEATURE_COLUMNS],expected[FEATURE_COLUMNS].astype('float32'),rtol=2e-5,atol=2e-5)
    changed=raw.copy();year=changed.station_hour_ts.dt.tz_convert('America/New_York').dt.year;changed.loc[year==2025,'ride_count']=1000
    before=actual.loc[actual.role=='train',FEATURE_COLUMNS];after=stream(changed).loc[lambda x:x.role=='train',FEATURE_COLUMNS]
    np.testing.assert_array_equal(before,after)


def test_real_gaps_restart_warmup_and_do_not_fill_absent_hours():
    raw=history().drop(index=range(600,624)).reset_index(drop=True);actual=stream(raw)
    assert len(actual)==len(raw)-2*168
    assert not actual.station_hour_ts.isin(history().station_hour_ts.iloc[600:624]).any()


@pytest.mark.parametrize('zero_hours',[24,168,672])
def test_calendar_scale_inactivity_is_observed_zero_not_missing_data(zero_hours):
    raw=history(start='2024-01-01',periods=1400);raw.loc[300:300+zero_hours-1,'ride_count']=0
    actual=stream(raw);assert len(actual)==len(raw)-168
    if zero_hours>=168:
        first=raw.station_hour_ts.iloc[300+168];row=actual.loc[actual.station_hour_ts==first].iloc[0]
        assert row.lag_168h==0 and row.rolling_24h_mean==0


def test_rejects_future_years_out_of_order_and_mixed_station_streams():
    with pytest.raises(ValueError,match='2024'):list(station_feature_stream([history('2026-01-01',200)],0))
    raw=history(periods=300)
    with pytest.raises(ValueError,match='unordered'):list(station_feature_stream([raw.iloc[:200],raw.iloc[190:]],0))
    with pytest.raises(ValueError,match='Station changed'):list(station_feature_stream([raw.iloc[:200],raw.iloc[200:].assign(station_id='other')],0))


def test_fixed_calibrator_blind_spot_is_visible_from_training_bins_only():
    policy={'states':{'synthetic_development':{'rows':100,'probabilities':[[0,.9,.1],[0,.8,.2]],'training_class_prior':[0,.85,.15]}}}
    audit=policy_support_diagnostics(policy)
    assert audit[0]['distinct_bin_actions']==1 and audit[0]['constant_action']==1


def artifact_frame(year):
    f=history(f'{year}-01-01',200).tail(2).copy()
    for name in FEATURE_COLUMNS:f[name]=1.
    return f


def test_artifact_contract_requires_training_predictions_guards_and_matching_keys(tmp_path):
    source=tmp_path/'fixture-model';source.write_bytes(b'contract fixture, not a trained model')
    provenance={'source_snapshot':'synthetic fixture',**{k:'0'*64 for k in ['feature_source_sha256','station_map_sha256','fit_row_manifest_sha256','policy_sha256','guard_configuration_sha256']}}
    sink=DevelopmentArtifactSink(tmp_path/'run',provenance);sink.register_model(source)
    for role,year in [('train',2024),('validation',2025)]:sink.write('features',role,artifact_frame(year))
    with pytest.raises(ValueError,match='predictions'):sink.finalize()
    for role,year in [('train',2024),('validation',2025)]:sink.write('predictions',role,artifact_frame(year).assign(forecast=1.,decision=1))
    with pytest.raises(ValueError,match='guard'):sink.finalize()
    sink.write_guards([{'station_id':'dev','passes':True}],{'fixture':'unchanged'});manifest=sink.finalize()
    assert manifest['status']=='auditable_model_run' and not manifest['model_trained_by_sink']


def test_artifact_role_fence_and_protected_paths(tmp_path):
    protected=tmp_path/'frozen';protected.mkdir()
    with pytest.raises(ValueError):DevelopmentArtifactSink(protected/'nested',{},[protected])
    sink=DevelopmentArtifactSink(tmp_path/'prep',{})
    with pytest.raises(ValueError,match='2024'):sink.write('features','validation',artifact_frame(2024))
    with pytest.raises(ValueError,match='2024'):sink.write('features','validation',artifact_frame(2026))
    assert sink.finalize()['status']=='preparation_only'


def test_disk_matrix_and_stream_metric_reduction(tmp_path):
    source=tmp_path/'features.parquet';frame=artifact_frame(2024);frame.to_parquet(source,index=False)
    result=write_development_memmap([source],'train',tmp_path/'matrix',max_rows=2)
    matrix=np.memmap(tmp_path/'matrix/X.f32',mode='r',dtype='float32',shape=(2,len(FEATURE_COLUMNS)))
    np.testing.assert_array_equal(matrix,frame[FEATURE_COLUMNS].to_numpy(dtype='float32'));del matrix
    with pytest.raises(ValueError,match='row budget'):write_development_memmap([source],'train',tmp_path/'over-budget',max_rows=1)
    assert json.loads((tmp_path/'over-budget/state.json').read_text())['status']=='incomplete'
    counter=MetricAccumulator();counter.update([0,4],[1,3],[0,1],[1,1]);counter.update([10],[8],[2],[1]);m=counter.result()
    assert m['mae']==pytest.approx(4/3) and m['mean_operational_penalty']==pytest.approx(25/3)

def test_parquet_microsecond_timestamp_alignment_matches_nanoseconds(tmp_path):
    raw=history(periods=500);raw['station_hour_ts']=raw.station_hour_ts.dt.as_unit('us')
    p=tmp_path/'microsecond.parquet';raw.to_parquet(p,index=False);loaded=pd.read_parquet(p)
    actual=stream(loaded);expected=stream(history(periods=500))
    np.testing.assert_array_equal(actual[FEATURE_COLUMNS],expected[FEATURE_COLUMNS])


def test_development_season_needs_weeks_not_just_many_hourly_rows():
    from jobs.station_robustness import checks
    raw=history('2025-03-01',periods=720)
    results=checks(raw,np.full(len(raw),2),np.ones(len(raw),dtype=int),{'dev':[0,1]})
    spring=next(x for x in results if x['season']=='spring')
    assert spring['hours']>=500 and spring['week_blocks']<8 and not spring['support_sufficient'] and not spring['passes']


def test_retention_rejects_same_row_count_with_different_identity(tmp_path):
    checkpoint=tmp_path/'fake-model';checkpoint.write_bytes(b'non-ML fixture')
    sink=DevelopmentArtifactSink(tmp_path/'mismatch',{});sink.register_model(checkpoint)
    for role,year in [('train',2024),('validation',2025)]:
        f=artifact_frame(year);sink.write('features',role,f);sink.write('predictions',role,f.assign(station_id='wrong',forecast=1.,decision=1))
    sink.write_guards([{'passes':True}],{})
    with pytest.raises(ValueError,match='row keys'):sink.finalize()
