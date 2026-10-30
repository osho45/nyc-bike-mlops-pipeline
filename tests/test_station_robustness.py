import numpy as np
import pandas as pd
import pytest
from jobs.station_robustness import calendar,blended_actions,apply_policy,checks,guarded_policy,CONSTRAINTS


def year_frame():
    ts=pd.date_range('2025-01-01T05:00Z','2026-01-01T04:00Z',freq='h')
    return pd.DataFrame({'station_id':'a','station_hour_ts':ts,'ride_count':(np.arange(len(ts))%10==0).astype(int)})


def test_shrinkage_endpoints_and_count_integrity():
    p=np.array([[0,.2,.8],[0,.9,.1]]);baseline=np.array([1,2]);saved=p.copy()
    np.testing.assert_array_equal(blended_actions(p,baseline,1),baseline)
    np.testing.assert_array_equal(blended_actions(p,baseline,0),[2,1])
    np.testing.assert_array_equal(p,saved)
    with pytest.raises(ValueError):blended_actions(p,baseline,1.1)


def test_paired_weekly_guards_pass_identical_baseline_and_reject_bad_policy():
    f=year_frame();baseline=np.ones(len(f),dtype=int)
    identical=checks(f,baseline,baseline,{'a':[0,0]})
    assert len(identical)==5 and all(r['passes'] for r in identical)
    assert all(r['upper_one_sided_bound']==0 for r in identical)
    bad=checks(f,np.full(len(f),2),baseline,{'a':[0,0]})
    assert all(not r['passes'] for r in bad)
    assert all(r['upper_one_sided_bound']>r['tolerance'] for r in bad)


def test_seasonal_guard_avoids_throwing_away_safe_seasons_and_never_uses_inference_labels():
    f=year_frame();winter=calendar(f)[0]=='winter'
    baseline=np.where(winter,1,2);policy=np.where(winter,2,1);prob=np.eye(3)[policy]
    station=guarded_policy(f,prob,baseline,{'a':[0,0]})
    assert station['fallback_stations']==['a']
    seasonal=guarded_policy(f,prob,baseline,{'a':[0,0]},seasonal=True)
    assert seasonal['fallback_stations']==[] and seasonal['fallback_cells']==[['a','winter']]
    d=apply_policy(seasonal,f,prob,baseline)
    assert all(r['passes'] for r in checks(f,d,baseline,{'a':[0,0]}))
    np.testing.assert_array_equal(d,apply_policy(seasonal,f.assign(ride_count=999),prob,baseline))
    np.testing.assert_array_equal(d[winter],baseline[winter])
    np.testing.assert_array_equal(d[~winter],policy[~winter])


def test_insufficient_support_is_not_claimed_as_noninferiority():
    f=year_frame().iloc[:48];baseline=np.ones(len(f),dtype=int)
    r=checks(f,np.full(len(f),2),baseline,{'a':[0,0]})
    assert not any(x['passes'] for x in r)
    assert not any(x['support_sufficient'] for x in r)
    assert all(x['passes'] for x in checks(f,baseline,baseline,{'a':[0,0]}))


def test_local_seasons_and_week_blocks_use_new_york_time():
    f=pd.DataFrame({'station_hour_ts':pd.to_datetime(['2025-03-01T04:00Z','2025-03-01T05:00Z','2025-03-10T03:00Z','2025-03-10T04:00Z'])})
    season,week=calendar(f)
    assert list(season[:2])==['winter','spring']
    assert week[2]!=week[3]
