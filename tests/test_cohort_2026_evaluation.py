from tools.evaluate_cohort_2026 import classify_guard,complete_guards


def row(**changes):
    return {'station_id':'a','dimension':'season','season':'summer','mean_delta':-.1,'passes':False,'support_sufficient':True,'identical_to_baseline':False,**changes}


def test_mean_regressions_and_uncertainty_failures_are_separate():
    assert classify_guard(row())['diagnostic_classification']=='uncertainty_bound_failure'
    r=classify_guard(row(mean_delta=.1))
    assert r['observed_mean_regression'] and r['diagnostic_classification']=='observed_regression_and_bound_failure'


def test_short_season_not_reported_as_observed_guard_regression():
    r=classify_guard(row(mean_delta=.1,support_sufficient=False))
    assert r['diagnostic_classification']=='insufficient_support' and r['observed_mean_regression']
    r=classify_guard(row(passes=True,support_sufficient=False,identical_to_baseline=True,mean_delta=0))
    assert r['diagnostic_classification']=='baseline_identical'


def test_missing_station_season_coverage_is_explicit_and_never_dropped():
    result=complete_guards([row(passes=True)],['a','b'])
    assert len(result)==10 and sum(r['passes'] for r in result)==1
    missing=[r for r in result if r['station_id']=='b']
    assert len(missing)==5 and all(r['hours']==0 and r['mean_delta'] is None and not r['passes'] for r in missing)
    assert all(r['diagnostic_classification']=='insufficient_support' for r in missing)
