import copy,json
import numpy as np
import pandas as pd
import pytest
from scipy.stats import poisson
from jobs.decision_policies import actions,expected_cost_action,fit_empirical,evaluate
from tools.bounded_ml_readiness import count_classes


def frame(values):return pd.DataFrame({'station_id':['a']*len(values),'ride_count':values})


def test_cost_matrix_orientation_and_asymmetry():
    # Equal Normal/High probabilities: predicting High costs 5, Normal costs 7.5.
    assert expected_cost_action([[0,.5,.5]]).tolist()==[2]
    assert expected_cost_action([[0,.8,.2]]).tolist()==[1]
    assert expected_cost_action([[1,0,0],[0,1,0],[0,0,1]]).tolist()==[0,1,2]
    with pytest.raises(ValueError):expected_cost_action([[0,.8,.3]])


def test_zero_thresholds_poisson_policy_does_not_classify_every_positive_mean_high():
    f=frame([0,1]);thresholds={'a':[0,0]};mu=np.array([.1,1.])
    assert actions({'kind':'original'},f,mu,thresholds).tolist()==[2,2]
    assert actions({'kind':'poisson_expected_cost','scale':1},f,mu,thresholds).tolist()==[1,2]
    np.testing.assert_array_equal(mu,[.1,1.])


def test_fractional_quantiles_are_integer_count_class_probabilities():
    f=frame([0,2]);mu=np.array([.8,4.]);q={'a':[.25,2.75]}
    p=np.array([[poisson.pmf(0,x),poisson.pmf(1,x)+poisson.pmf(2,x),poisson.sf(2,x)] for x in mu])
    np.testing.assert_array_equal(actions({'kind':'poisson_expected_cost','scale':1},f,mu,q),expected_cost_action(p))


def test_decisions_change_penalty_without_changing_count_metrics_or_actual_thresholds():
    f=frame([0,0,0,1]);q={'a':[0,0]};before=copy.deepcopy(q);mu=np.array([.1,.1,.1,1.])
    original=evaluate(f,mu,actions({'kind':'original'},f,mu,q),q)
    optimized=evaluate(f,mu,actions({'kind':'poisson_expected_cost','scale':1},f,mu,q),q)
    for key in ['mae','rmse','wape']:assert original[key]==optimized[key]
    assert optimized['mean_operational_penalty']<original['mean_operational_penalty']
    assert before==q and original['zero_demand_high_action_rate']==1 and optimized['zero_demand_high_action_rate']==0


def test_empirical_calibrator_serialization_and_frozen_training_state():
    train=frame([0]*90+[1]*10);mu=np.array([.1]*100);q={'a':[0,0]}
    fitted=fit_empirical(train,mu,q,bins=8);saved=json.dumps(fitted,sort_keys=True)
    restored=json.loads(saved);v=frame([999,999]);np.testing.assert_array_equal(actions(restored,v,[.1,.1],q),[1,1])
    np.testing.assert_array_equal(actions(restored,v.assign(ride_count=0),[.1,.1],q),[1,1])
    assert json.dumps(fitted,sort_keys=True)==saved
    assert fitted['states']['a']['training_class_prior']==[0,.9,.1]
    with pytest.raises(KeyError):actions(restored,v.assign(station_id='unknown'),[.1,.1],{'unknown':[0,0]})


def test_threshold_shifts_preserve_target_definition_and_reject_invalid_forecasts():
    f=frame([0,1]);q={'a':[0,0]};mu=np.array([.1,1.])
    assert actions({'kind':'threshold_shift','low_shift':0,'high_shift':.5},f,mu,q).tolist()==[1,2]
    assert count_classes(f.ride_count,0,0).tolist()==[1,2]
    with pytest.raises(ValueError):actions({'kind':'original'},f,[np.nan,1],q)
    with pytest.raises(ValueError):evaluate(f,mu,[1,4],q)
