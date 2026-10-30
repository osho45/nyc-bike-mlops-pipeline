import numpy as np
import pytest
from jobs.native_fit_memory import fit_settings,validate_deferred,release_python_labels


def test_fit_variants_never_change_original_parameters():
 base={'objective':'poisson','num_threads':2,'max_bin':63,'num_leaves':15}
 assert fit_settings(base,'compact')==base
 assert fit_settings(base,'deferred')==base
 assert fit_settings(base,'hist8')['histogram_pool_size']==8
 assert fit_settings(base,'one_thread')['num_threads']==1
 assert 'histogram_pool_size' not in base and base['num_threads']==2
 with pytest.raises(ValueError):fit_settings(base,'full_network')


def test_deferred_validation_is_restricted_to_fixed_budget():
 validate_deferred(100,False)
 with pytest.raises(ValueError):validate_deferred(100,True)
 with pytest.raises(ValueError):validate_deferred(30,False)


def test_label_release_drops_python_cache_without_modifying_native_labels():
 class Fixture:
  def __init__(self):self.native=np.arange(5,dtype='float32');self.label=self.native.copy()
  def set_label(self,value):raise AssertionError('Native label mutation is forbidden')
 d=Fixture();before=d.native.copy();assert release_python_labels(d)==20 and d.label is None
 np.testing.assert_array_equal(before,d.native)
 assert release_python_labels(d)==0


def test_incremental_poisson_predictions_match_prefix_and_fail_misalignment():
 from tools.native_deferred_curves import add_tree_prediction
 raw=np.zeros(3);first=np.array([.1,.2,.3]);second=np.array([.01,-.02,.05])
 np.testing.assert_array_equal(add_tree_prediction(raw,first),np.exp(first))
 np.testing.assert_array_equal(add_tree_prediction(raw,second),np.exp(first+second))
 with pytest.raises(ValueError,match='alignment'):add_tree_prediction(raw,np.zeros(2))
 with np.errstate(over='ignore'):
  with pytest.raises(ValueError,match='Non-finite'):add_tree_prediction(raw,np.full(3,1e6))
