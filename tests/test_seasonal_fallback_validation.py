from copy import deepcopy
import pytest
from tools.validate_seasonal_fallback import acceptance


def guards():
    return [{'station_id':str(i),'dimension':'annual' if season=='all' else 'season','season':season,'passes':True} for i in range(48) for season in ['all','winter','spring','summer','fall']]


def test_acceptance_requires_no_aggregate_increase():
    g=guards();before={'total_operational_penalty':100}
    assert acceptance(before,{'total_operational_penalty':100},g,g)
    assert acceptance(before,{'total_operational_penalty':99},g,g)
    assert not acceptance(before,{'total_operational_penalty':101},g,g)


def test_aggregate_gain_cannot_hide_remaining_guard_failure():
    g=guards();after=deepcopy(g);after[7]['passes']=False
    assert not acceptance({'total_operational_penalty':100},{'total_operational_penalty':90},g,after)


def test_guard_coverage_cannot_drop_or_duplicate_checks():
    g=guards();m={'total_operational_penalty':100}
    with pytest.raises(ValueError):acceptance(m,m,g,g[:-1])
    bad=deepcopy(g);bad[1]=deepcopy(bad[2])
    with pytest.raises(ValueError,match='coverage'):acceptance(m,m,g,bad)
