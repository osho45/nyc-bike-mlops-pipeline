import pytest
from jobs.source_versions import publication_decision,revision,month_lock
from jobs.asset_lineage import unique_lineages

def state(rev=1,digest='a'*64,parent=None):return {'source_revision':rev,'sha256':digest,'supersedes_sha256':parent}

def test_explicit_chain_and_idempotent_replay():
    first=state();second=state(2,'b'*64,first['sha256'])
    assert publication_decision(None,first)=='publish'
    assert publication_decision(first,first)=='replay'
    assert publication_decision(first,second)=='publish'
    assert publication_decision(second,first)=='stale'
    assert publication_decision(second,second)=='replay'

@pytest.mark.parametrize('current,incoming',[(None,state(2,'b'*64,'a'*64)),(state(),state(1,'b'*64)),(state(),state(2,'b'*64,'c'*64)),(state(),state(3,'b'*64,'a'*64))])
def test_conflicts_and_skipped_predecessors_rejected(current,incoming):
    with pytest.raises(ValueError):publication_decision(current,incoming)

@pytest.mark.parametrize('value',[0,-1,True,1.5,'01'])
def test_invalid_revision_rejected(value):
    with pytest.raises(ValueError):revision(value)

def test_coalesced_corrected_versions_sorted_before_processing():
    base={'dataset':'citibike','source_year':2024,'source_month':1,'source_filepath':'source','source_filename':'source'}
    first={**base,**state()};second={**base,**state(2,'b'*64,'a'*64)}
    assert unique_lineages([second,first,second])==[first,second]

def test_publication_lock_releases_after_exception(tmp_path):
    with pytest.raises(RuntimeError):
        with month_lock(tmp_path,2024,1):raise RuntimeError('simulated job failure')
    with month_lock(tmp_path,2024,1):pass
