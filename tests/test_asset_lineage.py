from types import SimpleNamespace
import json
import pytest
from jobs.asset_lineage import unique_lineages,run_asset_stage,event_payload

def lineage(month=1):
    return {'dataset':'citibike','source_year':2024,'source_month':month,'sha256':str(month)*64,'source_filepath':'/data/sample.csv','source_filename':'sample.csv'}

def test_duplicate_events_execute_once_and_preserve_canonical_run():
    calls=[]
    def runner(cmd,**kwargs):
        calls.append(cmd);return SimpleNamespace(returncode=0,stdout='LINEAGE_JSON:'+json.dumps({'ingestion_run_id':'original-committed-run'}),stderr='')
    result=run_asset_stage('bronze',[SimpleNamespace(extra=lineage()),lineage()],'config','new-attempt',runner)
    assert len(calls)==1 and result['ingestion_run_id']=='original-committed-run'

def test_coalesced_distinct_months_all_processed_and_propagated():
    calls=[]
    items=[{**lineage(month),'ingestion_run_id':f'run-{month}'} for month in (1,2)]
    def runner(cmd,**kwargs):calls.append(cmd);return SimpleNamespace(returncode=0,stdout='done',stderr='')
    result=run_asset_stage('silver',[{'dataset':'citibike','lineages':items},items[0]],'config','attempt',runner)
    assert len(calls)==2 and [item['source_month'] for item in result['lineages']]==[1,2]
    assert unique_lineages([result])==items

def test_conflicting_versions_cannot_arbitrarily_overwrite_a_month():
    with pytest.raises(ValueError,match='Conflicting source versions'):
        unique_lineages([lineage(),{**lineage(),'sha256':'a'*64}])

def test_conflicting_canonical_runs_fail():
    with pytest.raises(ValueError,match='canonical'):
        unique_lineages([{**lineage(),'ingestion_run_id':'one'},{**lineage(),'ingestion_run_id':'two'}])

@pytest.mark.parametrize('events',[[],[{}],[{'lineages':[]}],[{**lineage(),'sha256':'bad'}],[{**lineage(),'source_month':13}]])
def test_malformed_events_fail_before_job_execution(events):
    with pytest.raises(ValueError):unique_lineages(events)

def test_failed_job_never_returns_success_payload():
    def runner(*args,**kwargs):return SimpleNamespace(returncode=1,stdout='',stderr='injected failure')
    with pytest.raises(RuntimeError,match='failed'):run_asset_stage('bronze',[lineage()],'config','attempt',runner)
