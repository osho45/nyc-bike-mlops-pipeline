import json,time
import pytest
from tools.multi_year import ordered_run
from tools.ingestion_resources import check,ResourceStop

def test_ordering_year_gate_and_recovery_stop_before_next_month():
    items=[{'year':2025,'month':1},{'year':2024,'month':2},{'year':2024,'month':1}];trace=[]
    ordered_run(items,lambda i:trace.append(('month',i['year'],i['month'])),lambda y:trace.append(('year',y)),lambda i:None)
    assert trace==[('month',2024,1),('month',2024,2),('year',2024),('month',2025,1),('year',2025)]
    trace=[]
    def fail(year):raise ValueError('audit fails')
    with pytest.raises(ValueError):ordered_run(items,lambda i:trace.append(i['year']),fail,lambda i:None)
    assert trace==[2024,2024]

def test_duplicates_and_resources_fail_before_execution():
    trace=[];item={'year':2024,'month':1}
    with pytest.raises(ValueError):ordered_run([item,item],trace.append,lambda y:None,lambda i:None)
    assert not trace
    def no_disk(item):raise ResourceStop('disk')
    with pytest.raises(ResourceStop):ordered_run([item],trace.append,lambda y:None,no_disk)
    assert not trace

def test_host_disk_guard_rejects_missing_stale_low_and_stop(tmp_path):
    directory=tmp_path/'resources';directory.mkdir();path=directory/'host-disk.json'
    with pytest.raises(ResourceStop):check(tmp_path)

    path.write_text(json.dumps({'timestamp':time.time()-180,'free_bytes':100*1024**3}))
    with pytest.raises(ResourceStop,match='stale'):check(tmp_path)
    path.write_text(json.dumps({'timestamp':time.time(),'free_bytes':11*1024**3}))
    with pytest.raises(ResourceStop,match='reserve'):check(tmp_path,1024**3)
    path.write_text(json.dumps({'timestamp':time.time(),'free_bytes':100*1024**3}))
    assert check(tmp_path,1024**3)['free_bytes']==100*1024**3
    (tmp_path/'resource-stop.json').write_text('controlled stop')
    with pytest.raises(ResourceStop):check(tmp_path)

def test_progress_never_claims_training_ready_before_year_gates(tmp_path):
    from tools.ingestion_status import progress
    (tmp_path/'checkpoints').mkdir();(tmp_path/'plan.json').write_text(json.dumps({'months':[{'year':2024,'month':1}]}))
    (tmp_path/'controlled-run.json').write_text(json.dumps({'status':'running'}))
    report=progress(tmp_path)
    assert not report['ready_for_model_training'] and not report['model_training_performed']
