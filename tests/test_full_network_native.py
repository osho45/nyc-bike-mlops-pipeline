"""No real training: full-cohort input integrity and resource admission contracts."""
import json
import numpy as np
import pytest
from jobs.full_native_data import StationMatrixSequence
from tools.full_network_supervisor import admission, runtime_reasons, schedule, GIB


def matrix(tmp_path, sizes=(3, 4, 5)):
    values=np.arange(sum(sizes)*2,dtype='float32').reshape(-1,2)
    parts=[];offset=0
    for i,size in enumerate(sizes):
        path=tmp_path/f'{i}.f32';values[offset:offset+size].tofile(path)
        parts.append({'x':path.name,'rows':size});offset+=size
    labels=np.arange(len(values),dtype='float32');labels.tofile(tmp_path/'labels.f32')
    path=tmp_path/'manifest.json';path.write_text(json.dumps({'parts':parts,'features':['a','b'],'label_file':'labels.f32'}))
    return StationMatrixSequence(path),values,labels


def test_cross_station_reader_and_owned_samples(tmp_path):
    seq,x,y=matrix(tmp_path)
    try:
        np.testing.assert_array_equal(seq[2:9],x[2:9])
        sample=seq[4];assert sample.dtype==np.float64 and sample.flags.owndata
        np.testing.assert_array_equal(seq[7:11],x[7:11])
        np.testing.assert_array_equal(sample,x[4])
        np.testing.assert_array_equal(seq[4],x[4])
        np.testing.assert_array_equal(seq.labels,y)
        assert seq[5:5].shape==(0,2)
        with pytest.raises(IndexError):seq[-1]
        with pytest.raises(IndexError):seq[len(seq)]
        with pytest.raises(ValueError):seq[::2]
    finally:seq.close()


def test_reader_rejects_truncation_and_unbounded_batch(tmp_path):
    seq,x,_=matrix(tmp_path,(5000,))
    try:
        with pytest.raises(ValueError,match='Bounded'):seq[:4097]
        (tmp_path/'0.f32').write_bytes(b'')
        with pytest.raises(ValueError,match='Truncated'):seq[0:1]
    finally:seq.close()


def test_all_development_months_and_stations_exactly_once():
    plan=schedule();assert len({key for key,_,_ in plan})==len(plan)
    raw=[args for key,_,args in plan if key.startswith('raw-')]
    assert {(int(a[3]),int(a[5])) for a in raw}=={(y,m) for y in [2024,2025] for m in range(1,13)}
    codes=[]
    for key,cap,args in plan:
        assert cap in [384,768,1536]
        assert (cap==384)==key.startswith('raw-')
        if key.startswith('features-'):codes.extend(range(int(args[3]),int(args[5])))
    assert codes==list(range(1898))
    assert [key for key,_,_ in plan[-4:]]==['seal','construct','fit','evaluate']


def test_actual_ram_admission_not_four_gib_requirement():
    sample={'host_available_bytes':3*GIB,'vm_available_bytes':3*GIB,'disk_free_bytes':20*GIB}
    assert admission(sample,1536*1024**2)==[]
    assert runtime_reasons(sample)==[]
    sample['host_available_bytes']=549249024
    assert 'host free RAM' in admission(sample,768*1024**2)[0]
    assert runtime_reasons(sample)
    sample['host_available_bytes']=3*GIB;sample['disk_free_bytes']=9*GIB
    assert admission(sample,1536*1024**2) and runtime_reasons(sample)


def test_authorization_excludes_test_and_deployment():
    from pathlib import Path
    config=json.loads(Path('config/full_network_native_authorized.json').read_text())
    assert config['stations']==1898 and config['allowed_years']==[2024,2025]
    assert config['training_enabled'] and not config['deployment_enabled']
    assert not config['automatic_resource_failure_retry']


def test_raw_phase_admission_keeps_bounded_headroom():
    cap=384*1024**2
    sample={'host_available_bytes':640*1024**2,'vm_available_bytes':GIB,'disk_free_bytes':20*GIB}
    assert not admission(sample,cap)
    sample['host_available_bytes']-=1
    assert admission(sample,cap)
    sample['host_available_bytes']=256*1024**2
    assert not runtime_reasons(sample,cap)
    sample['host_available_bytes']-=1
    assert runtime_reasons(sample,cap)
    # A smaller raw reserve must never carry over into fitting.
    sample['host_available_bytes']=768*1024**2
    assert admission(sample,1536*1024**2) and runtime_reasons(sample,1536*1024**2)


def test_insufficient_ram_exits_without_worker_or_stale_lease(tmp_path,monkeypatch):
    import sys
    import tools.full_network_supervisor as supervisor
    calls=[]
    def docker(*args):
        calls.append(args)
        return supervisor.EXPECTED_IMAGE if args[0]=='image' else 'npipe:////./pipe/docker_engine'
    monkeypatch.setattr(supervisor,'ROOT',tmp_path)
    monkeypatch.setattr(supervisor,'preserve_contract',lambda:None)
    monkeypatch.setattr(supervisor,'docker',docker)
    monkeypatch.setattr(supervisor,'resources',lambda:{'host_available_bytes':500*1024**2,'vm_available_bytes':2*GIB,'disk_free_bytes':20*GIB})
    monkeypatch.setattr(sys,'argv',['controller','--fail-fast'])
    supervisor.main()
    assert json.loads((tmp_path/'status.json').read_text())['state']=='blocked_resources'
    assert not (tmp_path/'controller-lease.json').exists()
    assert all(args[0]!='run' for args in calls)


def test_heartbeat_replace_retries_transient_windows_sharing_error(tmp_path,monkeypatch):
    from pathlib import Path
    import tools.full_network_supervisor as supervisor
    replace=Path.replace;calls=[]
    def transient(path,target):
        calls.append(path)
        if len(calls)<3:raise PermissionError('reader sharing lock')
        return replace(path,target)
    monkeypatch.setattr(Path,'replace',transient)
    monkeypatch.setattr(supervisor.time,'sleep',lambda _:None)
    supervisor.atomic(tmp_path/'resources.json',{'fresh':True})
    assert len(calls)==3 and json.loads((tmp_path/'resources.json').read_text())['fresh']


def test_heartbeat_sharing_retry_is_bounded_and_preserves_old_value(tmp_path,monkeypatch):
    from pathlib import Path
    import tools.full_network_supervisor as supervisor
    target=tmp_path/'resources.json';target.write_text('{"old":true}');calls=[]
    def locked(*args):calls.append(1);raise PermissionError('persistent lock')
    monkeypatch.setattr(Path,'replace',locked)
    monkeypatch.setattr(supervisor.time,'sleep',lambda _:None)
    with pytest.raises(PermissionError):supervisor.atomic(target,{'fresh':True})
    assert len(calls)==41 and json.loads(target.read_text())=={'old':True}
