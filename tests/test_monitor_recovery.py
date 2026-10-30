import json,time
from pathlib import Path
import pytest
from tools import ingestion_resources as resources

def pulse(free=100*1024**3,stamp=None,seq=1):
    return {'timestamp':time.time() if stamp is None else stamp,'free_bytes':free,'sequence':seq,'producer_id':'unit'}

def paths(tmp_path):
    directory=tmp_path/'resources';directory.mkdir();return directory/'host-disk.json',directory/'host-disk.jsonl'

def test_missing_primary_uses_complete_journal_without_refreshing_timestamp(tmp_path,monkeypatch):
    path,journal=paths(tmp_path);data=pulse();journal.write_text(json.dumps(data)+'\n{"unfinished":')
    monkeypatch.setattr(resources.time,'sleep',lambda _:None)
    result=resources.check(tmp_path)
    assert result['heartbeat_source']=='journal' and result['timestamp']==data['timestamp']

def test_transient_missing_reads_retry_then_succeed(tmp_path,monkeypatch):
    path,_=paths(tmp_path);path.write_text(json.dumps(pulse()));original=Path.read_text;calls=[]
    def read(self,*args,**kwargs):
        if self==path and len(calls)<2:calls.append(True);raise FileNotFoundError('bind-mount replace gap')
        return original(self,*args,**kwargs)
    monkeypatch.setattr(Path,'read_text',read);monkeypatch.setattr(resources.time,'sleep',lambda _:None)
    assert resources.check(tmp_path)['heartbeat_source']=='primary' and len(calls)==2

def test_cached_heartbeat_expires_when_producer_stops(tmp_path,monkeypatch):
    path,_=paths(tmp_path);stamp=time.time();path.write_text(json.dumps(pulse(stamp=stamp)))
    resources.check(tmp_path);path.unlink();monkeypatch.setattr(resources.time,'sleep',lambda _:None)
    assert resources.check(tmp_path)['heartbeat_source']=='cached'
    monkeypatch.setattr(resources.time,'time',lambda:stamp+121)
    with pytest.raises(resources.ResourceStop,match='stale'):resources.check(tmp_path)

def test_newest_low_disk_reading_is_never_hidden_by_old_cache(tmp_path):
    path,journal=paths(tmp_path);stamp=time.time();path.write_text(json.dumps(pulse(stamp=stamp)));resources.check(tmp_path)
    journal.write_text(json.dumps(pulse(free=9*1024**3,stamp=stamp+1,seq=2))+'\n')
    with pytest.raises(resources.ResourceStop,match='reserve'):resources.check(tmp_path)

def test_stale_primary_and_journal_and_stop_flags_fail_closed(tmp_path):
    path,journal=paths(tmp_path);old=pulse(stamp=time.time()-121);path.write_text(json.dumps(old));journal.write_text(json.dumps(old)+'\n')
    with pytest.raises(resources.ResourceStop,match='stale'):resources.check(tmp_path)
    path.write_text(json.dumps(pulse()));(tmp_path/'resource-stop.json').write_text('real stop')
    with pytest.raises(resources.ResourceStop,match='real stop'):resources.check(tmp_path)

def test_malformed_primary_recovers_newest_valid_journal(tmp_path,monkeypatch):
    path,journal=paths(tmp_path);path.write_text('{"partial"');data=pulse(seq=3);journal.write_text(json.dumps(data)+'\n')
    monkeypatch.setattr(resources.time,'sleep',lambda _:None)
    result=resources.check(tmp_path);assert result['sequence']==3 and result['heartbeat_source']=='journal'
