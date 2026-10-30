"""Fail-closed host resource checks; Docker bind-mount disk reports are misleading."""
import json,math,time
from pathlib import Path

class ResourceStop(RuntimeError):pass

_LAST_GOOD={}

def _valid(data):
    if not isinstance(data,dict):raise ValueError('Heartbeat must be an object')
    stamp=data.get('timestamp');free=data.get('free_bytes')
    if isinstance(stamp,bool) or not isinstance(stamp,(float,int)) or not math.isfinite(stamp):raise ValueError('Invalid heartbeat timestamp')
    if isinstance(free,bool) or not isinstance(free,int) or free<0:raise ValueError('Invalid host free-space reading')
    seq=data.get('sequence',0)
    if isinstance(seq,bool) or not isinstance(seq,int) or seq<0:raise ValueError('Invalid heartbeat sequence')
    if stamp>time.time()+30:raise ValueError('Heartbeat clock is in the future')
    return data

def _journal_last(path):
    # Append-only fallback survives bind-mount replace gaps and partial last lines.
    with path.open('rb') as stream:
        stream.seek(0,2);size=stream.tell();start=max(0,size-32768);stream.seek(start);lines=stream.read().decode('utf-8-sig').splitlines()
    if start:lines=lines[1:]
    for line in reversed(lines):
        try:return _valid(json.loads(line.lstrip('\ufeff')))
        except (ValueError,TypeError):continue
    raise ValueError('No valid complete journal heartbeat')

def heartbeat(root):
    root=Path(root).resolve();path=root/'resources'/'host-disk.json';candidates=[];errors=[]
    for delay in [0,.02,.05]:
        if delay:time.sleep(delay)
        try:
            candidates.append((_valid(json.loads(path.read_text(encoding='utf-8-sig'))),'primary'));break
        except (OSError,ValueError,TypeError) as error:errors.append(type(error).__name__)
    try:candidates.append((_journal_last(root/'resources'/'host-disk.jsonl'),'journal'))
    except (OSError,ValueError,TypeError):pass
    if root in _LAST_GOOD:
        try:candidates.append((_valid(_LAST_GOOD[root]),'cached'))
        except ValueError:pass
    if not candidates:raise ResourceStop('No valid host disk monitor heartbeat: '+','.join(errors))
    data,source=max(candidates,key=lambda item:(item[0]['timestamp'],item[0].get('sequence',0)))
    # Never refresh a cached reading's producer timestamp. A dead producer still
    # fails closed after 120 seconds, regardless of consumer polls or fallbacks.
    if time.time()-data['timestamp']>120:raise ResourceStop('Host disk monitor heartbeat is stale')
    _LAST_GOOD[root]=dict(data)
    return {**data,'heartbeat_source':source,'heartbeat_age_seconds':max(0,time.time()-data['timestamp'])}

def check(root,source_bytes=0):
    root=Path(root);stop=root/'resource-stop.json'
    if stop.exists():raise ResourceStop(stop.read_text())
    data=heartbeat(root)
    reserve=10*1024**3+5*source_bytes
    if data['free_bytes']<reserve:raise ResourceStop(f'Host disk reserve breached: {data["free_bytes"]} < {reserve}')
    if stop.exists():raise ResourceStop(stop.read_text())
    return data
