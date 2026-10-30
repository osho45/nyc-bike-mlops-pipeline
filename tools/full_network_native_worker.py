"""Explicitly authorized full-cohort phases; no 2026 IO or production updates."""
import argparse,json,time,threading,os,hashlib,shutil,gc
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import lightgbm as lgb
from jobs.native_lgbm import MatrixWriter,BoundedRawWriters,PhaseMonitor,canonical_keys
from jobs.full_native_data import StationMatrixSequence
from jobs.scaling import FEATURE_COLUMNS,station_feature_stream,digest
from jobs.ml_features import build_training_features
from jobs.native_fit_memory import release_python_labels,trim_allocator
from tools.native_network_capacity import DATA_PARAMS,TRAIN_PARAMS
from tools.bounded_ml_readiness import atomic

ROOT=Path('/opt/airflow/artifacts/full-network-native-01')
PLAN=Path('/opt/airflow/artifacts/network-scaling-prep-02/stage_plans/all_eligible.json')
TRANSFER=Path('/opt/airflow/artifacts/training-transfer-01/manifest.json')

def guard():
    data=json.loads((ROOT/'resources.json').read_text());age=time.time()-data['timestamp']
    limit=int(Path('/sys/fs/cgroup/memory.max').read_text());reserve=256*1024**2 if limit==384*1024**2 else 1024**3
    if not 0<=age<=30 or data['host_available_bytes']<reserve or data['vm_available_bytes']<512*1024**2 or data['disk_free_bytes']<10*1024**3:raise RuntimeError('Runtime memory/disk/heartbeat reserve breached')
    if (ROOT/'resource-stop.json').exists():raise RuntimeError('Resource-stop marker exists')
    limit=int(Path('/sys/fs/cgroup/memory.max').read_text());swap=int(Path('/sys/fs/cgroup/memory.swap.max').read_text());quota,period=map(int,Path('/sys/fs/cgroup/cpu.max').read_text().split())
    if limit not in [384*1024**2,768*1024**2,1536*1024**2] or swap!=0 or not 0<quota<=2*period:raise RuntimeError('Unexpected hard resource limits')

def verified_source():
    frozen=json.loads((ROOT/'source_contract.json').read_text())
    for name,h in frozen['code_sha256'].items():
        if digest(name)!=h:raise RuntimeError('Source code changed during run')
    if digest(PLAN)!=frozen['cohort_sha256'] or digest(TRANSFER)!=frozen['transfer_sha256']:raise RuntimeError('Frozen input manifest changed')

def attempt(parent):
    parent=Path(parent);parent.mkdir(parents=True,exist_ok=True);i=1
    while (parent/f'attempt-{i:03d}').exists():i+=1
    folder=parent/f'attempt-{i:03d}';folder.mkdir();return folder

def done_path(path):
    data=json.loads(Path(path).read_text());folder=ROOT/data['path']
    if not folder.is_dir():raise ValueError('Checkpoint data missing')
    return folder

def raw_month(year,month,ids,codes):
    checkpoint=ROOT/f'raw/{year}-{month:02d}/done.json'
    if checkpoint.exists():return {'reused':True}
    records=[x for x in json.loads(TRANSFER.read_text())['files'] if x['category']=='development' and x.get('year')==year and x.get('month')==month and '/station_hourly_observations/data/' in x['path']]
    if len(records)!=1:raise ValueError('One approved source partition required')
    rec=records[0];source=Path('/opt/airflow')/rec['path'];assert digest(source)==rec['sha256']
    folder=attempt(checkpoint.parent);writer=BoundedRawWriters(folder/'parts',8);selected=0
    try:
        for batch in pq.ParquetFile(source).iter_batches(batch_size=32768,columns=['station_id','station_hour_ts','ride_count','target_year','target_month'],use_threads=False):
            guard();part=batch.filter(pc.is_in(batch.column(0),value_set=pa.array(ids)));selected+=part.num_rows
            for (y,station),frame in part.to_pandas().groupby(['target_year','station_id']):
                if int(y)!=year or set(frame.target_month)!={month}:raise ValueError('Source month/role mismatch')
                writer.append(y,codes[str(station)],frame)
    finally:writer.close()
    start=pd.Timestamp(year=year,month=month,day=1,tz='America/New_York');end=start+pd.offsets.MonthBegin(1);hours=len(pd.date_range(start,end,freq='h',inclusive='left'));assert selected==len(ids)*hours
    hashes={str(x.relative_to(folder)):digest(x) for x in folder.rglob('*.parquet')};atomic(folder/'sha256.json',hashes);atomic(checkpoint,{'path':str(folder.relative_to(ROOT)),'rows':selected,'source_sha256':rec['sha256']})
    return {'year':year,'month':month,'rows':selected}

def features(first,last,ids,codes):
    completed=0;raw_cache={}
    for year in [2024,2025]:
        for month in range(1,13):
            raw=done_path(ROOT/f'raw/{year}-{month:02d}/done.json');raw_cache[(year,month)]=(raw,json.loads((raw/'sha256.json').read_text()))
    for code in range(first,min(last,len(ids))):
        parent=ROOT/f'stations/{code:04d}';marker=parent/'done.json'
        if marker.exists():completed+=1;continue
        paths=[]
        for year in [2024,2025]:
            for month in range(1,13):
                raw,hashes=raw_cache[(year,month)];parts=sorted((raw/f'parts/{year}/{code}').glob('*.parquet'));assert parts
                for part in parts:assert digest(part)==hashes[str(part.relative_to(raw))]
                paths.extend(parts)
        history=pd.concat([pd.read_parquet(p) for p in paths],ignore_index=True).sort_values('station_hour_ts').reset_index(drop=True);times=pd.DatetimeIndex(pd.to_datetime(history.station_hour_ts,utc=True))
        assert len(history)==17544 and set(history.station_id.astype(str))=={ids[code]} and not times.duplicated().any() and (times[1:]-times[:-1]==pd.Timedelta(hours=1)).all()
        expected=build_training_features(history);expected['station_code']=code;expected[FEATURE_COLUMNS]=expected[FEATURE_COLUMNS].astype('float32');offset=0;folder=attempt(parent);writers={role:MatrixWriter(folder/role,role) for role in ['train','validation']};source=pd.Series(history.ride_count.to_numpy(),index=times)
        try:
            for batch in station_feature_stream((history.iloc[i:i+4096] for i in range(0,len(history),4096)),code):
                target=pd.DatetimeIndex(pd.to_datetime(batch.station_hour_ts,utc=True));np.testing.assert_array_equal(batch.ride_count,source.reindex(target))
                for lag in [1,2,24,168]:np.testing.assert_array_equal(batch[f'lag_{lag}h'],source.reindex(target-pd.Timedelta(hours=lag)))
                np.testing.assert_allclose(batch[FEATURE_COLUMNS],expected.iloc[offset:offset+len(batch)][FEATURE_COLUMNS],rtol=2e-5,atol=2e-5);assert target.equals(pd.DatetimeIndex(expected.iloc[offset:offset+len(batch)].station_hour_ts));offset+=len(batch)
                for role,part in batch.groupby('role'):writers[role].append(part)
            manifests={role:w.seal() for role,w in writers.items()};assert manifests['train']['rows']==8616 and manifests['validation']['rows']==8760 and offset==17376
        except BaseException:
            for writer in writers.values():writer.close()
            raise
        atomic(marker,{'path':str(folder.relative_to(ROOT)),'station_id':ids[code],'station_code':code,'manifests':manifests,'all_lags_and_continuity_passed':True});completed+=1
        print(f'station {code+1}/{len(ids)} complete: {ids[code]}',flush=True);del history,expected,source;gc.collect();guard()
    return {'completed_in_block':completed,'stations_complete':sum(1 for _ in (ROOT/'stations').glob('*/done.json'))}

def seal(ids):
    if (ROOT/'sealed.json').exists():return {'reused':True}
    folder=attempt(ROOT/'matrices');manifests={}
    for role in ['train','validation']:
        label=folder/f'{role}.labels.f32';parts=[];h=hashlib.sha256();rows=0
        with label.open('xb') as out:
            for code in range(len(ids)):
                marker=ROOT/f'stations/{code:04d}/done.json';saved=json.loads(marker.read_text());assert saved['station_id']==ids[code] and saved['station_code']==code
                station=done_path(marker);meta=json.loads((station/role/'manifest.json').read_text());assert meta==saved['manifests'][role] and meta['features']==FEATURE_COLUMNS and meta['role']==role and meta['rows']==(8616 if role=='train' else 8760)
                for name,expected in meta['files'].items():assert digest(station/role/name)==expected
                with (station/role/'y.f32').open('rb') as inp:shutil.copyfileobj(inp,out,1024*1024)
                parts.append({'x':os.path.relpath(station/role/'X.f32',folder),'keys':os.path.relpath(station/role/'keys.parquet',folder),'rows':meta['rows'],'files':meta['files']});rows+=meta['rows']
                for batch in pq.ParquetFile(station/role/'keys.parquet').iter_batches(batch_size=4096,use_threads=False):h.update(canonical_keys(batch.to_pandas()))
                guard()
        assert rows==len(ids)*(8616 if role=='train' else 8760)
        manifest={'role':role,'rows':rows,'features':FEATURE_COLUMNS,'label_file':label.name,'label_sha256':digest(label),'key_sha256':h.hexdigest(),'parts':parts};atomic(folder/f'{role}.json',manifest);manifests[role]=str((folder/f'{role}.json').relative_to(ROOT))
    atomic(ROOT/'sealed.json',{'path':str(folder.relative_to(ROOT)),'roles':manifests,'manifest_sha256':{role:digest(ROOT/path) for role,path in manifests.items()}});return {'rows':{role:len(ids)*(8616 if role=='train' else 8760) for role in manifests}}

def seq_for(role):
    marker=json.loads((ROOT/'sealed.json').read_text());path=ROOT/marker['roles'][role];assert digest(path)==marker['manifest_sha256'][role];data=json.loads(path.read_text());assert data['role']==role and data['features']==FEATURE_COLUMNS and sum(p['rows'] for p in data['parts'])==data['rows'];assert digest(path.parent/data['label_file'])==data['label_sha256']
    for part in data['parts']:
        parent=(path.parent/part['x']).parent
        for name,h in part['files'].items():assert digest(parent/name)==h
    return StationMatrixSequence(path)

def construct(ids):
    marker=ROOT/'native/done.json'
    if marker.exists():return {'reused':True}
    seq=seq_for('train');folder=attempt(ROOT/'native')
    try:
        data=lgb.Dataset(seq,label=seq.labels,feature_name=FEATURE_COLUMNS,params=DATA_PARAMS,free_raw_data=True).construct();assert data.num_data()==len(ids)*8616 and data.get_feature_name()==FEATURE_COLUMNS
        labels=data.get_label()
        for offset in range(0,len(seq),4096):np.testing.assert_array_equal(labels[offset:offset+4096],seq.labels[offset:offset+4096])
        del labels;release_python_labels(data);trim_allocator();data.save_binary(folder/'train.bin');sha=digest(folder/'train.bin')
        atomic(marker,{'path':str(folder.relative_to(ROOT)),'train_sha256':sha,'rows':len(seq),'bin_samples':seq.scalar_reads,'validation_used_for_bins':False})
    finally:seq.close()
    return {'train_rows':len(ids)*8616,'native_sha256':sha}

def fit(ids):
    if (ROOT/'fit/done.json').exists():return {'reused':True}
    source=done_path(ROOT/'native/done.json');expected=json.loads((ROOT/'native/done.json').read_text());assert digest(source/'train.bin')==expected['train_sha256'];folder=attempt(ROOT/'fit');data=lgb.Dataset(str(source/'train.bin'),params=DATA_PARAMS,free_raw_data=True).construct();assert data.num_data()==len(ids)*8616 and data.get_feature_name()==FEATURE_COLUMNS;release_python_labels(data);trim_allocator();history={};start=time.monotonic()
    def checkpoint(env):
        if (env.iteration+1)%10==0:
            guard();env.model.save_model(str(folder/f'iteration-{env.iteration+1:03d}.txt'));print(f'global fit {env.iteration+1}/100',flush=True)
    model=lgb.train(TRAIN_PARAMS,data,num_boost_round=100,valid_sets=[data],valid_names=['train'],callbacks=[lgb.record_evaluation(history),checkpoint]);seconds=time.monotonic()-start
    assert model.current_iteration()==100 and all(np.isfinite(v).all() for v in history['train'].values()) and history['train']['poisson'][-1]<=history['train']['poisson'][0]
    model.save_model(str(folder/'model.txt'));atomic(folder/'training_curves.json',history);atomic(ROOT/'fit/done.json',{'path':str(folder.relative_to(ROOT)),'model_sha256':digest(folder/'model.txt'),'fit_seconds':seconds,'rounds':100})
    return {'fit_seconds':seconds,'training_rows':len(ids)*8616,'model_sha256':digest(folder/'model.txt')}

def evaluate(ids):
    if (ROOT/'evaluation/done.json').exists():return {'reused':True}
    modelpath=done_path(ROOT/'fit/done.json')/'model.txt';assert digest(modelpath)==json.loads((ROOT/'fit/done.json').read_text())['model_sha256'];model=lgb.Booster(model_file=str(modelpath));folder=attempt(ROOT/'evaluation');metrics={};hashes={};curves=None;start=time.monotonic()
    for role in ['train','validation']:
        seq=seq_for(role);offset=0;key_hash=hashlib.sha256();absolute=square=total=0.;writer=None;curve_abs=np.zeros(100);curve_sq=np.zeros(100)
        try:
            for part in seq.parts:
                for batch in pq.ParquetFile(seq.root/part['keys']).iter_batches(batch_size=4096,use_threads=False):
                    keys=batch.to_pandas();n=len(keys);x=seq[offset:offset+n];target=keys.ride_count.to_numpy(dtype='float64');np.testing.assert_array_equal(target,seq.labels[offset:offset+n]);pred=model.predict(x,num_threads=2)
                    if not np.isfinite(pred).all() or (pred<0).any():raise ValueError('Invalid global forecast')
                    err=pred-target;absolute+=float(np.abs(err).sum());square+=float((err**2).sum());total+=float(target.sum());key_hash.update(canonical_keys(keys));keys['forecast']=pred;table=pa.Table.from_pandas(keys,preserve_index=False)
                    if writer is None:writer=pq.ParquetWriter(folder/f'{role}_predictions.parquet',table.schema,compression='zstd')
                    writer.write_table(table)
                    if role=='validation':
                        raw=np.zeros(n)
                        for i in range(100):
                            raw+=model.predict(x,start_iteration=i,num_iteration=1,raw_score=True,num_threads=2);step=np.exp(raw);error=step-target;curve_abs[i]+=np.abs(error).sum();curve_sq[i]+=(error**2).sum()
                        np.testing.assert_allclose(np.exp(raw),pred,rtol=1e-12,atol=1e-12)
                    offset+=n;guard()
        finally:
            if writer:writer.close()
            seq.close()
        assert offset==len(ids)*(8616 if role=='train' else 8760) and key_hash.hexdigest()==seq.manifest['key_sha256'];metrics[role]={'rows':offset,'mae':absolute/offset,'rmse':float(np.sqrt(square/offset)),'wape':absolute/total};hashes[role]={'file_sha256':digest(folder/f'{role}_predictions.parquet'),'row_key_sha256':key_hash.hexdigest()}
        if role=='validation':curves={'mae':(curve_abs/offset).tolist(),'rmse':np.sqrt(curve_sq/offset).tolist(),'wape':(curve_abs/total).tolist()};np.testing.assert_allclose(curves['mae'][-1],metrics[role]['mae'],rtol=1e-10)
    atomic(folder/'validation_curves.json',curves);atomic(folder/'metrics.json',metrics);atomic(folder/'retention.json',hashes);atomic(ROOT/'evaluation/done.json',{'path':str(folder.relative_to(ROOT)),'metrics':metrics,'seconds':time.monotonic()-start});return {'metrics':metrics,'seconds':time.monotonic()-start}

class Watch:
    def __enter__(self):self.event=threading.Event();self.thread=threading.Thread(target=self.loop,daemon=True);self.thread.start();return self
    def loop(self):
        while not self.event.wait(2):
            try:guard()
            except Exception as error:atomic(ROOT/'resource-stop.json',{'error':repr(error)});os._exit(75)
    def __exit__(self,*args):self.event.set();self.thread.join(3)

def main():
    p=argparse.ArgumentParser();p.add_argument('--task',choices=['raw','features','seal','construct','fit','evaluate'],required=True);p.add_argument('--key',required=True);p.add_argument('--year',type=int);p.add_argument('--month',type=int);p.add_argument('--first',type=int,default=0);p.add_argument('--last',type=int,default=0);a=p.parse_args();guard();verified_source();cohort=json.loads(PLAN.read_text());ids=cohort['stations'];codes=cohort['prospective_station_codes'];assert len(ids)==1898 and ids==sorted(set(ids)) and codes=={s:i for i,s in enumerate(ids)}
    if a.task=='raw' and (a.year not in [2024,2025] or a.month not in range(1,13)):raise ValueError('Development months only')
    folder=attempt(ROOT/'jobs'/a.key)
    if int(Path('/sys/fs/cgroup/memory.max').read_text())==384*1024**2 and a.task!='raw':raise RuntimeError('Reduced cap is approved only for raw extraction')
    try:
        with Watch(),PhaseMonitor(folder,a.task) as monitor:
            before=time.monotonic()
            result=raw_month(a.year,a.month,ids,codes) if a.task=='raw' else features(a.first,a.last,ids,codes) if a.task=='features' else globals()[a.task](ids)
            seconds=time.monotonic()-before
        memory=monitor.summary();events=dict(x.split() for x in memory['memory_events'].splitlines());assert memory['kernel_container_peak_bytes']<=.85*memory['memory_limit_bytes'] and not any(int(events[x]) for x in ['max','oom','oom_kill'])
        result.update(status='complete',correctness_passed=True,seconds=seconds,memory=memory,read_2026=False);atomic(folder/'result.json',result);atomic(ROOT/'jobs'/a.key/'done.json',{'path':str(folder.relative_to(ROOT))});print(json.dumps(result),flush=True)
    except BaseException as error:atomic(folder/'failure.json',{'error':repr(error)});raise

if __name__=='__main__':main()
