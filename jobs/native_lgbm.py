"""Disk-backed native LightGBM data adapter and bounded phase instrumentation."""
import hashlib,json,os,threading,time
from collections import OrderedDict
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lightgbm import Sequence
from jobs.scaling import BASE_COLUMNS,FEATURE_COLUMNS,digest,validate_development


def canonical_keys(frame):
    f=pd.DataFrame({'station':frame.station_id.astype(str),'utc_ns':pd.DatetimeIndex(pd.to_datetime(frame.station_hour_ts,utc=True)).as_unit('ns').asi8,'actual':frame.ride_count.astype('float64')})
    return pd.util.hash_pandas_object(f,index=False).to_numpy().tobytes()


class MatrixWriter:
    def __init__(self,root,role):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=False);self.role=role;self.x=(self.root/'X.f32').open('xb');self.y=(self.root/'y.f32').open('xb');self.keys=None;self.n=0;self.key_hash=hashlib.sha256()
    def append(self,frame):
        validate_development(frame,self.role);x=frame[FEATURE_COLUMNS].to_numpy(dtype='float32');y=frame.ride_count.to_numpy(dtype='float32')
        if not np.isfinite(x).all() or not np.isfinite(y).all() or (y<0).any() or not np.equal(y,np.floor(y)).all():raise ValueError('Invalid matrix data')
        if not np.array_equal(frame.ride_count.to_numpy(dtype='float64'),y.astype('float64')):raise ValueError('Target counts are not exactly representable in native float32 labels')
        x.tofile(self.x);y.tofile(self.y);self.key_hash.update(canonical_keys(frame));self.n+=len(frame)
        table=pa.Table.from_pandas(frame[BASE_COLUMNS],preserve_index=False)
        if self.keys is None:self.keys=pq.ParquetWriter(self.root/'keys.parquet',table.schema,compression='zstd')
        self.keys.write_table(table)
    def close(self):
        self.x.close();self.y.close()
        if self.keys:self.keys.close()
    def seal(self):
        self.close()
        if not self.n:raise ValueError('Empty native matrix')
        manifest={'status':'complete','role':self.role,'rows':self.n,'features':FEATURE_COLUMNS,'dtype':'float32','key_sha256':self.key_hash.hexdigest(),'files':{name:digest(self.root/name) for name in ['X.f32','y.f32','keys.parquet']}}
        (self.root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');return manifest


class DiskSequence(Sequence):
    batch_size=4096
    def __init__(self,root):
        self.root=Path(root);self.manifest=json.loads((self.root/'manifest.json').read_text())
        if self.manifest['features']!=FEATURE_COLUMNS or self.manifest['dtype']!='float32':raise ValueError('Feature contract mismatch')
        self.rows=self.manifest['rows'];self.matrix=np.memmap(self.root/'X.f32',mode='r',dtype='float32',shape=(self.rows,len(FEATURE_COLUMNS)))
        self.labels=np.memmap(self.root/'y.f32',mode='r',dtype='float32',shape=(self.rows,));self.scalar_reads=0;self.batch_reads=0;self.max_batch_seen=0
    def __len__(self):return self.rows
    def __getitem__(self,index):
        if isinstance(index,(int,np.integer)):
            self.scalar_reads+=1
            # LightGBM's Sequence sampling API requires double-precision samples.
            # Convert one sampled row, NOT the whole matrix.
            return np.asarray(self.matrix[index],dtype='float64')
        if isinstance(index,slice):
            if index.step not in [None,1]:raise ValueError('Contiguous batches required')
            start,stop,step=index.indices(self.rows)
            if stop-start>self.batch_size:raise ValueError('Native row batch exceeds limit')
            self.batch_reads+=1;self.max_batch_seen=max(self.max_batch_seen,stop-start)
            return np.asarray(self.matrix[index],dtype='float32',order='C')
        if isinstance(index,(list,np.ndarray)):
            if len(index)>self.batch_size:raise ValueError('Indexed batch exceeds limit')
            return np.asarray(self.matrix[index],dtype='float32',order='C')
        raise TypeError('Unsupported native sequence index')
    def verify(self):
        for name,h in self.manifest['files'].items():
            if digest(self.root/name)!=h:raise ValueError('Matrix hash mismatch')
        offset=0;keys=hashlib.sha256()
        for batch in pq.ParquetFile(self.root/'keys.parquet').iter_batches(batch_size=self.batch_size,use_threads=False):
            frame=batch.to_pandas();validate_development(frame,self.manifest['role']);n=len(frame);np.testing.assert_array_equal(self.labels[offset:offset+n],frame.ride_count.to_numpy(dtype='float32'));keys.update(canonical_keys(frame));offset+=n
        if offset!=len(self) or keys.hexdigest()!=self.manifest['key_sha256']:raise ValueError('Matrix key/label alignment mismatch')
        return True


class BoundedRawWriters:
    """LRU capped writers; eviction creates a new immutable part on reopening."""
    def __init__(self,root,max_open=8):self.root=Path(root);self.max_open=max_open;self.open=OrderedDict();self.parts={};self.peak_open=0
    def append(self,year,code,frame):
        key=(int(year),int(code));table=pa.Table.from_pandas(frame,preserve_index=False)
        if key in self.open:writer=self.open.pop(key);self.open[key]=writer
        else:
            if len(self.open)>=self.max_open:_,writer=self.open.popitem(last=False);writer.close()
            part=self.parts.get(key,0);self.parts[key]=part+1;folder=self.root/str(year)/str(code);folder.mkdir(parents=True,exist_ok=True);path=folder/f'part-{part:06d}.parquet'
            if path.exists():raise FileExistsError('Refuse raw-part overwrite')
            self.open[key]=pq.ParquetWriter(path,table.schema,compression='zstd');self.peak_open=max(self.peak_open,len(self.open))
        self.open[key].write_table(table)
    def close(self):
        for writer in self.open.values():writer.close()
        self.open.clear()


class PhaseMonitor:
    def __init__(self,output,phase):
        self.output=Path(output);self.phase=phase;self.done=threading.Event();self.rows=[];self.started=time.monotonic();self.thread=threading.Thread(target=self._loop,daemon=True)
    def _sample(self):
        current=int(Path('/sys/fs/cgroup/memory.current').read_text());limit=int(Path('/sys/fs/cgroup/memory.max').read_text());stats=dict(line.split() for line in Path('/sys/fs/cgroup/memory.stat').read_text().splitlines());rss=int(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmRSS:')))*1024
        row={'elapsed':time.monotonic()-self.started,'phase':self.phase,'current_bytes':current,'anon_bytes':int(stats['anon']),'file_bytes':int(stats['file']),'working_set_bytes':max(0,current-int(stats.get('inactive_file',0))),'process_rss_bytes':rss};self.rows.append(row)
        if current>.85*limit:
            (self.output/'resource-stop.json').write_text(json.dumps({'reason':'85% container memory gate exceeded','sample':row,'limit':limit}));os._exit(75)
    def _loop(self):
        with (self.output/'memory.csv').open('w',buffering=1) as f:
            f.write('elapsed,phase,current_bytes,anon_bytes,file_bytes,working_set_bytes,process_rss_bytes\n')
            while not self.done.is_set():
                self._sample();f.write(','.join(str(self.rows[-1][k]) for k in ['elapsed','phase','current_bytes','anon_bytes','file_bytes','working_set_bytes','process_rss_bytes'])+'\n');self.done.wait(.02)
    def __enter__(self):self.thread.start();return self
    def mark(self,name):self.phase=name;self._sample()
    def __exit__(self,*args):self.done.set();self.thread.join(2);self._sample()
    def summary(self):
        phases={}
        for name in sorted({x['phase'] for x in self.rows}):
            subset=[x for x in self.rows if x['phase']==name];phases[name]={k:max(x[k] for x in subset) for k in ['current_bytes','anon_bytes','file_bytes','working_set_bytes','process_rss_bytes']}
        return {'elapsed_seconds':time.monotonic()-self.started,'kernel_container_peak_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'memory_limit_bytes':int(Path('/sys/fs/cgroup/memory.max').read_text()),'memory_events':Path('/sys/fs/cgroup/memory.events').read_text(),'phase_sampled_peaks':phases,'sample_interval_seconds':.02}
