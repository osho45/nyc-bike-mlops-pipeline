"""Development-only bounded feature streams, metric reduction and artifact contracts.

No model fitting/prediction APIs are imported or invoked by this module.
"""
import hashlib,json
from pathlib import Path
import numpy as np
import pandas as pd
from jobs.ml_features import REQUIRED_FEATURES,build_training_features,COST_MATRIX

FEATURE_COLUMNS=REQUIRED_FEATURES+['station_code']
BASE_COLUMNS=['station_id','station_hour_ts','ride_count']
ROLE_YEAR={'train':2024,'validation':2025}


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def validate_development(frame,role=None):
    if frame.empty:return
    local=pd.to_datetime(frame.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
    allowed={ROLE_YEAR[role]} if role is not None else {2024,2025}
    if not set(local.dt.year).issubset(allowed):raise ValueError('Only 2024 training / 2025 validation permitted')


def station_feature_stream(chunks,station_code,max_batch_rows=4096):
    """One station, increasing UTC hours, 168 raw-row carry across batches/years.

    Canonical sharding/sorting is the caller's responsibility. Unsorted input
    is rejected; a gap restarts warmup rather than manufacturing observations.
    """
    tail=None;previous=None;station=None
    for chunk in chunks:
        if len(chunk)>max_batch_rows:raise ValueError('Input chunk exceeds configured row cap')
        if chunk.empty:continue
        chunk=chunk[BASE_COLUMNS].copy();chunk['station_hour_ts']=pd.to_datetime(chunk.station_hour_ts,utc=True)
        validate_development(chunk)
        if chunk.station_id.astype(str).nunique()!=1:raise ValueError('One station per stream required')
        identity=str(chunk.station_id.iloc[0])
        if station is not None and identity!=station:raise ValueError('Station changed within stream')
        station=identity;times=chunk.station_hour_ts
        if not times.is_monotonic_increasing or times.duplicated().any() or (previous is not None and times.iloc[0]<=previous):raise ValueError('Duplicate or unordered station hours')
        if (pd.DatetimeIndex(times).as_unit('ns').asi8%(3600*10**9)!=0).any():raise ValueError('Station hours must be UTC-hour aligned')
        y=chunk.ride_count.to_numpy(dtype=float)
        if not np.isfinite(y).all() or (y<0).any() or not np.equal(y,np.floor(y)).all():raise ValueError('Counts must be finite nonnegative integers')
        previous=times.iloc[-1];work=pd.concat([tail,chunk],ignore_index=True) if tail is not None else chunk
        built=build_training_features(work);built=built.loc[built.station_hour_ts>=times.iloc[0]].copy();built['station_code']=station_code
        built[FEATURE_COLUMNS]=built[FEATURE_COLUMNS].astype('float32');local=built.station_hour_ts.dt.tz_convert('America/New_York');built['role']=np.where(local.dt.year==2024,'train','validation')
        tail=work[BASE_COLUMNS].tail(168).copy()
        if len(built):yield built[BASE_COLUMNS+FEATURE_COLUMNS+['role']]


def parquet_feature_batches(paths,role,batch_rows=4096):
    """Projected row batches; never concatenates the complete training population."""
    import pyarrow.parquet as pq
    if role not in ROLE_YEAR:raise ValueError('Unknown development role')
    for path in sorted(map(Path,paths)):
        for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_rows,columns=BASE_COLUMNS+FEATURE_COLUMNS,use_threads=False):
            frame=batch.to_pandas();validate_development(frame,role)
            matrix=frame[FEATURE_COLUMNS].to_numpy(dtype='float32')
            if not np.isfinite(matrix).all():raise ValueError('Nonfinite feature matrix')
            y=frame.ride_count.to_numpy(dtype=float)
            if not np.isfinite(y).all() or (y<0).any() or not np.equal(y,np.floor(y)).all():raise ValueError('Invalid target counts')
            frame[FEATURE_COLUMNS]=matrix
            yield frame


class MetricAccumulator:
    """Associative aggregate metrics from EXISTING counts/actions, without inference."""
    def __init__(self):self.n=0;self.actual=0.;self.absolute=0.;self.square=0.;self.cost=0.
    def update(self,actual,forecast,actual_class,decision):
        y=np.asarray(actual,dtype=float);p=np.asarray(forecast,dtype=float);a=np.asarray(actual_class);d=np.asarray(decision)
        if not(y.shape==p.shape==a.shape==d.shape) or y.ndim!=1 or not np.isfinite(y).all() or not np.isfinite(p).all() or (y<0).any() or (p<0).any() or not np.isin(a,[0,1,2]).all() or not np.isin(d,[0,1,2]).all():raise ValueError('Invalid recorded metric batch')
        err=p-y;self.n+=len(y);self.actual+=float(y.sum());self.absolute+=float(np.abs(err).sum());self.square+=float(np.square(err).sum());self.cost+=float(COST_MATRIX[a.astype(int),d.astype(int)].sum())
    def result(self):
        if not self.n:raise ValueError('No metric observations')
        return {'rows':self.n,'mae':self.absolute/self.n,'rmse':float(np.sqrt(self.square/self.n)),'wape':self.absolute/self.actual if self.actual else None,'mean_operational_penalty':self.cost/self.n,'total_operational_penalty':self.cost}


class DevelopmentArtifactSink:
    """New run only; full features, recorded predictions, hashes and guard evidence.

    Preparation can finalize without a model. A future model run cannot finalize
    without training+validation predictions, feature matrices and guard records.
    """
    def __init__(self,root,provenance,read_only_roots=()):
        self.root=Path(root).resolve()
        if any(self.root.is_relative_to(Path(p).resolve()) for p in read_only_roots):raise ValueError('Output overlaps protected inputs')
        self.root.mkdir(parents=True,exist_ok=False);self.provenance=provenance;self.files=[];self.counts={};self.parts={};self.keys={};self.model=None;self.guards=False
    def write(self,kind,role,frame):
        if kind not in ['features','predictions'] or role not in ROLE_YEAR:raise ValueError('Unsupported artifact role/kind')
        validate_development(frame,role)
        required=BASE_COLUMNS+FEATURE_COLUMNS if kind=='features' else BASE_COLUMNS+['forecast','decision']
        if any(c not in frame for c in required):raise ValueError('Missing audit artifact columns')
        y=frame.ride_count.to_numpy(dtype=float)
        if not np.isfinite(y).all() or (y<0).any() or not np.equal(y,np.floor(y)).all():raise ValueError('Invalid audit target counts')
        if kind=='predictions' and self.model is None:raise ValueError('Register model hash before recording predictions')
        numeric=FEATURE_COLUMNS if kind=='features' else ['forecast']
        if not np.isfinite(frame[numeric].to_numpy(dtype=float)).all():raise ValueError('Nonfinite audit values')
        if kind=='predictions' and ((frame.forecast<0).any() or not frame.decision.isin([0,1,2]).all()):raise ValueError('Invalid recorded predictions')
        folder=self.root/role/kind;folder.mkdir(parents=True,exist_ok=True);key=(role,kind);part=self.parts.get(key,0);self.parts[key]=part+1
        canonical=pd.DataFrame({'station_id':frame.station_id.astype(str),'utc_ns':pd.DatetimeIndex(pd.to_datetime(frame.station_hour_ts,utc=True)).as_unit('ns').asi8,'actual':frame.ride_count.astype('float64')})
        self.keys.setdefault(key,hashlib.sha256()).update(pd.util.hash_pandas_object(canonical,index=False).to_numpy().tobytes())
        path=folder/f'part-{part:06d}.parquet';frame[required].to_parquet(path,index=False)
        self.files.append({'role':role,'kind':kind,'path':str(path.relative_to(self.root)),'rows':len(frame),'sha256':digest(path)});self.counts[key]=self.counts.get(key,0)+len(frame)
    def register_model(self,path):
        if self.model is not None:raise ValueError('Model already registered')
        self.model={'path':str(Path(path).resolve()),'sha256':digest(path)}
    def write_guards(self,rows,configuration):
        if not rows:raise ValueError('Guard diagnostics cannot be empty')
        path=self.root/'guard_diagnostics.json';path.write_text(json.dumps({'role':'validation','configuration':configuration,'outcomes':rows},indent=2));self.guards=digest(path)
    def finalize(self):
        if self.model:
            if digest(self.model['path'])!=self.model['sha256']:raise ValueError('Model changed during run')
            for role in ROLE_YEAR:
                if not self.counts.get((role,'features')) or self.counts.get((role,'features'))!=self.counts.get((role,'predictions')):raise ValueError('Require complete training/validation features and predictions')
                if self.keys[(role,'features')].hexdigest()!=self.keys[(role,'predictions')].hexdigest():raise ValueError('Feature/prediction row keys differ')
            if not self.guards:raise ValueError('Missing guard diagnostics')
            if digest(self.root/'guard_diagnostics.json')!=self.guards:raise ValueError('Guard diagnostics changed')
            required=['feature_source_sha256','station_map_sha256','fit_row_manifest_sha256','policy_sha256','guard_configuration_sha256']
            if not self.provenance.get('source_snapshot') or any(not isinstance(self.provenance.get(k),str) or len(self.provenance[k])!=64 or any(c not in '0123456789abcdef' for c in self.provenance[k]) for k in required):raise ValueError('Missing source, fit-row, policy or guard provenance hashes')
        for item in self.files:
            if digest(self.root/item['path'])!=item['sha256']:raise ValueError('Artifact changed')
        manifest={'status':'auditable_model_run' if self.model else 'preparation_only','provenance':self.provenance,'feature_order':FEATURE_COLUMNS,'model':self.model,'files':self.files,'guard_diagnostics_sha256':self.guards or None,'calendar_roles':ROLE_YEAR,'model_trained_by_sink':False}
        (self.root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');return manifest


def memory_plan(train_rows,val_rows,feature_count=14,frame_bytes_per_row=136):
    return {'train_rows':int(train_rows),'validation_rows':int(val_rows),'float32_train_matrix_bytes':int(train_rows)*feature_count*4,'float32_train_and_validation_features_labels_bytes':int(train_rows+val_rows)*(feature_count+1)*4,'estimated_one_train_dataframe_bytes':int(train_rows)*int(frame_bytes_per_row),'dataframe_bytes_per_row_assumption':int(frame_bytes_per_row),'not_included':'native bins, gradients, raw buffers, pandas copies, model, interpreter and OS cache; not a measured training peak'}

def write_development_memmap(paths,role,root,max_rows):
    """Disk-backed raw matrices, not a training algorithm or native memory guarantee."""
    root=Path(root);root.mkdir(parents=True,exist_ok=False);rows=0;parts=[]
    state=root/'state.json';state.write_text(json.dumps({'status':'building','role':role}))
    try:
        with (root/'X.f32').open('wb') as xfile,(root/'y.f32').open('wb') as yfile:
            for batch in parquet_feature_batches(paths,role):
                if rows+len(batch)>max_rows:raise ValueError('Matrix row budget exceeded; no silent truncation')
                batch[FEATURE_COLUMNS].to_numpy(dtype='float32').tofile(xfile);batch.ride_count.to_numpy(dtype='float32').tofile(yfile)
                key_path=root/f'keys-{len(parts):06d}.parquet';batch[BASE_COLUMNS].to_parquet(key_path,index=False)
                parts.append({'path':key_path.name,'rows':len(batch),'sha256':digest(key_path)});rows+=len(batch)
        if not rows:raise ValueError('No development rows')
        manifest={'status':'complete','role':role,'rows':rows,'columns':FEATURE_COLUMNS,'dtype':'float32','X_sha256':digest(root/'X.f32'),'y_sha256':digest(root/'y.f32'),'row_keys':parts,'training_performed':False}
        (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');state.write_text(json.dumps({'status':'complete'}));return manifest
    except BaseException as error:
        state.write_text(json.dumps({'status':'incomplete','error':repr(error)}));raise


def policy_support_diagnostics(policy):
    """Inspect existing TRAINING calibration bins; no forecasts/actions on new rows."""
    rows=[]
    for station,state in policy['states'].items():
        p=np.asarray(state['probabilities'],dtype=float)
        if p.ndim!=2 or p.shape[1]!=3 or not np.isfinite(p).all() or (p<0).any() or not np.allclose(p.sum(axis=1),1):raise ValueError('Invalid stored probabilities')
        choices=(p@COST_MATRIX).argmin(axis=1)
        rows.append({'station_id':station,'training_rows':state['rows'],'bins':len(p),'distinct_bin_actions':int(len(set(choices))),'constant_action':int(choices[0]) if len(set(choices))==1 else None,'training_class_prior':state['training_class_prior']})
    return rows
