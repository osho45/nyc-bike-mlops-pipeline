"""Bounded count-demand experiment on immutable Gold observations; no HPO or serving writes."""
import argparse, hashlib, json, os, threading, time
from pathlib import Path
import numpy as np
import pandas as pd
from jobs.ml_features import REQUIRED_FEATURES, COST_MATRIX, build_training_features

FEATURES = REQUIRED_FEATURES + ['station_code']


def atomic(path, value):
    temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
    temporary.replace(path)


def split_name(times):
    local=pd.to_datetime(times,utc=True).dt.tz_convert('America/New_York')
    return np.select([local.dt.year==2024,local.dt.year==2025,(local.dt.year==2026)&(local.dt.month<=9)],['train','validation','test'],default='outside')


def select_cohort(totals, n=12):
    eligible=totals.loc[(totals.hours>=8000)&(totals.rides>0)].sort_values(['rides','station_id']).reset_index(drop=True)
    if len(eligible)<n:raise ValueError('Insufficient 2024 station coverage for cohort')
    chosen=eligible.iloc[np.linspace(0,len(eligible)-1,n).round().astype(int)].copy()
    chosen['training_volume_band']=np.repeat(['low','medium_low','medium_high','high'],n//4)
    return chosen.to_dict('records')


def count_classes(values,q25,q75):
    return np.where(np.asarray(values)<q25,0,np.where(np.asarray(values)<=q75,1,2))


def metrics(frame,predictions,thresholds):
    y=frame.ride_count.to_numpy(dtype=float);p=np.asarray(predictions,dtype=float)
    if not len(y) or p.shape!=y.shape or not np.isfinite(p).all() or (p<0).any():raise ValueError('Invalid prediction vector')
    q25=frame.station_id.map(lambda s:thresholds[str(s)][0]).to_numpy()
    q75=frame.station_id.map(lambda s:thresholds[str(s)][1]).to_numpy()
    err=p-y;cost=COST_MATRIX[count_classes(y,q25,q75),count_classes(p,q25,q75)]
    return {'rows':len(y),'actual_rides':float(y.sum()),'mae':float(np.abs(err).mean()),'rmse':float(np.sqrt(np.square(err).mean())),
            'wape':float(np.abs(err).sum()/y.sum()) if y.sum() else None,'mean_operational_penalty':float(cost.mean()),'total_operational_penalty':float(cost.sum())}


def sliced_metrics(frame,predictions,thresholds):
    frame=frame.reset_index(drop=True).copy();local=pd.to_datetime(frame.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
    frame['season']=local.dt.month.map({12:'winter',1:'winter',2:'winter',3:'spring',4:'spring',5:'spring',6:'summer',7:'summer',8:'summer',9:'fall',10:'fall',11:'fall'})
    frame['demand_level']=count_classes(frame.ride_count,frame.station_id.map(lambda s:thresholds[str(s)][0]),frame.station_id.map(lambda s:thresholds[str(s)][1]))
    result={}
    for column in ['station_id','season','demand_level']:
        result[column]={str(k):metrics(frame.loc[idx],np.asarray(predictions)[idx],thresholds) for k,idx in frame.groupby(column).groups.items()}
    return result


def features(history,codes):
    parts=[];audit=[]
    for station,raw in history.groupby('station_id',sort=True):
        raw=raw.sort_values('station_hour_ts').reset_index(drop=True)
        if raw.station_hour_ts.duplicated().any() or not np.isfinite(raw.ride_count).all() or (raw.ride_count<0).any():raise ValueError('Invalid Gold station observations')
        f=build_training_features(raw)
        if len(f):
            # Independent timestamp-based oracle, not shift-position parity.
            indexed=raw.set_index('station_hour_ts').ride_count
            sample=f.iloc[len(f)//2];ts=sample.station_hour_ts
            for lag in [1,2,24,168]:assert sample[f'lag_{lag}h']==indexed.loc[ts-pd.Timedelta(hours=lag)]
            for width in [6,24]:
                previous=np.array([indexed.loc[ts-pd.Timedelta(hours=h)] for h in range(1,width+1)],dtype=float)
                np.testing.assert_allclose(sample[f'rolling_{width}h_mean'],previous.mean(),atol=1e-8)
                np.testing.assert_allclose(sample[f'rolling_{width}h_std'],previous.std(ddof=1),atol=1e-8)
            changed=raw.copy();changed.loc[changed.station_hour_ts>=ts,'ride_count']=999999
            mutated=build_training_features(changed).set_index('station_hour_ts').loc[ts,REQUIRED_FEATURES].to_numpy(dtype=float)
            np.testing.assert_allclose(sample[REQUIRED_FEATURES].to_numpy(dtype=float),mutated,atol=1e-8)
        f['station_code']=codes[str(station)];f['split']=split_name(f.station_hour_ts)
        assert np.isfinite(f[FEATURES].to_numpy(dtype=float)).all()
        audit.append({'station_id':str(station),'raw_rows':len(raw),'feature_rows':len(f),'warmup_or_gap_rows':len(raw)-len(f),'target_mutation_and_lag_oracle_passed':True})
        parts.append(f)
    return pd.concat(parts,ignore_index=True),audit


def historical_fit(train):
    local=pd.to_datetime(train.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
    t=train.assign(dow=local.dt.dayofweek,hour=local.dt.hour)
    return {'station_mean':t.groupby('station_id').ride_count.mean().to_dict(),
            'weekly_mean':{'|'.join(map(str,k)):float(v) for k,v in t.groupby(['station_id','dow','hour']).ride_count.mean().items()}}


def baseline_predictions(frame,historical):
    local=pd.to_datetime(frame.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
    means=[historical['weekly_mean'].get(f'{s}|{d}|{h}',historical['station_mean'][str(s)]) for s,d,h in zip(frame.station_id,local.dt.dayofweek,local.dt.hour)]
    return {'last_hour':frame.lag_1h.to_numpy(),'previous_day':frame.lag_24h.to_numpy(),'previous_week':frame.lag_168h.to_numpy(),'training_weekly_mean':np.array(means)}


def run(root,output,n=12):
    import joblib,resource,pyarrow as pa
    from lightgbm import LGBMRegressor
    from pyiceberg.table import StaticTable
    from pyiceberg.expressions import EqualTo,In,And
    import lightgbm,sklearn
    started=time.monotonic();output.mkdir(parents=True,exist_ok=False)
    def log(event,**data):
        record={'timestamp':time.time(),'event':event,**data};print(json.dumps(record),flush=True)
        with (output/'progress.jsonl').open('a') as f:f.write(json.dumps(record)+'\n')
    memory=[];done=threading.Event()
    def monitor():
        while not done.is_set():
            row={'elapsed':time.monotonic()-started}
            for name in ['memory.current','memory.peak','memory.max']:
                row[name]=int(Path('/sys/fs/cgroup',name).read_text())
            memory.append(row);done.wait(.5)
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    checkpoints={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (root/'checkpoints').glob('????-??.json')}
    try:
        controlled=json.loads((root/'controlled-run.json').read_text());assert controlled['status']=='complete' and controlled['verified_years']==[2024,2025,2026]
        expected={2024:list(range(1,13)),2025:list(range(1,13)),2026:list(range(1,10))}
        for year,months in expected.items():
            gate=json.loads((root/'years'/f'{year}.json').read_text());assert gate['verified'] and gate['months']==months
            for m in months:
                cp=json.loads((root/'checkpoints'/f'{year}-{m:02d}.json').read_text());assert cp['verified'] and gate['source_sha256'][str(m)]==cp['source']['sha256']
        directory=root/'warehouse/gold/station_hourly_observations/metadata';version=(directory/'version-hint.text').read_text().strip()
        metadata=directory/f'v{version}.metadata.json';table=StaticTable.from_metadata(str(metadata));snapshot=table.current_snapshot()
        config={'seed':42,'station_count':n,'station_selection':'2024-only evenly spaced training-volume ranks; >=8000 training hours; four volume bands','train_year':2024,'validation_year':2025,'final_test':'2026-01 through 2026-09','selection_metric':'validation WAPE, then operational penalty','target':'hourly departures ride_count at station/hour t','forecast_protocol':'rolling one-hour-ahead; realized observations strictly before t; zero observation latency assumed','features':FEATURES,'parameters':{'objective':'poisson','n_estimators':100,'num_leaves':15,'max_bin':63,'learning_rate':.05,'min_child_samples':40,'n_jobs':2,'random_state':42,'deterministic':True,'force_col_wise':True,'verbosity':-1},'hpo_trials':0,'table_uuid':str(table.metadata.table_uuid),'snapshot_id':snapshot.snapshot_id,'metadata':str(metadata),'versions':{'lightgbm':lightgbm.__version__,'sklearn':sklearn.__version__,'pandas':pd.__version__,'numpy':np.__version__}}
        atomic(output/'config.json',config);pa.set_cpu_count(2);pa.set_io_thread_count(2)
        columns=('station_id','station_hour_ts','ride_count','target_year','target_month')
        totals={};scanned=0
        for batch in table.scan(row_filter=EqualTo('target_year',2024),selected_fields=('station_id','ride_count')).to_arrow_batch_reader():
            b=batch.to_pandas();g=b.groupby('station_id').ride_count.agg(['sum','count']);scanned+=len(b)
            for station,row in g.iterrows():
                a=totals.setdefault(str(station),[0,0]);a[0]+=int(row['sum']);a[1]+=int(row['count'])
        cohort=select_cohort(pd.DataFrame([{'station_id':s,'rides':v[0],'hours':v[1]} for s,v in totals.items()]),n)
        atomic(output/'cohort.json',cohort);stations=[c['station_id'] for c in cohort];codes={s:i for i,s in enumerate(stations)}
        atomic(output/'station_codes.json',codes)
        log('COHORT_LOCKED',training_rows_scanned=scanned,stations=stations)
        def extract(years):
            parts=[]
            predicate=And(In('target_year',set(years)),In('station_id',set(stations)))
            for batch in table.scan(row_filter=predicate,selected_fields=columns).to_arrow_batch_reader():parts.append(batch.to_pandas())
            result=pd.concat(parts,ignore_index=True);result['station_id']=result.station_id.astype(str)
            expected_split=split_name(result.station_hour_ts)
            if (expected_split=='outside').any():raise ValueError('Out-of-policy Gold timestamps')
            local=pd.to_datetime(result.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
            assert np.array_equal(local.dt.year,result.target_year) and np.array_equal(local.dt.month,result.target_month)
            log('EXTRACTED',years=years,rows=len(result));return result
        raw=extract([2024,2025]);prepared,audit=features(raw,codes)
        train=prepared.loc[prepared.split=='train'].copy();val=prepared.loc[prepared.split=='validation'].copy()
        assert len(train) and len(val) and train.station_hour_ts.max()<val.station_hour_ts.min()
        thresholds={s:[float(g.ride_count.quantile(.25)),float(g.ride_count.quantile(.75))] for s,g in train.groupby('station_id')}
        assert set(thresholds)==set(stations)
        historical=historical_fit(train);atomic(output/'baselines.json',historical);atomic(output/'thresholds.json',thresholds)
        log('FIT_STARTED',training_rows=len(train),validation_rows=len(val));fit_started=time.monotonic()
        model=LGBMRegressor(**config['parameters']);model.fit(train[FEATURES].astype('float32'),train.ride_count)
        fit_seconds=time.monotonic()-fit_started;joblib.dump(model,output/'count_model.joblib');model.booster_.save_model(str(output/'count_model.txt'))
        def evaluate(frame):
            predictions=baseline_predictions(frame,historical);predictions['lightgbm_poisson']=np.maximum(model.predict(frame[FEATURES].astype('float32')),0)
            return {name:metrics(frame,pred,thresholds) for name,pred in predictions.items()}, {name:sliced_metrics(frame,pred,thresholds) for name,pred in predictions.items()},predictions
        validation,validation_slices,_=evaluate(val)
        ordered=sorted(validation,key=lambda k:(validation[k]['wape'],validation[k]['mean_operational_penalty'],k));winner=ordered[0]
        lock={'locked_at':time.time(),'selected_model':winner,'validation_metrics':validation,'model_sha256':hashlib.sha256((output/'count_model.joblib').read_bytes()).hexdigest(),'parameters':config['parameters'],'test_outcomes_read':False,'refit_on_validation':False}
        atomic(output/'selection_lock.json',lock);atomic(output/'validation_slices.json',validation_slices)
        log('SELECTION_LOCKED',winner=winner,fit_seconds=fit_seconds,validation_metrics=validation)
        # First read of any 2026 outcomes occurs only AFTER persisted model/selection lock.
        test_raw=extract([2026]);test_prepared,test_audit=features(pd.concat([raw,test_raw],ignore_index=True),codes)
        test=test_prepared.loc[test_prepared.split=='test'].copy();assert len(test) and val.station_hour_ts.max()<test.station_hour_ts.min()
        final,final_slices,predictions=evaluate(test)
        atomic(output/'test_slices.json',final_slices)
        export=test[['station_id','station_hour_ts','ride_count']].copy()
        for name,pred in predictions.items():export[name]=pred
        export.to_parquet(output/'test_predictions.parquet',index=False)
        assert checkpoints=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (root/'checkpoints').glob('????-??.json')}
        assert (directory/'version-hint.text').read_text().strip()==version
        assert hashlib.sha256((output/'count_model.joblib').read_bytes()).hexdigest()==lock['model_sha256']
        result={'status':'complete','validation':validation,'test':final,'validation_selected_model':winner,'fit_seconds':fit_seconds,'elapsed_seconds':time.monotonic()-started,'rows':{'train':len(train),'validation':len(val),'test':len(test)},'feature_audit':audit,'final_feature_audit':test_audit,'source_checkpoints_unchanged':True,'source_snapshot_unchanged':True,'test_used_for_selection':False,'test_protocol':'rolling-origin one-hour-ahead with observed past test demand; no refitting','process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cgroup_peak_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'cgroup_limit_bytes':int(Path('/sys/fs/cgroup/memory.max').read_text()),'cgroup_memory_events':Path('/sys/fs/cgroup/memory.events').read_text()}
        atomic(output/'metrics.json',result);log('COMPLETED',**{k:result[k] for k in ['rows','fit_seconds','elapsed_seconds','cgroup_peak_bytes']})
    except BaseException as error:
        atomic(output/'failure.json',{'error':repr(error),'elapsed_seconds':time.monotonic()-started});raise
    finally:
        done.set();thread.join(2);atomic(output/'memory.json',memory)


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path('/opt/airflow/artifacts/monthly-ingestion-01'));p.add_argument('--output',type=Path,required=True);p.add_argument('--stations',type=int,default=12);a=p.parse_args()
    if a.stations<4 or a.stations>24 or a.stations%4:raise ValueError('Bounded cohort must be 4, 8, 12, 16, 20 or 24 stations')
    if not a.output.resolve().is_relative_to(Path('/opt/airflow/artifacts')):raise ValueError('Write only to a new artifact directory')
    run(a.root,a.output,a.stations)

if __name__=='__main__':main()
