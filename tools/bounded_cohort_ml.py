"""Bounded stratified-cohort Poisson training with immutable validation guard gates."""
import argparse,gc,hashlib,json,resource,threading,time
from pathlib import Path
from urllib.parse import urlparse,unquote
import numpy as np
import pandas as pd
from tools.bounded_ml_readiness import atomic,FEATURES,features,baseline_predictions,historical_fit
from jobs.cohort_selection import select_stations
from jobs.decision_policies import fit_empirical,actions,evaluate,station_metrics
from jobs.station_robustness import CONSTRAINTS,checks


class ResourceLimit(RuntimeError):pass


def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def run(output,root,desired):
    import joblib,pyarrow as pa,pyarrow.parquet as pq,pyarrow.compute as pc
    from pyiceberg.table import StaticTable
    from pyiceberg.expressions import In
    from lightgbm import LGBMRegressor
    start=time.monotonic();output.mkdir(parents=True,exist_ok=False);done=threading.Event();memory_stop=threading.Event()
    def log(event,**data):
        row={'timestamp':time.time(),'event':event,**data};print(json.dumps(row),flush=True)
        with (output/'progress.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
    def monitor():
        with (output/'memory.csv').open('w',buffering=1) as f:
            f.write('elapsed,current_bytes,peak_bytes\n')
            while not done.is_set():
                current=int(Path('/sys/fs/cgroup/memory.current').read_text());peak=int(Path('/sys/fs/cgroup/memory.peak').read_text());limit=int(Path('/sys/fs/cgroup/memory.max').read_text())
                f.write(f'{time.monotonic()-start},{current},{peak}\n')
                if current>.95*limit:memory_stop.set()
                done.wait(.5)
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    def guard():
        from tools.ingestion_resources import check
        check(root)
        if memory_stop.is_set():raise ResourceLimit('Charged memory exceeded 95% of the 2 GiB cap; stopped before next phase')
    frozen_path=Path('/opt/airflow/artifacts/station-robustness-01/policy_lock.json')
    frozen=json.loads(frozen_path.read_text());assert frozen['constraints']==CONSTRAINTS
    hashes={str(p):sha(p) for folder in ['ml-readiness-01','ml-decisions-01','station-robustness-01'] for p in (root.parent/folder).rglob('*') if p.is_file()};hashes.update({str(p):sha(p) for p in (root/'checkpoints').glob('????-??.json')});atomic(output/'input_hashes.json',hashes)
    def preservation():
        assert all(sha(Path(p))==h for p,h in hashes.items())
    def resource_summary():return {'elapsed_seconds':time.monotonic()-start,'peak_cgroup_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'cgroup_limit_bytes':int(Path('/sys/fs/cgroup/memory.max').read_text()),'process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cgroup_events':Path('/sys/fs/cgroup/memory.events').read_text()}
    result={'status':'started','deployment_performed':False,'test_previously_inspected':True,'test_used_for_selection':False}
    try:
        guard();old=json.loads(Path('/opt/airflow/artifacts/ml-readiness-01/config.json').read_text())
        table=StaticTable.from_metadata(old['metadata']);assert table.current_snapshot().snapshot_id==old['snapshot_id']
        pa.set_cpu_count(2);pa.set_io_thread_count(1)
        config={'maximum_stations':desired,'selection':'12 equally allocated training-demand x seasonal-CV strata; sparse extremes within strata; 2024/2025 eligibility only','parameters':old['parameters'],'policy':{'kind':'empirical_expected_cost','bins':8,'strength':20,'shrinkage':0},'constraints':CONSTRAINTS,'constraint_source_sha256':sha(frozen_path),'metadata':old['metadata'],'snapshot_id':old['snapshot_id'],'feature_columns':FEATURES,'train':2024,'validation':2025,'reused_diagnostics':'2026-01 through 2026-09','batch_rows':32768,'spark_jobs':0,'hyperparameter_trials':0,'count_model_selection':'single fixed Poisson model; do not refit validation','gate':'stop after validation if ANY annual/station-season frozen guard fails; no station removal or guard weakening','resource_limit':'2 GiB cgroup, 95% charged-memory safe-stop, 2 CPUs, zero extra swap','forecast_protocol':old['forecast_protocol']}
        atomic(output/'config.json',config);log('PLAN_FIXED')
        def batches(years,columns,stations=None):
            scanned=0
            for task in table.scan(row_filter=In('target_year',set(years))).plan_files():
                guard()
                if task.delete_files:raise ValueError('Direct bounded reader requires no Iceberg row-delete files')
                uri=str(task.file.file_path);path=Path(unquote(urlparse(uri).path)) if uri.startswith('file:') else Path(uri)
                for b in pq.ParquetFile(path).iter_batches(batch_size=32768,columns=columns,use_threads=False):
                    mask=pc.is_in(b.column(b.schema.get_field_index('target_year')),value_set=pa.array(years,type=pa.int32()))
                    if stations is not None:mask=pc.and_(mask,pc.is_in(b.column(b.schema.get_field_index('station_id')),value_set=pa.array(stations)))
                    filtered=b.filter(mask)
                    if filtered.num_rows:yield filtered
                    scanned+=b.num_rows
                log('SOURCE_FILE_READ',years=years,file=path.name,scanned_rows=scanned)
        aggregate={}
        for batch in batches([2024,2025],['station_id','target_year','target_month','ride_count']):
            b=batch.to_pandas();b['positive_hours']=b.ride_count.gt(0).astype('int32')
            groups=b.groupby(['station_id','target_year','target_month']).agg(hours=('ride_count','size'),rides=('ride_count','sum'),positive_hours=('positive_hours','sum'))
            for key,row in groups.iterrows():
                value=aggregate.setdefault(key,[0,0,0]);value[0]+=int(row.hours);value[1]+=int(row.rides);value[2]+=int(row.positive_hours)
        monthly=pd.DataFrame([{'station_id':str(s),'target_year':y,'target_month':m,'hours':v[0],'rides':v[1],'positive_hours':v[2]} for (s,y,m),v in aggregate.items()]);monthly.to_parquet(output/'development_population_profiles.parquet',index=False)
        cohort,coverage=select_stations(monthly,desired);atomic(output/'cohort.json',cohort);atomic(output/'coverage.json',coverage);codes={r['station_id']:i for i,r in enumerate(cohort)};atomic(output/'station_codes.json',codes);log('COHORT_LOCKED',**coverage)
        del aggregate,monthly;gc.collect()
        def extract(years):
            writers={};dest=output/'raw';dest.mkdir(exist_ok=True)
            try:
                for batch in batches(years,['station_id','station_hour_ts','ride_count','target_year','target_month'],list(codes)):
                    pdf=batch.to_pandas()
                    for (year,station),g in pdf.groupby(['target_year','station_id']):
                        key=(int(year),str(station));table_part=pa.Table.from_pandas(g,preserve_index=False)
                        if key not in writers:
                            folder=dest/str(year);folder.mkdir(exist_ok=True);writers[key]=pq.ParquetWriter(folder/f'{codes[str(station)]}.parquet',table_part.schema,compression='zstd')
                        writers[key].write_table(table_part)
            finally:
                for writer in writers.values():writer.close()
        extract([2024,2025]);guard()
        engineered=output/'features';engineered.mkdir();thresholds={};audit=[];historical={};tails=output/'tails';tails.mkdir()
        for station,code in codes.items():
            raw=pd.concat([pd.read_parquet(output/'raw'/str(year)/f'{code}.parquet') for year in [2024,2025]],ignore_index=True)
            f,a=features(raw,{station:code});audit.extend(a);train=f.loc[f.split=='train'].copy();val=f.loc[f.split=='validation'].copy()
            if train.empty or val.empty:raise ValueError('Selected station lacks usable training/validation')
            thresholds[station]=[float(train.ride_count.quantile(.25)),float(train.ride_count.quantile(.75))]
            historical[station]=historical_fit(train)
            for name,part in [('train',train),('validation',val)]:
                part[FEATURES]=part[FEATURES].astype('float32');folder=engineered/name;folder.mkdir(exist_ok=True);part[['station_id','station_hour_ts','ride_count']+FEATURES].to_parquet(folder/f'{code}.parquet',index=False)
            raw.sort_values('station_hour_ts').tail(168).to_parquet(tails/f'{code}.parquet',index=False)
            del raw,f,train,val;guard()
        atomic(output/'thresholds.json',thresholds);atomic(output/'historical_baselines.json',historical);atomic(output/'feature_audit.json',audit)
        def combined(split):return pd.concat([pd.read_parquet(engineered/split/f'{code}.parquet') for code in codes.values()],ignore_index=True)
        train=combined('train');guard();fit_start=time.monotonic();model=LGBMRegressor(**config['parameters']);model.fit(train[FEATURES],train.ride_count);fit_seconds=time.monotonic()-fit_start
        joblib.dump(model,output/'count_model.joblib');model.booster_.save_model(str(output/'count_model.txt'));log('MODEL_FITTED',rows=len(train),fit_seconds=fit_seconds)
        fit_start=time.monotonic();train_mu=np.maximum(model.predict(train[FEATURES]),0);policy=fit_empirical(train,train_mu,thresholds,bins=8,strength=20);calibration_seconds=time.monotonic()-fit_start
        atomic(output/'empirical_policy.json',policy)
        # Retain the already computed training forecasts; never invoke another predict.
        recorded=train[['station_id','station_hour_ts','ride_count']].copy();recorded['forecast']=train_mu
        recorded.to_parquet(output/'training_predictions.parquet',index=False)
        feature_files=sorted((output/'features').rglob('*.parquet'))
        atomic(output/'retention_manifest.json',{'model_sha256':sha(output/'count_model.joblib'),'policy_sha256':sha(output/'empirical_policy.json'),'training_predictions_sha256':sha(output/'training_predictions.parquet'),'training_rows':len(recorded),'feature_order':FEATURES,'full_feature_matrices':{str(p.relative_to(output)):sha(p) for p in feature_files},'source_config_sha256':sha(output/'config.json'),'training_predictions_are_in_sample':True})
        training_rows=len(train);del recorded,train,train_mu;gc.collect();guard()
        def evaluation(frame):
            predictions={'lightgbm_poisson':np.maximum(model.predict(frame[FEATURES]),0),'last_hour':frame.lag_1h.to_numpy(),'previous_day':frame.lag_24h.to_numpy(),'previous_week':frame.lag_168h.to_numpy()}
            decisions={name:actions(policy if name=='lightgbm_poisson' else {'kind':'original'},frame,mu,thresholds) for name,mu in predictions.items()}
            metrics={name:evaluate(frame,mu,decisions[name],thresholds) for name,mu in predictions.items()}
            station={name:station_metrics(frame,mu,decisions[name],thresholds) for name,mu in predictions.items()}
            gates=checks(frame,decisions['lightgbm_poisson'],decisions['last_hour'],thresholds)
            return predictions,decisions,metrics,station,gates
        val=combined('validation');pred,dec,validation,station,gate=evaluation(val)
        atomic(output/'validation_metrics.json',validation);atomic(output/'validation_station_metrics.json',station);atomic(output/'validation_guards.json',gate)
        val_export=val[['station_id','station_hour_ts','ride_count']].copy()
        for name,mu in pred.items():val_export[name]=mu;val_export[name+'_action']=dec[name]
        val_export.to_parquet(output/'validation_predictions.parquet',index=False)
        failures=[g for g in gate if not g['passes']];atomic(output/'guard_failures.json',failures)
        result.update(coverage=coverage,rows={'train':training_rows,'validation':len(val)},validation=validation,guard_count=len(gate),validation_guard_failures=len(failures),fit_seconds=fit_seconds,calibration_seconds=calibration_seconds)
        del val,val_export,pred,dec;gc.collect();guard()
        if failures:
            preservation();result.update(status='stopped_validation_guards',reused_holdout_evaluated=False,input_hashes_unchanged=True,**resource_summary());atomic(output/'metrics.json',result);log('STOPPED_GUARDS',failures=len(failures),stations=len({g['station_id'] for g in failures}));return
        lock={'locked_at':time.time(),'policy_sha256':sha(output/'empirical_policy.json'),'model_sha256':sha(output/'count_model.joblib'),'constraints':CONSTRAINTS,'all_validation_guards_passed':True,'test_previously_inspected':True,'test_opened':False};atomic(output/'selection_lock.json',lock);log('ALL_VALIDATION_GUARDS_PASSED',checks=len(gate))
        extract([2026]);test_parts=[]
        for station,code in codes.items():
            source=output/'raw/2026'/f'{code}.parquet'
            if not source.exists():continue
            raw=pd.concat([pd.read_parquet(tails/f'{code}.parquet'),pd.read_parquet(source)],ignore_index=True);f,_=features(raw,{station:code});test=f.loc[f.split=='test'].copy();test[FEATURES]=test[FEATURES].astype('float32');test_parts.append(test[['station_id','station_hour_ts','ride_count']+FEATURES]);del raw,f
        test=pd.concat(test_parts,ignore_index=True);del test_parts;guard();local=pd.to_datetime(test.station_hour_ts,utc=True).dt.tz_convert('America/New_York');assert local.dt.year.eq(2026).all() and local.dt.month.le(9).all()
        pred,dec,diagnostics,station,gate=evaluation(test);atomic(output/'reused_holdout_metrics.json',diagnostics);atomic(output/'reused_holdout_station_metrics.json',station);atomic(output/'reused_holdout_guards.json',gate)
        export=test[['station_id','station_hour_ts','ride_count']].copy()
        for name,mu in pred.items():export[name]=mu;export[name+'_action']=dec[name]
        export.to_parquet(output/'reused_holdout_predictions.parquet',index=False)
        assert lock['model_sha256']==sha(output/'count_model.joblib') and lock['policy_sha256']==sha(output/'empirical_policy.json');preservation()
        result['rows']['reused_holdout']=len(test);result.update(status='complete',reused_holdout_evaluated=True,reused_holdout=diagnostics,reused_guard_failures=sum(not g['passes'] for g in gate),input_hashes_unchanged=True,**resource_summary());atomic(output/'metrics.json',result);log('COMPLETE',**resource_summary())
    except BaseException as error:
        result.update(status='stopped_error',error=repr(error),**resource_summary());atomic(output/'metrics.json',result);raise
    finally:done.set();thread.join(2)


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--root',type=Path,default=Path('/opt/airflow/artifacts/monthly-ingestion-01'));p.add_argument('--stations',type=int,default=48);a=p.parse_args()
    if not a.output.resolve().is_relative_to(Path('/opt/airflow/artifacts')) or a.output.resolve().is_relative_to(a.root.resolve()):raise ValueError('Choose a new experiment output in artifacts')
    run(a.output,a.root,a.stations)

if __name__=='__main__':main()
