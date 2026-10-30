"""Single-pass, frozen 48-station 2026 reused-holdout evaluation. No fitting/selection."""
import argparse,gc,hashlib,json,shutil,threading,time
from pathlib import Path
from urllib.parse import urlparse,unquote
import numpy as np
import pandas as pd
from tools.bounded_ml_readiness import atomic,FEATURES,features
from jobs.ml_features import COST_MATRIX
from jobs.decision_policies import actions,evaluate,station_metrics
from jobs.station_robustness import CONSTRAINTS,apply_policy,empirical_probabilities,checks,calendar


def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def classify_guard(row):
    row=dict(row);mean=row.get('mean_delta')
    row['observed_mean_regression']=None if mean is None else mean>0
    if row['passes']:
        reason='baseline_identical' if row['identical_to_baseline'] else 'passes_bound'
    elif not row['support_sufficient']:reason='insufficient_support'
    elif mean is not None and mean>0:reason='observed_regression_and_bound_failure'
    else:reason='uncertainty_bound_failure'
    row['diagnostic_classification']=reason
    return row


def complete_guards(rows,stations):
    result={(x['station_id'],x['dimension'],x['season']):classify_guard(x) for x in rows}
    for station in stations:
        for season in ['all','winter','spring','summer','fall']:
            dim='annual' if season=='all' else 'season';key=(station,dim,season)
            if key not in result:
                result[key]=classify_guard({'station_id':station,'dimension':dim,'season':season,'hours':0,'positive_hours':0,'week_blocks':0,'policy_penalty':None,'baseline_penalty':None,'mean_delta':None,'upper_one_sided_bound':None,'cluster_standard_error':None,'tolerance':None,'support_sufficient':False,'identical_to_baseline':False,'passes':False})
    if len(result)!=len(stations)*5:raise ValueError('Unexpected station/season guard coverage')
    return list(result.values())


def run(base,overlay,root,output):
    import joblib,resource,pyarrow as pa,pyarrow.compute as pc,pyarrow.parquet as pq
    import lightgbm,sklearn
    from pyiceberg.table import StaticTable
    from pyiceberg.expressions import EqualTo
    start=time.monotonic();output.mkdir(parents=True,exist_ok=False);done=threading.Event();memory_stop=threading.Event()
    def log(event,**data):
        row={'timestamp':time.time(),'event':event,**data};print(json.dumps(row),flush=True)
        with (output/'progress.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
    def monitor():
        with (output/'memory.csv').open('w',buffering=1) as f:
            f.write('elapsed,current_bytes,peak_bytes\n')
            while not done.is_set():
                current=int(Path('/sys/fs/cgroup/memory.current').read_text());peak=int(Path('/sys/fs/cgroup/memory.peak').read_text());limit=int(Path('/sys/fs/cgroup/memory.max').read_text());f.write(f'{time.monotonic()-start},{current},{peak}\n')
                if current>.95*limit:memory_stop.set()
                done.wait(.5)
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    def guard():
        from tools.ingestion_resources import check
        check(root)
        if memory_stop.is_set():raise RuntimeError('95% memory safety headroom exceeded')
    hashes={str(p):sha(p) for folder in [base,overlay] for p in folder.rglob('*') if p.is_file()};hashes.update({str(p):sha(p) for p in (root/'checkpoints').glob('????-??.json')});atomic(output/'preserved_input_hashes.json',hashes)
    try:
        guard();config=json.loads((base/'config.json').read_text());old_lock=json.loads((overlay/'validation_overlay_lock.json').read_text());validation=json.loads((overlay/'metrics.json').read_text())
        assert validation['accepted_for_validation'] and validation['after_guard_passes']==240
        assert config['constraints']==old_lock['constraints']==CONSTRAINTS
        assert config['feature_columns']==FEATURES
        assert sha(base/'count_model.joblib')==old_lock['model_sha256'] and sha(base/'empirical_policy.json')==old_lock['empirical_policy_sha256']
        frozen=output/'frozen';frozen.mkdir();manifest={}
        for name in ['cohort.json','station_codes.json','count_model.joblib','count_model.txt','empirical_policy.json','thresholds.json','config.json']:
            p=base/name;target=frozen/name;shutil.copyfile(p,target);assert sha(target)==sha(p);manifest[name]=sha(target)
        for name in ['validation_overlay_lock.json','fallback_spec.json']:
            target=frozen/name;shutil.copyfile(overlay/name,target);manifest[name]=sha(target)
        (frozen/'tails').mkdir()
        for p in (base/'tails').glob('*.parquet'):
            target=frozen/'tails'/p.name;shutil.copyfile(p,target);manifest['tails/'+p.name]=sha(target)
        code=frozen/'source';code.mkdir()
        for name in ['jobs/ml_features.py','jobs/decision_policies.py','jobs/station_robustness.py','tools/bounded_ml_readiness.py','tools/evaluate_cohort_2026.py']:
            p=Path(name);target=code/p.name;shutil.copyfile(p,target);manifest['source/'+p.name]=sha(target)
        metadata=Path(config['metadata']);manifest['source_iceberg_metadata']=sha(metadata)
        table=StaticTable.from_metadata(str(metadata));assert table.current_snapshot().snapshot_id==config['snapshot_id']
        codes=json.loads((frozen/'station_codes.json').read_text());cohort=json.loads((frozen/'cohort.json').read_text());assert len(codes)==len(cohort)==48 and set(codes)=={x['station_id'] for x in cohort}
        thresholds=json.loads((frozen/'thresholds.json').read_text());policy=json.loads((frozen/'empirical_policy.json').read_text());spec=json.loads((frozen/'fallback_spec.json').read_text());assert spec==old_lock['fallback_spec']
        assert set(thresholds)==set(policy['states'])==set(codes)
        lock={'locked_at':time.time(),'hash_algorithm':'sha256','files':manifest,'cohort_stations':sorted(codes),'cohort_size':48,'feature_order':FEATURES,'cost_matrix':COST_MATRIX.tolist(),'guard_configuration':CONSTRAINTS,'fallback_spec':spec,'snapshot_id':config['snapshot_id'],'table_uuid':str(table.metadata.table_uuid),'evaluation_period_local':'2026-01-01 through 2026-09-30 America/New_York','protocol':config['forecast_protocol'],'reused_holdout':True,'model_fit_allowed':False,'recalibration_allowed':False,'policy_adjustment_allowed':False,'deployment_allowed':False,'model_predict_calls_planned':1,'versions':{'lightgbm':lightgbm.__version__,'sklearn':sklearn.__version__,'numpy':np.__version__,'pandas':pd.__version__,'pyarrow':pa.__version__}}
        atomic(output/'evaluation_lock.json',lock);lock_hash=sha(output/'evaluation_lock.json');atomic(output/'evaluation_lock_sha256.json',{'sha256':lock_hash});log('CONFIGURATION_FROZEN',evaluation_lock_sha256=lock_hash,model_sha256=manifest['count_model.joblib'],policy_sha256=manifest['empirical_policy.json'],cohort_sha256=manifest['cohort.json'])
        pa.set_cpu_count(2);pa.set_io_thread_count(1);writers={};rawdir=output/'raw_2026';rawdir.mkdir();scanned=selected=0;read_started=time.monotonic()
        try:
            for task in table.scan(row_filter=EqualTo('target_year',2026)).plan_files():
                guard()
                if task.delete_files:raise RuntimeError('Row deletes unsupported by bounded Parquet reader')
                uri=str(task.file.file_path);path=Path(unquote(urlparse(uri).path)) if uri.startswith('file:') else Path(uri)
                for batch in pq.ParquetFile(path).iter_batches(batch_size=32768,columns=['station_id','station_hour_ts','ride_count','target_year','target_month'],use_threads=False):
                    mask=pc.and_(pc.equal(batch.column(3),2026),pc.and_(pc.is_in(batch.column(4),value_set=pa.array(list(range(1,10)),type=pa.int32())),pc.is_in(batch.column(0),value_set=pa.array(list(codes)))))
                    part=batch.filter(mask);scanned+=batch.num_rows;selected+=part.num_rows
                    if not part.num_rows:continue
                    for station,g in part.to_pandas().groupby('station_id'):
                        station=str(station);p=pa.Table.from_pandas(g,preserve_index=False)
                        if station not in writers:writers[station]=pq.ParquetWriter(rawdir/f'{codes[station]}.parquet',p.schema,compression='zstd')
                        writers[station].write_table(p)
                log('FROZEN_SOURCE_READ',file=path.name,source_rows_scanned=scanned,cohort_rows_selected=selected)
        finally:
            for writer in writers.values():writer.close()
        extraction_seconds=time.monotonic()-read_started;frame_parts=[];coverage=[];audit=[]
        for station,code_value in codes.items():
            p=rawdir/f'{code_value}.parquet'
            if not p.exists():coverage.append({'station_id':station,'raw_2026_rows':0,'scored_rows':0,'reason':'no 2026 observations'});continue
            raw=pd.read_parquet(p);timestamps=pd.to_datetime(raw.station_hour_ts,utc=True).dt.tz_convert('America/New_York');assert timestamps.dt.year.eq(2026).all() and timestamps.dt.month.between(1,9).all()
            context=pd.read_parquet(frozen/'tails'/f'{code_value}.parquet');assert pd.to_datetime(context.station_hour_ts,utc=True).dt.tz_convert('America/New_York').dt.year.eq(2025).all()
            prepared,diagnostics=features(pd.concat([context,raw],ignore_index=True),{station:code_value});audit.extend(diagnostics);test=prepared.loc[prepared.split=='test'].copy();test[FEATURES]=test[FEATURES].astype('float32')
            coverage.append({'station_id':station,'raw_2026_rows':len(raw),'scored_rows':len(test),'warmup_or_gap_rows':len(raw)-len(test),'first_scored':str(test.station_hour_ts.min()) if len(test) else None,'last_scored':str(test.station_hour_ts.max()) if len(test) else None})
            frame_parts.append(test[['station_id','station_hour_ts','ride_count']+FEATURES]);del raw,context,prepared,test;guard()
        frame=pd.concat(frame_parts,ignore_index=True);del frame_parts;gc.collect();assert len(frame)>0 and not frame.duplicated(['station_id','station_hour_ts']).any();assert np.isfinite(frame[FEATURES].to_numpy()).all()
        atomic(output/'coverage.json',coverage);atomic(output/'feature_audit.json',audit);guard()
        model=joblib.load(frozen/'count_model.joblib');predict_start=time.monotonic();mu=np.maximum(model.predict(frame[FEATURES]),0);predict_seconds=time.monotonic()-predict_start
        log('SINGLE_MODEL_PREDICTION_COMPLETE',rows=len(frame),predict_calls=1,predict_seconds=predict_seconds)
        action_start=time.monotonic();probability=empirical_probabilities(policy,frame,mu);d=apply_policy(spec,frame,probability,actions({'kind':'original'},frame,frame.lag_1h.to_numpy(),thresholds));action_seconds=time.monotonic()-action_start
        prediction={'frozen_poisson_fallback':mu,'last_hour':frame.lag_1h.to_numpy(),'previous_day':frame.lag_24h.to_numpy(),'previous_week':frame.lag_168h.to_numpy()};decisions={'frozen_poisson_fallback':d}
        for name in ['last_hour','previous_day','previous_week']:decisions[name]=actions({'kind':'original'},frame,prediction[name],thresholds)
        aggregate={name:evaluate(frame,values,decisions[name],thresholds) for name,values in prediction.items()}
        by_station={name:station_metrics(frame,values,decisions[name],thresholds) for name,values in prediction.items()};seasons,_=calendar(frame);by_season={};by_cell={}
        for name,values in prediction.items():
            by_season[name]={};by_cell[name]=[]
            for season in ['winter','spring','summer','fall']:
                idx=np.flatnonzero(seasons==season)
                if len(idx):by_season[name][season]=evaluate(frame.iloc[idx],values[idx],decisions[name][idx],thresholds)
            for station in codes:
                for season in ['winter','spring','summer','fall']:
                    idx=np.flatnonzero((frame.station_id.to_numpy()==station)&(seasons==season))
                    if len(idx):by_cell[name].append({'station_id':station,'season':season,**evaluate(frame.iloc[idx],values[idx],decisions[name][idx],thresholds)})
        outcomes=complete_guards(checks(frame,d,decisions['last_hour'],thresholds),list(codes));counts={name:sum(x['diagnostic_classification']==name for x in outcomes) for name in ['passes_bound','baseline_identical','insufficient_support','observed_regression_and_bound_failure','uncertainty_bound_failure']}
        atomic(output/'aggregate_metrics.json',aggregate);atomic(output/'station_metrics.json',by_station);atomic(output/'season_metrics.json',by_season);atomic(output/'station_season_metrics.json',by_cell);atomic(output/'guard_outcomes.json',outcomes);atomic(output/'failed_guards.json',[x for x in outcomes if not x['passes']]);pd.DataFrame(outcomes).to_csv(output/'all_240_guard_outcomes.csv',index=False)
        export=frame[['station_id','station_hour_ts','ride_count']].copy()
        for name,values in prediction.items():export[name]=values;export[name+'_action']=decisions[name]
        export.to_parquet(output/'predictions_and_actions.parquet',index=False)
        assert sha(output/'evaluation_lock.json')==lock_hash
        for name,h in manifest.items():assert sha(metadata if name=='source_iceberg_metadata' else frozen/name)==h
        assert all(sha(Path(p))==h for p,h in hashes.items())
        result={'status':'complete_with_guard_failures' if any(not x['passes'] for x in outcomes) else 'complete_guards_passed','reused_holdout':True,'evaluation_period':'Jan-Sep 2026; annual guards applied to available year-to-date observations','cohort_size':48,'stations_scored':frame.station_id.nunique(),'rows':len(frame),'source_rows_scanned':scanned,'cohort_observations_selected':selected,'model_predict_calls':1,'aggregate':aggregate,'guard_classifications':counts,'guard_passes':sum(x['passes'] for x in outcomes),'guard_failures':sum(not x['passes'] for x in outcomes),'guard_count':len(outcomes),'annual_passes':sum(x['passes'] for x in outcomes if x['dimension']=='annual'),'annual_count':48,'season_passes':sum(x['passes'] for x in outcomes if x['dimension']=='season'),'season_count':192,'extraction_seconds':extraction_seconds,'predict_seconds':predict_seconds,'action_seconds':action_seconds,'elapsed_seconds':time.monotonic()-start,'peak_cgroup_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'cgroup_limit_bytes':int(Path('/sys/fs/cgroup/memory.max').read_text()),'process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cgroup_events':Path('/sys/fs/cgroup/memory.events').read_text(),'frozen_configuration_unchanged':True,'source_artifacts_and_checkpoints_unchanged':True,'model_refitted':False,'policy_adjusted':False,'deployment_performed':False,'evaluation_lock_sha256':lock_hash}
        atomic(output/'metrics.json',result);log('EVALUATION_COMPLETE',status=result['status'],guard_classifications=counts,rows=len(frame))
    except BaseException as error:atomic(output/'failure.json',{'error':repr(error),'elapsed_seconds':time.monotonic()-start});raise
    finally:done.set();thread.join(2)


def main():
    p=argparse.ArgumentParser();p.add_argument('--base',type=Path,default=Path('/opt/airflow/artifacts/cohort-48-01'));p.add_argument('--overlay',type=Path,default=Path('/opt/airflow/artifacts/cohort-48-fallback-01'));p.add_argument('--root',type=Path,default=Path('/opt/airflow/artifacts/monthly-ingestion-01'));p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if not a.output.resolve().is_relative_to(Path('/opt/airflow/artifacts')) or any(a.output.resolve().is_relative_to(p.resolve()) for p in [a.base,a.overlay,a.root]):raise ValueError('Choose a separate new evaluation output')
    run(a.base,a.overlay,a.root,a.output)

if __name__=='__main__':main()
