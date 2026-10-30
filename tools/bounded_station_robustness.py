"""Validation-only station guard selection; reused holdout opened only after freeze."""
import argparse,hashlib,json,resource,threading,time
from pathlib import Path
import numpy as np
import pandas as pd
from jobs.decision_policies import actions,evaluate,station_metrics
from jobs.station_robustness import CONSTRAINTS,empirical_probabilities,apply_policy,checks,guarded_policy,calendar
from tools.bounded_ml_readiness import atomic,features,FEATURES


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def profiles(frame):
    frame=frame.reset_index(drop=True).copy();frame['season']=calendar(frame)[0];rows=[]
    for columns in [['station_id'],['station_id','season']]:
        for key,g in frame.groupby(columns):
            key=key if isinstance(key,tuple) else (key,)
            rows.append({'station_id':str(key[0]),'season':str(key[1]) if len(key)>1 else 'all','hours':len(g),'positive_hours':int(g.ride_count.gt(0).sum()),'zero_fraction':float(g.ride_count.eq(0).mean()),'mean_rides':float(g.ride_count.mean()),'ride_sum':int(g.ride_count.sum())})
    return rows


def slice_penalties(frame,mu,decision,thresholds):
    result=station_metrics(frame,mu,decision,thresholds);season=calendar(frame)[0];rows=[]
    for s in sorted(frame.station_id.unique()):
        for label in sorted(set(season)):
            idx=np.flatnonzero((frame.station_id.to_numpy()==s)&(season==label))
            if len(idx):rows.append({'station_id':str(s),'season':label,**evaluate(frame.iloc[idx],np.asarray(mu)[idx],np.asarray(decision)[idx],thresholds)})
    return {'annual':result,'seasonal':rows}


def run(output,base,previous,root):
    import joblib,pyarrow as pa
    from pyiceberg.table import StaticTable
    from pyiceberg.expressions import And,In
    started=time.monotonic();output.mkdir(parents=True,exist_ok=False);stop=threading.Event();memory=[]
    def log(event,**data):
        row={'timestamp':time.time(),'event':event,**data};print(json.dumps(row),flush=True)
        with (output/'progress.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
    def monitor():
        while not stop.is_set():memory.append({'elapsed':time.monotonic()-started,'memory_current':int(Path('/sys/fs/cgroup/memory.current').read_text())});stop.wait(.5)
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    hashes={str(p):sha(p) for folder in [base,previous] for p in folder.rglob('*') if p.is_file()};hashes.update({str(p):sha(p) for p in (root/'checkpoints').glob('????-??.json')})
    atomic(output/'input_hashes.json',hashes)
    try:
        config=json.loads((base/'config.json').read_text());thresholds=json.loads((base/'thresholds.json').read_text());codes=json.loads((base/'station_codes.json').read_text());empirical=json.loads((previous/'policy_lock.json').read_text())['policy']
        specification={'constraints':CONSTRAINTS,'selection':'lowest 2025 mean penalty among candidates passing all annual and station-season guards; name tie-break','shrinkage_weights':[0,.25,.5,.75,1],'guard_variants':['whole-station fallback','station-season fallback with annual escalation'],'selection_scope':'all 12 stations equally; four previously flagged stations used for descriptive reporting only','test_previously_inspected':True,'forecast_refitted':False,'source_snapshot':config['snapshot_id'],'model_sha256':sha(base/'count_model.joblib'),'unknown_station_scope':'no forecast or calibrator extrapolation beyond original 12-station cohort'}
        atomic(output/'prespecified_plan.json',specification);log('CONSTRAINTS_PRESPECIFIED')
        table=StaticTable.from_metadata(config['metadata']);assert table.current_snapshot().snapshot_id==config['snapshot_id'];pa.set_cpu_count(2);pa.set_io_thread_count(2)
        parts=[batch.to_pandas() for batch in table.scan(row_filter=And(In('target_year',{2024,2025}),In('station_id',set(codes))),selected_fields=('station_id','station_hour_ts','ride_count','target_year','target_month')).to_arrow_batch_reader()]
        prepared,_=features(pd.concat(parts,ignore_index=True),codes);train=prepared.loc[prepared.split=='train'].reset_index(drop=True);val=prepared.loc[prepared.split=='validation'].reset_index(drop=True)
        model=joblib.load(base/'count_model.joblib');mu=np.maximum(model.predict(val[FEATURES].astype('float32')),0);baseline=actions({'kind':'original'},val,val.lag_1h.to_numpy(),thresholds);prob=empirical_probabilities(empirical,val,mu)
        atomic(output/'training_profiles.json',profiles(train));atomic(output/'validation_profiles.json',profiles(val))
        candidates={f'global_shrink_{a}':{'kind':'shrinkage','alpha':a,'fallback_stations':[],'fallback_cells':[]} for a in specification['shrinkage_weights']}
        candidates['guarded_station']=guarded_policy(val,prob,baseline,thresholds)
        candidates['guarded_seasonal']=guarded_policy(val,prob,baseline,thresholds,seasonal=True)
        validation={};validation_checks={};validation_slices={};selection_start=time.monotonic()
        for name,spec in candidates.items():
            d=apply_policy(spec,val,prob,baseline);validation[name]=evaluate(val,mu,d,thresholds);validation_checks[name]=checks(val,d,baseline,thresholds);validation_slices[name]=slice_penalties(val,mu,d,thresholds)
        eligible=[name for name in candidates if all(row['passes'] for row in validation_checks[name])]
        assert eligible,'Baseline-only candidate must remain eligible'
        winner=min(eligible,key=lambda n:(validation[n]['mean_operational_penalty'],n))
        lock={'locked_at':time.time(),'winner':winner,'policy':candidates[winner],'all_candidates':candidates,'eligible':eligible,'constraints':CONSTRAINTS,'empirical_policy_sha256':sha(previous/'policy_lock.json'),'count_model_sha256':sha(base/'count_model.joblib'),'test_opened':False,'test_used_for_selection':False,'test_previously_inspected':True,'empirical_policy':empirical}
        atomic(output/'policy_lock.json',lock);atomic(output/'validation_metrics.json',validation);atomic(output/'validation_checks.json',validation_checks);atomic(output/'validation_slices.json',validation_slices)
        selection_seconds=time.monotonic()-selection_start;log('POLICY_FROZEN',winner=winner,eligible=eligible,validation_penalty=validation[winner]['mean_operational_penalty'],fallbacks=candidates[winner])
        test=pd.read_parquet(base/'test_predictions.parquet').reset_index(drop=True);local=pd.to_datetime(test.station_hour_ts,utc=True).dt.tz_convert('America/New_York');assert local.dt.year.eq(2026).all() and local.dt.month.le(9).all()
        test_mu=test.lightgbm_poisson.to_numpy(copy=True);forecast_hash=hashlib.sha256(test_mu.tobytes()).hexdigest();test_baseline=actions({'kind':'original'},test,test.last_hour.to_numpy(),thresholds);test_prob=empirical_probabilities(empirical,test,test_mu)
        log('REUSED_HOLDOUT_OPENED',rows=len(test));atomic(output/'reused_holdout_profiles.json',profiles(test))
        final={};final_checks={};final_slices={}
        for name,spec in candidates.items():
            d=apply_policy(spec,test,test_prob,test_baseline);final[name]=evaluate(test,test_mu,d,thresholds);final_checks[name]=checks(test,d,test_baseline,thresholds);final_slices[name]=slice_penalties(test,test_mu,d,thresholds)
        for name,d in [('original_mapping',actions({'kind':'original'},test,test_mu,thresholds)),('last_hour_baseline',test_baseline)]:
            final[name]=evaluate(test,test_mu,d,thresholds);final_slices[name]=slice_penalties(test,test_mu,d,thresholds)
        # Reference action metrics use unchanged Poisson counts for comparability, not lag-count forecasts.
        val_original=actions({'kind':'original'},val,mu,thresholds)
        validation['original_mapping']=evaluate(val,mu,val_original,thresholds);validation['last_hour_baseline']=evaluate(val,mu,baseline,thresholds)
        validation_slices['original_mapping']=slice_penalties(val,mu,val_original,thresholds);validation_slices['last_hour_baseline']=slice_penalties(val,mu,baseline,thresholds)
        frozen=apply_policy(candidates[winner],test,test_prob,test_baseline);export=test[['station_id','station_hour_ts','ride_count','lightgbm_poisson']].copy();export['selected_action']=frozen;export['last_hour_action']=test_baseline;export.to_parquet(output/'reused_holdout_decisions.parquet',index=False)
        assert forecast_hash==hashlib.sha256(test.lightgbm_poisson.to_numpy().tobytes()).hexdigest();assert all(sha(Path(p))==h for p,h in hashes.items());assert json.loads((output/'policy_lock.json').read_text())==lock
        atomic(output/'validation_slices.json',validation_slices);atomic(output/'validation_metrics.json',validation);atomic(output/'reused_holdout_checks.json',final_checks);atomic(output/'reused_holdout_slices.json',final_slices)
        result={'status':'complete','selected_policy':winner,'selected_spec':candidates[winner],'validation':validation,'eligible':eligible,'reused_holdout':final,'rows':{'train':len(train),'validation':len(val),'reused_holdout':len(test)},'test_previously_inspected':True,'test_used_for_selection':False,'forecast_counts_unchanged':True,'input_hashes_unchanged':True,'forecast_sha256':forecast_hash,'selection_seconds':selection_seconds,'elapsed_seconds':time.monotonic()-started,'cgroup_peak_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'cgroup_limit_bytes':int(Path('/sys/fs/cgroup/memory.max').read_text()),'process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cgroup_events':Path('/sys/fs/cgroup/memory.events').read_text()}
        atomic(output/'metrics.json',result);log('COMPLETE',selected=winner,validation_penalty=validation[winner]['mean_operational_penalty'],diagnostic_penalty=final[winner]['mean_operational_penalty'],elapsed_seconds=result['elapsed_seconds'])
    except BaseException as error:atomic(output/'failure.json',{'error':repr(error)});raise
    finally:stop.set();thread.join(2);atomic(output/'memory.json',memory)


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--base',type=Path,default=Path('/opt/airflow/artifacts/ml-readiness-01'));p.add_argument('--previous',type=Path,default=Path('/opt/airflow/artifacts/ml-decisions-01'));p.add_argument('--root',type=Path,default=Path('/opt/airflow/artifacts/monthly-ingestion-01'));a=p.parse_args()
    if not a.output.resolve().is_relative_to(Path('/opt/airflow/artifacts')) or any(a.output.resolve().is_relative_to(x.resolve()) for x in [a.base,a.previous,a.root]):raise ValueError('Choose a separate new artifact directory')
    run(a.output,a.base,a.previous,a.root)

if __name__=='__main__':main()
