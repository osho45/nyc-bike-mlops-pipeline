"""2025-only audit of the frozen seasonal fallback over saved validation predictions."""
import argparse,hashlib,json,threading,time
from pathlib import Path
import numpy as np
import pandas as pd
from tools.bounded_ml_readiness import atomic
from jobs.decision_policies import actions,evaluate,station_metrics
from jobs.station_robustness import CONSTRAINTS,calendar,empirical_probabilities,apply_policy,guarded_policy,checks


def acceptance(before,after,before_checks,after_checks):
    if len(before_checks)!=240 or len(after_checks)!=240:raise ValueError('Expected all 48 annual and 192 seasonal guards')
    before_keys={(x['station_id'],x['dimension'],x['season']) for x in before_checks}
    after_keys={(x['station_id'],x['dimension'],x['season']) for x in after_checks}
    if len(before_keys)!=240 or len(after_keys)!=240 or before_keys!=after_keys:raise ValueError('Guard coverage changed')
    for rows in [before_checks,after_checks]:
        if sum(x['dimension']=='annual' for x in rows)!=48 or sum(x['dimension']=='season' for x in rows)!=192:raise ValueError('Annual/seasonal guard coverage changed')
    return all(x['passes'] for x in after_checks) and after['total_operational_penalty']<=before['total_operational_penalty']


def run(base,output):
    import resource
    start=time.monotonic();output.mkdir(parents=True,exist_ok=False);done=threading.Event()
    def log(event,**values):
        row={'timestamp':time.time(),'event':event,**values};print(json.dumps(row),flush=True)
        with (output/'progress.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
    def monitor():
        with (output/'memory.csv').open('w',buffering=1) as f:
            f.write('elapsed,current_bytes\n')
            while not done.is_set():f.write(f"{time.monotonic()-start},{Path('/sys/fs/cgroup/memory.current').read_text().strip()}\n");done.wait(.5)
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in base.rglob('*') if p.is_file()}
    atomic(output/'source_hashes.json',hashes)
    try:
        config=json.loads((base/'config.json').read_text());assert config['constraints']==CONSTRAINTS
        assert not (base/'raw/2026').exists()
        frozen=json.loads(Path('/opt/airflow/artifacts/station-robustness-01/policy_lock.json').read_text());assert frozen['constraints']==CONSTRAINTS
        plan={'scope':'2025 saved validation predictions/actions only; no source dataset scans or model loading','constraints':CONSTRAINTS,'aggregate_budget':'no increase in total 2025 operational penalty versus original empirical actions','fallback':'existing guarded_policy(seasonal=True), including annual escalation if needed','cohort':48,'model_sha256':hashes[str(base/'count_model.joblib')],'empirical_policy_sha256':hashes[str(base/'empirical_policy.json')],'read_2026':False,'deploy':False}
        atomic(output/'plan.json',plan);log('PLAN_SAVED')
        frame=pd.read_parquet(base/'validation_predictions.parquet').reset_index(drop=True);local=pd.to_datetime(frame.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
        assert local.dt.year.eq(2025).all() and frame.station_id.nunique()==48
        thresholds=json.loads((base/'thresholds.json').read_text());policy=json.loads((base/'empirical_policy.json').read_text());mu=frame.lightgbm_poisson.to_numpy(copy=True)
        baseline=frame.last_hour_action.to_numpy();original=frame.lightgbm_poisson_action.to_numpy()
        np.testing.assert_array_equal(actions(policy,frame,mu,thresholds),original)
        np.testing.assert_array_equal(actions({'kind':'original'},frame,frame.last_hour.to_numpy(),thresholds),baseline)
        before=checks(frame,original,baseline,thresholds);saved=json.loads((base/'validation_guards.json').read_text());assert before==saved
        failed=[x for x in before if not x['passes']];assert len(failed)==4 and all(x['dimension']=='season' for x in failed)
        spec=guarded_policy(frame,empirical_probabilities(policy,frame,mu),baseline,thresholds,seasonal=True)
        probability=empirical_probabilities(policy,frame,mu);after_actions=apply_policy(spec,frame,probability,baseline)
        after=checks(frame,after_actions,baseline,thresholds);bmetrics=evaluate(frame,mu,original,thresholds);ametrics=evaluate(frame,mu,after_actions,thresholds)
        assert all(x['tolerance']==y['tolerance'] for x,y in zip(before,after))
        seasons,_=calendar(frame);fallback_cells={tuple(c) for c in spec['fallback_cells']};fallback_mask=np.array([(str(s),season) in fallback_cells or str(s) in spec['fallback_stations'] for s,season in zip(frame.station_id,seasons)])
        np.testing.assert_array_equal(after_actions[~fallback_mask],original[~fallback_mask]);np.testing.assert_array_equal(after_actions[fallback_mask],baseline[fallback_mask])
        # Actual labels are required for validation, but never for applying the frozen overlay.
        np.testing.assert_array_equal(after_actions,apply_policy(spec,frame.drop(columns=['ride_count']),probability,baseline))
        np.testing.assert_array_equal(mu,frame.lightgbm_poisson.to_numpy())
        for k in ['mae','rmse','wape']:assert bmetrics[k]==ametrics[k]
        approved=acceptance(bmetrics,ametrics,before,after)
        before_map={(x['station_id'],x['season']):x for x in before};after_map={(x['station_id'],x['season']):x for x in after}
        changes=[]
        for key in before_map:
            b=before_map[key];a=after_map[key]
            changes.append({'station_id':key[0],'dimension':b['dimension'],'season':key[1],'hours':b['hours'],'positive_hours':b['positive_hours'],'week_blocks':b['week_blocks'],'before_penalty':b['policy_penalty'],'after_penalty':a['policy_penalty'],'baseline_penalty':b['baseline_penalty'],'before_mean_delta':b['mean_delta'],'after_mean_delta':a['mean_delta'],'before_upper_bound':b['upper_one_sided_bound'],'after_upper_bound':a['upper_one_sided_bound'],'tolerance':b['tolerance'],'before_passes':b['passes'],'after_passes':a['passes'],'after_identical_to_baseline':a['identical_to_baseline']})
        pd.DataFrame(changes).to_csv(output/'all_240_guard_changes.csv',index=False);atomic(output/'before_guards.json',before);atomic(output/'after_guards.json',after);atomic(output/'remaining_failures.json',[x for x in after if not x['passes']]);atomic(output/'fallback_spec.json',spec)
        atomic(output/'before_station_metrics.json',station_metrics(frame,mu,original,thresholds));atomic(output/'after_station_metrics.json',station_metrics(frame,mu,after_actions,thresholds))
        export=frame[['station_id','station_hour_ts','ride_count','lightgbm_poisson']].copy();export['original_action']=original;export['fallback_action']=after_actions;export['last_hour_action']=baseline;export['fallback_cell']=fallback_mask;export.to_parquet(output/'validation_decisions.parquet',index=False)
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in hashes.items())
        result={'status':'validation_only_complete','accepted_for_validation':approved,'before':bmetrics,'after':ametrics,'cohort_size':48,'rows':len(frame),'before_guard_passes':sum(x['passes'] for x in before),'after_guard_passes':sum(x['passes'] for x in after),'guard_count':240,'annual_after_passes':sum(x['passes'] for x in after if x['dimension']=='annual'),'seasonal_after_passes':sum(x['passes'] for x in after if x['dimension']=='season'),'fallback_spec':spec,'fallback_cell_hours':int(fallback_mask.sum()),'changed_actions':int((original!=after_actions).sum()),'aggregate_penalty_change':ametrics['total_operational_penalty']-bmetrics['total_operational_penalty'],'no_new_guard_failures':not any(b['passes'] and not a['passes'] for b,a in zip(before,after)),'forecasts_unchanged':True,'source_hashes_unchanged':True,'constraints_unchanged':True,'read_2026':False,'model_retrained':False,'production_deployed':False,'elapsed_seconds':time.monotonic()-start,'peak_cgroup_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'cgroup_limit_bytes':int(Path('/sys/fs/cgroup/memory.max').read_text()),'process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cgroup_events':Path('/sys/fs/cgroup/memory.events').read_text()}
        atomic(output/'metrics.json',result);log('COMPLETE',accepted=approved,guard_passes=result['after_guard_passes'],penalty_change=result['aggregate_penalty_change'],fallback=spec)
    except BaseException as error:atomic(output/'failure.json',{'error':repr(error)});raise
    finally:done.set();thread.join(2)


def main():
    p=argparse.ArgumentParser();p.add_argument('--base',type=Path,default=Path('/opt/airflow/artifacts/cohort-48-01'));p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if not a.output.resolve().is_relative_to(Path('/opt/airflow/artifacts')) or a.output.resolve().is_relative_to(a.base.resolve()):raise ValueError('Choose a separate new artifact directory')
    run(a.base,a.output)

if __name__=='__main__':main()
