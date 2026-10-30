"""Bounded 2024/2025 decision development over the preserved 12-station forecaster."""
import argparse,hashlib,json,resource,threading,time
from pathlib import Path
import numpy as np
import pandas as pd
from tools.bounded_ml_readiness import atomic,features,FEATURES,metrics,baseline_predictions
from jobs.decision_policies import actions,evaluate,station_metrics,fit_empirical


def file_hash(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def run(base,output,root):
    import joblib,pyarrow as pa
    from pyiceberg.table import StaticTable
    from pyiceberg.expressions import And,In
    started=time.monotonic();output.mkdir(parents=True,exist_ok=False);samples=[];stop=threading.Event()
    def log(event,**values):
        row={'timestamp':time.time(),'event':event,**values};print(json.dumps(row),flush=True)
        with (output/'progress.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
    def monitor():
        while not stop.is_set():
            samples.append({'elapsed':time.monotonic()-started,'memory_current':int(Path('/sys/fs/cgroup/memory.current').read_text())});stop.wait(.5)
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    preserved={str(p):file_hash(p) for p in base.rglob('*') if p.is_file()}
    preserved.update({str(p):file_hash(p) for p in (root/'checkpoints').glob('????-??.json')})
    atomic(output/'preserved_input_hashes.json',preserved)
    try:
        original=json.loads((base/'metrics.json').read_text());assert original['status']=='complete'
        config=json.loads((base/'config.json').read_text());thresholds=json.loads((base/'thresholds.json').read_text());codes=json.loads((base/'station_codes.json').read_text());historical=json.loads((base/'baselines.json').read_text())
        table=StaticTable.from_metadata(config['metadata']);assert table.current_snapshot().snapshot_id==config['snapshot_id'] and str(table.metadata.table_uuid)==config['table_uuid']
        model=joblib.load(base/'count_model.joblib');pa.set_cpu_count(2);pa.set_io_thread_count(2)
        configuration={'base_experiment':str(base),'count_model_sha256':file_hash(base/'count_model.joblib'),'source_snapshot':config['snapshot_id'],'count_model_retrained':False,'target_thresholds_changed':False,'development_years':[2024,2025],'selection_metric':'2025 mean operational penalty; deterministic name tie-break','test_period':'January-September 2026','test_previously_inspected_in_base_experiment':True,'forecast_protocol':config['forecast_protocol'],'threshold_grid':{'low_shift':[-.5,0,.25,.5,1,2],'high_shift':[0,.25,.5,1,2]},'asymmetric_scale_factors':[.5,.75,1,1.25,1.5],'poisson_distribution_scales':[.5,1,1.5],'empirical_bins':[8,16],'empirical_prior_strength':20,'test_action_evaluation_passes':1,'base_forecast_integrity_requirement':'bit-identical test count vector; original artifact hashes unchanged'}
        atomic(output/'config.json',configuration)
        chunks=[]
        for batch in table.scan(row_filter=And(In('target_year',{2024,2025}),In('station_id',set(codes))),selected_fields=('station_id','station_hour_ts','ride_count','target_year','target_month')).to_arrow_batch_reader():chunks.append(batch.to_pandas())
        raw=pd.concat(chunks,ignore_index=True);assert set(raw.target_year)=={2024,2025}
        frame,audit=features(raw,codes);train=frame.loc[frame.split=='train'].reset_index(drop=True);val=frame.loc[frame.split=='validation'].reset_index(drop=True)
        assert len(train)==original['rows']['train'] and len(val)==original['rows']['validation']
        train_mu=np.maximum(model.predict(train[FEATURES].astype('float32')),0);val_mu=np.maximum(model.predict(val[FEATURES].astype('float32')),0)
        check=metrics(val,val_mu,thresholds)
        for k in ['mae','rmse','wape','mean_operational_penalty']:np.testing.assert_allclose(check[k],original['validation']['lightgbm_poisson'][k],rtol=1e-12)
        log('DEVELOPMENT_DATA_ONLY',training_rows=len(train),validation_rows=len(val))
        candidates={'original_mapping':{'kind':'original'}}
        for a in configuration['threshold_grid']['low_shift']:
            for b in configuration['threshold_grid']['high_shift']:candidates[f'calibrated_thresholds_{a}_{b}']={'kind':'threshold_shift','low_shift':a,'high_shift':b}
        for factor in configuration['asymmetric_scale_factors']:candidates[f'asymmetric_scale_{factor}']={'kind':'asymmetric_scale','factor':factor}
        for scale in configuration['poisson_distribution_scales']:candidates[f'poisson_expected_cost_{scale}']={'kind':'poisson_expected_cost','scale':scale}
        calibration_start=time.monotonic()
        for bins in configuration['empirical_bins']:candidates[f'empirical_expected_cost_{bins}']=fit_empirical(train,train_mu,thresholds,bins=bins,strength=20)
        calibration_seconds=time.monotonic()-calibration_start
        validation={};validation_station={}
        selection_start=time.monotonic()
        for name,policy in candidates.items():
            decision=actions(policy,val,val_mu,thresholds)
            validation[name]=evaluate(val,val_mu,decision,thresholds)
        winner=min(validation,key=lambda name:(validation[name]['mean_operational_penalty'],name))
        families={}
        for name,policy in candidates.items():
            family=policy['kind'];previous=families.get(family)
            if previous is None or (validation[name]['mean_operational_penalty'],name)<(validation[previous]['mean_operational_penalty'],previous):families[family]=name
        selected={name:candidates[name] for name in families.values()}
        validation_baselines={}
        for name,mu in baseline_predictions(val,historical).items():validation_baselines[name]=evaluate(val,mu,actions({'kind':'original'},val,mu,thresholds),thresholds)
        for name,policy in selected.items():validation_station[name]=station_metrics(val,val_mu,actions(policy,val,val_mu,thresholds),thresholds)
        selection_seconds=time.monotonic()-selection_start
        lock={'locked_at':time.time(),'winner':winner,'policy':candidates[winner],'family_winners':selected,'validation_winner_penalty':validation[winner]['mean_operational_penalty'],'count_model_sha256':configuration['count_model_sha256'],'actual_class_thresholds_sha256':file_hash(base/'thresholds.json'),'test_data_opened_in_this_run':False,'test_was_previously_inspected':True,'selection_uses_test_results':False}
        atomic(output/'validation_candidates.json',validation);atomic(output/'validation_stations.json',validation_station);atomic(output/'validation_baselines.json',validation_baselines);atomic(output/'policy_lock.json',lock)
        log('POLICY_LOCKED',winner=winner,validation_penalty=lock['validation_winner_penalty'],candidates=len(candidates),family_winners=list(selected))
        # Only now open the previously evaluated test artifact. Do not refit/retune after this point.
        test=pd.read_parquet(base/'test_predictions.parquet').reset_index(drop=True)
        assert len(test)==original['rows']['test']
        local=pd.to_datetime(test.station_hour_ts,utc=True).dt.tz_convert('America/New_York');assert local.dt.year.eq(2026).all() and local.dt.month.between(1,9).all()
        test_mu=test.lightgbm_poisson.to_numpy(copy=True);forecast_hash=hashlib.sha256(test_mu.tobytes()).hexdigest();original_model_hash=lock['count_model_sha256']
        log('TEST_OPENED_AFTER_LOCK',rows=len(test),forecast_sha256=forecast_hash)
        final={};final_stations={};export=test[['station_id','station_hour_ts','ride_count','lightgbm_poisson']].copy();pred_start=time.monotonic()
        for name,policy in selected.items():
            decision=actions(policy,test,test_mu,thresholds)
            final[name]=evaluate(test,test_mu,decision,thresholds);final_stations[name]=station_metrics(test,test_mu,decision,thresholds);export[name]=decision
        test_decision_seconds=time.monotonic()-pred_start
        baselines={};baseline_stations={}
        for name in ['last_hour','previous_day','previous_week','training_weekly_mean']:
            mu=test[name].to_numpy();decision=actions({'kind':'original'},test,mu,thresholds)
            baselines[name]=evaluate(test,mu,decision,thresholds);baseline_stations[name]=station_metrics(test,mu,decision,thresholds)
        for result in final.values():
            for key in ['mae','rmse','wape']:np.testing.assert_allclose(result[key],original['test']['lightgbm_poisson'][key],rtol=1e-12)
        assert forecast_hash==hashlib.sha256(test.lightgbm_poisson.to_numpy().tobytes()).hexdigest()
        assert preserved=={name:file_hash(Path(name)) for name in preserved}
        assert file_hash(base/'count_model.joblib')==original_model_hash
        assert json.loads((output/'policy_lock.json').read_text())==lock
        export.to_parquet(output/'test_decisions.parquet',index=False)
        atomic(output/'test_stations.json',final_stations);atomic(output/'test_baseline_stations.json',baseline_stations)
        result={'status':'complete','selected_policy':winner,'validation_candidates':validation,'validation_baselines':validation_baselines,'test_policies':final,'test_baselines':baselines,'rows':{'train':len(train),'validation':len(val),'test':len(test)},'candidate_count':len(candidates),'calibration_seconds':calibration_seconds,'selection_seconds':selection_seconds,'test_decision_and_metrics_seconds':test_decision_seconds,'elapsed_seconds':time.monotonic()-started,'process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cgroup_peak_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'cgroup_limit_bytes':int(Path('/sys/fs/cgroup/memory.max').read_text()),'cgroup_events':Path('/sys/fs/cgroup/memory.events').read_text(),'forecast_counts_unchanged':True,'source_artifacts_and_checkpoints_unchanged':True,'test_used_for_policy_selection':False,'test_previously_inspected':True,'test_forecast_sha256':forecast_hash,'feature_audit':audit}
        atomic(output/'metrics.json',result);log('COMPLETE',selected_policy=winner,test_penalty=final[winner]['mean_operational_penalty'],elapsed_seconds=result['elapsed_seconds'])
    except BaseException as error:
        atomic(output/'failure.json',{'error':repr(error)});raise
    finally:
        stop.set();thread.join(2);atomic(output/'memory.json',samples)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base',type=Path,default=Path('/opt/airflow/artifacts/ml-readiness-01'));parser.add_argument('--root',type=Path,default=Path('/opt/airflow/artifacts/monthly-ingestion-01'));parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    if not args.output.resolve().is_relative_to(Path('/opt/airflow/artifacts')):raise ValueError('New output must be inside artifacts')
    run(args.base,args.output,args.root)

if __name__=='__main__':main()
