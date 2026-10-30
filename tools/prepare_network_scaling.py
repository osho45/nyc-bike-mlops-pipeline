"""Development-only network capacity estimate + bounded feature benchmark. No ML calls."""
import argparse,hashlib,json,threading,time
from pathlib import Path
import numpy as np
import pandas as pd
from jobs.cohort_selection import eligible_stations,select_stations
from jobs.scaling import FEATURE_COLUMNS,station_feature_stream,DevelopmentArtifactSink,MetricAccumulator,memory_plan,policy_support_diagnostics,write_development_memmap,digest
from jobs.ml_features import build_training_features


def run(base,output):
    import resource
    started=time.monotonic();output.mkdir(parents=True,exist_ok=False);done=threading.Event();samples=[]
    def monitor():
        while not done.is_set():
            samples.append({'elapsed':time.monotonic()-started,'current_bytes':int(Path('/sys/fs/cgroup/memory.current').read_text()),'peak_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text())});done.wait(.5)
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    # Enumerate only this development experiment, which never scored 2026.
    hashes={str(p):digest(p) for p in base.rglob('*') if p.is_file()}
    (output/'preserved_development_hashes.json').write_text(json.dumps(hashes,indent=2)+'\n')
    try:
        monthly=pd.read_parquet(base/'development_population_profiles.parquet');assert set(monthly.target_year)=={2024,2025}
        selection_start=time.monotonic();eligible=eligible_stations(monthly);eligibility_seconds=time.monotonic()-selection_start
        # Refactoring must preserve the previously frozen 48-station selection.
        old=json.loads((base/'cohort.json').read_text());again,_=select_stations(monthly,48);assert old==again
        train_raw=sum(s['hours'] for s in eligible.training);val_raw=sum(s['hours'] for s in eligible.calibration)
        all_full=all(s['hours']==8784 for s in eligible.training) and all(s['hours']==8760 for s in eligible.calibration)
        assert all_full,'Monthly coverage not sufficient for the exact no-gap volume estimate'
        train_rows=train_raw-168*len(eligible);val_rows=val_raw
        eligible.assign(training=eligible.training.map(json.dumps),calibration=eligible.calibration.map(json.dumps),training_season_means=eligible.training_season_means.map(json.dumps)).to_csv(output/'eligible_stations.csv',index=False)
        codes=json.loads((base/'station_codes.json').read_text());rank=sorted(old,key=lambda x:(x['training_mean'],x['station_id']));bench=[rank[i]['station_id'] for i in np.linspace(0,len(rank)-1,6).round().astype(int)]
        provenance={'development_years':[2024,2025],'population_source_sha256':digest(base/'development_population_profiles.parquet'),'feature_source_sha256':digest('jobs/ml_features.py'),'scaling_source_sha256':digest('jobs/scaling.py'),'benchmark_selection':'six evenly spaced 2024 training-demand ranks from the existing 48-station development cohort','model_training_allowed':False,'model_inference_allowed':False}
        sink=DevelopmentArtifactSink(output/'feature_benchmark',provenance,read_only_roots=[base]);benchmarks=[];frame_bytes=[];full_feature_paths=[]
        for station in bench:
            code=codes[station];load_start=time.monotonic();raw=pd.concat([pd.read_parquet(base/'raw'/str(year)/f'{code}.parquet') for year in [2024,2025]],ignore_index=True).sort_values('station_hour_ts').reset_index(drop=True);load_seconds=time.monotonic()-load_start
            expected=build_training_features(raw);expected['station_code']=code;expected[FEATURE_COLUMNS]=expected[FEATURE_COLUMNS].astype('float32')
            chunks=(raw.iloc[i:i+4096] for i in range(0,len(raw),4096));actual=[];stream_start=time.monotonic()
            for batch in station_feature_stream(chunks,code):
                actual.append(batch)
                for role,part in batch.groupby('role'):sink.write('features',role,part)
            stream_seconds=time.monotonic()-stream_start;actual=pd.concat(actual,ignore_index=True)
            np.testing.assert_allclose(actual[FEATURE_COLUMNS],expected[FEATURE_COLUMNS],rtol=2e-5,atol=2e-5);pd.testing.assert_series_equal(pd.to_datetime(actual.station_hour_ts,utc=True),pd.to_datetime(expected.station_hour_ts,utc=True),check_names=False)
            frame_bytes.append(float(actual.drop(columns=['role']).memory_usage(deep=True).sum()/len(actual)))
            benchmarks.append({'station_id':station,'source_rows':len(raw),'feature_rows':len(actual),'load_sort_seconds':load_seconds,'stream_features_and_write_seconds':stream_seconds,'feature_parity_passed':True,'max_input_rows':4096,'max_raw_carry_rows':168})
        retention=sink.finalize();assert retention['status']=='preparation_only'
        # Demonstrate disk matrix construction on ONE existing training shard, not a model fit.
        one=base/'features/train'/f'{codes[bench[0]]}.parquet';matrix=write_development_memmap([one],'train',output/'memmap_probe',max_rows=9000)
        mapped=np.memmap(output/'memmap_probe/X.f32',mode='r',dtype='float32',shape=(matrix['rows'],len(FEATURE_COLUMNS)));sample=pd.read_parquet(one);np.testing.assert_array_equal(mapped,sample[FEATURE_COLUMNS].to_numpy(dtype='float32'));del mapped
        # Streaming aggregation of SAVED 2025 outputs; never execute a predictor.
        import pyarrow.parquet as pq
        thresholds=json.loads((base/'thresholds.json').read_text());counter=MetricAccumulator();reduce_start=time.monotonic()
        columns=['station_id','station_hour_ts','ride_count','lightgbm_poisson','lightgbm_poisson_action']
        for chunk in pq.ParquetFile(base/'validation_predictions.parquet').iter_batches(batch_size=4096,columns=columns,use_threads=False):
            f=chunk.to_pandas();assert pd.to_datetime(f.station_hour_ts,utc=True).dt.tz_convert('America/New_York').dt.year.eq(2025).all();q25=f.station_id.map(lambda s:thresholds[s][0]);q75=f.station_id.map(lambda s:thresholds[s][1]);actual=np.where(f.ride_count<q25,0,np.where(f.ride_count<=q75,1,2));counter.update(f.ride_count,f.lightgbm_poisson,actual,f.lightgbm_poisson_action)
        reduce_seconds=time.monotonic()-reduce_start;recorded=json.loads((base/'validation_metrics.json').read_text())['lightgbm_poisson'];reduced=counter.result()
        for key in ['mae','rmse','wape','mean_operational_penalty','total_operational_penalty']:np.testing.assert_allclose(reduced[key],recorded[key],rtol=1e-12)
        policy=policy_support_diagnostics(json.loads((base/'empirical_policy.json').read_text()));(output/'training_policy_support.json').write_text(json.dumps(policy,indent=2)+'\n')
        bytes_per_row=int(np.ceil(np.mean(frame_bytes)));capacity=memory_plan(train_rows,val_rows,frame_bytes_per_row=bytes_per_row)
        raw_bench=sum(x['source_rows'] for x in benchmarks);feature_bench=sum(x['feature_rows'] for x in benchmarks);stream_seconds=sum(x['stream_features_and_write_seconds'] for x in benchmarks)
        old_metrics=json.loads((base/'metrics.json').read_text());factor=train_rows/old_metrics['rows']['train']
        result={'status':'preparation_complete_no_model_execution','observed_stations':monthly.station_id.nunique(),'eligible_stations':len(eligible),'unsupported_stations':monthly.station_id.nunique()-len(eligible),'development_source_rows':int(monthly.hours.sum()),'eligible_raw_rows':{'train':train_raw,'validation':val_raw},'eligible_feature_rows':{'train':train_rows,'validation':val_rows},'row_volume_basis':'all eligible stations have complete 8784/8760-hour calendars; 168 training warmup hours each; saved annual integrity gates required before use','expected_guard_checks':5*len(eligible),'capacity':capacity,'benchmarks':benchmarks,'eligibility_seconds':eligibility_seconds,'stream_feature_rows_per_second':feature_bench/stream_seconds,'linear_feature_processing_reference_seconds':(train_rows+val_rows)*stream_seconds/feature_bench,'feature_reference_caveat':'six warm-cache station files, external bucket sort/global raw sharding excluded; not a full-network runtime measurement','recorded_validation_reduction_seconds':reduce_seconds,'recorded_validation_reduction_rows':reduced['rows'],'streamed_metrics_match_saved':True,'old_48_station_fit_seconds':old_metrics['fit_seconds'],'linear_training_time_reference_seconds':old_metrics['fit_seconds']*factor,'training_reference_caveat':'arithmetic extrapolation only; native dataset memory, histogram construction, IO and nonlinear scaling unmeasured; no training performed','constant_action_calibrators_in_48_training_states':sum(x['constant_action'] is not None for x in policy),'bounded_feature_parity_passed':True,'memmap_probe_rows':matrix['rows'],'full_network_matrix_written':False,'model_training_calls':0,'model_predict_calls':0,'read_2026_outcomes':False,'elapsed_seconds':time.monotonic()-started,'peak_cgroup_bytes':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'cgroup_limit_bytes':int(Path('/sys/fs/cgroup/memory.max').read_text()),'process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'cgroup_events':Path('/sys/fs/cgroup/memory.events').read_text()}
        assert all(digest(p)==h for p,h in hashes.items());result['development_inputs_unchanged']=True
        (output/'metrics.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2),flush=True)
    except BaseException as error:
        (output/'failure.json').write_text(json.dumps({'status':'failed_preparation','error':repr(error),'model_training_calls':0,'model_predict_calls':0},indent=2)+'\n');raise
    finally:
        done.set();thread.join(2);(output/'memory.json').write_text(json.dumps(samples,indent=2)+'\n')


def main():
    p=argparse.ArgumentParser();p.add_argument('--base',type=Path,default=Path('/opt/airflow/artifacts/cohort-48-01'));p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if not a.output.resolve().is_relative_to(Path('/opt/airflow/artifacts')) or a.output.resolve().is_relative_to(a.base.resolve()):raise ValueError('Use a separate new preparation directory')
    run(a.base,a.output)

if __name__=='__main__':main()
