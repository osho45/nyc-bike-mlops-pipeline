"""Phase-separated native LightGBM capacity stages. Only 2024/2025, smoke, 96, 192 and 384."""
import argparse,gc,hashlib,json,time,shutil,platform
from pathlib import Path
from urllib.parse import unquote,urlparse
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import lightgbm as lgb
from jobs.native_lgbm import MatrixWriter,DiskSequence,BoundedRawWriters,PhaseMonitor,canonical_keys
from jobs.scaling import FEATURE_COLUMNS,station_feature_stream,digest
from jobs.ml_features import build_training_features
from tools.bounded_ml_readiness import atomic

DATA_PARAMS={'max_bin':63,'bin_construct_sample_cnt':200000,'data_random_seed':42,'feature_pre_filter':False,'min_data_in_bin':3,'num_threads':2,'verbosity':-1}
TRAIN_PARAMS={**DATA_PARAMS,'objective':'poisson','metric':['poisson','l1','rmse'],'num_leaves':15,'learning_rate':.05,'min_data_in_leaf':40,'seed':42,'deterministic':True,'force_col_wise':True,'num_threads':2,'verbosity':-1}
BASE=Path('/opt/airflow/artifacts/cohort-48-01')
ROOT=Path('/opt/airflow/artifacts/monthly-ingestion-01')
PLANS=Path('/opt/airflow/artifacts/network-scaling-prep-02/stage_plans')

def plan_for_stage(stage):
    if stage not in ['smoke','96','192','384']:raise ValueError('Capacity stage not authorized')
    return PLANS/f"{'96' if stage=='smoke' else stage}.json"


def guard():
    from tools.ingestion_resources import check
    check(ROOT)
    if int(Path('/sys/fs/cgroup/memory.max').read_text())!=2*1024**3 or int(Path('/sys/fs/cgroup/memory.swap.max').read_text())!=0:raise ValueError('Require 2 GiB hard cap and zero extra swap')
    quota,period=map(int,Path('/sys/fs/cgroup/cpu.max').read_text().split())
    if quota>2*period:raise ValueError('At most two CPUs authorized')


def phase_admission(result):
    if result['status']!='complete' or not result.get('correctness_passed'):raise ValueError('Phase correctness/admission failed')
    if result['memory']['kernel_container_peak_bytes']>.85*2*1024**3:raise ValueError('Phase memory admission failed')
    events=dict(line.split() for line in result['memory']['memory_events'].splitlines())
    if any(int(events[k]) for k in ['max','oom','oom_kill']):raise ValueError('Memory events block the next capacity phase')


def admission(smoke):
    results={phase:json.loads((smoke/phase/'result.json').read_text()) for phase in ['prepare','construct','fit','export']}
    for result in results.values():phase_admission(result)
    if digest(smoke/'capacity_model.txt')!=results['fit']['model_sha256']:raise ValueError('Smoke model hash changed')
    return results


def prepare(run,stage,mon):
    from pyiceberg.table import StaticTable
    from pyiceberg.expressions import In
    plan_path=plan_for_stage(stage);plan=json.loads(plan_path.read_text());population=pd.read_parquet(BASE/'development_population_profiles.parquet');assert set(population.target_year)=={2024,2025}
    station_ids=plan['stations']
    if stage=='smoke':
        volume=population.loc[(population.target_year==2024)&population.station_id.isin(station_ids)].groupby('station_id').rides.sum().sort_values();station_ids=[str(volume.index[i]) for i in np.linspace(0,len(volume)-1,4).round().astype(int)]
    station_ids=sorted(station_ids);codes={s:i for i,s in enumerate(station_ids)};count=len(codes);assert count==(4 if stage=='smoke' else int(stage))
    config=json.loads((BASE/'config.json').read_text());table=StaticTable.from_metadata(config['metadata']);assert table.current_snapshot().snapshot_id==config['snapshot_id'];pa.set_cpu_count(2);pa.set_io_thread_count(1)
    inputs={str(p):digest(p) for p in [plan_path,BASE/'config.json',BASE/'development_population_profiles.parquet',Path(config['metadata'])]}
    inputs.update({str(p):digest(p) for p in ROOT.joinpath('checkpoints').glob('202[45]-??.json')})
    experiment={'stage':stage,'stations':station_ids,'station_codes':codes,'source_snapshot':config['snapshot_id'],'source_metadata':config['metadata'],'input_sha256':inputs,'features':FEATURE_COLUMNS,'data_params':DATA_PARAMS,'train_params':TRAIN_PARAMS,'rounds':10 if stage=='smoke' else 100,'training_year':2024,'validation_year':2025,'models_or_policies_reused':False,'calibration_performed':False,'holdout_allowed':False,'larger_stages_enabled':False}
    atomic(run/'experiment.json',experiment)
    raw=BoundedRawWriters(run/'raw',8);scanned=0;selected=0;mon.mark('raw_extraction')
    try:
        for task in table.scan(row_filter=In('target_year',{2024,2025})).plan_files():
            guard()
            if task.delete_files:raise ValueError('Row-delete files not supported by bounded reader')
            uri=str(task.file.file_path);path=Path(unquote(urlparse(uri).path)) if uri.startswith('file:') else Path(uri)
            for batch in pq.ParquetFile(path).iter_batches(batch_size=32768,columns=['station_id','station_hour_ts','ride_count','target_year','target_month'],use_threads=False):
                scanned+=batch.num_rows;mask=pc.and_(pc.is_in(batch.column(0),value_set=pa.array(station_ids)),pc.is_in(batch.column(3),value_set=pa.array([2024,2025],type=pa.int32())));part=batch.filter(mask);selected+=part.num_rows
                if part.num_rows:
                    for (year,station),frame in part.to_pandas().groupby(['target_year','station_id']):raw.append(year,codes[str(station)],frame)
    finally:raw.close()
    mon.mark('streamed_features_and_matrix_write');matrices={role:MatrixWriter(run/'matrices'/role,role) for role in ['train','validation']};audit=[]
    try:
        for station,code in codes.items():
            paths=sorted((run/'raw').glob(f'*/{code}/*.parquet'));n=sum(pq.ParquetFile(p).metadata.num_rows for p in paths)
            if n!=17544 or n>18000:raise ValueError('Station history coverage/size gate failed')
            history=pd.concat([pd.read_parquet(p) for p in paths],ignore_index=True).sort_values('station_hour_ts').reset_index(drop=True)
            if set(history.station_id.astype(str))!={station}:raise ValueError('Station-key mismatch')
            times=pd.DatetimeIndex(pd.to_datetime(history.station_hour_ts,utc=True));assert not times.duplicated().any() and (times[1:]-times[:-1]==pd.Timedelta(hours=1)).all()
            expected=build_training_features(history);expected['station_code']=code;expected[FEATURE_COLUMNS]=expected[FEATURE_COLUMNS].astype('float32');offset=0;station_counts={'train':0,'validation':0};source=pd.Series(history.ride_count.to_numpy(),index=times)
            for features in station_feature_stream((history.iloc[i:i+4096] for i in range(0,len(history),4096)),code):
                target=pd.DatetimeIndex(pd.to_datetime(features.station_hour_ts,utc=True));np.testing.assert_array_equal(features.ride_count,source.reindex(target))
                for lag in [1,2,24,168]:np.testing.assert_array_equal(features[f'lag_{lag}h'],source.reindex(target-pd.Timedelta(hours=lag)))
                expected_batch=expected.iloc[offset:offset+len(features)];np.testing.assert_allclose(features[FEATURE_COLUMNS],expected_batch[FEATURE_COLUMNS],rtol=2e-5,atol=2e-5)
                if not pd.DatetimeIndex(expected_batch.station_hour_ts).equals(target):raise ValueError('Feature row order mismatch')
                offset+=len(features)
                for role,frame in features.groupby('role'):matrices[role].append(frame);station_counts[role]+=len(frame)
            assert station_counts=={'train':8616,'validation':8760} and offset==len(expected)
            audit.append({'station_id':station,'code':code,'rows':station_counts,'contiguous_raw_hours':17544,'all_lags_and_streamed_feature_order_passed':True});del history,expected;guard()
        manifests={role:writer.seal() for role,writer in matrices.items()}
    except BaseException:
        for writer in matrices.values():writer.close()
        raise
    atomic(run/'feature_audit.json',audit);atomic(run/'matrix_manifests.json',manifests)
    assert all(digest(p)==h for p,h in inputs.items())
    return {'correctness_passed':True,'stations':count,'raw_rows_scanned':scanned,'raw_rows_selected':selected,'matrix_rows':{k:v['rows'] for k,v in manifests.items()},'max_open_raw_writers':raw.peak_open,'max_station_history_rows':17544,'max_stream_batch_rows':4096,'max_carry_rows':168,'input_hashes_unchanged':True}


def construct(run,mon):
    mon.mark('sequence_native_construction');train=DiskSequence(run/'matrices/train');valid=DiskSequence(run/'matrices/validation');train.verify();valid.verify()
    td=lgb.Dataset(train,label=train.labels,feature_name=FEATURE_COLUMNS,params=DATA_PARAMS,free_raw_data=True).construct();vd=lgb.Dataset(valid,label=valid.labels,reference=td,feature_name=FEATURE_COLUMNS,params=DATA_PARAMS,free_raw_data=True).construct()
    assert vd.reference is td and td.num_data()==len(train) and vd.num_data()==len(valid)
    assert td.get_feature_name()==vd.get_feature_name()==FEATURE_COLUMNS
    np.testing.assert_array_equal(td.get_label(),train.labels);np.testing.assert_array_equal(vd.get_label(),valid.labels)
    assert [td.feature_num_bin(i) for i in range(len(FEATURE_COLUMNS))]==[vd.feature_num_bin(i) for i in range(len(FEATURE_COLUMNS))]
    native=run/'native';native.mkdir(exist_ok=False);td.save_binary(native/'train.bin');vd.save_binary(native/'validation.bin')
    hashes={p.name:digest(p) for p in native.iterdir()};atomic(native/'sha256.json',hashes)
    return {'correctness_passed':True,'train_rows':len(train),'validation_rows':len(valid),'feature_order':td.get_feature_name(),'training_only_bin_sampling':True,'validation_references_training_bins':True,'train_sampled_rows':train.scalar_reads,'train_batch_reads':train.batch_reads,'validation_sampled_rows':valid.scalar_reads,'validation_batch_reads':valid.batch_reads,'max_native_input_batch_rows':max(train.max_batch_seen,valid.max_batch_seen),'binaries':hashes,'native_bin_counts':[td.feature_num_bin(i) for i in range(len(FEATURE_COLUMNS))]}


def fit(run,mon):
    exp=json.loads((run/'experiment.json').read_text());mon.mark('binary_reload');td=lgb.Dataset(str(run/'native/train.bin'),params=DATA_PARAMS,free_raw_data=True).construct();vd=lgb.Dataset(str(run/'native/validation.bin'),reference=td,params=DATA_PARAMS,free_raw_data=True).construct()
    for role,dataset in [('train',td),('validation',vd)]:
        source=DiskSequence(run/'matrices'/role);source.verify();assert dataset.num_data()==len(source) and dataset.get_feature_name()==FEATURE_COLUMNS;np.testing.assert_array_equal(dataset.get_label(),source.labels)
    history={};iterations=[];mon.mark('model_fitting');fit_start=time.monotonic()
    def callback(env):
        if (env.iteration+1)%20==0:
            guard();path=run/'checkpoints'/f'iteration-{env.iteration+1:03d}.txt';path.parent.mkdir(exist_ok=True);env.model.save_model(str(path));iterations.append(env.iteration+1)
    model=lgb.train(TRAIN_PARAMS,td,num_boost_round=exp['rounds'],valid_sets=[td,vd],valid_names=['train','validation'],callbacks=[lgb.record_evaluation(history),callback]);fit_seconds=time.monotonic()-fit_start
    assert model.current_iteration()==exp['rounds'] and np.isfinite(np.array(list(history['train'].values()))).all() and np.isfinite(np.array(list(history['validation'].values()))).all()
    assert history['train']['poisson'][-1]<=history['train']['poisson'][0]
    model.save_model(str(run/'capacity_model.txt'));atomic(run/'training_curves.json',history)
    return {'correctness_passed':True,'fit_seconds':fit_seconds,'rounds':model.current_iteration(),'model_sha256':digest(run/'capacity_model.txt'),'first_train_poisson':history['train']['poisson'][0],'last_train_poisson':history['train']['poisson'][-1],'validation_rmse_last':history['validation']['rmse'][-1],'checkpoint_iterations':iterations,'native_binary_reload_label_order_passed':True,'early_stopping_or_hpo':False,'calibration_performed':False,'policy_guard_certification':'not evaluated; resource/correctness capacity stage only'}


def export(run,mon):
    model=lgb.Booster(model_file=str(run/'capacity_model.txt'));assert model.feature_name()==FEATURE_COLUMNS;mon.mark('batched_prediction_and_retention');metrics={};rowhashes={};counts={};predict_seconds=0
    for role in ['train','validation']:
        seq=DiskSequence(run/'matrices'/role);seq.verify();offset=0;hashes=hashlib.sha256();writer=None;absolute=square=actual_total=0.
        try:
            for batch in pq.ParquetFile(seq.root/'keys.parquet').iter_batches(batch_size=4096,use_threads=False):
                keys=batch.to_pandas();n=len(keys);matrix=seq[offset:offset+n];np.testing.assert_array_equal(seq.labels[offset:offset+n],keys.ride_count.to_numpy(dtype='float32'))
                before=time.monotonic();pred=model.predict(matrix,num_threads=2);predict_seconds+=time.monotonic()-before
                if pred.shape!=(n,) or not np.isfinite(pred).all() or (pred<0).any():raise ValueError('Invalid native predictions')
                target=keys.ride_count.to_numpy(dtype=float);err=pred-target;absolute+=float(np.abs(err).sum());square+=float((err**2).sum());actual_total+=float(target.sum());hashes.update(canonical_keys(keys));keys['forecast']=pred;table=pa.Table.from_pandas(keys,preserve_index=False)
                if writer is None:writer=pq.ParquetWriter(run/f'{role}_predictions.parquet',table.schema,compression='zstd')
                writer.write_table(table);offset+=n
        finally:
            if writer:writer.close()
        assert offset==len(seq) and hashes.hexdigest()==seq.manifest['key_sha256'];counts[role]=offset;rowhashes[role]=hashes.hexdigest();metrics[role]={'rows':offset,'mae':absolute/offset,'rmse':float(np.sqrt(square/offset)),'wape':absolute/actual_total}
    retention={'model_sha256':digest(run/'capacity_model.txt'),'feature_order':FEATURE_COLUMNS,'source_experiment_sha256':digest(run/'experiment.json'),'matrix_manifests':json.loads((run/'matrix_manifests.json').read_text()),'prediction_row_key_sha256':rowhashes,'prediction_files':{role:digest(run/f'{role}_predictions.parquet') for role in ['train','validation']},'counts':counts,'no_full_feature_dataframe':True,'calibration_or_frozen_policy_changes':False,'decision_guard_diagnostics':{'status':'not_evaluated_capacity_only','reason':'No new policy calibrated or frozen policy applied to a different forecaster'}};atomic(run/'retention_manifest.json',retention)
    return {'correctness_passed':True,'prediction_rows':counts,'metrics':metrics,'prediction_seconds':predict_seconds,'prediction_rows_per_second':sum(counts.values())/predict_seconds,'all_row_keys_labels_and_feature_order_passed':True,'max_prediction_batch_rows':4096}


def main():
    p=argparse.ArgumentParser();p.add_argument('--stage',choices=['smoke','96','192','384'],required=True);p.add_argument('--phase',choices=['prepare','construct','fit','export'],required=True);p.add_argument('--run',type=Path,required=True);p.add_argument('--smoke-run',type=Path);p.add_argument('--previous-run',type=Path);a=p.parse_args()
    if not a.run.resolve().is_relative_to(Path('/opt/airflow/artifacts')) or a.run.resolve().is_relative_to(BASE):raise ValueError('Separate new artifact run required')
    guard()
    if a.stage in ['96','192','384']:
        if a.smoke_run is None:raise ValueError('Completed smoke admission required')
        admission(a.smoke_run)
    if a.stage in ['192','384']:
        required_previous={'192':'96','384':'192'}[a.stage]
        if a.previous_run is None:raise ValueError(f'Completed {required_previous}-station admission required')
        admission(a.previous_run)
        if json.loads((a.previous_run/'experiment.json').read_text())['stage']!=required_previous:raise ValueError(f'Previous stage must be {required_previous}')
    if a.phase=='prepare':
        a.run.mkdir(parents=True,exist_ok=False);source=a.run/'source';source.mkdir();hashes={}
        for name in ['jobs/native_lgbm.py','tools/native_network_capacity.py','jobs/scaling.py','jobs/ml_features.py','config/native_network_capacity.yml']:
            path=Path(name);shutil.copyfile(path,source/path.name);hashes[name]=digest(path)
        atomic(a.run/'code_sha256.json',hashes)
        atomic(a.run/'software.json',{'lightgbm':lgb.__version__,'numpy':np.__version__,'pandas':pd.__version__,'pyarrow':pa.__version__,'python':platform.python_version()})
    else:
        previous={'construct':'prepare','fit':'construct','export':'fit'}[a.phase];prior=json.loads((a.run/previous/'result.json').read_text());phase_admission(prior)
        assert json.loads((a.run/'experiment.json').read_text())['stage']==a.stage
        for path,h in json.loads((a.run/'experiment.json').read_text())['input_sha256'].items():
            if digest(path)!=h:raise ValueError('Source input changed')
        for path,h in json.loads((a.run/'code_sha256.json').read_text()).items():
            if digest(path)!=h:raise ValueError('Code changed between native phases')
        if a.phase in ['fit','export']:
            for name,h in json.loads((a.run/'native/sha256.json').read_text()).items():
                if digest(a.run/'native'/name)!=h:raise ValueError('Native binary changed')
    folder=a.run/a.phase;folder.mkdir(exist_ok=False)
    try:
        with PhaseMonitor(folder,a.phase) as monitor:
            before=time.monotonic();result=globals()[a.phase](a.run,a.stage,monitor) if a.phase=='prepare' else globals()[a.phase](a.run,monitor);seconds=time.monotonic()-before
        result.update(status='complete',phase=a.phase,phase_work_seconds=seconds,memory=monitor.summary(),stage=a.stage,larger_stages_enabled=False,read_2026=False,deployment_performed=False)
        phase_admission(result)
        atomic(folder/'result.json',result);print(json.dumps(result),flush=True)
    except BaseException as error:atomic(folder/'failure.json',{'error':repr(error),'stage':a.stage,'phase':a.phase});raise

if __name__=='__main__':main()
