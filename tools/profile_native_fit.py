"""Phase-profiled fitting from immutable 2024/2025 native binaries, <=384 stations."""
import argparse,json,time,shutil
from pathlib import Path
import numpy as np
import lightgbm as lgb
from jobs.native_lgbm import PhaseMonitor,DiskSequence
from jobs.native_fit_memory import fit_settings,validate_deferred,release_python_labels,trim_allocator
from jobs.scaling import FEATURE_COLUMNS,digest
from tools.native_network_capacity import DATA_PARAMS,TRAIN_PARAMS,guard,phase_admission,export
from tools.profile_native_construction import snapshot,SOURCES
from tools.bounded_ml_readiness import atomic

ROOT=Path('/opt/airflow/artifacts/native-fit-memory-01')
CODE=['tools/profile_native_fit.py','jobs/native_fit_memory.py','jobs/native_lgbm.py','tools/native_network_capacity.py','tools/profile_native_construction.py']

def booster_state(model,td,vd):
    buffers=model.__dict__.get('_Booster__inner_predict_buffer',[])
    return {'python_prediction_buffer_bytes':sum(x.nbytes for x in buffers if isinstance(x,np.ndarray)),
            'python_label_cache_bytes':sum(x.label.nbytes for x in [td,vd] if x is not None and isinstance(x.label,np.ndarray)),
            'native_train_rows':td.num_data(),'native_validation_rows':0 if vd is None else vd.num_data(),
            'thread_count':int(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('Threads:')))}

def main():
    p=argparse.ArgumentParser();p.add_argument('--stations',choices=['96','192','384'],default='384');p.add_argument('--variant',choices=['baseline','compact','hist8','one_thread','deferred'],required=True);p.add_argument('--run-name',required=True);p.add_argument('--phase',choices=['fit','export'],default='fit');a=p.parse_args();guard()
    if not a.run_name or a.run_name in ['.','..'] or Path(a.run_name).name!=a.run_name:raise ValueError('Simple new run name required')
    source=SOURCES[a.stations];exp=json.loads((source/'experiment.json').read_text());assert exp['training_year']==2024 and exp['validation_year']==2025 and len(exp['stations'])==int(a.stations)
    params=fit_settings(TRAIN_PARAMS,a.variant);run=ROOT/a.run_name;folder=run/a.phase
    if a.phase=='fit':
        run.mkdir(parents=True,exist_ok=False)
        for name in CODE:
            dest=run/'source'/Path(name).name;dest.parent.mkdir(exist_ok=True);shutil.copyfile(name,dest)
        atomic(run/'experiment.json',{'source':str(source),'stations':int(a.stations),'variant':a.variant,'train_params':params,'rounds':100,'early_stopping':False,'source_experiment_sha256':digest(source/'experiment.json'),'source_native_sha256':json.loads((source/'native/sha256.json').read_text()),'code_sha256':{x:digest(x) for x in CODE},'training_year':2024,'validation_year':2025,'read_2026':False,'full_network_training_enabled':False})
    else:
        phase_admission(json.loads((run/'fit/result.json').read_text()))
        saved=json.loads((run/'experiment.json').read_text());assert saved['variant']==a.variant and saved['train_params']==params
        for name,h in saved['code_sha256'].items():
            if digest(name)!=h:raise ValueError('Code changed between fitting/export phases')
    recorded=json.loads((run/'experiment.json').read_text())
    assert digest(source/'experiment.json')==recorded['source_experiment_sha256']
    for name,h in recorded['source_native_sha256'].items():assert digest(source/'native'/name)==h
    folder.mkdir(exist_ok=False)
    try:
        with PhaseMonitor(folder,a.phase) as mon:
            before=time.monotonic();snapshot(folder,'start')
            if a.phase=='fit':
                deferred=a.variant=='deferred';compact=a.variant in ['compact','deferred']
                if deferred:validate_deferred(100,False)
                mon.mark('binary_reload');start=time.monotonic()
                td=lgb.Dataset(str(source/'native/train.bin'),params=DATA_PARAMS,free_raw_data=True).construct()
                vd=None if deferred else lgb.Dataset(str(source/'native/validation.bin'),reference=td,params=DATA_PARAMS,free_raw_data=True).construct()
                reload_seconds=time.monotonic()-start;snapshot(folder,'after_binary_reload')
                mon.mark('label_and_feature_integrity');start=time.monotonic();released=0
                for role,dataset in [('train',td)]+([] if deferred else [('validation',vd)]):
                    manifest=json.loads((source/f'matrices/{role}/manifest.json').read_text());assert dataset.num_data()==manifest['rows'] and dataset.get_feature_name()==FEATURE_COLUMNS
                    expected=np.memmap(source/f'matrices/{role}/y.f32',mode='r',dtype='float32',shape=(manifest['rows'],))
                    labels=dataset.get_label()
                    for offset in range(0,len(expected),4096):np.testing.assert_array_equal(labels[offset:offset+4096],expected[offset:offset+4096])
                    expected._mmap.close();del expected,labels
                    if compact:released+=release_python_labels(dataset)
                if compact:trim_allocator()
                integrity_seconds=time.monotonic()-start;snapshot(folder,'after_integrity_and_optional_trim')
                mon.mark('model_fitting');history={};checkpoints=[];state_samples=[];checkpoint_seconds=0.;start=time.monotonic()
                def checkpoint(env):
                    nonlocal checkpoint_seconds
                    iteration=env.iteration+1
                    if iteration in [1,20,40,60,80,100]:
                        state_samples.append({'iteration':iteration,**booster_state(env.model,td,vd)});snapshot(folder,f'iteration_{iteration}')
                    if iteration%20==0:
                        guard();mon.mark('checkpoint_generation');snapshot(folder,f'checkpoint_{iteration}_before');t=time.monotonic()
                        dest=run/'checkpoints'/f'iteration-{iteration:03d}.txt';dest.parent.mkdir(exist_ok=True);env.model.save_model(str(dest));checkpoint_seconds+=time.monotonic()-t;checkpoints.append({'iteration':iteration,'bytes':dest.stat().st_size,'sha256':digest(dest)})
                        snapshot(folder,f'checkpoint_{iteration}_after');mon.mark('model_finalization' if iteration==100 else 'model_fitting')
                sets=[td] if deferred else [td,vd];names=['train'] if deferred else ['train','validation']
                model=lgb.train(params,td,num_boost_round=100,valid_sets=sets,valid_names=names,callbacks=[lgb.record_evaluation(history),checkpoint]);fit_seconds=time.monotonic()-start
                assert model.current_iteration()==100 and all(np.isfinite(values).all() for metrics in history.values() for values in metrics.values())
                snapshot(folder,'after_lgb_train');mon.mark('final_model_save');model.save_model(str(run/'capacity_model.txt'));atomic(run/'training_curves.json',history);snapshot(folder,'after_final_save')
                result={'binary_reload_seconds':reload_seconds,'integrity_seconds':integrity_seconds,'fit_seconds_including_checkpoints':fit_seconds,'checkpoint_save_seconds':checkpoint_seconds,'rounds':100,'model_sha256':digest(run/'capacity_model.txt'),'baseline_model_sha_identical':digest(run/'capacity_model.txt')==digest(source/'capacity_model.txt'),'history_last':{role:{metric:values[-1] for metric,values in metrics.items()} for role,metrics in history.items()},'checkpoints':checkpoints,'state_samples':state_samples,'released_python_label_bytes':released,'validation_during_fit':not deferred,'deferred_validation_required':deferred,'checkpoint_selection':'fixed iteration 100; no validation-based selection'}
            else:
                (run/'matrices').symlink_to(source/'matrices',target_is_directory=True);shutil.copyfile(source/'matrix_manifests.json',run/'matrix_manifests.json')
                result=export(run,mon)
            seconds=time.monotonic()-before
        result.update(status='complete',correctness_passed=True,phase=a.phase,stations=int(a.stations),variant=a.variant,phase_work_seconds=seconds,memory=mon.summary(),full_network_training_enabled=False,read_2026=False)
        phase_admission(result);atomic(folder/'result.json',result);print(json.dumps(result),flush=True)
    except BaseException as error:atomic(folder/'failure.json',{'error':repr(error)});raise

if __name__=='__main__':main()
