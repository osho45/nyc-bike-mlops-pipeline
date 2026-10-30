"""Isolated 4 GiB capacity worker. Only saved 384-station 2024/2025 inputs."""
import argparse,json,time,threading,os,shutil,hashlib
from pathlib import Path
import numpy as np
import lightgbm as lgb
import pyarrow.parquet as pq
from jobs.native_4g_resources import runtime_guard,phase_admission,new_phase,exclusive_worker
from jobs.native_lgbm import PhaseMonitor,canonical_keys
from jobs.native_streaming import BlockBufferedDiskSequence
from jobs.native_fit_memory import release_python_labels,trim_allocator,validate_deferred
from jobs.scaling import FEATURE_COLUMNS,digest
from tools.native_network_capacity import DATA_PARAMS,TRAIN_PARAMS,export
from tools.native_deferred_curves import add_tree_prediction
from tools.bounded_ml_readiness import atomic

ROOT=Path('/opt/airflow/artifacts/native-4g-validation-01')
WORK=ROOT/'runs/run01'
BASE=Path('/opt/airflow/artifacts/native-network-capacity-03/384')
CODE=['tools/native_4g_preflight.py','tools/native_4g_worker.py','jobs/native_4g_resources.py','jobs/native_lgbm.py','jobs/native_streaming.py','jobs/native_fit_memory.py','tools/native_network_capacity.py','tools/native_deferred_curves.py','config/native_training_4g.yml','docker-compose.native-4g.yml']

class ResourceWatchdog:
    def __enter__(self):
        self.done=threading.Event();self.thread=threading.Thread(target=self.loop,daemon=True);self.thread.start();return self
    def loop(self):
        while not self.done.wait(2):
            try:runtime_guard(ROOT)
            except Exception as error:
                atomic(ROOT/'resource-stop.json',{'reason':repr(error),'timestamp':time.time()});os._exit(75)
    def __exit__(self,*args):self.done.set();self.thread.join(3)

def verify_phase(name):
    result=json.loads((WORK/name/'result.json').read_text());phase_admission(result);return result

def construct(mon):
    train=BlockBufferedDiskSequence(BASE/'matrices/train');valid=BlockBufferedDiskSequence(BASE/'matrices/validation')
    try:
        mon.mark('matrix_integrity');train.verify();valid.verify();train.release_cache();valid.release_cache();runtime_guard(ROOT)
        mon.mark('native_construction');td=lgb.Dataset(train,label=train.labels,feature_name=FEATURE_COLUMNS,params=DATA_PARAMS,free_raw_data=True).construct();vd=lgb.Dataset(valid,label=valid.labels,reference=td,feature_name=FEATURE_COLUMNS,params=DATA_PARAMS,free_raw_data=True).construct()
        assert td.num_data()==3308544 and vd.num_data()==3363840 and vd.reference is td
        assert td.get_feature_name()==vd.get_feature_name()==FEATURE_COLUMNS and valid.scalar_reads==0
        np.testing.assert_array_equal(td.get_label(),train.labels);np.testing.assert_array_equal(vd.get_label(),valid.labels)
        assert [td.feature_num_bin(i) for i in range(14)]==[vd.feature_num_bin(i) for i in range(14)]
        folder=WORK/'native';folder.mkdir(exist_ok=False);td.save_binary(folder/'train.bin');vd.save_binary(folder/'validation.bin')
        hashes={p.name:digest(p) for p in folder.iterdir()};assert all(sha==digest(BASE/'native'/name) for name,sha in hashes.items());atomic(folder/'sha256.json',hashes)
        return {'rows':{'train':len(train),'validation':len(valid)},'native_sha256':hashes,'baseline_binaries_identical':True,'train_samples':train.scalar_reads,'validation_samples':valid.scalar_reads}
    finally:train.close();valid.close()

def fit(mon,variant,run):
    verify_phase('construct')
    for name,sha in json.loads((WORK/'native/sha256.json').read_text()).items():assert digest(WORK/'native'/name)==sha
    deferred=variant=='deferred'
    if deferred:validate_deferred(100,False)
    mon.mark('binary_reload');td=lgb.Dataset(str(WORK/'native/train.bin'),params=DATA_PARAMS,free_raw_data=True).construct();vd=None if deferred else lgb.Dataset(str(WORK/'native/validation.bin'),reference=td,params=DATA_PARAMS,free_raw_data=True).construct()
    for role,data in [('train',td)]+([] if deferred else [('validation',vd)]):
        rows=3308544 if role=='train' else 3363840;assert data.num_data()==rows and data.get_feature_name()==FEATURE_COLUMNS
        labels=data.get_label();expected=np.memmap(BASE/f'matrices/{role}/y.f32',mode='r',dtype='float32',shape=(rows,))
        for start in range(0,rows,4096):np.testing.assert_array_equal(labels[start:start+4096],expected[start:start+4096])
        expected._mmap.close();del expected,labels;release_python_labels(data)
    trim_allocator();history={};mon.mark('model_fitting');start=time.monotonic()
    def checkpoint(env):
        if (env.iteration+1)%20==0:
            runtime_guard(ROOT);folder=run/'checkpoints';folder.mkdir(exist_ok=True);env.model.save_model(str(folder/f'iteration-{env.iteration+1:03d}.txt'));print(f'checkpoint {variant} {env.iteration+1}/100',flush=True)
    model=lgb.train(TRAIN_PARAMS,td,num_boost_round=100,valid_sets=[td] if deferred else [td,vd],valid_names=['train'] if deferred else ['train','validation'],callbacks=[lgb.record_evaluation(history),checkpoint]);seconds=time.monotonic()-start
    assert model.current_iteration()==100 and all(np.isfinite(v).all() for metrics in history.values() for v in metrics.values())
    model.save_model(str(run/'capacity_model.txt'));assert digest(run/'capacity_model.txt')==digest(BASE/'capacity_model.txt');atomic(run/'training_curves.json',history)
    for n in [20,40,60,80,100]:assert digest(run/f'checkpoints/iteration-{n:03d}.txt')==digest(BASE/f'checkpoints/iteration-{n:03d}.txt')
    return {'fit_seconds':seconds,'model_sha256':digest(run/'capacity_model.txt'),'model_and_checkpoints_identical':True,'validation_during_fit':not deferred}

def curves(mon,run):
    verify_phase('deferred/fit');model=lgb.Booster(model_file=str(run/'capacity_model.txt'));seq=BlockBufferedDiskSequence(BASE/'matrices/validation');seq.verify();seq.release_cache();absolute=np.zeros(100);square=np.zeros(100);poisson=np.zeros(100);offset=0;key_hash=hashlib.sha256()
    try:
        for batch in pq.ParquetFile(BASE/'matrices/validation/keys.parquet').iter_batches(batch_size=4096,use_threads=False):
            keys=batch.to_pandas();n=len(keys);x=seq[offset:offset+n];target=keys.ride_count.to_numpy(dtype='float64');np.testing.assert_array_equal(target,seq.labels[offset:offset+n]);raw=np.zeros(n)
            for i in range(100):
                pred=add_tree_prediction(raw,model.predict(x,start_iteration=i,num_iteration=1,raw_score=True,num_threads=2));error=pred-target;absolute[i]+=np.abs(error).sum();square[i]+=(error**2).sum();poisson[i]+=(pred-target*raw).sum()
            offset+=n;key_hash.update(canonical_keys(keys));runtime_guard(ROOT)
        assert offset==3363840 and key_hash.hexdigest()==seq.manifest['key_sha256']
    finally:seq.close()
    output={'l1':(absolute/offset).tolist(),'rmse':np.sqrt(square/offset).tolist(),'poisson':(poisson/offset).tolist()};baseline=json.loads((BASE/'training_curves.json').read_text())['validation']
    for metric,values in output.items():np.testing.assert_allclose(values,baseline[metric],rtol=1e-10,atol=1e-10)
    atomic(run/'deferred_validation_curves.json',output);return {'validation_rows':offset,'rounds':100,'baseline_curves_match':True}

def main():
    global WORK
    p=argparse.ArgumentParser();p.add_argument('--run-name',default='run01');p.add_argument('--task',choices=['status','construct','fit','curves','export'],default='status');p.add_argument('--variant',choices=['compact','deferred'],default='compact');a=p.parse_args()
    # A direct Compose invocation cannot bypass resource admission or allocate training arrays first.
    runtime_guard(ROOT,startup=True)
    if not a.run_name or a.run_name in ['.','..'] or Path(a.run_name).name!=a.run_name:raise ValueError('Simple new run name required')
    WORK=ROOT/'runs'/a.run_name
    if a.task=='status':print(json.dumps({'status':'admitted','stations':384,'memory_gib':4,'full_network_training_enabled':False}));return
    if a.task!='construct':
        provenance=json.loads((WORK/'provenance.json').read_text())
        if digest(BASE/'experiment.json')!=provenance['source_experiment_sha256']:raise ValueError('Source experiment changed')
        for name,sha in provenance['code_sha256'].items():
            if digest(name)!=sha:raise ValueError('Code/config changed between 4 GiB phases')
    source=json.loads((BASE/'experiment.json').read_text());assert source['training_year']==2024 and source['validation_year']==2025 and len(source['stations'])==384
    run=WORK/a.variant;name='construct' if a.task=='construct' else f'{a.variant}/{a.task}'
    if a.task in ['curves','export']:verify_phase(f'{a.variant}/fit')
    if a.task=='curves' and a.variant!='deferred':raise ValueError('Deferred curves apply only to deferred variant')
    if a.task=='export' and a.variant=='deferred':verify_phase('deferred/curves')
    with exclusive_worker(ROOT):
        folder=new_phase(WORK,name)
        try:
            with ResourceWatchdog(),PhaseMonitor(folder,a.task) as mon:
                before=time.monotonic()
                if a.task=='construct':
                    archived=WORK/'source';archived.mkdir(exist_ok=False)
                    for name in CODE:shutil.copyfile(name,archived/Path(name).name)
                    atomic(WORK/'provenance.json',{'code_sha256':{name:digest(name) for name in CODE},'source_experiment_sha256':digest(BASE/'experiment.json')})
                    result=construct(mon)
                elif a.task=='fit':
                    run.mkdir(exist_ok=True);atomic(run/'experiment.json',{'variant':a.variant,'stations':384,'rounds':100,'early_stopping':False,'source_experiment_sha256':digest(BASE/'experiment.json'),'training_year':2024,'validation_year':2025});result=fit(mon,a.variant,run)
                elif a.task=='curves':result=curves(mon,run)
                else:
                    (run/'matrices').symlink_to(BASE/'matrices',target_is_directory=True);shutil.copyfile(BASE/'matrix_manifests.json',run/'matrix_manifests.json');result=export(run,mon)
                    for role in ['train','validation']:assert digest(run/f'{role}_predictions.parquet')==digest(BASE/f'{role}_predictions.parquet')
                seconds=time.monotonic()-before
            result.update(status='complete',correctness_passed=True,phase_work_seconds=seconds,memory=mon.summary(),read_2026=False,full_network_training_enabled=False);phase_admission(result);atomic(folder/'result.json',result);print(json.dumps(result),flush=True)
        except BaseException as error:atomic(folder/'failure.json',{'error':repr(error)});raise

if __name__=='__main__':main()
