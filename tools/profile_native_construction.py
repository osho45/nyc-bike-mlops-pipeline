"""Bounded 384-stage construction profiling; no feature preparation or holdout access."""
import argparse,json,time,gc,ctypes,shutil,platform
from pathlib import Path
import numpy as np
import lightgbm as lgb
from jobs.native_lgbm import DiskSequence,PhaseMonitor
from jobs.native_streaming import BufferedDiskSequence,BlockBufferedDiskSequence
from jobs.scaling import FEATURE_COLUMNS,digest
from tools.native_network_capacity import DATA_PARAMS,guard,phase_admission
from tools.bounded_ml_readiness import atomic

class SequenceShard(lgb.Sequence):
 batch_size=4096
 def __init__(self,parent,start,rows):self.parent=parent;self.start=start;self.rows=rows
 def __len__(self):return self.rows
 def __getitem__(self,index):
  if isinstance(index,(int,np.integer)):
   if not 0<=index<self.rows:raise IndexError(index)
   return self.parent[self.start+index]
  if not isinstance(index,slice) or index.step not in [None,1]:raise ValueError('Contiguous shard input required')
  start,stop,_=index.indices(self.rows)
  return self.parent[self.start+start:self.start+stop]

def input_data(seq,variant):
 return [SequenceShard(seq,start,min(131072,len(seq)-start)) for start in range(0,len(seq),131072)] if variant=='shards' else seq

ROOT=Path('/opt/airflow/artifacts/native-memory-optimization-01')
SOURCES={str(n):Path(f'/opt/airflow/artifacts/native-network-capacity-0{suffix}/{n}') for n,suffix in [(96,1),(192,2),(384,3)]}

def snapshot(folder,name):
 stats=dict(line.split() for line in Path('/sys/fs/cgroup/memory.stat').read_text().splitlines())
 rollup={line.split(':')[0]:int(line.split()[1])*1024 for line in Path('/proc/self/smaps_rollup').read_text().splitlines() if ':' in line and line.split()[1].isdigit()}
 value={'name':name,'memory_current':int(Path('/sys/fs/cgroup/memory.current').read_text()),'memory_peak':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'cgroup':{k:int(stats[k]) for k in ['anon','file','inactive_file','active_file','file_mapped','kernel']},'smaps':rollup}
 with (folder/'snapshots.jsonl').open('a') as f:f.write(json.dumps(value)+'\n')
 return value

def main():
 p=argparse.ArgumentParser();p.add_argument('--stations',choices=['96','192','384'],default='384');p.add_argument('--variant',choices=['baseline','stream','buffered','bins31','sample50k','shards'],required=True);p.add_argument('--phase',choices=['construct','fit','export'],default='construct');p.add_argument('--run-name',required=True);a=p.parse_args();guard()
 
 if not a.run_name or Path(a.run_name).name!=a.run_name:raise ValueError('Simple new run name required')
 source=SOURCES[a.stations];exp=json.loads((source/'experiment.json').read_text());assert exp['training_year']==2024 and exp['validation_year']==2025 and len(exp['stations'])==int(a.stations)
 run=ROOT/a.run_name;folder=run/a.phase;folder.mkdir(parents=True,exist_ok=False)
 params=dict(DATA_PARAMS)
 if a.variant=='bins31':params['max_bin']=31
 if a.variant=='sample50k':params['bin_construct_sample_cnt']=50000
 if a.phase=='construct':
  atomic(run/'experiment.json',{'source':str(source),'stations':int(a.stations),'variant':a.variant,'params':params,'source_experiment_sha256':digest(source/'experiment.json'),'source_matrix_manifests':json.loads((source/'matrix_manifests.json').read_text()),'read_2026':False,'policy_changes':False,'code_sha256':{name:digest(name) for name in ['jobs/native_streaming.py','tools/profile_native_construction.py','jobs/native_lgbm.py','tools/native_network_capacity.py']}})
  for name in ['jobs/native_streaming.py','tools/profile_native_construction.py','jobs/native_lgbm.py','tools/native_network_capacity.py']:
   dest=run/'source'/Path(name).name;dest.parent.mkdir(exist_ok=True);shutil.copyfile(name,dest)
 else:
  previous='construct' if a.phase=='fit' else 'fit';phase_admission(json.loads((run/previous/'result.json').read_text()))
  saved=json.loads((run/'experiment.json').read_text());assert saved['params']==params and saved['variant']==a.variant
  for name,h in saved['code_sha256'].items():
   if digest(name)!=h:raise ValueError('Code changed between optimization phases')
  if digest(source/'experiment.json')!=saved['source_experiment_sha256']:raise ValueError('Source experiment changed')
  for name,h in json.loads((run/'native/sha256.json').read_text()).items():
   if digest(run/'native'/name)!=h:raise ValueError('Native binary changed')
 try:
  with PhaseMonitor(folder,a.phase) as mon:
   before=time.monotonic();snapshot(folder,'start')
   if a.phase=='construct':
    mon.mark('matrix_verification');cls=DiskSequence if a.variant=='baseline' else BufferedDiskSequence if a.variant=='stream' else BlockBufferedDiskSequence
    train=cls(source/'matrices/train');valid=cls(source/'matrices/validation');train.verify();valid.verify();snapshot(folder,'after_matrix_verification')
    if isinstance(train,BufferedDiskSequence):train.release_cache();valid.release_cache()
    snapshot(folder,'after_cache_advice');mon.mark('training_sampling_and_construction')
    td=lgb.Dataset(input_data(train,a.variant),label=train.labels,feature_name=FEATURE_COLUMNS,params=params,free_raw_data=True).construct();snapshot(folder,'after_train_construct')
    mon.mark('validation_reference_construction');vd=lgb.Dataset(input_data(valid,a.variant),label=valid.labels,reference=td,feature_name=FEATURE_COLUMNS,params=params,free_raw_data=True).construct();snapshot(folder,'after_validation_construct')
    assert td.get_feature_name()==vd.get_feature_name()==FEATURE_COLUMNS and vd.reference is td
    for seq,dataset in [(train,td),(valid,vd)]:assert len(seq)==dataset.num_data();np.testing.assert_array_equal(seq.labels,dataset.get_label())
    bins=[td.feature_num_bin(i) for i in range(len(FEATURE_COLUMNS))];assert bins==[vd.feature_num_bin(i) for i in range(len(FEATURE_COLUMNS))] and valid.scalar_reads==0
    native=run/'native';native.mkdir();mon.mark('binary_save');td.save_binary(native/'train.bin');vd.save_binary(native/'validation.bin');snapshot(folder,'after_binary_save')
    hashes={x.name:digest(x) for x in native.iterdir()};atomic(native/'sha256.json',hashes)
    result={'rows':{'train':len(train),'validation':len(valid)},'bins':bins,'train_sampled_rows':train.scalar_reads,'validation_sampled_rows':valid.scalar_reads,'binaries':hashes,'baseline_binary_sha_equal':{name:h==digest(source/'native'/name) for name,h in hashes.items()},'sample_block_reads':getattr(train,'sample_block_reads',None),'advice_calls':getattr(train,'advice_calls',0)+getattr(valid,'advice_calls',0)}
    for seq in [train,valid]:
     if isinstance(seq,BufferedDiskSequence):seq.close()
   elif a.phase=='fit':
    from tools.native_network_capacity import TRAIN_PARAMS
    settings={**TRAIN_PARAMS,**params};td=lgb.Dataset(str(run/'native/train.bin'),params=params).construct();vd=lgb.Dataset(str(run/'native/validation.bin'),reference=td,params=params).construct();snapshot(folder,'binary_reload');mon.mark('model_fitting');history={};start=time.monotonic()
    def checkpoint(env):
     if (env.iteration+1)%20==0:
      guard();dest=run/'checkpoints'/f'iteration-{env.iteration+1:03d}.txt';dest.parent.mkdir(exist_ok=True);env.model.save_model(str(dest))
    model=lgb.train(settings,td,num_boost_round=100,valid_sets=[td,vd],valid_names=['train','validation'],callbacks=[lgb.record_evaluation(history),checkpoint]);elapsed=time.monotonic()-start
    model.save_model(str(run/'capacity_model.txt'));atomic(run/'training_curves.json',history);snapshot(folder,'after_fit')
    result={'fit_seconds':elapsed,'rounds':model.current_iteration(),'model_sha256':digest(run/'capacity_model.txt'),'history_last':{role:{metric:values[-1] for metric,values in metrics.items()} for role,metrics in history.items()}}
   else:
    from tools.native_network_capacity import export
    # Reuse the verified exporter with immutable source matrices through a read-only symlink.
    (run/'matrices').symlink_to(source/'matrices',target_is_directory=True)
    shutil.copyfile(source/'matrix_manifests.json',run/'matrix_manifests.json')
    result=export(run,mon);snapshot(folder,'after_export')
   seconds=time.monotonic()-before
  result.update(status='complete',correctness_passed=True,phase=a.phase,variant=a.variant,stations=int(a.stations),phase_work_seconds=seconds,memory=mon.summary(),read_2026=False,full_network_training_enabled=False)
  phase_admission(result);atomic(folder/'result.json',result);print(json.dumps(result),flush=True)
 except BaseException as error:atomic(folder/'failure.json',{'error':repr(error)});raise

if __name__=='__main__':main()
