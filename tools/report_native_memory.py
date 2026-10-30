"""Reconcile saved native memory experiments; no training or prediction."""
import json,hashlib,csv
from pathlib import Path
import os
os.environ["MPLCONFIGDIR"]=str(Path("artifacts/native-memory-optimization-01/matplotlib-cache").resolve())
import numpy as np
import pyarrow.parquet as pq
from jobs.scaling import digest
from jobs.native_lgbm import canonical_keys
from tools.native_network_capacity import phase_admission
from tools.report_native_384 import project_network

ROOT=Path('artifacts/native-memory-optimization-01')
BASE=Path('artifacts/native-network-capacity-03/384')

def reconcile_export(run):
 result=json.loads((run/'export/result.json').read_text());history=json.loads((run/'training_curves.json').read_text());retention=json.loads((run/'retention_manifest.json').read_text())
 for role,expected_rows in [('train',3308544),('validation',3363840)]:
  absolute=square=total=0.;n=0;keys=hashlib.sha256()
  for batch in pq.ParquetFile(run/f'{role}_predictions.parquet').iter_batches(batch_size=4096,use_threads=False):
   f=batch.to_pandas();target=f.ride_count.to_numpy(dtype=float);error=f.forecast.to_numpy()-target
   assert np.isfinite(error).all() and (f.forecast>=0).all()
   absolute+=float(np.abs(error).sum());square+=float((error**2).sum());total+=float(target.sum());n+=len(f);keys.update(canonical_keys(f))
  assert n==expected_rows and keys.hexdigest()==retention['prediction_row_key_sha256'][role]
  assert digest(run/f'{role}_predictions.parquet')==retention['prediction_files'][role]
  for metric,value in [('mae',absolute/n),('rmse',float(np.sqrt(square/n))),('wape',absolute/total)]:np.testing.assert_allclose(value,result['metrics'][role][metric],rtol=1e-12)
  for metric,history_key in [('mae','l1'),('rmse','rmse')]:np.testing.assert_allclose(result['metrics'][role][metric],history[role][history_key][-1],rtol=1e-10)
 return result['metrics']

def allocation_plot(summary):
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 fig,axes=plt.subplots(1,2,figsize=(11,4.2),layout='constrained')
 snapshots=[summary['experiments'][name]['snapshots'][-1] for name in ['baseline384','buffered384']]
 bottom=np.zeros(2)
 for label,color,values in [
  ('Anonymous','#3478a4',[x['cgroup']['anon']/1024**2 for x in snapshots]),
  ('File-backed','#e6a03b',[x['cgroup']['file']/1024**2 for x in snapshots]),
  ('Other charged','#b7c1ca',[(x['memory_current']-x['cgroup']['anon']-x['cgroup']['file'])/1024**2 for x in snapshots])]:
  axes[0].bar(['Fresh baseline','Block-buffered'],values,bottom=bottom,label=label,color=color,width=.55);bottom+=values
 axes[0].set_ylabel('Charged memory (MiB)');axes[0].set_title('384 stations: after binary save');axes[0].legend(frameon=False,fontsize=8)
 names=['baseline384','stream384','buffered384','shards384','bins31384','sample50k384'];labels=['Baseline','Per-row','Block','Shards','31 bins','50k sample']
 values=[summary['experiments'][name]['construct']['phase_work_seconds'] for name in names]
 bars=axes[1].bar(labels,values,color=['#aab8c4','#aab8c4','#3478a4','#aab8c4','#aab8c4','#aab8c4'])
 axes[1].bar_label(bars,fmt='%.1f',padding=3,fontsize=8);axes[1].set_ylim(0,max(values)*1.17);axes[1].set_ylabel('Seconds (including matrix verification)');axes[1].set_title('Construction runtime: one run per variant');axes[1].tick_params(axis='x',labelrotation=25)
 for ax in axes:
  ax.spines[['top','right']].set_visible(False);ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True)
 target=Path('docs/assets/native_memory_optimization.png');target.parent.mkdir(parents=True,exist_ok=True);fig.savefig(target,dpi=160);plt.close(fig)


def main():
 names=['baseline384','stream384','buffered384','buffered96','buffered192','shards384','bins31384','sample50k384'];runs={}
 for name in names:
  run=ROOT/name;construct=json.loads((run/'construct/result.json').read_text());phase_admission(construct);n=construct['stations']
  assert construct['rows']=={'train':n*8616,'validation':n*8760} and construct['validation_sampled_rows']==0
  for filename,sha in construct['binaries'].items():assert digest(run/'native'/filename)==sha
  with (run/'construct/memory.csv').open() as f:memory_samples=list(csv.DictReader(f))
  starts={}
  for sample in memory_samples:starts.setdefault(sample['phase'],float(sample['elapsed']))
  intervals={phase:(list(starts.values())[i+1] if i+1<len(starts) else construct['phase_work_seconds'])-start for i,(phase,start) in enumerate(starts.items())}
  runs[name]={'construct':construct,'phase_sampled_durations_seconds':intervals,'snapshots':[json.loads(line) for line in (run/'construct/snapshots.jsonl').read_text().splitlines()]}
  for phase in ['fit','export']:
   path=run/phase/'result.json'
   if path.exists():runs[name][phase]=json.loads(path.read_text());phase_admission(runs[name][phase])
  if name in ['buffered384','bins31384','sample50k384']:
   runs[name]['verified_metrics']=reconcile_export(run)
   assert digest(run/'capacity_model.txt')==runs[name]['fit']['model_sha256']
  if name in ['baseline384','stream384','buffered384','buffered96','buffered192','shards384']:assert all(construct['baseline_binary_sha_equal'].values())
 baseline_audit=json.loads((BASE/'feature_audit.json').read_text());assert len(baseline_audit)==384 and all(x['all_lags_and_streamed_feature_order_passed'] for x in baseline_audit)
 for name in ['buffered384','bins31384','sample50k384']:
  exp=json.loads((ROOT/name/'experiment.json').read_text())
  for code,h in exp['code_sha256'].items():assert digest(ROOT/name/'source'/Path(code).name)==h
  for role,manifest in exp['source_matrix_manifests'].items():
   for file,h in manifest['files'].items():assert digest(BASE/'matrices'/role/file)==h
 for role in ['train','validation']:
  assert digest(ROOT/'buffered384'/f'{role}_predictions.parquet')==digest(BASE/f'{role}_predictions.parquet')
 assert digest(ROOT/'buffered384/capacity_model.txt')==digest(BASE/'capacity_model.txt')
 for path,sha in json.loads((ROOT/'protected_sha256.json').read_text()).items():assert digest(path)==sha
 baseline=json.loads((BASE/'construct/result.json').read_text())
 projection=project_network({n:runs[f'buffered{n}']['construct']['memory']['kernel_container_peak_bytes'] for n in [96,192,384]})
 previous=json.loads(Path('artifacts/native-network-capacity-03/capacity_summary.json').read_text());phases=dict(previous['projection_1898_bytes_with_50_percent_margin']);phases['construct']=projection
 summary={'experiments':runs,'selected_candidate':'buffered_63_bins_200000_samples','construction_peak_reduction_fraction':1-runs['buffered384']['construct']['memory']['kernel_container_peak_bytes']/baseline['memory']['kernel_container_peak_bytes'],'identical_model_and_prediction_file_sha256':True,'protected_artifacts_unchanged':True,'protected_artifact_count':len(json.loads((ROOT/'protected_sha256.json').read_text())),'prediction_metrics_independently_verified':True,'construction_projection_1898_bytes_50_percent_margin':projection,'full_network_phase_projection_bytes_50_percent_margin':phases,'full_network_peak_projection_bytes':max(phases.values()),'full_network_safe_under_2gib':max(phases.values())<.85*2*1024**3,'projection_basis':'Optimized construction 96/192/384 conservative upper envelope plus 50 percent; other phases retain prior conservative projections because not optimized. Estimates only.','full_network_training_enabled':False,'read_2026':False,'frozen_policy_changes':False,'deployment':False}
 allocation_plot(summary)
 (ROOT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');Path('docs/audit/native_memory_optimization.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({'reduction':summary['construction_peak_reduction_fraction'],'construction_projected_gib':projection/1024**3,'full_network_projected_gib':max(phases.values())/1024**3,'safe_2gib':summary['full_network_safe_under_2gib']}))

if __name__=='__main__':main()
