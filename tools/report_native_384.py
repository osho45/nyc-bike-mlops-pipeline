"""Read-only reconciliation of the authorized 384-station capacity run."""
import json,hashlib,math
import numpy as np
import pyarrow.parquet as pq
from jobs.native_lgbm import canonical_keys
from pathlib import Path
from tools.report_native_capacity import stage_results
from jobs.scaling import digest


def project_network(measurements,target=1898,margin=1.5):
 """Conservative envelope for a distant extrapolation; never an execution gate override."""
 sizes=sorted(measurements)
 if sizes!=[96,192,384] or target<=sizes[-1] or margin<1:raise ValueError('Require measured 96/192/384 and conservative larger-stage projection')
 if any(not math.isfinite(value) or value<0 for value in measurements.values()):raise ValueError('Invalid measurements')
 latest=measurements[384]
 slopes=[max(0,(measurements[b]-measurements[a])/(b-a)) for a in sizes for b in sizes if b>a]
 return math.ceil(max(latest*target/384,latest+(target-384)*max(slopes))*margin)


def main():
 root=Path('artifacts/native-network-capacity-03');run=root/'384'
 old=Path('artifacts/native-network-capacity-01')
 stage96=stage_results(old/'96');stage192=stage_results(Path('artifacts/native-network-capacity-02/192'));current=stage_results(run)
 projected=json.loads(Path('artifacts/native-network-capacity-02/capacity_summary.json').read_text())['projection_384_bytes_with_25_percent_margin']
 network_plan=json.loads(Path('artifacts/network-scaling-prep-02/stage_plans/all_eligible.json').read_text())
 assert len(network_plan['stations'])==1898
 memory_projection={p:project_network({96:stage96[p]['memory']['kernel_container_peak_bytes'],192:stage192[p]['memory']['kernel_container_peak_bytes'],384:current[p]['memory']['kernel_container_peak_bytes']}) for p in current}
 runtime_projection={p:project_network({96:stage96[p]['phase_work_seconds'],192:stage192[p]['phase_work_seconds'],384:current[p]['phase_work_seconds']}) for p in current}
 retention=json.loads((run/'retention_manifest.json').read_text())
 assert retention['model_sha256']==digest(run/'capacity_model.txt')
 assert retention['source_experiment_sha256']==digest(run/'experiment.json')
 for role,manifest in retention['matrix_manifests'].items():
  for name,expected in manifest['files'].items():
   if digest(run/'matrices'/role/name)!=expected:raise ValueError('Retained matrix changed')
 plan=json.loads(Path('artifacts/network-scaling-prep-02/stage_plans/384.json').read_text())
 experiment=json.loads((run/'experiment.json').read_text())
 assert experiment['stations']==sorted(plan['stations'])
 assert experiment['station_codes']=={station:i for i,station in enumerate(sorted(plan['stations']))}
 audit=json.loads((run/'feature_audit.json').read_text())
 assert len(audit)==384 and all(x['all_lags_and_streamed_feature_order_passed'] and x['contiguous_raw_hours']==17544 for x in audit)
 for role,rows in [('train',3308544),('validation',3363840)]:
  absolute=square=total=0.;count=0;key_hash=hashlib.sha256()
  for batch in pq.ParquetFile(run/f'{role}_predictions.parquet').iter_batches(batch_size=4096,use_threads=False):
   frame=batch.to_pandas();target=frame.ride_count.to_numpy(dtype=float);error=frame.forecast.to_numpy()-target
   assert np.isfinite(error).all() and (frame.forecast>=0).all()
   key_hash.update(canonical_keys(frame));absolute+=float(np.abs(error).sum());square+=float((error**2).sum());total+=float(target.sum());count+=len(frame)
  assert count==rows and key_hash.hexdigest()==retention['prediction_row_key_sha256'][role]
  for metric,value in [('mae',absolute/count),('rmse',float(np.sqrt(square/count))),('wape',absolute/total)]:
   np.testing.assert_allclose(value,current['export']['metrics'][role][metric],rtol=1e-12)
 for role,expected in retention['prediction_files'].items():
  if digest(run/f'{role}_predictions.parquet')!=expected:raise ValueError('Retained predictions changed')
 for name,expected in json.loads((run/'native/sha256.json').read_text()).items():
  if digest(run/'native'/name)!=expected:raise ValueError('Native binary changed')
 checkpoint_hashes={}
 for iteration in [20,40,60,80,100]:
  checkpoint=run/'checkpoints'/f'iteration-{iteration:03d}.txt'
  assert checkpoint.read_text().count('\nTree=')==iteration
  checkpoint_hashes[str(iteration)]=digest(checkpoint)
 assert checkpoint_hashes['100']==digest(run/'capacity_model.txt')
 for name,expected in json.loads((run/'code_sha256.json').read_text()).items():
  if digest(Path(name))!=expected or digest(run/'source'/Path(name).name)!=expected:raise ValueError('Executed source changed')
 exp=json.loads((run/'experiment.json').read_text())
 for path,expected in exp['input_sha256'].items():
  local=Path(path.replace('/opt/airflow/',''))
  if digest(local)!=expected:raise ValueError('Source inputs changed')
 protected=json.loads((root/'protected_artifacts_sha256.json').read_text())
 for path,expected in protected.items():
  local=Path(path.replace('/opt/airflow/',''))
  if digest(local)!=expected:raise ValueError(f'Protected artifact changed: {local}')
 report={'stage_384':current,'scaling_measurements':{str(n):{phase:{'work_seconds':values[phase]['phase_work_seconds'],'kernel_peak_bytes':values[phase]['memory']['kernel_container_peak_bytes']} for phase in values} for n,values in [(96,stage96),(192,stage192),(384,current)]},'observed_vs_projected':{p:{'observed_peak_bytes':current[p]['memory']['kernel_container_peak_bytes'],'prior_projection_bytes':projected[p],'ratio':current[p]['memory']['kernel_container_peak_bytes']/projected[p]} for p in current},'projection_1898_bytes_with_50_percent_margin':memory_projection,'projection_1898_work_seconds_with_50_percent_margin':runtime_projection,'projection_1898_total_work_seconds':sum(runtime_projection.values()),'projection_basis':'Upper envelope of latest proportional scaling and worst nonnegative 96/192/384 pairwise slope extrapolated from 384; additional 50 percent margin. Estimate only, not a measured bound.','recommend_1898_authorization_resource_only':all(v<.85*2*1024**3 for v in memory_projection.values()),'full_network_expected_rows':{'train':1898*8616,'validation':1898*8760},'full_network_training_enabled':False,'checkpoint_sha256':checkpoint_hashes,'executed_source_hashes_verified':True,'native_and_export_metrics_agree':True,'protected_artifact_hashes_unchanged':True,'source_inputs_unchanged':True,'prediction_hashes_verified':True,'larger_stages_enabled':False,'operational_policy_guard_certification':False,'read_2026':False,'deployment_performed':False}
 (root/'capacity_summary.json').write_text(json.dumps(report,indent=2)+'\n')
 Path('docs/audit/native_capacity_384.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))

if __name__=='__main__':main()
