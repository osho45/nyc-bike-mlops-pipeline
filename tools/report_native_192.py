"""Read-only reconciliation of the authorized 192-station capacity run."""
import json,hashlib
import numpy as np
import pyarrow.parquet as pq
from jobs.native_lgbm import canonical_keys
from pathlib import Path
from tools.report_native_capacity import stage_results
from jobs.scaling import digest


def main():
 root=Path('artifacts/native-network-capacity-02');run=root/'192'
 old=Path('artifacts/native-network-capacity-01')
 previous=stage_results(old/'96');current=stage_results(run)
 projected=json.loads((old/'capacity_summary.json').read_text())['projection_192_bytes_with_25_percent_margin']
 forecasts={p:int(max(current[p]['memory']['kernel_container_peak_bytes'],previous[p]['memory']['kernel_container_peak_bytes']+(current[p]['memory']['kernel_container_peak_bytes']-previous[p]['memory']['kernel_container_peak_bytes'])*3)*1.25) for p in current}
 retention=json.loads((run/'retention_manifest.json').read_text())
 plan=json.loads(Path('artifacts/network-scaling-prep-02/stage_plans/192.json').read_text())
 experiment=json.loads((run/'experiment.json').read_text())
 assert experiment['stations']==sorted(plan['stations'])
 assert experiment['station_codes']=={station:i for i,station in enumerate(sorted(plan['stations']))}
 audit=json.loads((run/'feature_audit.json').read_text())
 assert len(audit)==192 and all(x['all_lags_and_streamed_feature_order_passed'] and x['contiguous_raw_hours']==17544 for x in audit)
 for role,rows in [('train',1654272),('validation',1681920)]:
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
 exp=json.loads((run/'experiment.json').read_text())
 for path,expected in exp['input_sha256'].items():
  local=Path(path.replace('/opt/airflow/',''))
  if digest(local)!=expected:raise ValueError('Source inputs changed')
 protected=json.loads((old/'protected_models_and_policies_sha256.json').read_text())
 for path,expected in protected.items():
  local=Path(path.replace('/opt/airflow/',''))
  if digest(local)!=expected:raise ValueError(f'Protected artifact changed: {local}')
 report={'stage_192':current,'observed_vs_projected':{p:{'observed_peak_bytes':current[p]['memory']['kernel_container_peak_bytes'],'prior_projection_bytes':projected[p],'ratio':current[p]['memory']['kernel_container_peak_bytes']/projected[p]} for p in current},'projection_384_bytes_with_25_percent_margin':forecasts,'projection_basis':'96/192 measured phase peaks linearly extrapolated to 384, with 25 percent margin; estimates only','recommend_controlled_384_authorization_resource_only':all(v<.85*2*1024**3 for v in forecasts.values()),'native_and_export_metrics_agree':True,'protected_artifact_hashes_unchanged':True,'source_inputs_unchanged':True,'prediction_hashes_verified':True,'larger_stages_enabled':False,'operational_policy_guard_certification':False,'read_2026':False,'deployment_performed':False}
 (root/'capacity_summary.json').write_text(json.dumps(report,indent=2)+'\n')
 Path('docs/audit/native_capacity_192.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))

if __name__=='__main__':main()
