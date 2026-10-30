"""Validate and report native capacity artifacts; never trains or predicts."""
import argparse,json
from pathlib import Path
import numpy as np
from jobs.scaling import digest


def stage_results(run):
    values={p:json.loads((run/p/'result.json').read_text()) for p in ['prepare','construct','fit','export']}
    for phase,result in values.items():
        if result['status']!='complete' or not result['correctness_passed']:raise ValueError('Incomplete/failed native stage')
        if result['memory']['kernel_container_peak_bytes']>.85*2*1024**3:raise ValueError('Kernel high-water memory gate failed')
        events=dict(line.split() for line in result['memory']['memory_events'].splitlines())
        if any(int(events[x]) for x in ['max','oom','oom_kill']):raise ValueError('Native memory-limit event')
    history=json.loads((run/'training_curves.json').read_text());output=values['export']['metrics']
    for role in ['train','validation']:
        np.testing.assert_allclose(output[role]['mae'],history[role]['l1'][-1],rtol=1e-10)
        np.testing.assert_allclose(output[role]['rmse'],history[role]['rmse'][-1],rtol=1e-10)
    if digest(run/'capacity_model.txt')!=values['fit']['model_sha256']:raise ValueError('Model hash changed')
    return values


def project_peak(smoke_peak,capacity_peak):
    linear=smoke_peak+(capacity_peak-smoke_peak)*(192-4)/(96-4)
    return int(max(capacity_peak,linear)*1.25)


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path('artifacts/native-network-capacity-01'));a=p.parse_args();smoke=stage_results(a.root/'smoke');capacity=stage_results(a.root/'96')
    projected={phase:project_peak(smoke[phase]['memory']['kernel_container_peak_bytes'],capacity[phase]['memory']['kernel_container_peak_bytes']) for phase in smoke}
    recommendation=all(v<.85*2*1024**3 for v in projected.values())
    report={'smoke':smoke,'stage_96':capacity,'projection_192_bytes_with_25_percent_margin':projected,'projection_basis':'two-point 4/96 station linear extrapolation with 25% headroom; estimate, not a measured 192-stage peak','recommend_controlled_192_authorization_resource_only':recommendation,'stage_192_executed':False,'larger_stages_enabled':False,'operational_policy_guard_certification':False,'calibration_or_frozen_policy_changes':False,'read_2026':False,'deployment_performed':False,'native_and_export_metrics_agree':True}
    (a.root/'capacity_summary.json').write_text(json.dumps(report,indent=2)+'\n');Path('docs/audit').mkdir(parents=True,exist_ok=True);Path('docs/audit/native_capacity.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'recommend_controlled_192':recommendation,'projected_peak_bytes':projected}))

if __name__=='__main__':main()
