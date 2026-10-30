"""Read-only reporting for a frozen bounded decision-policy experiment."""
import argparse,json
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def report(run,images,summary):
    m=json.loads((run/'metrics.json').read_text());lock=json.loads((run/'policy_lock.json').read_text());stations=json.loads((run/'test_stations.json').read_text())
    families=list(lock['family_winners']);rows=[]
    for split,key in [('validation','validation_candidates'),('test','test_policies')]:
        for name in families:rows.append({'split':split,'type':'policy','name':name,**m[key][name]})
        for name,values in m[f'{split}_baselines'].items():rows.append({'split':split,'type':'baseline','name':name,**values})
    pd.DataFrame(rows).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'comparison.csv',index=False)
    station_rows=[]
    for split in ['validation','test']:
        groups=json.loads((run/f'{split}_stations.json').read_text())
        for policy,values in groups.items():
            for station,value in values.items():station_rows.append({'split':split,'policy':policy,'station_id':station,**value})
    pd.DataFrame(station_rows).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'station_comparison.csv',index=False)
    images.mkdir(parents=True,exist_ok=True);winner=m['selected_policy']
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    labels=['Original mean mapping','Shifted action thresholds','Multiplicative action rule','Poisson expected cost','Empirical expected cost']
    fig,ax=plt.subplots(figsize=(10,4),layout='constrained');x=list(range(len(families)))
    ax.bar([v-.18 for v in x],[m['validation_candidates'][n]['mean_operational_penalty'] for n in families],width=.36,color='#94a3b8',label='2025 validation')
    ax.bar([v+.18 for v in x],[m['test_policies'][n]['mean_operational_penalty'] for n in families],width=.36,color='#2563eb',label='2026 post-freeze comparison')
    ax.set_xticks(x,labels,rotation=12,ha='right');ax.set(ylabel='Mean operational penalty',title='Frozen forecast, different decision policies | lower is better');ax.legend();ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
    fig.savefig(images/'decision_policy_comparison.png',dpi=150);plt.close(fig)
    names=list(stations[winner]);fig,ax=plt.subplots(figsize=(11,4),layout='constrained');x=list(range(len(names)))
    ax.bar([v-.18 for v in x],[stations['original_mapping'][n]['mean_operational_penalty'] for n in names],width=.36,color='#94a3b8',label='Original mapping')
    ax.bar([v+.18 for v in x],[stations[winner][n]['mean_operational_penalty'] for n in names],width=.36,color='#2563eb',label='Frozen empirical policy')
    ax.set_xticks(x,names,rotation=45,ha='right');ax.set(ylabel='Mean operational penalty',title='Station-level 2026 diagnostics | selected on 2025 only');ax.legend();ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
    fig.savefig(images/'decision_policy_stations.png',dpi=150);plt.close(fig)
    portable={k:m[k] for k in ['status','selected_policy','rows','candidate_count','calibration_seconds','selection_seconds','test_decision_and_metrics_seconds','elapsed_seconds','process_peak_rss_bytes','cgroup_peak_bytes','cgroup_limit_bytes','cgroup_events','forecast_counts_unchanged','source_artifacts_and_checkpoints_unchanged','test_used_for_policy_selection','test_previously_inspected','test_forecast_sha256','test_baselines','validation_baselines']}
    portable['validation_policy_comparison']={n:m['validation_candidates'][n] for n in families};portable['test_policy_comparison']=m['test_policies']
    summary.parent.mkdir(parents=True,exist_ok=True);summary.write_text(json.dumps(portable,indent=2)+'\n')


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,default=Path('artifacts/ml-decisions-01'));p.add_argument('--images',type=Path,default=Path('docs/images'));p.add_argument('--summary',type=Path,default=Path('docs/audit/decision_policies.json'));a=p.parse_args();report(a.run,a.images,a.summary)

if __name__=='__main__':main()
