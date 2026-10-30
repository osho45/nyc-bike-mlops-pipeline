"""Present frozen robustness outputs; no policy selection or model fitting."""
import argparse,json
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

FOUR=['4095.03','4534.06','4677.06','6339.06']


def report(run,images,summary):
    m=json.loads((run/'metrics.json').read_text());vc=json.loads((run/'validation_checks.json').read_text());tc=json.loads((run/'reused_holdout_checks.json').read_text());winner=m['selected_policy']
    rows=[{'split':split,'candidate':name,**v} for split in ['validation','reused_holdout'] for name,v in m[split].items()]
    pd.DataFrame(rows).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'candidate_comparison.csv',index=False)
    guardrows=[{'split':split,'candidate':name,**v} for split,source in [('validation',vc),('reused_holdout',tc)] for name,values in source.items() for v in values]
    pd.DataFrame(guardrows).to_csv(run/'guard_checks.csv',index=False)
    prows=[{'split':split,**v} for split,file in [('training','training_profiles.json'),('validation','validation_profiles.json'),('reused_holdout','reused_holdout_profiles.json')] for v in json.loads((run/file).read_text())]
    pd.DataFrame(prows).to_csv(run/'demand_profiles.csv',index=False)
    metrics=[]
    for split,file in [('validation','validation_slices.json'),('reused_holdout','reused_holdout_slices.json')]:
        source=json.loads((run/file).read_text())
        for policy,groups in source.items():
            for station,v in groups['annual'].items():metrics.append({'split':split,'policy':policy,'station_id':station,'season':'all',**v})
            for v in groups['seasonal']:metrics.append({'split':split,'policy':policy,**v})
    pd.DataFrame(metrics).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'station_season_metrics.csv',index=False)
    images.mkdir(parents=True,exist_ok=True);plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    fig,axes=plt.subplots(1,2,figsize=(11,4),layout='constrained')
    for ax,title,file in zip(axes,['2025 validation — policy selection','2026 reused holdout — diagnostics only'],['validation_slices.json','reused_holdout_slices.json']):
        data=json.loads((run/file).read_text());x=list(range(4))
        ax.bar([v-.18 for v in x],[data['last_hour_baseline']['annual'][s]['mean_operational_penalty'] for s in FOUR],width=.36,color='#94a3b8',label='Last-hour actions')
        ax.bar([v+.18 for v in x],[data[winner]['annual'][s]['mean_operational_penalty'] for s in FOUR],width=.36,color='#2563eb',label='Frozen empirical actions')
        ax.set_xticks(x,FOUR);ax.set(title=title,ylabel='Mean operational penalty');ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True);ax.legend()
    fig.savefig(images/'station_robustness.png',dpi=150);plt.close(fig)
    portable={k:m[k] for k in ['status','selected_policy','selected_spec','eligible','rows','test_previously_inspected','test_used_for_selection','forecast_counts_unchanged','input_hashes_unchanged','selection_seconds','elapsed_seconds','cgroup_peak_bytes','cgroup_limit_bytes','process_peak_rss_bytes','cgroup_events','forecast_sha256','validation','reused_holdout']}
    portable['validation_selected_checks']=vc[winner];portable['reused_holdout_selected_checks']=tc[winner]
    summary.parent.mkdir(parents=True,exist_ok=True);summary.write_text(json.dumps(portable,indent=2)+'\n')


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,default=Path('artifacts/station-robustness-01'));p.add_argument('--images',type=Path,default=Path('docs/images'));p.add_argument('--summary',type=Path,default=Path('docs/audit/station_robustness.json'));a=p.parse_args();report(a.run,a.images,a.summary)

if __name__=='__main__':main()
