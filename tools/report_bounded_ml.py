"""Present saved bounded experiment outputs without refitting or rescoring models."""
import argparse,json
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def report(run,images):
    m=json.loads((run/'metrics.json').read_text());images.mkdir(parents=True,exist_ok=True)
    rows=[{'split':split,'model':name,**values} for split in ['validation','test'] for name,values in m[split].items()]
    pd.DataFrame(rows).to_csv(run/'comparison.csv',index=False)
    slices=[]
    for split in ['validation','test']:
        source=json.loads((run/f'{split}_slices.json').read_text())
        for model,groups in source.items():
            for dimension,values in groups.items():
                slices.extend({'split':split,'model':model,'dimension':dimension,'group':group,**metrics} for group,metrics in values.items())
    pd.DataFrame(slices).to_csv(run/'sliced_metrics.csv',index=False)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    models=list(m['test']);labels=['Last hour','Previous day','Previous week','Training weekly mean','LightGBM Poisson']
    fig,axes=plt.subplots(1,3,figsize=(13,4.8),layout='constrained')
    for ax,key,title in zip(axes,['mae','rmse','mean_operational_penalty'],['MAE (rides / hour)','RMSE (rides / hour)','Mean operational penalty']):
        values=[m['test'][n][key] for n in models];bars=ax.barh(labels,values,color=['#94a3b8']*4+['#2563eb']);ax.invert_yaxis();ax.set_title(title);ax.set_xlim(0,max(values)*1.24)
        for b,v in zip(bars,values):ax.text(v+.025,b.get_y()+b.get_height()/2,f'{v:.3f}',va='center',fontsize=9)
        ax.grid(axis='x',alpha=.16);ax.set_axisbelow(True)
    fig.suptitle('Frozen Jan-Sep 2026 evaluation | 12-station pilot | lower is better',fontsize=14)
    fig.savefig(images/'ml_readiness_comparison.png',dpi=150);plt.close(fig)
    memory=json.loads((run/'memory.json').read_text());fig,ax=plt.subplots(figsize=(9,3.5),layout='constrained')
    ax.plot([r['elapsed'] for r in memory],[r['memory.current']/1024**3 for r in memory],color='#2563eb',label='Sampled cgroup memory')
    ax.axhline(m['cgroup_limit_bytes']/1024**3,color='#dc2626',linestyle='--',label='2 GiB hard limit')
    ax.set(xlabel='Seconds since experiment start',ylabel='GiB',title='Bounded experiment resource use',ylim=(0,2.15));ax.legend(loc='lower right');ax.grid(alpha=.2)
    fig.savefig(images/'ml_readiness_memory.png',dpi=150);plt.close(fig)
    portable={k:m[k] for k in ['status','validation','test','rows','validation_selected_model','fit_seconds','elapsed_seconds','process_peak_rss_bytes','cgroup_peak_bytes','cgroup_limit_bytes','cgroup_memory_events','source_checkpoints_unchanged','source_snapshot_unchanged','test_used_for_selection']}
    portable['scope']='12 stations selected by 2024-only demand ranks; rolling one-hour-ahead; not fleet-wide performance'
    portable['artifacts']='artifacts/ml-readiness-01'
    return portable


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,required=True);p.add_argument('--images',type=Path,default=Path('docs/images'));p.add_argument('--summary',type=Path,default=Path('docs/audit/ml_readiness.json'));a=p.parse_args()
    summary=report(a.run,a.images);a.summary.parent.mkdir(parents=True,exist_ok=True);a.summary.write_text(json.dumps(summary,indent=2)+'\n')

if __name__=='__main__':main()
