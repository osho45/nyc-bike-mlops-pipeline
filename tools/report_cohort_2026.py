"""Read-only reporting of the single frozen 2026 evaluation; never rescores."""
import argparse,json
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def report(run,images,summary):
    m=json.loads((run/'metrics.json').read_text());g=json.loads((run/'guard_outcomes.json').read_text());seasons=json.loads((run/'season_metrics.json').read_text());stations=json.loads((run/'station_metrics.json').read_text())
    pd.DataFrame([{'model':n,**v} for n,v in m['aggregate'].items()]).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'comparison.csv',index=False)
    pd.DataFrame([{'model':n,'station_id':station,**v} for n,data in stations.items() for station,v in data.items()]).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'station_comparison.csv',index=False)
    pd.DataFrame([{'model':n,'season':season,**v} for n,data in seasons.items() for season,v in data.items()]).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'season_comparison.csv',index=False)
    cells=json.loads((run/'station_season_metrics.json').read_text());pd.DataFrame([{'model':n,**v} for n,data in cells.items() for v in data]).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'station_season_comparison.csv',index=False)
    images.mkdir(parents=True,exist_ok=True);plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    names=list(m['aggregate']);labels=['Frozen model + fallback','Last hour','Previous day','Previous week'];fig,axes=plt.subplots(1,3,figsize=(13,4),layout='constrained')
    for ax,key,title in zip(axes,['mae','rmse','mean_operational_penalty'],['MAE (rides/hour)','RMSE (rides/hour)','Mean operational penalty']):
        values=[m['aggregate'][n][key] for n in names];bars=ax.barh(labels,values,color=['#2563eb']+['#94a3b8']*3);ax.invert_yaxis();ax.set(title=title,xlim=(0,max(values)*1.2));ax.grid(axis='x',alpha=.18);ax.set_axisbelow(True)
        for b,v in zip(bars,values):ax.text(v+.015,b.get_y()+b.get_height()/2,f'{v:.3f}',va='center',fontsize=9)
    fig.suptitle('Jan-Sep 2026 REUSED HOLDOUT | 48 frozen stations | identical rows',fontsize=13);fig.savefig(images/'cohort48_2026_comparison.png',dpi=150);plt.close(fig)
    fig,ax=plt.subplots(figsize=(8,3.6),layout='constrained');season_names=['winter','spring','summer','fall'];x=list(range(4))
    ax.bar([v-.18 for v in x],[seasons['frozen_poisson_fallback'][s]['mean_operational_penalty'] for s in season_names],width=.36,color='#2563eb',label='Frozen model + fallback')
    ax.bar([v+.18 for v in x],[seasons['last_hour'][s]['mean_operational_penalty'] for s in season_names],width=.36,color='#94a3b8',label='Last hour')
    ax.set_xticks(x,['Jan-Feb','Mar-May','Jun-Aug','September only']);ax.set(ylabel='Mean operational penalty',title='Seasonal reused-holdout diagnostics | partial winter and fall');ax.legend();ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True);fig.savefig(images/'cohort48_2026_seasons.png',dpi=150);plt.close(fig)
    summary.parent.mkdir(parents=True,exist_ok=True);summary.write_text(json.dumps({**m,'all_240_guard_outcomes':g,'season_metrics':seasons},indent=2)+'\n')


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,default=Path('artifacts/cohort-48-2026-01'));p.add_argument('--images',type=Path,default=Path('docs/images'));p.add_argument('--summary',type=Path,default=Path('docs/audit/cohort48_2026.json'));a=p.parse_args();report(a.run,a.images,a.summary)

if __name__=='__main__':main()
