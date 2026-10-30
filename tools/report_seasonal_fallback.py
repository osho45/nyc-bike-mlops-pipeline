"""Present the 2025-only seasonal fallback audit without recomputation."""
import argparse,json
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def report(run,images,summary):
    m=json.loads((run/'metrics.json').read_text());before=json.loads((run/'before_guards.json').read_text());after=json.loads((run/'after_guards.json').read_text());lookup={(x['station_id'],x['season']):x for x in after};failed=[x for x in before if not x['passes']]
    images.mkdir(parents=True,exist_ok=True);plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    labels=[f"{x['station_id']} / {x['season']}" for x in failed];y=list(range(len(failed)));fig,ax=plt.subplots(figsize=(9,3.8),layout='constrained')
    ax.barh(y,[x['upper_one_sided_bound'] for x in failed],color='#94a3b8',label='Before fallback: upper bound')
    ax.scatter([lookup[(x['station_id'],x['season'])]['upper_one_sided_bound'] for x in failed],y,color='#2563eb',s=55,label='After fallback: upper bound',zorder=4)
    ax.scatter([x['tolerance'] for x in failed],y,color='#dc2626',s=100,marker='|',label='Frozen maximum allowed bound',zorder=5)
    ax.set_yticks(y,labels);ax.invert_yaxis();ax.set(xlabel='One-sided upper bound on policy minus last-hour penalty',title='2025 seasonal fallback guard audit | all four pass after fallback',xlim=(-.02,.52));ax.grid(axis='x',alpha=.18);ax.set_axisbelow(True);ax.legend(loc='lower right',fontsize=8)
    fig.savefig(images/'seasonal_fallback_guards.png',dpi=150);plt.close(fig)
    summary.parent.mkdir(parents=True,exist_ok=True);summary.write_text(json.dumps({**m,'all_240_guard_changes':pd.read_csv(run/'all_240_guard_changes.csv').to_dict('records')},indent=2)+'\n')


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,default=Path('artifacts/cohort-48-fallback-01'));p.add_argument('--images',type=Path,default=Path('docs/images'));p.add_argument('--summary',type=Path,default=Path('docs/audit/seasonal_fallback.json'));a=p.parse_args();report(a.run,a.images,a.summary)

if __name__=='__main__':main()
