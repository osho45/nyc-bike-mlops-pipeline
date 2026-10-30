"""Read-only presentation of a gated larger-cohort ML run."""
import argparse,json
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def report(run,images,summary):
    m=json.loads((run/'metrics.json').read_text());coverage=json.loads((run/'coverage.json').read_text());cohort=pd.DataFrame(json.loads((run/'cohort.json').read_text()));guards=pd.DataFrame(json.loads((run/'validation_guards.json').read_text()));station=json.loads((run/'validation_station_metrics.json').read_text())
    cohort[['station_id','demand_band','seasonality_quartile','training_mean','training_zero_fraction','training_seasonal_cv']].to_csv(run/'cohort_coverage.csv',index=False)
    guards.to_csv(run/'validation_guard_checks.csv',index=False)
    pd.DataFrame([{'split':'2025 validation','model':name,**metrics} for name,metrics in m['validation'].items()]).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'comparison.csv',index=False)
    station_rows=[{'model':name,'station_id':s,**metrics} for name,stations in station.items() for s,metrics in stations.items()]
    pd.DataFrame(station_rows).drop(columns=['confusion_actual_rows_action_columns']).to_csv(run/'station_metrics.csv',index=False)
    images.mkdir(parents=True,exist_ok=True);plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    fig,axes=plt.subplots(1,2,figsize=(11,4),layout='constrained')
    names=list(m['validation']);labels=['Poisson + empirical','Last hour','Previous day','Previous week']
    for ax,key,title in zip(axes,['mae','mean_operational_penalty'],['Forecast MAE (rides/hour)','Mean operational penalty']):
        values=[m['validation'][n][key] for n in names];bars=ax.barh(labels,values,color=['#2563eb']+['#94a3b8']*3);ax.invert_yaxis();ax.set_title(title);ax.set_xlim(0,max(values)*1.2)
        for b,v in zip(bars,values):ax.text(v+.02,b.get_y()+b.get_height()/2,f'{v:.3f}',va='center')
        ax.grid(axis='x',alpha=.18);ax.set_axisbelow(True)
    fig.suptitle('48-station 2025 validation | stopped at seasonal guard gate',fontsize=13);fig.savefig(images/'cohort48_validation.png',dpi=150);plt.close(fig)
    fig,ax=plt.subplots(figsize=(8,3.6),layout='constrained');colors=cohort.demand_band.map({'low':'#16a34a','medium':'#2563eb','high':'#ea580c'})
    for band,g in cohort.groupby('demand_band'):ax.scatter(g.training_mean,g.training_zero_fraction*100,label=band,s=35,alpha=.8)
    ax.set(xlabel='2024 mean rides per station-hour',ylabel='2024 zero-demand hours (%)',title='Cohort demand and sparsity coverage');ax.legend(title='Training-demand band');ax.grid(alpha=.18);fig.savefig(images/'cohort48_coverage.png',dpi=150);plt.close(fig)
    portable={**m,'validation_failed_checks':guards.loc[~guards.passes].to_dict('records'),'validation_annual_checks':int((guards.dimension=='annual').sum()),'validation_season_checks':int((guards.dimension=='season').sum()),'coverage':coverage}
    summary.parent.mkdir(parents=True,exist_ok=True);summary.write_text(json.dumps(portable,indent=2)+'\n')


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,default=Path('artifacts/cohort-48-01'));p.add_argument('--images',type=Path,default=Path('docs/images'));p.add_argument('--summary',type=Path,default=Path('docs/audit/cohort48.json'));a=p.parse_args();report(a.run,a.images,a.summary)

if __name__=='__main__':main()
