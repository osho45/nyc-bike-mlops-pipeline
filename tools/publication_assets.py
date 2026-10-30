"""Regenerate publication plots using saved, verified summaries; never score data."""
import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault('MPLCONFIGDIR', str(ROOT/'artifacts/publication-review/matplotlib'))


def load(relative):
    return json.loads((ROOT/relative).read_text(encoding='utf-8-sig'))


def collect():
    ingestion=load('docs/audit/controlled_ingestion_progress.json')
    months=ingestion['verified_months']
    assert len(months)==33 and len({m['month'] for m in months})==33
    assert ingestion['run']['status']=='complete' and ingestion['verified_years']==[2024,2025,2026]
    totals={k:sum(m[k] for m in months) for k in ['source_rows','silver_rows','quarantine_rows','gold_bi_rows','gold_ml_rows']}
    assert totals['source_rows']==totals['silver_rows']+totals['quarantine_rows']==125217844
    forecast=load('docs/audit/native_capacity_384.json')['stage_384']
    saved=load('artifacts/native-network-capacity-03/384/export/result.json')
    assert forecast['export']['metrics']==saved['metrics']
    assert saved['metrics']['train']['rows']==3308544 and saved['metrics']['validation']['rows']==3363840
    policy=load('docs/audit/cohort48_2026.json')
    assert policy['aggregate']==load('artifacts/cohort-48-2026-01/aggregate_metrics.json')
    assert policy['reused_holdout'] and policy['cohort_size']==48
    baseline=policy['aggregate']['last_hour']['mean_operational_penalty']
    selected=policy['aggregate']['frozen_poisson_fallback']['mean_operational_penalty']
    reduction=100*(baseline-selected)/baseline
    assert round(reduction,2)==17.84
    construct=load('docs/audit/native_memory_optimization.json')['experiments']
    fitting=load('docs/audit/native_fit_memory.json')['runs']
    memory={'construction_baseline':construct['baseline384']['construct']['memory']['kernel_container_peak_bytes'],
            'construction_buffered':construct['buffered384']['construct']['memory']['kernel_container_peak_bytes'],
            'fit_baseline':fitting['baseline384']['fit']['memory']['kernel_container_peak_bytes'],
            'fit_compact':fitting['compact384']['fit']['memory']['kernel_container_peak_bytes'],
            'fit_deferred':fitting['deferred384']['fit']['memory']['kernel_container_peak_bytes']}
    assert all(fitting[v]['fit']['baseline_model_sha_identical'] for v in ['compact384','deferred384'])
    assert load('docs/audit/native_fit_memory.json')['deferred_prediction_files_byte_identical']
    sources=['docs/audit/controlled_ingestion_progress.json','docs/audit/native_capacity_384.json','docs/audit/cohort48_2026.json','docs/audit/native_memory_optimization.json','docs/audit/native_fit_memory.json']
    return {'ingestion':{'months':33,'period':'2024-01 through 2026-09','totals':totals},
            'forecast_384':saved['metrics'],'policy_48':{'reused_holdout':True,'rows':policy['rows'],'aggregate':policy['aggregate'],'penalty_reduction_percent':reduction,'guard_classifications':policy['guard_classifications'],'guard_passes':policy['guard_passes'],'guard_count':policy['guard_count']},
            'memory_384_bytes':memory,'source_sha256':{p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in sources},
            'scope':'Saved result summaries only; no raw data, predictions, model inference or 2026 rescore'}


def main():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
    data=collect();images=ROOT/'docs/images';images.mkdir(exist_ok=True)
    (ROOT/'docs/audit/publication_results.json').write_text(json.dumps(data,indent=2,allow_nan=False)+'\n')
    plt.rcParams.update({'font.family':['Arial','DejaVu Sans','sans-serif'],'font.size':11,'axes.spines.top':False,'axes.spines.right':False,'axes.titleweight':'bold','figure.facecolor':'white','savefig.facecolor':'white','svg.fonttype':'none'})
    blue='#2563eb';green='#0f766e';gray='#64748b'
    fig,axes=plt.subplots(1,3,figsize=(11,3.5),layout='constrained')
    for ax,metric,title in zip(axes,['mae','rmse','wape'],['MAE · rides/hour','RMSE · rides/hour','WAPE · %']):
        values=[data['forecast_384'][r][metric]*(100 if metric=='wape' else 1) for r in ['train','validation']]
        bars=ax.bar(['2024 training','2025 validation'],values,color=[gray,blue],width=.55)
        ax.bar_label(bars,fmt='%.2f',padding=4);ax.set_title(title,fontsize=11);ax.set_ylim(0,max(values)*1.22);ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True)
    fig.suptitle('384 stations · hourly demand forecasting',fontsize=16)
    fig.savefig(images/'forecast-metrics-384-stations.png',dpi=180);plt.close(fig)
    fig,ax=plt.subplots(figsize=(9,4),layout='constrained');names=['Last hour','Previous day','Previous week','Frozen model + policy'];keys=['last_hour','previous_day','previous_week','frozen_poisson_fallback']
    values=[data['policy_48']['aggregate'][k]['mean_operational_penalty'] for k in keys]
    bars=ax.barh(names,values,color=[gray,gray,gray,green],height=.6);ax.invert_yaxis();ax.bar_label(bars,fmt='%.4f',padding=5);ax.set_xlim(0,max(values)*1.2);ax.set_xlabel('Mean modeled penalty per station-hour · lower is better');ax.set_title('48 stations · Jan–Sep 2026 REUSED holdout',loc='left',fontsize=15)
    fig.text(.01,-.03,'17.84% below last hour; illustrative costs, not dollars. Guards: 189 pass, 3 supported failures, 48 insufficient support.',fontsize=9,color=gray)
    fig.savefig(images/'operational-penalty-48-stations.png',dpi=180,bbox_inches='tight');plt.close(fig)
    m=data['memory_384_bytes'];fig,axes=plt.subplots(1,2,figsize=(10,4),layout='constrained')
    for ax,keys,labels,title in [(axes[0],['construction_baseline','construction_buffered'],['Original','Buffered reader'],'Native construction'),(axes[1],['fit_baseline','fit_compact','fit_deferred'],['Baseline','Release labels','Defer validation'],'Native fitting')]:
        values=[m[k]/1024**3 for k in keys];bars=ax.bar(labels,values,color=[gray]+[blue]*(len(keys)-1),width=.55);ax.bar_label(bars,fmt='%.3f',padding=4);ax.set_title(title);ax.set_ylabel('Peak charged container memory · GiB');ax.set_ylim(0,max(values)*1.23);ax.tick_params(axis='x',labelsize=9)
    fig.suptitle('384-station memory optimization · saved measurements',fontsize=15)
    fig.savefig(images/'native-memory-optimization-384-stations.png',dpi=180);plt.close(fig)
    fig,ax=plt.subplots(figsize=(15,6));ax.set_xlim(-.3,15.2);ax.set_ylim(0,6);ax.axis('off')
    def box(x,y,label,color=blue,w=1.85):
        ax.add_patch(FancyBboxPatch((x,y),w,.85,boxstyle='round,pad=.08',facecolor='#eff6ff' if color==blue else '#f0fdfa',edgecolor=color,linewidth=1.3));ax.text(x+w/2,y+.425,label,ha='center',va='center',fontsize=10,color='#0f172a')
    def arrow(a,b,dashed=False):ax.add_patch(FancyArrowPatch(a,b,arrowstyle='-|>',mutation_scale=12,color=gray,linestyle='--' if dashed else '-',linewidth=1.3))
    for x,label in [(0,'Official Citi Bike\nmonthly archives'),(2.15,'Bronze\nraw + lineage'),(4.3,'Silver\nclean + quarantine'),(6.45,'Gold BI / ML\nstation-hour counts'),(8.6,'Past-only features\nlags + rolling'),(10.75,'LightGBM\n2024 training'),(12.9,'Validation\n2025 selection')]:box(x,3.75,label,w=1.8)
    for x in [1.8,3.95,6.1,8.25,10.4,12.55]:arrow((x,4.17),(x+.35,4.17))
    ax.text(4.35,5.4,'Airflow asset orchestration + resumable watcher delivery',ha='center',weight='bold',fontsize=12,color=blue)
    for x in [3,5.2,7.3]:arrow((x,5.1),(x,4.65),True)
    ax.text(4.4,3.2,'Apache Iceberg / local warehouse: Bronze, Silver, Gold',ha='center',fontsize=11,color=gray)
    for x,label in [(2.15,'Existing classifier /\nreviewed model'),(4.3,'Batch scoring\nIceberg predictions'),(6.45,'PostgreSQL\nserving cache'),(8.6,'FastAPI\nprediction lookup'),(10.75,'Streamlit\ncharts + filters')]:box(x,1.2,label,green,w=1.8)
    for x in [3.95,6.1,8.25,10.4]:arrow((x,1.62),(x+.35,1.62))
    arrow((13.8,3.65),(5.2,2.15),True)
    ax.text(10.5,2.65,'Manual promotion gate · experimental count models NOT deployed',ha='center',fontsize=10,color=gray,bbox={'facecolor':'white','edgecolor':'none','pad':3})
    ax.text(7.5,.35,'Docker Compose local runtime · Airflow metadata in PostgreSQL · training is explicitly invoked, not automatic retraining',ha='center',fontsize=11,color=gray)
    fig.suptitle('Citi Bike demand MLOps · implemented data and serving paths',fontsize=17,weight='bold')
    fig.savefig(images/'citibike-architecture.svg',bbox_inches='tight');plt.close(fig)
    print(json.dumps({'verified_source_trips':data['ingestion']['totals']['source_rows'],'penalty_reduction_percent':data['policy_48']['penalty_reduction_percent'],'plots':4}))


if __name__=='__main__':main()
