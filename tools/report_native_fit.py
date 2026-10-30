"""Read-only reconciliation of fitting-memory experiments and conservative projections."""
import json,hashlib,os
from pathlib import Path
os.environ['MPLCONFIGDIR']=str(Path('artifacts/native-fit-memory-01/matplotlib-cache').resolve())
import pyarrow.parquet as pq
import numpy as np
import lightgbm as lgb
from jobs.scaling import digest
from tools.native_network_capacity import phase_admission
from tools.report_native_384 import project_network
from jobs.native_lgbm import canonical_keys

ROOT=Path('artifacts/native-fit-memory-01');BASE=Path('artifacts/native-network-capacity-03/384')

def reconcile_deferred(run):
    result=json.loads((run/'export/result.json').read_text());phase_admission(result)
    native=json.loads((run/'training_curves.json').read_text());deferred=json.loads((run/'deferred_validation_curves.json').read_text())
    retention=json.loads((run/'retention_manifest.json').read_text())
    for role,rows in [('train',3308544),('validation',3363840)]:
        n=0;absolute=square=total=0.;keys_hash=hashlib.sha256()
        for batch in pq.ParquetFile(run/f'{role}_predictions.parquet').iter_batches(batch_size=4096,use_threads=False):
            frame=batch.to_pandas();target=frame.ride_count.to_numpy(dtype='float64');error=frame.forecast.to_numpy()-target
            assert np.isfinite(error).all() and (frame.forecast>=0).all()
            absolute+=float(np.abs(error).sum());square+=float((error**2).sum());total+=float(target.sum());n+=len(frame);keys_hash.update(canonical_keys(frame))
        assert n==rows and keys_hash.hexdigest()==retention['prediction_row_key_sha256'][role]
        assert digest(run/f'{role}_predictions.parquet')==retention['prediction_files'][role]
        for metric,value in [('mae',absolute/n),('rmse',float(np.sqrt(square/n))),('wape',absolute/total)]:np.testing.assert_allclose(value,result['metrics'][role][metric],rtol=1e-12)
        history=native[role] if role=='train' else deferred
        np.testing.assert_allclose(result['metrics'][role]['mae'],history['l1'][-1],rtol=1e-10)
        np.testing.assert_allclose(result['metrics'][role]['rmse'],history['rmse'][-1],rtol=1e-10)
    return result['metrics']


def prediction_structure(tree):
    keys=['split_feature','threshold','decision_type','default_left','missing_type','leaf_value']
    result={key:tree[key] for key in keys if key in tree}
    for key in ['left_child','right_child']:
        if key in tree:result[key]=prediction_structure(tree[key])
    return result

def prediction_signature(path):
    model=lgb.Booster(model_file=str(path));dump=model.dump_model()
    data={'features':dump['feature_names'],'objective':dump['objective'],'trees':[prediction_structure(tree['tree_structure']) for tree in dump['tree_info']]}
    return hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()

def chart(summary):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,2,figsize=(11,4.1),layout='constrained')
    names=['baseline384','compact384','hist8384','one384','deferred384'];labels=['Fresh baseline','Release labels','Histogram 8 MB','One thread','Deferred validation']
    peaks=[summary['runs'][name]['fit']['memory']['kernel_container_peak_bytes']/1024**3 for name in names]
    times=[summary['runs'][name]['fit']['fit_seconds_including_checkpoints'] for name in names]
    for ax,values,title,ylabel in [(axes[0],peaks,'384 stations: fit-container peak','GiB'),(axes[1],times,f"Fit time including checkpoints\nDeferred adds {summary['deferred_validation_curves']['phase_work_seconds']:.2f} s post-fit validation",'Seconds')]:
        bars=ax.bar(range(len(names)),values,color=['#a8b7c4','#3478a4','#a8b7c4','#a8b7c4','#489979']);ax.bar_label(bars,fmt='%.3f' if ylabel=='GiB' else '%.2f',padding=3,fontsize=8);ax.set_xticks(range(len(names)),labels,rotation=25,ha='right');ax.set_ylim(0,max(values)*1.2);ax.set_title(title);ax.set_ylabel(ylabel);ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True);ax.spines[['top','right']].set_visible(False)
    path=Path('docs/assets/native_fit_memory.png');path.parent.mkdir(parents=True,exist_ok=True);fig.savefig(path,dpi=160);plt.close(fig)

def main():
    names=['baseline384','compact384','hist8384','one384','deferred384','compact96','compact192','deferred96','deferred192'];runs={}
    baseline_signature=prediction_signature(BASE/'capacity_model.txt')
    for name in names:
        run=ROOT/name;r=json.loads((run/'fit/result.json').read_text());phase_admission(r);exp=json.loads((run/'experiment.json').read_text());source=Path(exp['source'].replace('/opt/airflow/',''))
        assert exp['training_year']==2024 and exp['validation_year']==2025 and exp['stations'] in [96,192,384] and not exp['early_stopping']
        for path,sha in exp['code_sha256'].items():assert digest(run/'source'/Path(path).name)==sha
        assert digest(source/'experiment.json')==exp['source_experiment_sha256']
        for path,sha in exp['source_native_sha256'].items():assert digest(source/'native'/path)==sha
        assert digest(run/'capacity_model.txt')==r['model_sha256']
        for checkpoint in r['checkpoints']:
            path=run/'checkpoints'/f"iteration-{checkpoint['iteration']:03d}.txt";assert digest(path)==checkpoint['sha256'] and path.read_text().count('\nTree=')==checkpoint['iteration']
            if r['baseline_model_sha_identical']:assert digest(path)==digest(source/'checkpoints'/path.name)
        n=exp['stations'];sample=r['state_samples'][-1];assert sample['native_train_rows']==n*8616 and sample['native_validation_rows']==(0 if exp['variant']=='deferred' else n*8760)
        labels=np.memmap(source/'matrices/validation/y.f32',mode='r',dtype='float32',shape=(n*8760,));label_sum=float(labels.sum(dtype='float64'));labels._mmap.close()
        metrics=r['history_last'].get('validation');native_metrics=None if metrics is None else {'mae':metrics['l1'],'rmse':metrics['rmse'],'wape':metrics['l1']*n*8760/label_sum}
        runs[name]={'fit':r,'native_validation_metrics':native_metrics,'prediction_signature_matches_384_baseline':prediction_signature(run/'capacity_model.txt')==baseline_signature if n==384 else None,'snapshots':[json.loads(line) for line in (run/'fit/snapshots.jsonl').read_text().splitlines()]}
    curves=json.loads((ROOT/'deferred384/validation_curves/result.json').read_text());phase_admission(curves)
    metrics=reconcile_deferred(ROOT/'deferred384')
    for role in ['train','validation']:assert digest(ROOT/'deferred384'/f'{role}_predictions.parquet')==digest(BASE/f'{role}_predictions.parquet')
    for path,sha in json.loads((ROOT/'protected_sha256.json').read_text()).items():assert digest(path)==sha
    projections={variant:project_network({n:runs[f'{variant}{n}']['fit']['memory']['kernel_container_peak_bytes'] for n in [96,192,384]}) for variant in ['compact','deferred']}
    construction=json.loads(Path('artifacts/native-memory-optimization-01/summary.json').read_text())['construction_projection_1898_bytes_50_percent_margin']
    summary={'runs':runs,'deferred_validation_curves':curves,'deferred_export_metrics':metrics,'deferred_prediction_files_byte_identical':True,'protected_hashes_unchanged':True,'protected_artifact_count':len(json.loads((ROOT/'protected_sha256.json').read_text())),'construction_projection_1898_bytes_50_percent_margin':construction,'fitting_projection_1898_bytes_50_percent_margin':projections,'full_network_safe_under_2gib':False,'projection_basis':'96/192/384 phase peaks; upper envelope of proportional scaling and worst nonnegative observed slope, plus 50 percent. Construction retains validated previous optimized projection. Estimates only.','compact_protocol':'Inline validation preserved; original parameters and checkpoint schedule','deferred_protocol':'Fixed 100 rounds, no early stopping; all validation curves restored in bounded post-fit pass','full_network_training_enabled':False,'read_2026':False,'production_changes':False}
    assert construction>.85*2*1024**3
    chart(summary);(ROOT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');Path('docs/audit/native_fit_memory.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({'construction_gib':construction/1024**3,'fitting_gib':{k:v/1024**3 for k,v in projections.items()},'safe_under_2gib':False}))

if __name__=='__main__':main()
