"""Recover every validation-round metric in bounded batches after fixed-budget training."""
import argparse,json,time,hashlib,shutil
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
import lightgbm as lgb
from jobs.native_lgbm import PhaseMonitor,canonical_keys
from jobs.native_streaming import BlockBufferedDiskSequence
from jobs.scaling import digest
from tools.native_network_capacity import guard,phase_admission
from tools.profile_native_fit import ROOT
from tools.bounded_ml_readiness import atomic


def add_tree_prediction(raw,tree_prediction):
    if raw.shape!=tree_prediction.shape:raise ValueError('Prediction row alignment mismatch')
    raw+=tree_prediction
    result=np.exp(raw)
    if not np.isfinite(result).all():raise ValueError('Non-finite reconstructed forecast')
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('--run-name',required=True);a=p.parse_args();guard()
    if Path(a.run_name).name!=a.run_name or a.run_name in ['.','..']:raise ValueError('Simple completed run name required')
    run=ROOT/a.run_name;exp=json.loads((run/'experiment.json').read_text());assert exp['variant']=='deferred' and exp['rounds']==100 and not exp['early_stopping'] and exp['stations'] in [96,192,384]
    phase_admission(json.loads((run/'fit/result.json').read_text()));source=Path(exp['source']);assert digest(run/'capacity_model.txt')==json.loads((run/'fit/result.json').read_text())['model_sha256']
    folder=run/'validation_curves';folder.mkdir(exist_ok=False);shutil.copyfile(__file__,folder/'source.py')
    with PhaseMonitor(folder,'postfit_validation_curves') as mon:
        before=time.monotonic();seq=BlockBufferedDiskSequence(source/'matrices/validation');seq.verify();seq.release_cache();model=lgb.Booster(model_file=str(run/'capacity_model.txt'))
        absolute=np.zeros(100);square=np.zeros(100);poisson=np.zeros(100);offset=0;keys_hash=hashlib.sha256();actual_total=0.
        try:
            for batch in pq.ParquetFile(source/'matrices/validation/keys.parquet').iter_batches(batch_size=4096,use_threads=False):
                keys=batch.to_pandas();n=len(keys);matrix=seq[offset:offset+n];target=keys.ride_count.to_numpy(dtype='float64');np.testing.assert_array_equal(target,seq.labels[offset:offset+n]);raw=np.zeros(n);actual_total+=float(target.sum())
                for i in range(100):
                    tree=model.predict(matrix,start_iteration=i,num_iteration=1,raw_score=True,num_threads=2)
                    pred=add_tree_prediction(raw,tree);error=pred-target;absolute[i]+=np.abs(error).sum();square[i]+=(error**2).sum();poisson[i]+=(pred-target*raw).sum()
                    if offset==0 and i in [0,19,99]:np.testing.assert_allclose(pred,model.predict(matrix,num_iteration=i+1,num_threads=2),rtol=1e-12,atol=1e-12)
                offset+=n;keys_hash.update(canonical_keys(keys));guard()
        finally:seq.close()
        assert offset==seq.rows and keys_hash.hexdigest()==seq.manifest['key_sha256']
        curves={'l1':(absolute/offset).tolist(),'rmse':np.sqrt(square/offset).tolist(),'poisson':(poisson/offset).tolist(),'wape':(absolute/actual_total).tolist()}
        reference=json.loads((source/'training_curves.json').read_text())['validation']
        differences={}
        for metric in ['l1','rmse','poisson']:
            np.testing.assert_allclose(curves[metric],reference[metric],rtol=1e-10,atol=1e-10);differences[metric]=float(np.max(np.abs(np.array(curves[metric])-reference[metric])))
        atomic(run/'deferred_validation_curves.json',curves);seconds=time.monotonic()-before
    result={'status':'complete','correctness_passed':True,'phase_work_seconds':seconds,'memory':mon.summary(),'validation_rows':offset,'rounds_evaluated':100,'max_difference_vs_native_baseline_curves':differences,'row_key_sha256':keys_hash.hexdigest(),'read_2026':False,'full_network_training_enabled':False}
    phase_admission(result);atomic(folder/'result.json',result);print(json.dumps(result),flush=True)

if __name__=='__main__':main()
