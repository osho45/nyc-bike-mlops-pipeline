import json
import numpy as np
import pandas as pd
import pytest
import pyarrow.parquet as pq
from jobs.native_lgbm import MatrixWriter,DiskSequence,BoundedRawWriters
from jobs.scaling import FEATURE_COLUMNS


def data(n=10):
 f=pd.DataFrame({'station_id':'synthetic','station_hour_ts':pd.date_range('2024-01-01',periods=n,freq='h',tz='America/New_York').tz_convert('UTC'),'ride_count':np.arange(n)%4})
 for j,name in enumerate(FEATURE_COLUMNS):f[name]=np.arange(n,dtype='float32')+j
 return f


def test_sequence_samples_float64_without_promoting_stored_matrix(tmp_path):
 writer=MatrixWriter(tmp_path/'train','train');f=data();writer.append(f.iloc[:4]);writer.append(f.iloc[4:]);manifest=writer.seal();seq=DiskSequence(tmp_path/'train')
 assert seq.verify() and len(seq)==10 and seq[2].dtype==np.float64 and seq[:3].dtype==np.float32 and seq.matrix.dtype==np.float32
 np.testing.assert_array_equal(seq[:10],f[FEATURE_COLUMNS].to_numpy(dtype='float32'))
 with pytest.raises(ValueError):seq[0:5000] if len(seq)>=5000 else seq[list(range(5000))]


def test_lru_writers_do_not_lose_reopened_station_parts(tmp_path):
 writers=BoundedRawWriters(tmp_path/'raw',max_open=2)
 for code in [0,1,2,0]:writers.append(2024,code,data(3).assign(station_id=str(code)))
 writers.close();parts=list((tmp_path/'raw/2024/0').glob('*.parquet'))
 assert writers.peak_open==2 and len(parts)==2 and sum(pq.ParquetFile(p).metadata.num_rows for p in parts)==6


def test_matrix_role_fence_and_hash_tamper_detection(tmp_path):
 writer=MatrixWriter(tmp_path/'bad','validation')
 try:
  with pytest.raises(ValueError):writer.append(data())
 finally:writer.close()
 writer=MatrixWriter(tmp_path/'train','train');writer.append(data());writer.seal();seq=DiskSequence(tmp_path/'train')
 with (tmp_path/'train/y.f32').open('r+b') as f:f.write(np.array([99],dtype='float32').tobytes())
 with pytest.raises(ValueError,match='hash'):seq.verify()

def test_capacity_projection_is_conservative_for_flat_or_decreasing_memory():
 from tools.report_native_capacity import project_peak
 assert project_peak(100,100)==125
 assert project_peak(120,100)==125
 assert project_peak(100,200)>250


def test_smoke_admission_rejects_memory_event_or_headroom_breach(tmp_path):
 from tools.native_network_capacity import admission
 from jobs.scaling import digest
 checkpoint=tmp_path/'capacity_model.txt';checkpoint.write_bytes(b'admission fixture, not a model')
 for phase in ['prepare','construct','fit','export']:
  folder=tmp_path/phase;folder.mkdir();result={'status':'complete','correctness_passed':True,'memory':{'kernel_container_peak_bytes':100*1024**2,'memory_events':'max 0\noom 0\noom_kill 0\n'}}
  if phase=='fit':result['model_sha256']=digest(checkpoint)
  (folder/'result.json').write_text(json.dumps(result))
 assert len(admission(tmp_path))==4
 path=tmp_path/'construct/result.json';result=json.loads(path.read_text());result['memory']['memory_events']='max 1\noom 0\noom_kill 0\n';path.write_text(json.dumps(result))
 with pytest.raises(ValueError,match='events'):admission(tmp_path)
 result['memory']['memory_events']='max 0\noom 0\noom_kill 0\n';result['memory']['kernel_container_peak_bytes']=2*1024**3;path.write_text(json.dumps(result))
 with pytest.raises(ValueError,match='memory'):admission(tmp_path)

def test_native_labels_never_silently_round_large_integer_counts(tmp_path):
 writer=MatrixWriter(tmp_path/'large','train')
 try:
  with pytest.raises(ValueError,match='exactly representable'):writer.append(data(1).assign(ride_count=2**24+1))
 finally:writer.close()


def test_authorized_stages_use_existing_manifests_and_network_is_blocked():
 from tools.native_network_capacity import plan_for_stage
 assert plan_for_stage("192").name=="192.json"
 assert plan_for_stage("smoke").name=="96.json"
 assert plan_for_stage("384").name=="384.json"
 for stage in ["1898","all"]:
  with pytest.raises(ValueError,match="not authorized"):plan_for_stage(stage)


def test_network_projection_enforces_conservative_margin_and_measured_sizes():
 from tools.report_native_384 import project_network
 assert project_network({96:100,192:200,384:400})==2966
 assert project_network({96:400,192:300,384:200})>=200*1898/384*1.5
 with pytest.raises(ValueError):project_network({96:100,192:200})
 with pytest.raises(ValueError):project_network({96:100,192:200,384:400},margin=.9)
 with pytest.raises(ValueError):project_network({96:100,192:200,384:float('nan')})
