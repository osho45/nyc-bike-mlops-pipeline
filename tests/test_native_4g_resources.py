import json
import pytest
import yaml
from pathlib import Path
from jobs.native_4g_resources import GIB,EXPECTED_IMAGE,admission_reasons,validate_limits,phase_admission,new_phase,exclusive_worker

def ready():
 return {'timestamp':100,'producer_id':'fixture','host_total_bytes':16*GIB,'host_available_bytes':7*GIB,'docker_total_bytes':8*GIB,'vm_available_bytes':6*GIB,'disk_free_bytes':20*GIB,'active_training_workers':0,'image_id':EXPECTED_IMAGE,'docker_architecture':'x86_64','cgroup_version':'2'}

def test_larger_host_admission_and_current_vm_rejection():
 s=ready();assert admission_reasons(s,now=100)==[]
 s.update(host_total_bytes=8259321856,host_available_bytes=1047891968,docker_total_bytes=3943489536,vm_available_bytes=2549878784)
 assert len(admission_reasons(s,now=100))==4

def test_runtime_reserves_and_heartbeat_fail_closed():
 s=ready();s['host_available_bytes']=3*GIB;assert not admission_reasons(s,startup=False,now=100)
 for field,value in [('timestamp',0),('timestamp',101),('host_available_bytes',GIB),('docker_total_bytes',4*GIB),('disk_free_bytes',GIB),('producer_id',''),('image_id','different'),('docker_architecture','aarch64'),('cgroup_version','1'),('active_training_workers',2)]:
  changed={**ready(),field:value};assert admission_reasons(changed,startup=False,now=100)

def test_4g_limits_do_not_accept_2g_or_extra_swap_or_unbounded_cpu():
 validate_limits(4*GIB,0,200000,100000)
 for memory,swap,quota in [(2*GIB,0,200000),(4*GIB,GIB,200000),(4*GIB,0,-1),(4*GIB,0,300000)]:
  with pytest.raises(ValueError):validate_limits(memory,swap,quota,100000)

def test_phase_memory_events_and_wrong_cap_block_restart():
 good={'status':'complete','correctness_passed':True,'memory':{'memory_limit_bytes':4*GIB,'kernel_container_peak_bytes':GIB,'memory_events':'max 0\noom 0\noom_kill 0\n'}}
 phase_admission(good)
 for key,value in [('memory_limit_bytes',2*GIB),('kernel_container_peak_bytes',4*GIB),('memory_events','max 1\noom 0\noom_kill 0\n')]:
  with pytest.raises(ValueError):phase_admission({**good,'memory':{**good['memory'],key:value}})

def test_restart_preserves_completed_phase_and_refuses_overwrite(tmp_path):
 folder=new_phase(tmp_path,'construct');marker=folder/'checkpoint.fixture';marker.write_bytes(b'preserve')
 with pytest.raises(FileExistsError):new_phase(tmp_path,'construct')
 assert marker.read_bytes()==b'preserve'
 new_phase(tmp_path,'compact/fit');assert marker.exists()

def test_exclusive_worker_and_safe_normal_restart(tmp_path):
 with exclusive_worker(tmp_path):
  with pytest.raises(FileExistsError):
   with exclusive_worker(tmp_path):pass
 with exclusive_worker(tmp_path):pass
 lease=tmp_path/'resources/worker-lease.json';lease.write_text(json.dumps({'token':'stale'}))
 with pytest.raises(FileExistsError):
  with exclusive_worker(tmp_path):pass
 assert json.loads(lease.read_text())['token']=='stale'

def test_resource_rejection_precedes_model_or_phase_creation(tmp_path,monkeypatch):
 from tools import native_4g_worker as worker
 monkeypatch.setattr(worker,'ROOT',tmp_path/'never-created')
 monkeypatch.setattr('sys.argv',['worker','--task','construct'])
 def reject(*args,**kwargs):raise RuntimeError('host admission blocked')
 def forbidden(*args,**kwargs):raise AssertionError('Model allocation before admission')
 monkeypatch.setattr(worker,'runtime_guard',reject);monkeypatch.setattr(worker.lgb,'Dataset',forbidden)
 with pytest.raises(RuntimeError,match='admission blocked'):worker.main()
 assert not worker.ROOT.exists()

def test_profile_is_isolated_and_2g_defaults_remain_unchanged():
 profile=yaml.safe_load(Path('docker-compose.native-4g.yml').read_text());service=profile['services']['trainer']
 assert profile['name']=='citibike-native-4g-validation' and service['profiles']==['native-4g']
 assert service['mem_limit']==service['memswap_limit']=='4g' and service['cpus']==2
 assert service['restart']=='no' and service['network_mode']=='none' and service['pull_policy']=='never'
 assert service['read_only'] and service['volumes'][0]['read_only']
 assert service['volumes'][1]['source']=='./artifacts/native-4g-validation-01'
 default=yaml.safe_load(Path('config/native_network_capacity.yml').read_text());assert default['resources']['memory_gib']==2 and not default['full_network_training_enabled']
 assert yaml.safe_load(Path('config/native_training_4g.yml').read_text())['allowed_stations']==[384]


def test_runtime_guard_reads_actual_cgroup_and_guest_reserves(tmp_path,monkeypatch):
 import time
 from jobs import native_4g_resources as resources
 root=tmp_path/'root';(root/'resources').mkdir(parents=True)
 sample=ready();sample['timestamp']=time.time();(root/'resources/host-memory.json').write_text(json.dumps(sample))
 mapping={
  '/proc/meminfo':tmp_path/'meminfo',
  '/sys/fs/cgroup/memory.max':tmp_path/'memory.max',
  '/sys/fs/cgroup/memory.swap.max':tmp_path/'swap.max',
  '/sys/fs/cgroup/cpu.max':tmp_path/'cpu.max'}
 mapping['/proc/meminfo'].write_text('MemTotal: 8388608 kB\nMemAvailable: 6291456 kB\n')
 mapping['/sys/fs/cgroup/memory.max'].write_text(str(4*GIB));mapping['/sys/fs/cgroup/memory.swap.max'].write_text('0');mapping['/sys/fs/cgroup/cpu.max'].write_text('200000 100000')
 monkeypatch.setattr(resources,'Path',lambda path:mapping.get(str(path),Path(path)))
 resources.runtime_guard(root,startup=True)
 mapping['/proc/meminfo'].write_text('MemTotal: 8388608 kB\nMemAvailable: 100 kB\n')
 with pytest.raises(RuntimeError,match='runtime reserve'):resources.runtime_guard(root)


def test_monitor_and_worker_leases_are_independent(tmp_path):
 with exclusive_worker(tmp_path,lease_name='monitor-lease.json'):
  with exclusive_worker(tmp_path):pass
 assert not (tmp_path/'resources/monitor-lease.json').exists()
