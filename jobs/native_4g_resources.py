"""Fail-closed resource admission for the isolated, opt-in 4 GiB worker."""
import json,time,math,uuid
from contextlib import contextmanager
from pathlib import Path

GIB=1024**3
EXPECTED_IMAGE="sha256:809353853ccd99ce12d5a4f359780f025f4932ac46fce3f065c8f57477c85c18"

def admission_reasons(sample,startup=True,now=None):
    now=time.time() if now is None else now
    reasons=[]
    stamp=sample.get('timestamp')
    if not isinstance(stamp,(int,float)) or not math.isfinite(stamp) or not 0<=now-stamp<=30:reasons.append('host resource heartbeat missing, stale, or from the future')
    required={'host_total_bytes':12*GIB,'host_available_bytes':6*GIB if startup else 2*GIB,'docker_total_bytes':6*GIB,'disk_free_bytes':10*GIB}
    if startup:required['vm_available_bytes']=5*GIB
    for name,minimum in required.items():
        value=sample.get(name)
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or value<minimum:reasons.append(f'{name} below required {minimum} bytes (observed {value})')
    if sample.get('docker_architecture') not in ['x86_64','amd64']:reasons.append('native amd64 Docker execution required; emulation is not validated')
    if str(sample.get('cgroup_version'))!='2':reasons.append('cgroup v2 is required')
    if sample.get('image_id')!=EXPECTED_IMAGE:reasons.append('verified local LightGBM image identity differs or is unavailable')
    if not sample.get('producer_id'):reasons.append('resource producer identity missing')
    if sample.get('active_training_workers',0)>1:reasons.append('more than one training worker is active')
    return reasons

def validate_limits(memory,swap,cpu_quota,cpu_period):
    if memory!=4*GIB or swap!=0 or cpu_quota<=0 or cpu_period<=0 or cpu_quota>2*cpu_period:raise ValueError('Require exactly 4 GiB, zero extra swap, and at most two CPUs')

def runtime_guard(root,startup=False):
    root=Path(root)
    if (root/'resource-stop.json').exists():raise RuntimeError('4 GiB resource-stop marker exists')
    sample=json.loads((root/'resources/host-memory.json').read_text())
    stats=dict(line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())
    available=int(stats['MemAvailable'].split()[0])*1024
    sample={**sample,'vm_available_bytes':available,'docker_total_bytes':int(stats['MemTotal'].split()[0])*1024}
    reasons=admission_reasons(sample,startup=startup)
    if available<GIB:reasons.append('Docker/WSL available memory below 1 GiB runtime reserve')
    quota,period=map(int,Path('/sys/fs/cgroup/cpu.max').read_text().split())
    validate_limits(int(Path('/sys/fs/cgroup/memory.max').read_text()),int(Path('/sys/fs/cgroup/memory.swap.max').read_text()),quota,period)
    if reasons:raise RuntimeError('; '.join(reasons))

def phase_admission(result):
    memory=result['memory']
    if result.get('status')!='complete' or not result.get('correctness_passed'):raise ValueError('Incomplete or failed 4 GiB phase')
    if memory['memory_limit_bytes']!=4*GIB or memory['kernel_container_peak_bytes']>.85*4*GIB:raise ValueError('4 GiB memory admission failed')
    events=dict(line.split() for line in memory['memory_events'].splitlines())
    if any(int(events[k]) for k in ['max','oom','oom_kill']):raise ValueError('Memory-limit event blocks restart/next phase')

def new_phase(root,name):
    folder=Path(root)/name;folder.mkdir(parents=True,exist_ok=False)
    return folder


@contextmanager
def exclusive_worker(root,lease_name="worker-lease.json"):
    path=Path(root)/'resources'/lease_name;path.parent.mkdir(parents=True,exist_ok=True)
    token=uuid.uuid4().hex
    with path.open('x') as f:json.dump({'token':token,'timestamp':time.time()},f)
    try:yield
    finally:
        if path.exists() and json.loads(path.read_text()).get('token')==token:path.unlink()


def write_atomic_json(path,value):
    """Stdlib-only heartbeat writer so the host controller needs no ML packages."""
    path=Path(path);temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
    temporary.replace(path)
