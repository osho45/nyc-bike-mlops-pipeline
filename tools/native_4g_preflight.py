"""Windows physical-RAM admission and heartbeat; never changes WSL or services."""
import argparse,ctypes,json,subprocess,shutil,time,uuid,sys,platform,os
from pathlib import Path
from jobs.native_4g_resources import admission_reasons,exclusive_worker,write_atomic_json as atomic
ROOT=Path('artifacts/native-4g-validation-01')

def host_memory():
    if sys.platform=='linux':
        if 'microsoft' in platform.release().lower():raise RuntimeError('Use native Windows Python for Docker Desktop; WSL guest RAM is not Windows physical RAM')
        values=dict(line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())
        return int(values['MemTotal'].split()[0])*1024,int(values['MemAvailable'].split()[0])*1024
    if sys.platform!='win32':raise RuntimeError('Supported host controllers: native Windows or native Linux')
    class Status(ctypes.Structure):
        _fields_=[('length',ctypes.c_ulong),('load',ctypes.c_ulong)]+[(x,ctypes.c_ulonglong) for x in ['total','available','page_total','page_available','virtual_total','virtual_available','extended']]
    s=Status();s.length=ctypes.sizeof(s)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(s)):raise OSError('GlobalMemoryStatusEx failed')
    return s.total,s.available

def docker(*args):
    result=subprocess.run(['docker',*args],capture_output=True,text=True,check=True,timeout=15)
    return result.stdout.strip()

def collect(producer_id,sequence,probe=None):
    endpoint=os.environ.get('DOCKER_HOST') or docker('context','inspect','--format','{{.Endpoints.docker.Host}}')
    if not endpoint.startswith(('unix://','npipe:////./pipe/')):raise RuntimeError('Run the memory controller on the daemon host; remote Docker contexts mix different hosts')
    total,available=host_memory();vm_available=None
    if probe:
        info=docker('exec',probe,'cat','/proc/meminfo')
        values=dict(line.split(':',1) for line in info.splitlines());vm_available=int(values['MemAvailable'].split()[0])*1024
    active=docker('ps','--filter','label=com.citibike.native-training=4g','--format','{{.Names}}').splitlines()
    return {'timestamp':time.time(),'producer_id':producer_id,'sequence':sequence,'host_platform':sys.platform,'docker_endpoint':endpoint,'docker_architecture':docker('info','--format','{{.Architecture}}'),'cgroup_version':docker('info','--format','{{.CgroupVersion}}'),'host_total_bytes':total,'host_available_bytes':available,'image_id':docker('image','inspect','citibike-airflow-spark-iceberg:3.2.2-python3.11','--format','{{.Id}}'),'docker_total_bytes':int(docker('info','--format','{{.MemTotal}}')),'vm_available_bytes':vm_available,'disk_free_bytes':shutil.disk_usage(Path.cwd()).free,'active_training_workers':len(active)}

def main():
    p=argparse.ArgumentParser();p.add_argument('--probe-container');p.add_argument('--monitor',action='store_true');p.add_argument('--samples',type=int);p.add_argument('--interval',type=float,default=5);a=p.parse_args()
    if not 1<=a.interval<=10 or (a.samples is not None and a.samples<1):raise ValueError('Require 1-10 second sampling and positive sample count')
    root=ROOT/'resources';root.mkdir(parents=True,exist_ok=True);producer=uuid.uuid4().hex;sequence=0
    with exclusive_worker(ROOT,lease_name="monitor-lease.json"):
        while True:
            sample=collect(producer,sequence,a.probe_container);reasons=admission_reasons(sample,startup=True)
            atomic(root/'host-memory.json',sample)
            with (root/'host-memory.jsonl').open('a') as f:f.write(json.dumps(sample)+'\n')
            report={'status':'ready' if not reasons else 'blocked','sample':sample,'startup_blockers':reasons,'worker_launched':False,'full_network_training_enabled':False}
            atomic(ROOT/'preflight.json',report)
            if not a.monitor:
                print(json.dumps(report,indent=2));return 0 if not reasons else 2
            print(json.dumps({'producer_id':producer,'sequence':sequence,'status':report['status']}),flush=True)
            sequence+=1
            if a.samples is not None and sequence>=a.samples:return 0
            time.sleep(a.interval)

if __name__=='__main__':raise SystemExit(main())
