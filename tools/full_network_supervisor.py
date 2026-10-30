"""Persistent local full-cohort supervisor with actual-RAM admission, not a 4 GiB requirement."""
import argparse,ctypes,json,os,shutil,subprocess,time,uuid,sys
from pathlib import Path
from tools.native_4g_preflight import host_memory
from tools.training_transfer import sha
from jobs.native_4g_resources import EXPECTED_IMAGE

ROOT=Path('artifacts/full-network-native-01')
CODE=['tools/full_network_supervisor.py','tools/full_network_native_worker.py','jobs/full_native_data.py','jobs/native_lgbm.py','jobs/native_streaming.py','jobs/native_fit_memory.py','jobs/scaling.py','jobs/ml_features.py','tools/native_network_capacity.py','tools/native_4g_preflight.py','jobs/native_4g_resources.py','config/full_network_native_authorized.json']
CODE+=['tools/bounded_ml_readiness.py','tools/training_transfer.py']
GIB=1024**3

def atomic(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
    # Windows readers may briefly deny FILE_SHARE_DELETE on the bind mount.
    # Retry only this sharing error, bounded well below the 30-second heartbeat.
    for retry in range(41):
        try:tmp.replace(path);return
        except PermissionError:
            if retry==40:raise
            time.sleep(.025)

def docker(*args):
    result=subprocess.run(['docker',*args],capture_output=True,text=True,timeout=30)
    if result.returncode:raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()

def resources():
    total,available=host_memory();info=docker('exec','citibike_controlled_worker_02','cat','/proc/meminfo');fields=dict(line.split(':',1) for line in info.splitlines())
    return {'timestamp':time.time(),'host_total_bytes':total,'host_available_bytes':available,'vm_total_bytes':int(fields['MemTotal'].split()[0])*1024,'vm_available_bytes':int(fields['MemAvailable'].split()[0])*1024,'vm_swap_used_bytes':(int(fields['SwapTotal'].split()[0])-int(fields['SwapFree'].split()[0]))*1024,'disk_free_bytes':shutil.disk_usage(Path.cwd()).free,'producer_pid':os.getpid()}


def heartbeat(sample):
    atomic(ROOT/'resources.json',sample)
    with (ROOT/'resources.jsonl').open('a') as log:log.write(json.dumps(sample)+'\n')

def admission(data,cap_bytes):
    reasons=[]
    reserve=GIB//4 if cap_bytes==384*1024**2 else GIB
    if data['host_available_bytes']<cap_bytes+reserve:reasons.append(f"Windows/host free RAM {data['host_available_bytes']} < worker cap plus phase reserve {cap_bytes+reserve}")
    if data['vm_available_bytes']<cap_bytes+GIB//2:reasons.append('Guest free/reclaimable RAM below worker cap plus 512 MiB reserve')
    if data['disk_free_bytes']<16*GIB:reasons.append('Disk free below 16 GiB startup requirement (includes 10 GiB reserve and remaining outputs)')
    return reasons

def runtime_reasons(data,cap_bytes=None):
    reserve=GIB//4 if cap_bytes==384*1024**2 else GIB
    return [name for name,bad in [(f'host physical RAM reserve <{reserve} bytes',data['host_available_bytes']<reserve),('guest RAM reserve <512 MiB',data['vm_available_bytes']<GIB//2),('disk reserve <10 GiB',data['disk_free_bytes']<10*GIB)] if bad]

def schedule():
    result=[]
    for year in [2024,2025]:
        for month in range(1,13):result.append((f'raw-{year}-{month:02d}',384,['--task','raw','--year',str(year),'--month',str(month)]))
    for first in range(0,1898,32):result.append((f'features-{first:04d}',768,['--task','features','--first',str(first),'--last',str(min(first+32,1898))]))
    result.extend([(name,1536 if name in ['construct','fit'] else 768,['--task',name]) for name in ['seal','construct','fit','evaluate']]);return result

def pid_alive(pid):
    if os.name!='nt':return Path(f'/proc/{pid}').exists()
    kernel=ctypes.windll.kernel32;kernel.OpenProcess.restype=ctypes.c_void_p;handle=kernel.OpenProcess(0x1000,False,int(pid))
    if not handle:return kernel.GetLastError()==5
    try:
        code=ctypes.c_ulong();kernel.GetExitCodeProcess(ctypes.c_void_p(handle),ctypes.byref(code));return code.value==259
    finally:kernel.CloseHandle(ctypes.c_void_p(handle))

def preserve_contract():
    cohort=Path('artifacts/network-scaling-prep-02/stage_plans/all_eligible.json');transfer=Path('artifacts/training-transfer-01/manifest.json')
    config=json.loads(Path('config/full_network_native_authorized.json').read_text());assert config['training_enabled'] and config['stations']==1898 and config['allowed_years']==[2024,2025]
    contract={'code_sha256':{name:sha(name) for name in CODE},'cohort_sha256':sha(cohort),'transfer_sha256':sha(transfer),'image_id':EXPECTED_IMAGE,'authorization':config}
    path=ROOT/'source_contract.json'
    if path.exists():
        if json.loads(path.read_text())!=contract:raise RuntimeError('Source/config changed; preserve old run and review before any resumption')
    else:
        if any(ROOT.glob('jobs/*')):raise RuntimeError('Existing phase outputs without a source contract')
        source=ROOT/'source';source.mkdir(exist_ok=False)
        for name in CODE:shutil.copyfile(name,source/Path(name).name)
        atomic(path,contract)
        old=json.loads(Path('artifacts/training-transfer-01/protected_sha256.json').read_text());atomic(ROOT/'protected_sha256.json',old)
    return contract

def phase_result(key):
    marker=ROOT/'jobs'/key/'done.json'
    if not marker.exists():return None
    folder=ROOT/json.loads(marker.read_text())['path'];result=json.loads((folder/'result.json').read_text());memory=result['memory'];events=dict(line.split() for line in memory['memory_events'].splitlines())
    if result['status']!='complete' or not result['correctness_passed'] or memory['kernel_container_peak_bytes']>.85*memory['memory_limit_bytes'] or any(int(events[k]) for k in ['max','oom','oom_kill']):raise RuntimeError('Completed phase admission failed')
    return result

def status(state,**extra):
    complete=sum(1 for _ in (ROOT/'stations').glob('*/done.json')) if (ROOT/'stations').exists() else 0
    value={'state':state,'timestamp':time.time(),'pid':os.getpid(),'stations_completed':complete,'stations_total':1898,'read_2026':False,**extra};atomic(ROOT/'status.json',value);print(json.dumps(value),flush=True)

def finalize(started):
    results={key:phase_result(key) for key,_,_ in schedule()};assert all(results.values())
    evaluation=json.loads((ROOT/'evaluation/done.json').read_text());fit=json.loads((ROOT/'fit/done.json').read_text());protected=json.loads((ROOT/'protected_sha256.json').read_text())
    for path,expected in protected.items():
        if sha(path)!=expected:raise RuntimeError('Protected existing artifact changed')
    report={'status':'complete','stations':1898,'training_year':2024,'validation_year':2025,'metrics':evaluation['metrics'],'model_sha256':fit['model_sha256'],'fit_seconds':fit['fit_seconds'],'phase_work_seconds':sum(r['seconds'] for r in results.values()),'wall_seconds_including_resource_wait':time.time()-started,'peak_container_bytes':max(r['memory']['kernel_container_peak_bytes'] for r in results.values()),'protected_artifacts_unchanged':True,'prediction_artifacts':evaluation['path'],'source_contract_sha256':sha(ROOT/'source_contract.json'),'per_phase_results':results,'operational_policy_changed':False,'read_2026':False,'deployment':False}
    atomic(ROOT/'final_report.json',report);(ROOT/'final_report.md').write_text(f"# Full-network native training\n\nStations: 1,898. Model fit: {fit['fit_seconds']:.3f} seconds.\n\nActual metrics:\n\n```json\n{json.dumps(evaluation['metrics'],indent=2)}\n```\n\nPeak container bytes: {report['peak_container_bytes']}. All original protected artifacts preserved. No 2026 access or deployment.\n")
    status('complete',metrics=evaluation['metrics'])

def main():
    p=argparse.ArgumentParser();p.add_argument('--resume',action='store_true');p.add_argument('--fail-fast',action='store_true',help='Report insufficient admission and exit instead of waiting indefinitely');a=p.parse_args();ROOT.mkdir(parents=True,exist_ok=True)
    if (ROOT/'resource-stop.json').exists():raise RuntimeError('Resource-stop marker requires investigation; no automatic OOM retry')
    lease=ROOT/'controller-lease.json'
    if lease.exists():
        old=json.loads(lease.read_text())
        if pid_alive(old['pid']):raise RuntimeError('Existing supervisor is alive; do not start another')
        if not a.resume:raise RuntimeError('Use --resume after checking the interrupted run')
        lease.unlink()
    token=uuid.uuid4().hex
    with lease.open('x') as f:json.dump({'pid':os.getpid(),'token':token},f)
    started=time.time()
    try:
        preserve_contract();endpoint=os.environ.get('DOCKER_HOST') or docker('context','inspect','--format','{{.Endpoints.docker.Host}}')
        if not endpoint.startswith(('unix://','npipe:////./pipe/')):raise RuntimeError('Local daemon required for physical RAM monitoring')
        if docker('image','inspect','citibike-airflow-spark-iceberg:3.2.2-python3.11','--format','{{.Id}}')!=EXPECTED_IMAGE:raise RuntimeError('Image identity mismatch')
        for key,mib,args in schedule():
            if phase_result(key):continue
            while True:
                sample=resources();heartbeat(sample);reasons=admission(sample,mib*1024**2)
                if not reasons:break
                if a.fail_fast:
                    status('blocked_resources',next_phase=key,reasons=reasons,resources=sample,worker_cap_mib=mib);return
                status('waiting_for_resources',next_phase=key,reasons=reasons,resources=sample);time.sleep(30)
            attempt=1
            while True:
                name=f'citibike_full_native_{key}_{attempt:03d}'
                inspection=subprocess.run(['docker','inspect','--format','{{json .State}}',name],capture_output=True,text=True)
                if inspection.returncode:break
                state=json.loads(inspection.stdout)
                if state['Running']:raise RuntimeError('Prior owned container still running; inspect it before resuming')
                if state.get('OOMKilled') or state['ExitCode']==75:raise RuntimeError('Prior resource/OOM failure requires investigation')
                attempt+=1
            repo=str(Path.cwd().resolve());docker('run','-d','--name',name,'--label','com.citibike.full-native=1','--network','none','--cpus','2',f'--memory={mib}m',f'--memory-swap={mib}m','--read-only','--tmpfs','/tmp:rw,nosuid,nodev,size=67108864','--mount',f'type=bind,source={repo},target=/opt/airflow,readonly','--mount',f'type=bind,source={str(ROOT.resolve())},target=/opt/airflow/artifacts/full-network-native-01','--workdir','/opt/airflow','--entrypoint','python','-e','PYTHONPATH=/opt/airflow','-e','PYTHONDONTWRITEBYTECODE=1','-e','OMP_NUM_THREADS=2','-e','OPENBLAS_NUM_THREADS=2','citibike-airflow-spark-iceberg:3.2.2-python3.11','-u','-m','tools.full_network_native_worker',*args,'--key',key)
            status('running',phase=key,container=name,cap_mib=mib)
            while True:
                sample=resources();heartbeat(sample);state=json.loads(docker('inspect','--format','{{json .State}}',name))
                if not state['Running']:break
                bad=runtime_reasons(sample,mib*1024**2)
                if bad:
                    atomic(ROOT/'resource-stop.json',{'phase':key,'reasons':bad,'resources':sample});docker('stop','-t','5',name);raise RuntimeError('Runtime resource reserve breached; owned worker stopped')
                time.sleep(2)
            logs=subprocess.run(['docker','logs',name],capture_output=True,text=True);(ROOT/f'{key}-{attempt:03d}.log').write_text(logs.stdout+'\n'+logs.stderr)
            atomic(ROOT/'jobs'/key/f'container-{attempt:03d}.json',state)
            if state['ExitCode']!=0 or state['OOMKilled'] or not phase_result(key):
                atomic(ROOT/'resource-stop.json',{'phase':key,'container':name,'state':state,'reason':'worker failed; do not automatically retry'});raise RuntimeError(f'Phase {key} failed; outputs preserved')
        finalize(started)
    except BaseException as error:
        status('stopped',error=repr(error));raise
    finally:
        if lease.exists() and json.loads(lease.read_text()).get('token')==token:lease.unlink()

if __name__=='__main__':main()
