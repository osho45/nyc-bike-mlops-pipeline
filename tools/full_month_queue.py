"""Validation-only file transport to a separately memory-capped Spark worker.

Airflow still invokes run_asset_stage and publishes its real asset outlets. Only
subprocess transport changes; no Docker socket is exposed to the scheduler.
"""
import argparse,hashlib,json,os,signal,subprocess,sys,threading,time
from pathlib import Path
from types import SimpleNamespace
from jobs.ml_utils import load_config,project_root
from tools.full_month_validate import MemoryMonitor

MODULES={'jobs.bronze_ingest','jobs.silver_wap','jobs.gold_bi','jobs.gold_ml'}

def isolated_config(root):
    root=Path(root).resolve()
    if not root.is_relative_to((project_root()/'artifacts').resolve()):raise ValueError('Validation root outside artifacts')
    cfg=load_config(root/'config.yml')
    permitted=cfg['catalog']['spark_catalog_name']=='citibike_fullmonth_events' or (cfg['catalog']['spark_catalog_name']=='citibike_monthly' and cfg.get('ingestion',{}).get('automated_monthly') is True)
    if (not permitted
        or Path(cfg['catalog']['warehouse_path']).resolve()!=root/'warehouse'
        or cfg.get('validation',{}).get('sample_mode')
        or cfg['spark'].get('spark.master')!='local[1]'
        or cfg['spark'].get('spark.driver.memory')!='1g'):
        raise ValueError('Isolated full-month event profile required')
    return cfg

def validate_command(root,command):
    cfg=isolated_config(root)
    modules=MODULES|({'tools.monthly_audit','tools.monthly_adopt','tools.year_audit'} if cfg.get('ingestion',{}).get('automated_monthly') else set())
    if len(command)<5 or command[1]!='-m' or command[2] not in modules:raise ValueError('Only medallion ETL/audit jobs are permitted')
    values=command[3:]
    if len(values)%2:raise ValueError('Expected flag/value arguments')
    args=dict(zip(values[::2],values[1::2]))
    allowed={'--config','--source_filepath','--source_filename','--source_year','--source_month','--run_id','--ingestion_run_id','--target_year','--target_month','--year','--month','--source_revision','--expected_checksum','--supersedes_sha256'}
    if len(args)*2!=len(values) or not set(args)<=allowed or Path(args.get('--config','')).resolve()!=root/'config.yml':raise ValueError('Invalid ETL arguments')
    if '--source_filepath' in args and not Path(args['--source_filepath']).resolve().is_relative_to(root/'incoming'):raise ValueError('Source outside isolated incoming directory')
    return [sys.executable,*command[1:]]

class FileQueueRunner:
    def __init__(self,root,stage,run_id,attempt):self.root=Path(root);self.stage=stage;self.run_id=run_id;self.attempt=attempt
    def __call__(self,command,**kwargs):
        validate_command(self.root,command)
        key=hashlib.sha256(json.dumps([self.stage,self.run_id,self.attempt,command]).encode()).hexdigest()[:24]
        queue=self.root/'queue';queue.mkdir(exist_ok=True)
        request=queue/(key+'.request.json');response=queue/(key+'.result.json')
        if not request.exists():
            temp=request.with_suffix('.tmp');temp.write_text(json.dumps({'key':key,'stage':self.stage,'run_id':self.run_id,'attempt':self.attempt,'command':command,'submitted_at':time.time()}));temp.replace(request)
        while not response.exists():
            if (self.root/'worker-fatal.json').exists():raise RuntimeError('Isolated Spark worker failed; inspect worker-fatal.json')
            time.sleep(1)
        result=json.loads(response.read_text())
        return SimpleNamespace(**{k:result[k] for k in ['returncode','stdout','stderr']})

def preserve_worker_attempt(root,resume):
    root=Path(root).resolve()
    if not root.is_relative_to((project_root()/'artifacts').resolve()):raise ValueError('Worker state outside artifacts')
    previous=[root/name for name in ['worker-memory.csv','worker-summary.json','worker-fatal.json','stop-worker'] if (root/name).exists()]
    if previous and not resume:raise ValueError('Existing worker state preserved; use --resume')
    if previous:
        archive=root/'worker-attempts'/str(time.time_ns());archive.mkdir(parents=True)
        for path in previous:path.rename(archive/path.name)
        for path in (root/'queue').glob('*.memory.csv'):
            if not path.with_name(path.name.removesuffix('.memory.csv')+'.result.json').exists():path.rename(archive/path.name)

def worker(root,resume=False):
    root=Path(root).resolve();isolated_config(root)
    if int(Path('/sys/fs/cgroup/memory.max').read_text())!=2*1024**3:raise ValueError('Worker requires the verified 2 GiB hard limit')
    preserve_worker_attempt(root,resume)
    queue=root/'queue';queue.mkdir(exist_ok=True)
    monitor=MemoryMonitor(root/'worker-memory.csv');thread=threading.Thread(target=monitor.run,daemon=True);thread.start();started=time.time();completed=[]
    try:
        while not (root/'stop-worker').exists():
            requests=sorted(queue.glob('*.request.json'),key=lambda p:p.stat().st_mtime)
            for path in requests:
                response=path.with_name(path.name.replace('.request.json','.result.json'))
                if response.exists():continue
                request=json.loads(path.read_text());command=validate_command(root,request['command'])
                print('BEGIN '+request['stage']+' '+request['key'],flush=True);t=time.monotonic()
                job_monitor=MemoryMonitor(queue/(request['key']+'.memory.csv'));job_thread=threading.Thread(target=job_monitor.run,daemon=True);job_thread.start()
                try:
                    process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
                    while True:
                        try:
                            stdout,stderr=process.communicate(timeout=5);break
                        except subprocess.TimeoutExpired:
                            cfg=isolated_config(root)
                            if cfg.get('ingestion',{}).get('enforce_resource_checks'):
                                from tools.ingestion_resources import check,ResourceStop
                                try:check(root)
                                except ResourceStop as error:
                                    (root/'resource-stop.json').write_text(json.dumps({'reason':str(error),'job':request['key']}))
                                    os.killpg(process.pid,signal.SIGTERM)
                                    try:stdout,stderr=process.communicate(timeout=15)
                                    except subprocess.TimeoutExpired:
                                        os.killpg(process.pid,signal.SIGKILL);stdout,stderr=process.communicate()
                                    stderr+='\nControlled resource stop: '+str(error);break
                    result=SimpleNamespace(returncode=process.returncode,stdout=stdout,stderr=stderr)
                finally:job_monitor.stop.set();job_thread.join(timeout=3)
                record={**request,'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr,'processing_seconds':round(time.monotonic()-t,3),'completed_at':time.time(),'memory':job_monitor.finish(),'memory_scope':'sampled peaks per job; kernel peak/events cumulative worker cgroup'}
                if result.returncode in [137,-9] or 'OutOfMemoryError' in result.stderr:
                    (root/'resource-stop.json').write_text(json.dumps({'reason':'Spark worker memory failure','job':request['key'],'returncode':result.returncode}))
                temp=response.with_suffix('.tmp');temp.write_text(json.dumps(record,indent=2));temp.replace(response);completed.append(request['key'])
                print('END '+request['stage']+' '+str(result.returncode)+' '+str(record['processing_seconds']),flush=True)
            time.sleep(.5)
    except Exception as error:
        (root/'worker-fatal.json').write_text(json.dumps({'error':repr(error)}));raise
    finally:
        monitor.stop.set();thread.join(timeout=3)
        (root/'worker-summary.json').write_text(json.dumps({'started_at':started,'finished_at':time.time(),'completed_jobs':completed,'memory':monitor.finish()},indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--root',required=True);parser.add_argument('--resume',action='store_true');args=parser.parse_args();worker(args.root,args.resume)
