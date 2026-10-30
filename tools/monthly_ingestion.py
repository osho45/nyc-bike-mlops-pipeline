"""Sequential official-month controller. Default action validates ONE missing month."""
import argparse,json,os,runpy,time,uuid
from pathlib import Path
import yaml

ROOT=Path('/opt/airflow/artifacts/monthly-ingestion-01')
URI='local://citibike/monthly-01/'
PREFIX='monthly_citibike_'
STAGES=['bronze','silver','bi','ml']
SESSION=None

def api(method,path,**kwargs):
    global SESSION
    import requests
    from tools.asset_validation_control import session,call
    for attempt in range(4):
        if SESSION is None:SESSION=session()
        try:return call(SESSION,method,path,**kwargs)
        except requests.RequestException as error:
            if error.response is not None and error.response.status_code==401:SESSION=None
            elif error.response is not None and error.response.status_code not in [408,429,500,502,503,504]:raise
            if attempt==3:raise
            time.sleep(2**attempt)

def prepare():
    from jobs.ml_utils import load_config
    if ROOT.exists():raise FileExistsError('Existing monthly state preserved; resume without prepare')
    ROOT.mkdir();(ROOT/'incoming').mkdir();(ROOT/'queue').mkdir();(ROOT/'checkpoints').mkdir()
    cfg=load_config('/opt/airflow/config/full_month_validation.yml')
    cfg['catalog'].update(spark_catalog_name='citibike_monthly',warehouse_path=str(ROOT/'warehouse'))
    cfg['storage'].update(incoming_directory=str(ROOT/'incoming'),warehouse_directory=str(ROOT/'warehouse'))
    cfg['assets']={key:URI+stage for key,stage in [('incoming_citibike_data_uri','incoming'),('bronze_citibike_data_uri','bronze'),('silver_citibike_data_uri','silver')]}
    cfg['ml'].update(models_dir=str(ROOT/'unused-models'),predictions_dir=str(ROOT/'unused-predictions'),split_policy='calendar_year')
    cfg['ingestion']={'automated_monthly':True,'archive_cache':'/opt/airflow/data/citibike/monthly/archives','reuse_directories':['/opt/airflow/data/citibike/full_month_validation/incoming'],'pilot_month':'2024-02','split_years':{'train':2024,'validation':2025,'test':2026},'history_warehouse':'/opt/airflow/artifacts/full-month-202401-01/warehouse'}
    (ROOT/'config.yml').write_text(yaml.safe_dump(cfg));print('Prepared isolated monthly controller; no downloads/events started')

def inventory():
    from jobs.monthly_archives import available_months,atomic_json
    items=available_months();keys={(x['year'],x['month']) for x in items};missing=[f'{year}-{month:02d}' for year in [2024,2025] for month in range(1,13) if (year,month) not in keys]
    atomic_json(ROOT/'plan.json',{'observed_at':time.time(),'months':items,'total_source_bytes':sum(x['bytes'] for x in items),'missing_required_months':missing,'execution_default_limit':1})
    return items

def watcher():
    password=json.loads(Path('/opt/airflow/auth/simple_auth_manager_passwords.json.generated').read_text())['admin']
    os.environ.update(AIRFLOW_BASE_URL='http://airflow-webserver:8080',AIRFLOW_USERNAME='admin',AIRFLOW_PASSWORD=password,WATCH_DIRECTORY=str(ROOT/'incoming'),TARGET_ASSET_URI=URI+'incoming',CONSUMER_DAG_IDS=','.join(PREFIX+s for s in STAGES))
    runpy.run_module('sftp_mock_daemon',run_name='__main__')

def activate(paused=False):
    for dag in ['1_bronze_ingestion','2_silver_wap_purification','3a_gold_bi_marts','3b_gold_ml_observations','4_ml_batch_demand_score']:
        if api('GET','/dags/'+dag)['is_paused'] is not True:raise ValueError('Keep original ingestion/scoring paused for this isolated run')
    if next(p for p in api('GET','/pools')['pools'] if p['name']=='citibike_spark')['slots']!=1:raise ValueError('Single Spark slot required')
    for stage in STAGES:api('PATCH','/dags/'+PREFIX+stage,json={'is_paused':paused})

def snapshots():
    result={}
    for name in ['bronze/citibike_rides_raw','silver/citibike_rides','quarantine/citibike_rides','gold/station_hourly_utilization','gold/station_hourly_observations','audit/ingestion_ledger']:
        directory=ROOT/'warehouse'/name/'metadata';hint=directory/'version-hint.text'
        if not hint.exists():continue
        meta=json.loads((directory/('v'+hint.read_text().strip()+'.metadata.json')).read_text());sid=meta.get('current-snapshot-id');current=next((s for s in meta['snapshots'] if s['snapshot-id']==sid),None)
        result[name]={'table_uuid':meta['table-uuid'],'snapshot_id':sid,'rows':int(current['summary']['total-records']) if current else 0}
    return result

def obtain(year,month,cfg):
    from jobs.monthly_archives import download_month,verified_local,atomic_json
    import shutil
    cache=Path(cfg['ingestion']['archive_cache']);cache.mkdir(parents=True,exist_ok=True);name=f'{year}{month:02d}-citibike-tripdata.zip';target=cache/name
    if not target.exists() or not target.with_suffix('.zip.manifest.json').exists():
        for directory in cfg['ingestion']['reuse_directories']:
            existing=Path(directory)/name
            if not existing.exists():continue
            meta=verified_local(existing,year,month)
            if target.exists():
                from jobs.citibike import checksum
                if checksum(target)!=meta['sha256']:raise ValueError('Incomplete reuse copy differs; preserved for review')
            else:
                try:os.link(existing,target)
                except OSError:shutil.copy2(existing,target)
            atomic_json(target.with_suffix('.zip.manifest.json'),{**meta,'source_filepath':str(target),'reused_from':str(existing)})
            break
    guard=None
    if cfg['ingestion'].get('enforce_resource_checks'):
        from tools.ingestion_resources import check
        guard=lambda:check(ROOT)
    return download_month(year,month,cache,max_mb=1536,resume=True,reuse=True,resource_check=guard)

def execute_month(item):
    from jobs.monthly_archives import atomic_json
    from tools.full_month_queue import isolated_config,FileQueueRunner
    from jobs.source_versions import month_lock
    cfg=isolated_config(ROOT);year,month=item['year'],item['month'];key=f'{year}-{month:02d}';path=ROOT/'checkpoints'/(key+'.json')
    checkpoint=json.loads(path.read_text()) if path.exists() else {'year':year,'month':month,'split':item['split'],'started_at':time.time(),'phase':'pending'}
    with month_lock(ROOT/'controller-locks',year,month):
        if checkpoint.get('verified'):
            tables=snapshots()
            if any(name not in tables or tables[name]['table_uuid']!=state['table_uuid'] for name,state in checkpoint['tables'].items()):raise ValueError('Verified checkpoint tables missing/replaced; operator recovery required')
            print('CHECKPOINT_REUSED '+key,flush=True);return checkpoint
        try:
            if cfg['ingestion'].get('enforce_resource_checks'):
                from tools.ingestion_resources import check
                check(ROOT,item['bytes'])
            if checkpoint.get('error') and checkpoint.get('delivery_id'):
                checkpoint.setdefault('previous_deliveries',[]).append({k:checkpoint.get(k) for k in ['delivery_id','phase','error','failed_at']})
                checkpoint['delivery_id']=str(uuid.uuid4());checkpoint.pop('error',None)
            meta=obtain(year,month,cfg);checkpoint['source']=meta;checkpoint['phase']='downloaded';atomic_json(path,checkpoint)
            # The cache is outside WATCH_DIRECTORY. Stage only after CRC/header/SHA verification.
            target=ROOT/'incoming'/meta['source_filename']
            if not target.exists():
                import shutil
                try:os.link(meta['source_filepath'],target)
                except OSError:shutil.copy2(meta['source_filepath'],target)
            else:
                from jobs.citibike import checksum
                if checksum(target)!=meta['sha256']:raise ValueError('Staged archive differs from verified cache; preserved')
            delivery=checkpoint.setdefault('delivery_id',str(uuid.uuid4()));checkpoint['phase']='submitted';atomic_json(path,checkpoint)
            assets={a['uri'].rsplit('/',1)[-1]:a for a in api('GET','/assets?limit=100')['assets'] if a['uri'].startswith(URI)}
            if set(assets)!=set(['incoming',*STAGES]):raise ValueError('Monthly asset registry incomplete')
            def events(stage):return api('GET',f"/assets/events?asset_id={assets[stage]['id']}&limit=100")['asset_events']
            if not any(e['extra'].get('monthly_delivery_id')==delivery for e in events('incoming')):
                atomic_json(target.with_suffix('.zip.manifest.json'),{**meta,'source_filepath':str(target),'monthly_delivery_id':delivery,'data_split':item['split']})
            while True:
                if cfg['ingestion'].get('enforce_resource_checks'):check(ROOT)
                matched={stage:[e for e in events(stage) if e['extra'].get('monthly_delivery_id')==delivery] for stage in ['incoming',*STAGES]}
                runs=[r for stage in ['incoming','bronze','silver'] for e in matched[stage] for r in e['created_dagruns']]
                if any(r['state']=='failed' for r in runs):raise RuntimeError('Airflow monthly run failed; source/checkpoints retained')
                if matched['bi'] and matched['ml'] and len(runs)>=4 and all(r['state']=='success' for r in runs):break
                print('WAIT '+key+' '+str({stage:len(rows) for stage,rows in matched.items()}),flush=True);time.sleep(5)
            checkpoint['phase']='auditing';checkpoint['airflow_events']=matched;atomic_json(path,checkpoint)
            result=FileQueueRunner(ROOT,'audit',delivery,1)(['python','-m','tools.monthly_audit','--config',str(ROOT/'config.yml'),'--year',str(year),'--month',str(month)])
            if result.returncode:raise RuntimeError('Monthly audit failed: '+result.stderr[-3000:])
            audit=json.loads((ROOT/'checkpoints'/(key+'.audit.json')).read_text())
            if not audit['verified'] or audit['source_sha256']!=meta['sha256']:raise ValueError('Monthly audit source mismatch')
            checkpoint.update(phase='verified',verified=True,completed_at=time.time(),audit=audit,tables=snapshots());atomic_json(path,checkpoint)
            print('VERIFIED '+key+' '+str(audit['metrics']),flush=True);return checkpoint
        except Exception as error:
            checkpoint.update(error=repr(error),failed_at=time.time());atomic_json(path,checkpoint);raise

def choose_missing(items):
    from tools.full_month_queue import isolated_config
    from jobs.monthly_archives import verified_local
    cfg=isolated_config(ROOT);directories=[cfg['ingestion']['archive_cache'],*cfg['ingestion']['reuse_directories']]
    for item in items:
        candidates=[Path(d)/item['key'] for d in directories]
        found=next((path for path in candidates if path.exists()),None)
        if found is None:return item
        verified_local(found,item['year'],item['month'])
    raise ValueError('No missing available archive; specify --month for cached-source validation')

def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','plan','pilot','run','watcher','pause','status']);p.add_argument('--month');p.add_argument('--all',action='store_true');a=p.parse_args(argv)
    if a.action=='prepare':prepare();return
    if a.action=='watcher':watcher();return
    if a.action=='pause':activate(paused=True);return
    if a.action=='status':print(json.dumps(snapshots(),indent=2));return
    items=inventory()
    if a.action=='plan':print(json.dumps(items,indent=2));return
    if a.action=='run' and a.all:
        gate=ROOT/'pilot-complete.json'
        if not gate.exists() or not json.loads(gate.read_text()).get('verified'):raise ValueError('Full run blocked until missing-month pilot is verified')
        if any((year,month) not in {(x['year'],x['month']) for x in items} for year in [2024,2025] for month in range(1,13)):raise ValueError('Required 2024/2025 source months missing from official listing')
        from tools.multi_year import run
        run();return
    elif a.month:
        selected=[x for x in items if f"{x['year']}-{x['month']:02d}"==a.month]
        if not selected:raise ValueError('Requested month is not in the official available inventory')
    else:selected=[choose_missing(items)]
    activate();start=time.monotonic()
    try:
        from jobs.source_versions import month_lock
        with month_lock(ROOT/'controller-locks',2013,1):
            for item in selected:
                result=execute_month(item)
                if a.action=='pilot':
                    from jobs.monthly_archives import atomic_json
                    atomic_json(ROOT/'pilot-complete.json',{'verified':True,'year':item['year'],'month':item['month'],'source_sha256':result['source']['sha256'],'verified_at':time.time()})
    finally:
        from jobs.monthly_archives import atomic_json
        atomic_json(ROOT/'controller-last-run.json',{'seconds':round(time.monotonic()-start,3),'requested_months':[f"{i['year']}-{i['month']:02d}" for i in selected],'full_run':bool(a.action=='run' and a.all),'model_training':False})

if __name__=='__main__':
    import sys
    if len(sys.argv)>1 and sys.argv[1] in ['pilot','run']:
        import threading
        from tools.full_month_validate import MemoryMonitor
        from jobs.monthly_archives import atomic_json
        monitor=MemoryMonitor(ROOT/'controller-memory.csv');thread=threading.Thread(target=monitor.run,daemon=True);thread.start()
        try:main()
        finally:
            monitor.stop.set();thread.join(timeout=3);atomic_json(ROOT/'controller-memory.json',monitor.finish())
    else:main()
