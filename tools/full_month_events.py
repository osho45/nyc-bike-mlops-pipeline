"""Opt-in controls for the isolated full-month watcher/asset validation only."""
import argparse,hashlib,json,os,runpy,shutil,time
from pathlib import Path
import yaml

ROOT=Path('/opt/airflow/artifacts/fullmonth-orchestration-01')
STAGES=['bronze','silver','bi','ml']
PREFIX='validation_fullmonth_'
URI='validation://citibike/fullmonth-events-01/'

def snapshot():
    from jobs.citibike import checksum
    result={}
    for name in ['bronze/citibike_rides_raw','silver/citibike_rides','quarantine/citibike_rides','gold/station_hourly_utilization','gold/station_hourly_observations','audit/ingestion_ledger']:
        directory=ROOT/'warehouse'/name/'metadata'
        if not (directory/'version-hint.text').exists():continue
        version=(directory/'version-hint.text').read_text().strip();path=directory/f'v{version}.metadata.json';meta=json.loads(path.read_text());sid=meta.get('current-snapshot-id')
        current=next((s for s in meta['snapshots'] if s['snapshot-id']==sid),None)
        result[name]={'version':version,'metadata_sha256':checksum(path),'snapshot_id':sid,'rows':int(current['summary']['total-records']) if current else 0,'schema':meta['schemas']}
    return result

def status(s):
    from tools.asset_validation_control import call
    result={'captured_at':time.time(),'dags':{},'assets':{},'snapshot':snapshot()}
    for stage in STAGES:
        dag=PREFIX+stage;details=[]
        for run in call(s,'GET',f'/dags/{dag}/dagRuns?limit=100')['dag_runs']:
            tasks=call(s,'GET',f"/dags/{dag}/dagRuns/{run['dag_run_id']}/taskInstances")['task_instances']
            details.append({k:run.get(k) for k in ['dag_run_id','state','run_type','start_date','end_date','queued_at','triggered_by']})
            details[-1]['tasks']=[{k:t.get(k) for k in ['task_id','state','try_number','start_date','end_date','duration','pool']} for t in tasks]
        result['dags'][stage]=details
    assets=[a for a in call(s,'GET','/assets?limit=100')['assets'] if a['uri'].startswith(URI)]
    for asset in assets:
        events=call(s,'GET',f"/assets/events?asset_id={asset['id']}&limit=100")
        result['assets'][asset['uri'].rsplit('/',1)[-1]]=events
    return result

def setup():
    from jobs.citibike import checksum
    from jobs.ml_utils import load_config
    baseline=json.loads(Path('/opt/airflow/artifacts/full-month-202401-01/validation.json').read_text());assert baseline['complete'] and baseline['idempotent_replay']
    source=Path('/opt/airflow/data/citibike/full_month_validation/incoming/202401-citibike-tripdata.zip')
    meta=json.loads(source.with_suffix('.zip.manifest.json').read_text());assert meta['sample'] is False and checksum(source)==baseline['source_sha256']==meta['sha256']
    ROOT.mkdir(exist_ok=False);incoming=ROOT/'incoming';incoming.mkdir();target=incoming/source.name
    try:os.link(source,target)
    except OSError:shutil.copy2(source,target)
    cfg=load_config('/opt/airflow/config/full_month_validation.yml');cfg['catalog']['spark_catalog_name']='citibike_fullmonth_events';cfg['catalog']['warehouse_path']=str(ROOT/'warehouse')
    cfg['storage']['incoming_directory']=str(incoming);cfg['storage']['warehouse_directory']=str(ROOT/'warehouse');cfg['ml']['models_dir']=str(ROOT/'unused-models');cfg['ml']['predictions_dir']=str(ROOT/'unused-predictions')
    cfg['assets']={key:URI+stage for key,stage in [('incoming_citibike_data_uri','incoming'),('bronze_citibike_data_uri','bronze'),('silver_citibike_data_uri','silver')]}
    (ROOT/'config.yml').write_text(yaml.safe_dump(cfg));(ROOT/'baseline.json').write_text(json.dumps(baseline,indent=2))
    meta['source_filepath']=str(target);meta['validation_fail_after_bronze_once']=True
    (ROOT/'manifest-template.json').write_text(json.dumps(meta,indent=2));(ROOT/'queue').mkdir()
    print(json.dumps({'root':str(ROOT),'source_sha256':meta['sha256'],'model_training':False,'scoring':False}))

def watcher():
    password=json.loads(Path('/opt/airflow/auth/simple_auth_manager_passwords.json.generated').read_text())['admin']
    os.environ.update(AIRFLOW_BASE_URL='http://airflow-webserver:8080',AIRFLOW_USERNAME='admin',AIRFLOW_PASSWORD=password,WATCH_DIRECTORY=str(ROOT/'incoming'),TARGET_ASSET_URI=URI+'incoming',CONSUMER_DAG_IDS=','.join(PREFIX+s for s in STAGES))
    runpy.run_module('sftp_mock_daemon',run_name='__main__')

def chain_complete(states,expected_runs):
    return all(len(runs)>=expected_runs and all(r['state']=='success' for r in runs) for runs in states.values())

def observe(s,label,expected_runs=1):
    from tools.asset_validation_control import call
    timeline=ROOT/('timeline-'+label+'.jsonl')
    while True:
        states={stage:call(s,'GET',f'/dags/{PREFIX+stage}/dagRuns?limit=100')['dag_runs'] for stage in STAGES}
        row={'time':time.time(),'runs':{stage:[{'run_id':r['dag_run_id'],'state':r['state']} for r in runs] for stage,runs in states.items()}}
        if states['bronze']:
            last=max(states['bronze'],key=lambda r:r['queued_at'])
            tasks=call(s,'GET',f"/dags/{PREFIX}bronze/dagRuns/{last['dag_run_id']}/taskInstances")['task_instances'];row['bronze_tasks']=[{k:t.get(k) for k in ['state','try_number']} for t in tasks]
            if any(t['state']=='up_for_retry' for t in tasks) and not (ROOT/'status-planned-retry.json').exists():
                (ROOT/'status-planned-retry.json').write_text(json.dumps(status(s),indent=2))
        with timeline.open('a') as stream:stream.write(json.dumps(row)+'\n')
        if chain_complete(states,expected_runs):
            (ROOT/('status-'+label+'-complete.json')).write_text(json.dumps(status(s),indent=2));return
        time.sleep(3)

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['setup','activate','drop','status','snapshot','pause','watcher','observe']);p.add_argument('--label',default='initial');p.add_argument('--expected-runs',type=int,default=1);a=p.parse_args()
    if a.action=='setup':setup();return
    if a.action=='watcher':watcher();return
    if a.action=='snapshot':print(json.dumps(snapshot(),indent=2));return
    if a.action=='drop':
        meta=json.loads((ROOT/'manifest-template.json').read_text())
        if a.label!='initial':meta.pop('validation_fail_after_bronze_once',None)
        meta['validation_delivery_label']=a.label
        target=ROOT/'incoming'/(meta['source_filename']+'.manifest.json');temp=target.with_suffix('.pending');temp.write_text(json.dumps(meta,indent=2));temp.replace(target)
        with (ROOT/'deliveries.jsonl').open('a') as stream:stream.write(json.dumps({'label':a.label,'dropped_at':time.time()})+'\n')
        print('Dropped manifest atomically: '+a.label);return
    from tools.asset_validation_control import session,call
    s=session()
    if a.action=='observe':observe(s,a.label,a.expected_runs);return
    if a.action=='status':
        result=status(s);(ROOT/('status-'+a.label+'.json')).write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2));return
    for stage in STAGES:call(s,'PATCH','/dags/'+PREFIX+stage,json={'is_paused':a.action=='pause'})
    # Production data processing/scoring remains disabled throughout the check.
    if a.action=='activate':
        for dag in ['1_bronze_ingestion','2_silver_wap_purification','3a_gold_bi_marts','3b_gold_ml_observations','4_ml_batch_demand_score']:
            assert call(s,'GET','/dags/'+dag)['is_paused'] is True
        assert next(p for p in call(s,'GET','/pools')['pools'] if p['name']=='citibike_spark')['slots']==1
    print('Full-month validation DAGs '+a.action)

if __name__=='__main__':main()
