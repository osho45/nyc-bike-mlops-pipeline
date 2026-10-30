"""Authenticated controls for isolated validation assets only. No model training."""
import argparse,json
from pathlib import Path
import requests,yaml
from tools.container_sample_check import prepare_config
from jobs.citibike import checksum

BASE='http://airflow-webserver:8080/api/v2'
STAGES=['bronze','silver','bi','ml']
ROOT=Path('/opt/airflow/artifacts/asset-validation-01')

def session():
    password=json.loads(Path('/opt/airflow/auth/simple_auth_manager_passwords.json.generated').read_text())['admin']
    r=requests.post('http://airflow-webserver:8080/auth/token',json={'username':'admin','password':password},timeout=20);r.raise_for_status()
    s=requests.Session();s.headers['Authorization']='Bearer '+r.json()['access_token'];return s

def call(s,method,path,**kwargs):
    r=s.request(method,BASE+path,timeout=30,**kwargs);r.raise_for_status();return r.json()

def status(s):
    result={'dags':{},'events':{}}
    assets=call(s,'GET','/assets?limit=100')['assets']
    event_data=call(s,'GET','/assets/events?limit=100')
    events=event_data.get('asset_events',event_data.get('events',[]))
    for asset in assets:
        if asset['uri'].startswith('validation://citibike/sample-20k/'):
            result['events'][asset['uri'].rsplit('/',1)[-1]]=sum(event['asset_id']==asset['id'] for event in events)
    for stage in STAGES:
        dag='validation_asset_'+stage;runs=call(s,'GET',f'/dags/{dag}/dagRuns?limit=100')['dag_runs']
        details=[]
        for run in runs:
            tasks=call(s,'GET',f"/dags/{dag}/dagRuns/{run['dag_run_id']}/taskInstances")['task_instances']
            details.append({'run_id':run['dag_run_id'],'state':run['state'],'tasks':[{'task_id':t['task_id'],'state':t['state'],'try_number':t['try_number']} for t in tasks]})
        result['dags'][stage]=details
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['setup','emit','status','pause']);p.add_argument('--fail-once',action='store_true');a=p.parse_args()
    if a.action=='setup':
        cfg=prepare_config(ROOT);ROOT.mkdir(parents=True,exist_ok=False);(ROOT/'config.yml').write_text(yaml.safe_dump(cfg))
        print(json.dumps({'config':str(ROOT/'config.yml'),'model_training':False}));return
    s=session()
    if a.action=='status':print(json.dumps(status(s),indent=2));return
    if a.action=='pause':
        for stage in STAGES:call(s,'PATCH','/dags/validation_asset_'+stage,json={'is_paused':True})
        print('Validation asset DAGs paused');return
    # Keep consumers enabled. Airflow 3.2.2 records events for paused DAGs but
    # does not queue their runs. Stop the scheduler externally to coalesce safely.
    for stage in STAGES:call(s,'PATCH','/dags/validation_asset_'+stage,json={'is_paused':False})
    path=Path('/opt/airflow/data/citibike/audit-source-cached/202401-citibike-tripdata.sample.csv')
    meta=json.loads(path.with_suffix(path.suffix+'.manifest.json').read_text())
    assert checksum(path)==meta['sha256'] and meta['sample'] is True
    meta['source_filepath']=str(path);meta['validation_fail_after_bronze_once']=a.fail_once
    assets=call(s,'GET','/assets?limit=100')['assets']
    asset=next(asset for asset in assets if asset['uri']=='validation://citibike/sample-20k/incoming')
    emitted=[call(s,'POST','/assets/events',json={'asset_id':asset['id'],'extra':meta})['id'] for _ in range(2)]
    call(s,'PATCH','/dags/validation_asset_bronze',json={'is_paused':False})
    print(json.dumps({'event_ids':emitted,'injected_failure_after_commit':a.fail_once,'source_checksum':meta['sha256']},indent=2))

if __name__=='__main__':main()
