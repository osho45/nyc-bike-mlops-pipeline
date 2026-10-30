"""Controls only the new correctness sample asset DAGs."""
import argparse,json
from pathlib import Path
from tools.asset_validation_control import session,call
from jobs.citibike import checksum
ROOT=Path('/opt/airflow/artifacts/correctness-01')
STAGES=['bronze','silver','bi','ml']

def status(s):
    result={}
    for stage in STAGES:
        dag='validation_correctness_'+stage;result[stage]=[]
        for run in call(s,'GET',f'/dags/{dag}/dagRuns?limit=100')['dag_runs']:
            tasks=call(s,'GET',f"/dags/{dag}/dagRuns/{run['dag_run_id']}/taskInstances")['task_instances']
            result[stage].append({'run_id':run['dag_run_id'],'state':run['state'],'attempts':[t['try_number'] for t in tasks]})
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['emit','status','pause']);p.add_argument('--case',choices=['batch','r2','stale','replay'],default='batch');a=p.parse_args();s=session()
    if a.action=='status':print(json.dumps(status(s),indent=2));return
    if a.action=='pause':
        for stage in STAGES:call(s,'PATCH','/dags/validation_correctness_'+stage,json={'is_paused':True})
        print('Correctness DAGs paused');return
    sources=json.loads((ROOT/'sources.json').read_text())
    keys={'batch':['feb','jan','jan'],'r2':['jan_r2'],'stale':['jan'],'replay':['jan_r2','jan','jan_r2']}[a.case];items=[sources[key] for key in keys]
    for item in items:assert checksum(item['source_filepath'])==item['sha256'] and item['sample']
    for stage in STAGES:call(s,'PATCH','/dags/validation_correctness_'+stage,json={'is_paused':False})
    assets=call(s,'GET','/assets?limit=100')['assets'];asset=next(item for item in assets if item['uri']=='validation://citibike/correctness-01/incoming')
    event=call(s,'POST','/assets/events',json={'asset_id':asset['id'],'extra':{'dataset':'citibike','lineages':items}})
    print(json.dumps({'case':a.case,'event_id':event['id'],'months':[(item['source_year'],item['source_month']) for item in items]}))

if __name__=='__main__':main()
