"""Control only the manual validation DAG; never print auth tokens/passwords."""
import argparse,json,os
from datetime import datetime,timezone
from pathlib import Path
import requests

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['probe','trigger','status']);p.add_argument('--run-id',default='bounded-sample-01');p.add_argument('--output',default='artifacts/container-sample-01');a=p.parse_args()
    base='http://airflow-webserver:8080';authfile=Path('/opt/airflow/auth/simple_auth_manager_passwords.json.generated')
    password=json.loads(authfile.read_text())['admin']
    r=requests.post(base+'/auth/token',json={'username':'admin','password':password},timeout=20);r.raise_for_status()
    headers={'Authorization':'Bearer '+r.json()['access_token']}
    prefix=base+'/api/v2/dags/validation_citibike_sample'
    if a.action=='probe':
        result={}
        for name,url in [('health',base+'/api/v2/monitor/health'),('dags',base+'/api/v2/dags?limit=100'),('imports',base+'/api/v2/importErrors')]:
            response=requests.get(url,headers=headers,timeout=20);response.raise_for_status();result[name]=response.json()
        print(json.dumps(result,indent=2));return
    if a.action=='trigger':
        response=requests.patch(prefix,json={'is_paused':False},headers=headers,timeout=20);response.raise_for_status()
        response=requests.post(prefix+'/dagRuns',json={'dag_run_id':a.run_id,'logical_date':datetime.now(timezone.utc).isoformat(),'conf':{'output':a.output}},headers=headers,timeout=20)
    else:response=requests.get(prefix+'/dagRuns/'+a.run_id,headers=headers,timeout=20)
    response.raise_for_status();print(json.dumps(response.json(),indent=2))

if __name__=='__main__':main()
