"""Create labeled synthetic window fixtures and inspect isolated recovery data."""
import argparse,json,hashlib
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
ROOT=Path('/opt/airflow/artifacts/correctness-01')

def connect(database=None):
    env=dict(line.split('=',1) for line in (ROOT/'postgres.env').read_text().splitlines())
    return psycopg2.connect(host=env['POSTGRES_CACHE_HOST'],port=5432,dbname=database or env['POSTGRES_DB'],user=env['POSTGRES_USER'],password=env['POSTGRES_PASSWORD'])

def fingerprint(database=None):
    with connect(database) as db:
        with db.cursor() as cur:
            cur.execute('SELECT * FROM station_hourly_demand_predictions ORDER BY station_id,prediction_hour_ts');rows=cur.fetchall()
            cur.execute('SELECT * FROM recovery_probe ORDER BY id');probe=cur.fetchall()
            return {'predictions':len(rows),'prediction_hash':hashlib.sha256(json.dumps(rows,default=str).encode()).hexdigest(),'recovery_probe':probe}

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['setup','fingerprint','uncommitted']);p.add_argument('--database');a=p.parse_args()
    if a.action=='fingerprint':print(json.dumps(fingerprint(a.database),default=str));return
    if a.action=='uncommitted':
        with connect() as db:
            with db.cursor() as cur:
                cur.execute('INSERT INTO recovery_probe VALUES(2,\'uncommitted-must-not-survive\')')
                (ROOT/'db_transaction.ready').write_text('Uncommitted INSERT exists; awaiting controlled database crash')
                cur.execute('SELECT pg_sleep(60)')
        return
    with connect() as db:
        with db.cursor() as cur:
            cur.execute(Path('sql/citibike_cache.sql').read_text(encoding='utf-8-sig'))
            cur.execute('CREATE TABLE recovery_probe(id INTEGER PRIMARY KEY,note TEXT)');cur.execute("INSERT INTO recovery_probe VALUES(1,'committed-must-survive')")
            hours=pd.date_range('2024-10-01',periods=1009,freq='h',tz='UTC');records=[]
            for i,hour in enumerate(hours):
                actual=i%3;pred=(i+1)%3;penalty=(actual-pred)*15 if actual>pred else (pred-actual)*10
                records.append(('MULTIWEEK_FIXTURE',hour.tz_convert('America/New_York').to_pydatetime(),hour.to_pydatetime(),'validation-only-synthetic-boundary','synthetic-boundary-fixture',pred,{0:'Low',1:'Normal',2:'High'}[pred],float(pred==0),float(pred==1),float(pred==2),actual,actual,float(penalty),0.0))
            execute_values(cur,'INSERT INTO station_hourly_demand_predictions(station_id,prediction_hour_ts,scored_at_ts,model_name,model_version,predicted_class,predicted_label,prob_class_0,prob_class_1,prob_class_2,baseline_predicted_class,actual_class,model_penalty,baseline_penalty) VALUES %s',records)
    print(json.dumps({'synthetic_fixture':True,'rows':1009,'expected_windows':{'All History':1009,'7 Days':168,'10 Days':240,'14 Days':336},'spans_new_york_dst_fall_back':True}))

if __name__=='__main__':main()
