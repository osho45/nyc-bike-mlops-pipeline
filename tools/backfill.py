"""Explicit, bounded historical scoring requests. Dry run unless --execute is supplied."""
import argparse,json,subprocess
import pandas as pd

def main():
    p=argparse.ArgumentParser();p.add_argument('--start',required=True);p.add_argument('--end',required=True);p.add_argument('--max-hours',type=int,default=24);p.add_argument('--execute',action='store_true');a=p.parse_args()
    start,end=pd.Timestamp(a.start),pd.Timestamp(a.end)
    if start.tzinfo is None or end.tzinfo is None:raise ValueError('Use explicit UTC timestamps ending in Z')
    start,end=start.tz_convert('UTC'),end.tz_convert('UTC')
    if start!=start.floor('h') or end!=end.floor('h') or end<start:raise ValueError('Provide increasing hour-aligned times')
    count=int((end-start)/pd.Timedelta(hours=1))+1
    if not 1<=count<=a.max_hours:raise ValueError('Request exceeds explicit hourly budget')
    for hour in pd.date_range(start,end,freq='h'):
        conf=json.dumps({'real_now':hour.isoformat(),'months_back':0})
        command=['docker','compose','exec','-T','airflow-webserver','airflow','dags','trigger','4_ml_batch_demand_score','--conf',conf]
        if a.execute:subprocess.run(command,check=True)
        else:print(json.dumps(command))

if __name__=='__main__':main()
