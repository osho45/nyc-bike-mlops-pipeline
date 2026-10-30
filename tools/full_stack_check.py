"""Opt-in full-month Spark validation. Uses a new isolated warehouse; preserves user data.

Run inside the Airflow image: python tools/full_stack_check.py --source /.../month.zip --year 2024 --month 1 --output artifacts/new-check
This does not validate the Airflow event scheduler or PostgreSQL.
"""
import argparse,json,subprocess,sys
from pathlib import Path
import yaml
from jobs.ml_utils import load_config,create_spark_session

def main():
    p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--year',type=int,required=True);p.add_argument('--month',type=int,required=True);p.add_argument('--output',required=True);p.add_argument('--config');a=p.parse_args()
    root=Path(__file__).resolve().parents[1];out=Path(a.output).resolve()
    if not out.is_relative_to(root):raise ValueError('Output must remain in this repository')
    out.mkdir(parents=True,exist_ok=False)
    cfg=load_config(a.config);cfg['catalog']['warehouse_path']=str(out/'warehouse');config=out/'config.yml';config.write_text(yaml.safe_dump(cfg))
    base=[sys.executable,'-m'];run='isolated-full-month-validation'
    commands=[['jobs.bronze_ingest','--source_filepath',str(Path(a.source).resolve()),'--source_year',str(a.year),'--source_month',str(a.month),'--run_id',run],['jobs.silver_wap','--ingestion_run_id',run,'--target_year',str(a.year),'--target_month',str(a.month)],['jobs.gold_bi','--year',str(a.year),'--month',str(a.month)],['jobs.gold_ml','--year',str(a.year),'--month',str(a.month)]]
    for command in commands:subprocess.run(base+command+['--config',str(config)],check=True,cwd=root)
    spark=create_spark_session(cfg,'CitiBikeFullMonthAudit')
    try:
        catalog=cfg['catalog']['spark_catalog_name']
        silver=spark.table(f'{catalog}.silver.citibike_rides').count()
        bi=spark.sql(f'SELECT sum(ride_count) n FROM {catalog}.gold.station_hourly_utilization').first()['n']
        ml=spark.sql(f'SELECT sum(ride_count) n FROM {catalog}.gold.station_hourly_observations').first()['n']
        assert silver==bi==ml and silver>0
        result={'spark_medallion_reconciled':True,'silver_rides':silver,'bi_rides':bi,'ml_rides':ml,'airflow_validated':False,'postgres_validated':False}
        (out/'validation.json').write_text(json.dumps(result,indent=2));print(result)
    finally:spark.stop()

if __name__=='__main__':main()
