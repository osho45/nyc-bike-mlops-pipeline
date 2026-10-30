"""Manual-only bounded validation; production asset ingestion remains untouched."""
from datetime import datetime,timezone,timedelta
from airflow.sdk import dag,task

@dag(dag_id='validation_citibike_sample',start_date=datetime(2024,1,1,tzinfo=timezone.utc),schedule=None,catchup=False,max_active_runs=1,tags=['validation','sample-only'])
def validation_citibike_sample():
    @task(execution_timeout=timedelta(minutes=30),retries=0)
    def run_bounded_pipeline(**context):
        import subprocess,sys
        conf=context['dag_run'].conf or {}
        source=conf.get('source','data/citibike/audit-source-cached/202401-citibike-tripdata.sample.csv')
        output=conf.get('output')
        if not output:raise ValueError('Explicit new artifacts/ output directory required')
        subprocess.run([sys.executable,'-m','tools.container_sample_check','--source',source,'--output',output],cwd='/opt/airflow',check=True)
    run_bounded_pipeline()

validation_citibike_sample()
