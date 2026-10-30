from datetime import datetime,timedelta
from airflow.sdk import dag,task,Asset
from jobs.asset_lineage import run_asset_stage

INCOMING_CITIBIKE_DATA=Asset(name='landing_zone/incoming_citibike_data',uri='file:///opt/airflow/data/citibike/incoming/citibike_data_drop')
BRONZE_CITIBIKE_DATA=Asset(name='warehouse/bronze/citibike_rides_raw',uri='iceberg://bronze.citibike_rides_raw')

@dag(dag_id='1_bronze_ingestion',start_date=datetime(2026,4,1),schedule=[INCOMING_CITIBIKE_DATA],catchup=False,max_active_runs=1)
def bronze_ingestion_dag():
    @task(task_id='invoke_bronze_spark_job',retries=2,retry_delay=timedelta(seconds=15),retry_exponential_backoff=True,pool="citibike_spark",outlets=[BRONZE_CITIBIKE_DATA])
    def execute_stage(**context):
        events=context.get("triggering_asset_events",{}).get(INCOMING_CITIBIKE_DATA,[])
        payload=run_asset_stage('bronze',events,"/opt/airflow/config/local.yml",context["run_id"])
        context["outlet_events"][BRONZE_CITIBIKE_DATA].extra=payload
    execute_stage()

bronze_ingestion_dag()
