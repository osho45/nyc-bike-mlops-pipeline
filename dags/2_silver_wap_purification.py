from datetime import datetime,timedelta
from airflow.sdk import dag,task,Asset
from jobs.asset_lineage import run_asset_stage

BRONZE_CITIBIKE_DATA=Asset(name='warehouse/bronze/citibike_rides_raw',uri='iceberg://bronze.citibike_rides_raw')
SILVER_CITIBIKE_DATA=Asset(name='warehouse/silver/citibike_rides',uri='iceberg://silver.citibike_rides')

@dag(dag_id='2_silver_wap_purification',start_date=datetime(2026,4,1),schedule=[BRONZE_CITIBIKE_DATA],catchup=False,max_active_runs=1)
def silver_wap_purification_dag():
    @task(task_id='invoke_silver_wap_job',retries=2,retry_delay=timedelta(seconds=15),retry_exponential_backoff=True,pool="citibike_spark",outlets=[SILVER_CITIBIKE_DATA])
    def execute_stage(**context):
        events=context.get("triggering_asset_events",{}).get(BRONZE_CITIBIKE_DATA,[])
        payload=run_asset_stage('silver',events,"/opt/airflow/config/local.yml",context["run_id"])
        context["outlet_events"][SILVER_CITIBIKE_DATA].extra=payload
    execute_stage()

silver_wap_purification_dag()
