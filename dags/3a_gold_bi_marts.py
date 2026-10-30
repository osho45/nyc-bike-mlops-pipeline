from datetime import datetime,timedelta
from airflow.sdk import dag,task,Asset
from jobs.asset_lineage import run_asset_stage

SILVER_CITIBIKE_DATA=Asset(name='warehouse/silver/citibike_rides',uri='iceberg://silver.citibike_rides')

@dag(dag_id='3a_gold_bi_marts',start_date=datetime(2026,4,1),schedule=[SILVER_CITIBIKE_DATA],catchup=False,max_active_runs=1)
def gold_bi_marts_dag():
    @task(task_id='run_bi_aggregations',retries=2,retry_delay=timedelta(seconds=15),retry_exponential_backoff=True,pool="citibike_spark")
    def execute_stage(**context):
        events=context.get("triggering_asset_events",{}).get(SILVER_CITIBIKE_DATA,[])
        payload=run_asset_stage('bi',events,"/opt/airflow/config/local.yml",context["run_id"])
    execute_stage()

gold_bi_marts_dag()
