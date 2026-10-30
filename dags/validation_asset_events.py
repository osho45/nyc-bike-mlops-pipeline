"""Isolated, sample-only asset chain using the same jobs/lineage logic as production."""
from datetime import datetime,timezone,timedelta
from pathlib import Path
from airflow.sdk import dag,task,Asset
from jobs.asset_lineage import run_asset_stage,unique_lineages

ROOT=Path('/opt/airflow/artifacts/asset-validation-01')
incoming=Asset(name='validation/citibike/incoming',uri='validation://citibike/sample-20k/incoming')
bronze=Asset(name='validation/citibike/bronze',uri='validation://citibike/sample-20k/bronze')
silver=Asset(name='validation/citibike/silver',uri='validation://citibike/sample-20k/silver')
bi=Asset(name='validation/citibike/bi',uri='validation://citibike/sample-20k/bi')
ml=Asset(name='validation/citibike/ml',uri='validation://citibike/sample-20k/ml')

def build(stage,source,target):
    @dag(dag_id='validation_asset_'+stage,start_date=datetime(2024,1,1,tzinfo=timezone.utc),schedule=[source],catchup=False,max_active_runs=1,tags=['validation','sample-only','asset-events'])
    def chain():
        @task(task_id='process_'+stage,outlets=[target],retries=1,retry_delay=timedelta(seconds=45),pool='citibike_spark')
        def process(**context):
            from jobs.lakehouse import sample_validation
            from jobs.ml_utils import load_config
            config=ROOT/'config.yml'
            if not sample_validation(load_config(config)):raise ValueError('Validation requires an isolated sample configuration')
            events=context.get('triggering_asset_events',{}).get(source,[])
            payload=run_asset_stage(stage,events,config,context['run_id'])
            # Explicit validation-only fault after commit, before asset publication.
            if stage=='bronze' and context['ti'].try_number==1 and any(item.get('validation_fail_after_bronze_once') for item in unique_lineages(events)):
                raise RuntimeError('Planned validation failure after Bronze commit, before asset publication')
            context['outlet_events'][target].extra=payload
        process()
    return chain()

validation_asset_bronze=build('bronze',incoming,bronze)
validation_asset_silver=build('silver',bronze,silver)
validation_asset_bi=build('bi',silver,bi)
validation_asset_ml=build('ml',silver,ml)
