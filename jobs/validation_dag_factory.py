"""Factory for isolated asset checks; production DAG configuration is not overridden."""
from datetime import datetime,timezone,timedelta
from airflow.sdk import dag,task
from jobs.asset_lineage import run_asset_stage,unique_lineages

def build(stage,source,target,root,prefix,full_month=False):
    @dag(dag_id=prefix+stage,start_date=datetime(2024,1,1,tzinfo=timezone.utc),schedule=[source],catchup=False,max_active_runs=1,tags=['validation','full-month' if full_month else 'sample-only'])
    def chain():
        @task(task_id='process_'+stage,outlets=[target],retries=1,retry_delay=timedelta(seconds=45),pool='citibike_spark')
        def process(**context):
            from jobs.lakehouse import sample_validation
            from jobs.ml_utils import load_config
            config=root/'config.yml'
            runner=None
            if full_month:
                from tools.full_month_queue import isolated_config,FileQueueRunner
                isolated_config(root)
                runner=FileQueueRunner(root,stage,context['run_id'],context['ti'].try_number)
            elif not sample_validation(load_config(config)):raise ValueError('Isolated sample configuration required')
            events=context.get('triggering_asset_events',{}).get(source,[])
            try:payload=run_asset_stage(stage,events,config,context['run_id'],runner=runner)
            except Exception:
                if (root/'resource-stop.json').exists():
                    from airflow.exceptions import AirflowFailException
                    raise AirflowFailException('Controlled resource stop; checkpoints preserved')
                raise
            if stage=='bronze' and context['ti'].try_number==1 and any(item.get('validation_fail_after_bronze_once') for item in unique_lineages(events)):
                raise RuntimeError('Planned validation failure after Bronze commit, before asset publication')
            context['outlet_events'][target].extra=payload
        process()
    return chain()
