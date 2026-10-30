"""Sequential controller stages one verified monthly source at a time."""
from datetime import datetime,timezone,timedelta
from airflow.sdk import Asset,dag,task
from jobs.validation_dag_factory import build
from tools.monthly_ingestion import ROOT,URI,PREFIX
assets={stage:Asset(name='monthly/citibike/'+stage,uri=URI+stage) for stage in ['incoming','bronze','silver','bi','ml']}
monthly_bronze=build('bronze',assets['incoming'],assets['bronze'],ROOT,PREFIX,full_month=True)
monthly_silver=build('silver',assets['bronze'],assets['silver'],ROOT,PREFIX,full_month=True)
monthly_bi=build('bi',assets['silver'],assets['bi'],ROOT,PREFIX,full_month=True)
monthly_ml=build('ml',assets['silver'],assets['ml'],ROOT,PREFIX,full_month=True)

@dag(dag_id=PREFIX+'downloads',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule='0 8 * * *',catchup=False,max_active_runs=1,tags=['citibike','downloads','opt-in'])
def downloads():
    # Deliberately not in the Spark pool: it waits for downstream Spark tasks.
    # Remains paused until explicitly enabled after pilot approval.
    @task(task_id='discover_download_and_wait',pool='default_pool',retries=1,retry_delay=timedelta(seconds=45))
    def coordinate(**context):
        from airflow.configuration import conf
        if conf.getint('core','parallelism')<2:raise ValueError('Coordinator needs one slot and downstream ETL another; use the external CLI with a one-slot executor')
        from tools.multi_year import run,bounded_dry_run
        conf_run=context['dag_run'].conf or {}
        if conf_run.get('dry_run'):
            result=bounded_dry_run(context['run_id'])
            if conf_run.get('fail_once') and context['ti'].try_number==1:raise RuntimeError('Planned dry coordinator failure after sandbox checkpoint')
            return result
        if not conf_run.get('controlled_run'):raise ValueError('Explicit controlled_run configuration required; automatic multi-year launch disabled')
        try:run()
        except Exception:
            if (ROOT/'resource-stop.json').exists():
                from airflow.exceptions import AirflowFailException
                raise AirflowFailException('Controlled resource stop; inspect controlled-run.json')
            raise
    coordinate()

monthly_downloads=downloads()
