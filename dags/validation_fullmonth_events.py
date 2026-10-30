"""Opt-in complete-month chain. No training or scoring DAG consumes these assets."""
from pathlib import Path
from airflow.sdk import Asset
from jobs.validation_dag_factory import build
root=Path('/opt/airflow/artifacts/fullmonth-orchestration-01')
assets={name:Asset(name='fullmonth/citibike/'+name,uri='validation://citibike/fullmonth-events-01/'+name) for name in ['incoming','bronze','silver','bi','ml']}
fullmonth_bronze=build('bronze',assets['incoming'],assets['bronze'],root,'validation_fullmonth_',full_month=True)
fullmonth_silver=build('silver',assets['bronze'],assets['silver'],root,'validation_fullmonth_',full_month=True)
fullmonth_bi=build('bi',assets['silver'],assets['bi'],root,'validation_fullmonth_',full_month=True)
fullmonth_ml=build('ml',assets['silver'],assets['ml'],root,'validation_fullmonth_',full_month=True)
