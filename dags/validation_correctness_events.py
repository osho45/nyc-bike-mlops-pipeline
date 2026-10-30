from pathlib import Path
from airflow.sdk import Asset
from jobs.validation_dag_factory import build
root=Path('/opt/airflow/artifacts/correctness-01')
assets={name:Asset(name='correctness/citibike/'+name,uri='validation://citibike/correctness-01/'+name) for name in ['incoming','bronze','silver','bi','ml']}
correctness_bronze=build('bronze',assets['incoming'],assets['bronze'],root,'validation_correctness_')
correctness_silver=build('silver',assets['bronze'],assets['silver'],root,'validation_correctness_')
correctness_bi=build('bi',assets['silver'],assets['bi'],root,'validation_correctness_')
correctness_ml=build('ml',assets['silver'],assets['ml'],root,'validation_correctness_')
