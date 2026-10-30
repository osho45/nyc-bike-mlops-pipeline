"""Explicit integration gaps; unavailable services must not be counted as passes."""
import ast,importlib.util,os,shutil,subprocess
from pathlib import Path
import pytest
import yaml

def test_compose_paths_and_separate_storage():
    cfg=yaml.safe_load(Path('config/local.yml').read_text())
    assert cfg['catalog']['warehouse_path']=='data/citibike/warehouse'
    compose=yaml.safe_load(Path('docker-compose.yaml').read_text())
    assert compose['name']=='nyc-bike-mlops'
    assert '5543:5432' in compose['services']['operational-cache-postgres']['ports']
    for source in Path('dags').glob('*.py'):ast.parse(source.read_text(encoding='utf-8-sig'))

def test_docker_compose_runtime_configuration():
    if not shutil.which('docker'):pytest.skip('Docker executable unavailable; compose runtime/build not validated')
    if not Path('.env').exists():pytest.skip('Run tools/init_local.py before Docker validation')
    subprocess.run(['docker','compose','config','--quiet'],check=True,capture_output=True)

def test_airflow_dag_imports():
    if not importlib.util.find_spec('airflow'):pytest.skip('Airflow unavailable; DAG scheduling not validated')
    from airflow.models import DagBag
    bag=DagBag(dag_folder='dags',include_examples=False)
    assert not bag.import_errors
    assert '1_bronze_ingestion' in bag.dags and '4_ml_batch_demand_score' in bag.dags

def test_spark_iceberg_runtime():
    if os.getenv('RUN_SPARK_INTEGRATION')!='1':pytest.skip('Opt-in Spark/Java/Iceberg integration not executed in this environment')
    from jobs.ml_utils import create_spark_session,load_config
    spark=create_spark_session(load_config(),'CitiBikeIntegrationReadiness')
    try:
        assert spark.sql('SELECT 1').first()[0]==1
        assert spark.conf.get('spark.sql.session.timeZone')=='UTC'
        # This is readiness only; run tools/full_stack_check.py for medallion jobs.
    finally:spark.stop()
