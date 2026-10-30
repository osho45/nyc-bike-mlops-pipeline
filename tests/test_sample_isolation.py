from pathlib import Path
import pytest
from jobs.lakehouse import sample_validation
from jobs.ml_utils import project_root
from tools.container_sample_check import prepare_config

def test_sample_catalogue_and_paths_are_isolated():
    cfg=prepare_config(project_root()/'artifacts/test-isolation')
    assert sample_validation(cfg)
    assert cfg['spark']['spark.driver.memory']=='512m'
    assert cfg['catalog']['spark_catalog_name']=='citibike_validation'
    cfg['catalog']['spark_catalog_name']='citibike'
    with pytest.raises(ValueError,match='requires'):sample_validation(cfg)

def test_sample_warehouse_cannot_be_production():
    cfg=prepare_config(project_root()/'artifacts/test-isolation')
    cfg['catalog']['warehouse_path']=str(project_root()/'data/citibike/warehouse')
    with pytest.raises(ValueError,match='requires'):sample_validation(cfg)
    with pytest.raises(ValueError,match='under repository artifacts'):prepare_config(project_root()/'data/test')

def test_production_config_never_enables_sample_mode():
    from jobs.ml_utils import load_config
    assert sample_validation(load_config()) is False
