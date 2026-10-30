from jobs.ml_utils import load_config,get_gold_ml_table
from jobs.lakehouse import sample_validation

def test_full_month_profile_is_separate_and_rejects_samples():
    cfg=load_config('config/full_month_validation.yml')
    assert cfg['catalog']['warehouse_path']!='data/citibike/warehouse'
    assert cfg['ml']['models_dir']!='models/citibike'
    assert sample_validation(cfg) is False
    assert get_gold_ml_table(cfg)=='citibike_fullmonth.gold.station_hourly_observations'
    assert cfg['spark']['spark.master']=='local[1]'
