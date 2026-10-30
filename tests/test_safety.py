import pandas as pd
import pytest
from jobs.citibike import clean_rides,deduplicate_rides

def test_duplicate_contract(ride):
    clean,_=clean_rides(pd.DataFrame([ride,ride]),2024,1)
    assert len(deduplicate_rides(clean))==1
    clean,_=clean_rides(pd.DataFrame([ride,{**ride,'start_station_id':'other'}]),2024,1)
    with pytest.raises(ValueError,match='Conflicting duplicate'):deduplicate_rides(clean)

def test_checkpoint_preservation(tmp_path):
    from jobs.ml_experiments import export_champion
    target=tmp_path/'champion_model.pkl';target.write_bytes(b'original')
    with pytest.raises(FileExistsError):export_champion(None,None,tmp_path,None,[],'station',{})
    assert target.read_bytes()==b'original'

def test_destructive_reset_requires_explicit_confirmation():
    from jobs.reset_runtime_state import reset_environment
    with pytest.raises(ValueError,match='Destructive reset disabled'):reset_environment()

def test_calendar_is_local_but_lags_are_utc(history):
    from jobs.ml_features import add_calendar_features
    frame=add_calendar_features(history.iloc[:1])
    assert frame.hour_of_day.iloc[0]==19 # UTC midnight is previous NYC evening in January.
