import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pytest
import pandas as pd
from jobs.citibike import COLUMNS

def pytest_configure(config):
    # Docker Desktop may hold handles on a previous bind-mounted test directory.
    # Use a fresh repository-local path instead of deleting a shared fixed directory.
    if config.option.basetemp is None:
        import uuid
        artifact_root=Path(__file__).resolve().parents[1]/'artifacts'
        artifact_root.mkdir(parents=True,exist_ok=True)
        # Keep Windows/PyArrow metadata paths below legacy MAX_PATH limits.
        config.option.basetemp=str(artifact_root/('pytest-'+uuid.uuid4().hex[:12]))

@pytest.fixture
def ride():
    return dict(zip(COLUMNS,['ride-1','classic_bike','2024-01-01 12:00:00','2024-01-01 12:12:00','Start','001.20','End','A-2','40.7','-73.9','40.8','-73.8','member']))

@pytest.fixture
def history():
    import numpy as np
    return pd.DataFrame({'station_id':'001.20','station_hour_ts':pd.date_range('2024-01-01',periods=500,freq='h'),'ride_count':np.arange(500)%17,'target_year':2024,'target_month':1})
