import importlib.util
from pathlib import Path
import pandas as pd
import duckdb
import pytest
from fastapi.testclient import TestClient

@pytest.fixture
def window_client(tmp_path,monkeypatch):
    path=tmp_path/'windows.duckdb'
    with duckdb.connect(str(path)) as db:
        db.execute(Path('sql/citibike_cache.sql').read_text(encoding='utf-8-sig'))
        for hour in pd.date_range('2024-10-01',periods=1009,freq='h',tz='UTC'):
            db.execute('INSERT INTO station_hourly_demand_predictions(station_id,prediction_hour_ts) VALUES (?,?)',['DST',hour.to_pydatetime()])
    monkeypatch.setenv('PREDICTION_CACHE_BACKEND','duckdb');monkeypatch.setenv('DUCKDB_CACHE_PATH',str(path))
    spec=importlib.util.spec_from_file_location('window_api','fastapi/main.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    with TestClient(module.app) as client:
        client.fixture_path=path;yield client

@pytest.mark.parametrize('days,expected',[(7,168),(10,240),(14,336)])
def test_exclusive_utc_boundaries_including_dst(window_client,days,expected):
    data=window_client.get('/predictions/DST',params={'lookback_days':days}).json()
    assert data['count']==data['total_count']==expected
    latest=pd.Timestamp(data['latest_prediction_hour_ts']);cutoff=latest-pd.Timedelta(days=days)
    assert pd.Timestamp(data['predictions'][0]['prediction_hour_ts'])==cutoff+pd.Timedelta(hours=1)
    assert all(pd.Timestamp(row['prediction_hour_ts'])>cutoff for row in data['predictions'])

def test_all_history_paginates_without_loss_or_duplicates(window_client):
    first=window_client.get('/predictions/DST').json();assert first['count']==1000 and first['total_count']==1009
    second=window_client.get('/predictions/DST',params={'before':first['next_before'],'through':first['latest_prediction_hour_ts']}).json()
    assert second['count']==9 and second['next_before'] is None
    timestamps=[row['prediction_hour_ts'] for row in first['predictions']+second['predictions']]
    assert len(set(timestamps))==1009

def test_invalid_filter_and_cursor_rejected(window_client):
    for params in [{'lookback_days':0},{'lookback_days':367},{'before':'not-a-time'},{'through':'NaT'}]:
        assert window_client.get('/predictions/DST',params=params).status_code==422

def test_pagination_anchor_excludes_new_rows_until_next_refresh(window_client):
    first=window_client.get('/predictions/DST').json()
    with duckdb.connect(str(window_client.fixture_path)) as db:
        db.execute("INSERT INTO station_hourly_demand_predictions(station_id,prediction_hour_ts) VALUES ('DST','2024-11-12 01:00:00+00')")
    next_page=window_client.get('/predictions/DST',params={'before':first['next_before'],'through':first['latest_prediction_hour_ts']}).json()
    assert next_page['total_count']==1009 and next_page['count']==9
    assert window_client.get('/predictions/DST').json()['total_count']==1010
