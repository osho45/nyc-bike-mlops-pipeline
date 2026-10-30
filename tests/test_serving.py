import importlib.util
from pathlib import Path
import duckdb
import pytest
from fastapi.testclient import TestClient

@pytest.fixture
def client(tmp_path,monkeypatch):
    path=tmp_path/'cache.duckdb'
    with duckdb.connect(str(path)) as conn:
        conn.execute(Path('sql/citibike_cache.sql').read_text(encoding='utf-8-sig'))
        conn.execute("INSERT INTO station_hourly_demand_predictions(station_id,prediction_hour_ts,predicted_class) VALUES ('001.20','2024-01-15 17:00:00+00',1)")
    monkeypatch.setenv('PREDICTION_CACHE_BACKEND','duckdb');monkeypatch.setenv('DUCKDB_CACHE_PATH',str(path))
    spec=importlib.util.spec_from_file_location('test_citibike_api','fastapi/main.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    with TestClient(module.app) as test:yield test

def test_cache_api_contract(client):
    assert client.get('/health').json()['dataset']=='citibike'
    assert client.get('/ready').status_code==200
    assert client.get('/stations').json()=={'stations':['001.20']}
    assert client.get('/predictions/001.20').json()['count']==1
    assert client.get('/predictions/001.20/2024-01-15T17:00:00Z').status_code==200
    assert client.get('/predictions/001.20/2024-01-15T12:00:00-05:00').status_code==200
    assert client.get('/predictions/unknown').json()['count']==0
    assert client.get('/predictions/unknown/2024-01-15T17:00').status_code==404

@pytest.mark.parametrize('suffix',['?limit=0','?limit=10001','/not-a-date','/2024-01-15T17:01:00'])
def test_bad_query_rejected(client,suffix):
    assert client.get('/predictions/001.20'+suffix).status_code==422

def test_no_fake_readiness_when_store_missing(client,monkeypatch,tmp_path):
    monkeypatch.setenv('DUCKDB_CACHE_PATH',str(tmp_path/'missing.duckdb'))
    assert client.get('/health').status_code==200
    assert client.get('/ready').status_code==503

def test_empty_dashboard_starts(monkeypatch):
    from streamlit.testing.v1 import AppTest
    monkeypatch.setenv('STATION_ID','')
    app=AppTest.from_file('streamlit/app.py').run(timeout=20)
    assert not app.exception
    assert any('station ID' in item.value for item in app.info)

def test_dashboard_with_real_smoke_prediction(monkeypatch):
    from streamlit.testing.v1 import AppTest
    import requests
    path=Path('artifacts/citibike-real-audit-03/api_cache.duckdb')
    if not path.exists():pytest.skip('Optional downloaded real-source audit is not present')
    with duckdb.connect(str(path),read_only=True) as db:
        records=db.execute('SELECT * FROM station_hourly_demand_predictions').fetchdf().to_json(orient='records',date_format='iso')
    import json
    records=json.loads(records);station=records[0]['station_id']
    class Response:
        status_code=200
        def raise_for_status(self):pass
        def json(self):return {'station_id':station,'count':len(records),'predictions':records}
    monkeypatch.setenv('STATION_ID',station);monkeypatch.setattr(requests,'get',lambda *args,**kwargs:Response())
    app=AppTest.from_file('streamlit/app.py').run(timeout=30)
    assert not app.exception and len(app.metric)==4
    app.selectbox[0].select('7 Days').run()
    assert not app.exception
