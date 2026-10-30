import importlib.util
import pytest
import psycopg2
from fastapi.testclient import TestClient

def module():
    spec=importlib.util.spec_from_file_location('pool_test_api','fastapi/main.py');app=importlib.util.module_from_spec(spec);spec.loader.exec_module(app);return app

class Connection:
    def __init__(self,stale=False,query_failure=False):self.closed=False;self.stale=stale;self.query_failure=query_failure
    def cursor(self,**kwargs):return self
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def execute(self,query,*args):
        if self.stale or (self.query_failure and 'station_hourly' in query):
            self.closed=True;raise psycopg2.OperationalError('server restarted')
    def rollback(self):
        if self.closed:raise psycopg2.InterfaceError('connection already closed')

class Pool:
    def __init__(self,connections):self.connections=connections;self.discarded=[]
    def getconn(self):return self.connections.pop(0)
    def putconn(self,connection,close=False):
        if close:self.discarded.append(connection)
        else:self.connections.append(connection)
    def closeall(self):pass

def test_stale_socket_discarded_before_request_without_500(monkeypatch):
    app=module();stale=Connection(stale=True);pool=Pool([stale,Connection()])
    monkeypatch.setenv('PREDICTION_CACHE_BACKEND','postgres');monkeypatch.setattr(app,'ThreadedConnectionPool',lambda *args:pool)
    with TestClient(app.app) as client:assert client.get('/ready').status_code==200
    assert stale in pool.discarded

def test_closed_rollback_cannot_mask_503_and_next_request_recovers(monkeypatch):
    app=module();broken=Connection(query_failure=True);pool=Pool([broken,Connection()])
    monkeypatch.setenv('PREDICTION_CACHE_BACKEND','postgres');monkeypatch.setattr(app,'ThreadedConnectionPool',lambda *args:pool)
    with TestClient(app.app) as client:
        assert client.get('/ready').status_code==503
        assert client.get('/ready').status_code==200
    assert broken in pool.discarded

def test_pool_exhaustion_returns_503(monkeypatch):
    app=module();pool=Pool([])
    def busy():raise app.PoolError('pool exhausted')
    pool.getconn=busy
    monkeypatch.setenv('PREDICTION_CACHE_BACKEND','postgres');monkeypatch.setattr(app,'ThreadedConnectionPool',lambda *args:pool)
    with TestClient(app.app) as client:assert client.get('/ready').status_code==503
