"""Citi Bike serving API. PostgreSQL readiness is separate from process liveness."""
from contextlib import asynccontextmanager,contextmanager
from datetime import datetime,timezone
import logging,os
from threading import Lock
from typing import Annotated
import pandas as pd
import psycopg2
from psycopg2.pool import ThreadedConnectionPool,PoolError
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI,Depends,HTTPException,Query

pool=None
pool_lock=Lock()

def return_connection(active_pool,connection,discard=False):
    try:
        if not connection.closed:connection.rollback()
    except psycopg2.Error:discard=True
    try:active_pool.putconn(connection,close=discard or bool(connection.closed))
    except PoolError:pass # Pool may have been closed during application shutdown.

def dsn():
    return psycopg2.extensions.make_dsn(host=os.getenv('POSTGRES_CACHE_HOST','localhost'),port=os.getenv('POSTGRES_CACHE_PORT','5543'),dbname=os.getenv('POSTGRES_CACHE_DB','citibike_cache'),user=os.getenv('POSTGRES_CACHE_USER','cache_user'),password=os.getenv('POSTGRES_CACHE_PASSWORD'),connect_timeout=3)

@asynccontextmanager
async def lifespan(app):
    global pool
    try:
        if os.getenv('PREDICTION_CACHE_BACKEND')!='duckdb':pool=ThreadedConnectionPool(1,5,dsn())
    except psycopg2.Error:logging.warning('PostgreSQL unavailable: readiness remains 503; no fake results served')
    yield
    if pool:pool.closeall();pool=None

app=FastAPI(title='NYC Citi Bike Station Demand',lifespan=lifespan)

class DuckCursor:
    def __init__(self,connection):self.connection=connection
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def execute(self,query,params=()):
        self.result=self.connection.execute(query.replace('%s','?'),params);return self
    def fetchall(self):
        columns=[d[0] for d in self.result.description];return [dict(zip(columns,r)) for r in self.result.fetchall()]
    def fetchone(self):
        row=self.result.fetchone();return dict(zip([d[0] for d in self.result.description],row)) if row else None

class DuckConnection:
    def __init__(self,connection):self.connection=connection
    def cursor(self,**kwargs):return DuckCursor(self.connection)

def get_db():
    global pool
    if os.getenv('PREDICTION_CACHE_BACKEND')=='duckdb':
        import duckdb
        try:connection=duckdb.connect(os.environ['DUCKDB_CACHE_PATH'],read_only=True)
        except (duckdb.Error,KeyError):raise HTTPException(503,'Local DuckDB cache unavailable')
        try:yield DuckConnection(connection)
        except duckdb.Error:raise HTTPException(503,'Local prediction table unavailable')
        finally:connection.close()
        return
    if pool is None:
        with pool_lock:
            if pool is None:
                try:pool=ThreadedConnectionPool(1,5,dsn())
                except psycopg2.Error:raise HTTPException(503,'PostgreSQL unavailable')
    active_pool=pool
    # Validate a borrowed socket before executing a request. A DB restart can
    # leave pooled sockets stale even after PostgreSQL is healthy again.
    for attempt in range(2):
        connection=None
        try:
            connection=active_pool.getconn()
            with connection.cursor() as cursor:cursor.execute('SELECT 1')
            connection.rollback();break
        except PoolError:raise HTTPException(503,'Prediction connection pool busy')
        except psycopg2.Error:
            if connection is not None:return_connection(active_pool,connection,discard=True)
            if attempt:raise HTTPException(503,'PostgreSQL unavailable')
    try:yield connection
    except psycopg2.Error:
        raise HTTPException(503,'Prediction store unavailable')
    finally:return_connection(active_pool,connection)

def clean_record(record):
    result={}
    for k,v in record.items():
        if v is None or (not isinstance(v,(list,dict)) and pd.isna(v)):result[k]=None
        elif isinstance(v,(datetime,pd.Timestamp)):result[k]=pd.Timestamp(v).isoformat()
        else:result[k]=v.item() if hasattr(v,'item') else v
    return result

@app.get('/health')
def health():return {'status':'ok','dataset':'citibike','target':'next_hour_station_ride_demand_class'}

@app.get('/ready')
def ready(db=Depends(get_db)):
    with db.cursor() as cur:cur.execute('SELECT 1 FROM station_hourly_demand_predictions LIMIT 1')
    return {'status':'ready'}

@app.get('/stations')
def stations(db=Depends(get_db)):
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT DISTINCT station_id FROM station_hourly_demand_predictions ORDER BY station_id');rows=cur.fetchall()
    return {'stations':[str(r['station_id']) for r in rows]}

@app.get('/predictions/{station_id}')
def predictions(station_id:str,limit:Annotated[int,Query(ge=1,le=10000)]=1000,lookback_days:Annotated[int|None,Query(ge=1,le=366)]=None,before:str|None=None,through:str|None=None,db=Depends(get_db)):
    def utc(value):
        try:
            ts=pd.Timestamp(value)
            if pd.isna(ts):raise ValueError('Invalid timestamp')
            return ts.tz_localize('UTC') if ts.tzinfo is None else ts.tz_convert('UTC')
        except (ValueError,TypeError):raise HTTPException(422,'Provide an ISO timestamp for pagination')
    before_ts=utc(before) if before is not None else None
    through_ts=utc(through) if through is not None else None
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        clause='station_id=%s';params=[station_id]
        if through_ts is not None:clause+=' AND prediction_hour_ts<=%s';params.append(through_ts.to_pydatetime())
        cur.execute('SELECT MAX(prediction_hour_ts) AS latest FROM station_hourly_demand_predictions WHERE '+clause,params)
        latest=cur.fetchone()['latest']
        if latest is None:return {'dataset':'citibike','station_id':station_id,'count':0,'total_count':0,'predictions':[],'next_before':None,'latest_prediction_hour_ts':None}
        latest=utc(latest);cutoff=latest-pd.Timedelta(days=lookback_days) if lookback_days else None
        # A pinned anchor keeps pagination stable while newer predictions arrive.
        clause='station_id=%s AND prediction_hour_ts<=%s';params=[station_id,latest.to_pydatetime()]
        if cutoff is not None:clause+=' AND prediction_hour_ts>%s';params.append(cutoff.to_pydatetime())
        cur.execute('SELECT COUNT(*) AS total_count FROM station_hourly_demand_predictions WHERE '+clause,params);total=int(cur.fetchone()['total_count'])
        if before_ts is not None:clause+=' AND prediction_hour_ts<%s';params=params+[before_ts.to_pydatetime()]
        cur.execute('SELECT * FROM station_hourly_demand_predictions WHERE '+clause+' ORDER BY prediction_hour_ts DESC LIMIT %s',params+[limit+1]);page=cur.fetchall()
    more=len(page)>limit;rows=[clean_record(r) for r in reversed(page[:limit])]
    return {'dataset':'citibike','station_id':station_id,'count':len(rows),'total_count':total,'predictions':rows,'next_before':rows[0]['prediction_hour_ts'] if more else None,'latest_prediction_hour_ts':latest.isoformat(),'window_start_exclusive':cutoff.isoformat() if cutoff is not None else None,'lookback_days':lookback_days}

@app.get('/predictions/{station_id}/{prediction_hour_ts}')
def prediction(station_id:str,prediction_hour_ts:str,db=Depends(get_db)):
    try:
        ts=pd.Timestamp(prediction_hour_ts)
        ts=ts.tz_localize('UTC') if ts.tzinfo is None else ts.tz_convert('UTC')
        if ts!=ts.floor('h'):raise ValueError('Timestamp is not hour aligned')
    except (ValueError,TypeError):raise HTTPException(422,'Provide an hour-aligned UTC timestamp')
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT * FROM station_hourly_demand_predictions WHERE station_id=%s AND prediction_hour_ts=%s',(station_id,ts.to_pydatetime()));row=cur.fetchone()
    if row is None:raise HTTPException(404,'Prediction not found')
    return clean_record(row)
