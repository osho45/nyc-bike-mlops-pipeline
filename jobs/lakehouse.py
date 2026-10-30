"""Shared Citi Bike Spark/Iceberg medallion jobs. Requires Java + Iceberg runtime."""
from __future__ import annotations
import argparse,json,hashlib,tempfile
from pathlib import Path
from jobs.citibike import COLUMNS,STRINGS,checksum,read_chunks,month_bounds,validate_year_month
from jobs.ml_utils import load_config,create_spark_session,project_root
from jobs.source_versions import revision,publication_decision,month_lock

def warehouse_path(cfg):
    path=Path(cfg['catalog']['warehouse_path'])
    return path if path.is_absolute() else project_root()/path

def add_columns(spark,table,columns):
    present=set(spark.table(table).columns)
    missing=[f'{name} {kind}' for name,kind in columns.items() if name not in present]
    if missing:spark.sql(f'ALTER TABLE {table} ADD COLUMNS ({", ".join(missing)})')

def publication_state(frame):
    from pyspark.sql import functions as F
    columns=frame.columns
    if 'source_checksum' not in columns:return None
    expressions=[F.col('source_checksum').alias('sha256'),F.coalesce(F.col('source_revision'),F.lit(1)).alias('source_revision') if 'source_revision' in columns else F.lit(1).alias('source_revision')]
    for name in ['ingestion_run_id','supersedes_sha256']:
        if name in columns:expressions.append(F.col(name))
    rows=frame.select(*expressions).distinct().limit(2).collect()
    if len(rows)>1:raise ValueError('A published month contains mixed source identities')
    return rows[0].asDict() if rows else None

def validation_checkpoint(cfg,name):
    if not cfg.get('validation',{}).get('pause_at')==name:return
    if not sample_validation(cfg):raise ValueError('Crash checkpoints require isolated sample validation')
    import os,time
    path=warehouse_path(cfg).parent/'checkpoints';path.mkdir(exist_ok=True)
    (path/(name+'.ready')).write_text(json.dumps({'pid':os.getpid(),'checkpoint':name}))
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:time.sleep(.2)
    raise TimeoutError('Controlled crash checkpoint timed out without publication continuation')


def sample_validation(cfg):
    """Samples may only enter an explicitly isolated validation catalogue/warehouse."""
    if not cfg.get('validation',{}).get('sample_mode'):return False
    warehouse=Path(cfg['catalog']['warehouse_path'])
    if not warehouse.is_absolute():warehouse=project_root()/warehouse
    if cfg['catalog']['spark_catalog_name']!='citibike_validation' or not warehouse.resolve().is_relative_to((project_root()/'artifacts').resolve()):
        raise ValueError('Sample validation requires citibike_validation catalogue under repository artifacts/')
    return True


def main(stage):
    p=argparse.ArgumentParser();p.add_argument('--config',required=True)
    for flag in ['source_filepath','source_filename','run_id','ingestion_run_id']:p.add_argument('--'+flag)
    for flag in ['source_year','source_month','target_year','target_month','year','month']:p.add_argument('--'+flag,type=int)
    p.add_argument('--source_revision',type=int);p.add_argument('--supersedes_sha256');p.add_argument('--expected_checksum')
    a=p.parse_args();cfg=load_config(a.config);year=a.source_year or a.target_year or a.year;month=a.source_month or a.target_month or a.month;validate_year_month(year,month)
    if stage=='bronze' and a.source_filepath:
        path=Path(a.source_filepath);manifest=path.with_suffix(path.suffix+'.manifest.json')
        if ('.sample.' in path.name or (manifest.exists() and json.loads(manifest.read_text()).get('sample'))) and not sample_validation(cfg):raise ValueError('Sample sources cannot populate production monthly marts')
    spark=create_spark_session(cfg,'CitiBike-'+stage)
    try:
        catalog=cfg['catalog']['spark_catalog_name']
        for ns in ['audit','bronze','silver','quarantine','gold']:spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}')
        if stage=='bronze':bronze(spark,catalog,cfg,a,year,month)
        elif stage=='silver':silver(spark,catalog,cfg,a,year,month)
        else:gold(spark,catalog,year,month,stage,cfg)
    finally:spark.stop()


def bronze(spark,catalog,cfg,a,year,month):
    with month_lock(warehouse_path(cfg),year,month):return _bronze(spark,catalog,cfg,a,year,month)

def _bronze(spark,catalog,cfg,a,year,month):
    from pyspark.sql import functions as F
    if not a.source_filepath or not a.run_id:raise ValueError('Source path and run_id required')
    source=Path(a.source_filepath);metadata=source.with_suffix(source.suffix+'.manifest.json')
    bounded=sample_validation(cfg)
    if metadata.exists() and json.loads(metadata.read_text()).get('sample') and not bounded:raise ValueError('Sample sources cannot populate production monthly marts')
    if '.sample.' in source.name and not bounded:raise ValueError('Sample input is not a complete month')
    meta=json.loads(metadata.read_text()) if metadata.exists() else {}
    digest=checksum(source)
    if meta.get('sha256',digest)!=digest or getattr(a,'expected_checksum',None) not in (None,digest):raise ValueError('Source checksum does not match its manifest/event')
    rev=revision(getattr(a,'source_revision',None) if getattr(a,'source_revision',None) is not None else meta.get('source_revision',1))
    parent=getattr(a,'supersedes_sha256',None) or meta.get('supersedes_sha256')
    if meta.get('source_revision',rev)!=rev or meta.get('supersedes_sha256',parent)!=parent:raise ValueError('Source revision does not match its manifest')
    if rev>1 and (not parent or len(parent)!=64):raise ValueError('Corrected source requires its predecessor checksum')
    table=f'{catalog}.bronze.citibike_rides_raw';ledger=f'{catalog}.audit.ingestion_ledger'
    schema=', '.join(f'{c} STRING' for c in COLUMNS)
    spark.sql(f'CREATE TABLE IF NOT EXISTS {table} ({schema}, source_member STRING, source_filename STRING, source_checksum STRING, source_year INT, source_month INT, ingestion_run_id STRING, ingested_at_utc TIMESTAMP) USING iceberg PARTITIONED BY (source_year,source_month)')
    add_columns(spark,table,{'source_revision':'INT','supersedes_sha256':'STRING'})
    spark.sql(f'CREATE TABLE IF NOT EXISTS {ledger} (source_filename STRING, file_checksum STRING, ingestion_run_id STRING, status STRING, finished_at TIMESTAMP) USING iceberg')
    prior=spark.table(table).where((F.col('source_checksum')==digest)&(F.col('source_year')==year)&(F.col('source_month')==month)&(F.coalesce(F.col('source_revision'),F.lit(1))==rev)).select('ingestion_run_id').first()
    if prior:
        if not spark.table(ledger).where((F.col('file_checksum')==digest)&(F.col('ingestion_run_id')==prior['ingestion_run_id'])).limit(1).count():
            spark.createDataFrame([(source.name,digest,prior['ingestion_run_id'],'SUCCESS')],['source_filename','file_checksum','ingestion_run_id','status']).withColumn('finished_at',F.current_timestamp()).writeTo(ledger).append()
        print('LINEAGE_JSON:'+json.dumps({'ingestion_run_id':prior['ingestion_run_id'],'checksum':digest,'idempotent':True}));return
    staging=project_root()/'data/citibike/staging';staging.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=staging,prefix='normalize-') as temp:
        assert Path(temp).resolve().is_relative_to(staging.resolve())
        count=0
        for i,chunk in enumerate(read_chunks(source)):
            chunk.to_parquet(Path(temp)/f'part-{i:06}.parquet',index=False);count+=len(chunk)
            if bounded and count>20000:raise ValueError('Validation sample exceeds 20,000-row budget')
        if not count:raise ValueError('No source rows')
        normalized_files=list(Path(temp).glob('*.parquet'))
        normalized_bytes=sum(path.stat().st_size for path in normalized_files)
        frame=spark.read.parquet(temp)
        frame=frame.select(*[F.col(c).cast('string') for c in COLUMNS+['source_member']]).withColumn('source_filename',F.lit(a.source_filename or source.name)).withColumn('source_checksum',F.lit(digest)).withColumn('source_year',F.lit(year)).withColumn('source_month',F.lit(month)).withColumn('ingestion_run_id',F.lit(a.run_id)).withColumn('ingested_at_utc',F.current_timestamp())
        frame=frame.withColumn('source_revision',F.lit(rev)).withColumn('supersedes_sha256',F.lit(parent).cast('string'))
        frame.writeTo(table).append()
    spark.createDataFrame([(source.name,digest,a.run_id,'SUCCESS')],['source_filename','file_checksum','ingestion_run_id','status']).withColumn('finished_at',F.current_timestamp()).writeTo(ledger).append()
    result={'ingestion_run_id':a.run_id,'checksum':digest,'rows':count,'normalized_parquet_bytes':normalized_bytes,'normalized_files':len(normalized_files)}
    print('LINEAGE_JSON:'+json.dumps(result))
    return result


def silver(spark,catalog,cfg,a,year,month):
    with month_lock(warehouse_path(cfg),year,month):return _silver(spark,catalog,cfg,a,year,month)

def _silver(spark,catalog,cfg,a,year,month):
    from pyspark.sql import functions as F
    if not a.ingestion_run_id:raise ValueError('ingestion_run_id required')
    raw=spark.table(f'{catalog}.bronze.citibike_rides_raw').where((F.col('ingestion_run_id')==a.ingestion_run_id)&(F.col('source_year')==year)&(F.col('source_month')==month))
    if 'source_revision' not in raw.columns:raw=raw.withColumn('source_revision',F.lit(1))
    else:raw=raw.withColumn('source_revision',F.coalesce(F.col('source_revision'),F.lit(1)))
    if 'supersedes_sha256' not in raw.columns:raw=raw.withColumn('supersedes_sha256',F.lit(None).cast('string'))
    incoming=publication_state(raw)
    if incoming is None:raise ValueError('No source publication for requested month/run')
    target=f'{catalog}.silver.citibike_rides';current=None
    if spark.catalog.tableExists(target):current=publication_state(spark.table(target).where((F.col('source_year')==year)&(F.col('source_month')==month)))
    decision=publication_decision(current,incoming)
    if decision!='publish':
        print('PUBLICATION_JSON:'+json.dumps({**current,'decision':decision}));return
    total=raw.count()
    if not total:raise ValueError('No Bronze rows for the resolved run')
    for c in COLUMNS:raw=raw.withColumn(c,F.when(F.length(F.trim(F.col(c)))>0,F.trim(F.col(c))))
    ls,le=F.to_timestamp('started_at'),F.to_timestamp('ended_at');start=F.to_utc_timestamp(ls,'America/New_York');end=F.to_utc_timestamp(le,'America/New_York')
    def unambiguous(local,utc):
        return local.isNotNull() & (F.from_utc_timestamp(utc,'America/New_York')==local) & (F.from_utc_timestamp(utc+F.expr("INTERVAL 1 HOUR"),'America/New_York')!=local) & (F.from_utc_timestamp(utc-F.expr("INTERVAL 1 HOUR"),'America/New_York')!=local)
    duration=(F.unix_micros(end)-F.unix_micros(start))/F.lit(1000000.0)
    checks=[('invalid_or_ambiguous_time',unambiguous(ls,start)&unambiguous(le,end)),('outside_source_month',F.date_format(ls,'yyyy-MM')==f'{year}-{month:02}'),('missing_ride_id',F.col('ride_id').isNotNull()),('missing_start_station',F.col('start_station_id').isNotNull()),('invalid_membership',F.col('member_casual').isin('member','casual')),('unsupported_bike_type',F.col('rideable_type').isin('classic_bike','electric_bike','docked_bike')),('invalid_duration',(duration>0)&(duration<=86400))]
    for c in ['start_lat','start_lng','end_lat','end_lng']:
        value=F.col(c).cast('double');limit=90 if c.endswith('lat') else 180
        checks.append(('invalid_coordinates',F.col(c).isNull()|(value.isNotNull()&(~F.isnan(value))&(F.abs(value)<=limit))))
    condition=F.lit(True);failures=[]
    for label,check in checks:
        check=F.coalesce(check,F.lit(False));condition=condition&check
        failures.append(F.when(~check,F.lit(label)))
    # First failure wins, without embedding the previous expression twice per rule.
    # The recursive CASE tree exhausted the JVM heap during real Spark MERGE codegen.
    reason=F.coalesce(*failures)
    invalid=raw.where(~condition).withColumn('rejection_reason',reason).withColumn('quarantined_at',F.current_timestamp())
    quarantine=f'{catalog}.quarantine.citibike_rides'
    if not spark.catalog.tableExists(quarantine):invalid.limit(0).writeTo(quarantine).using('iceberg').create()
    add_columns(spark,quarantine,{'source_revision':'INT','supersedes_sha256':'STRING'})
    if 'supersedes_sha256' not in invalid.columns:invalid=invalid.withColumn('supersedes_sha256',F.lit(None).cast('string'))
    # Idempotent per-run quarantine replacement via MERGE, not an unbounded retry append.
    invalid.createOrReplaceTempView('rejected_citibike')
    spark.sql(f'MERGE INTO {quarantine} t USING rejected_citibike s ON t.ingestion_run_id=s.ingestion_run_id AND coalesce(t.ride_id,\'\')=coalesce(s.ride_id,\'\') AND t.source_member=s.source_member WHEN NOT MATCHED THEN INSERT *')
    selected=[start.alias(c) if c=='started_at' else end.alias(c) if c=='ended_at' else F.col(c).cast('double').alias(c) if c in ['start_lat','start_lng','end_lat','end_lng'] else F.col(c) for c in raw.columns]
    clean=raw.where(condition).select(*selected).withColumn('duration_seconds',(F.unix_micros('ended_at')-F.unix_micros('started_at'))/F.lit(1000000.0)).withColumn('published_at_utc',F.current_timestamp())
    # Exact repeated records collapse; conflicting duplicate ride payloads fail the audit.
    conflicts=clean.groupBy('ride_id').agg(F.countDistinct(F.sha2(F.to_json(F.struct(*[F.col(c) for c in COLUMNS])),256)).alias('variants')).where('variants > 1').limit(1).count()
    if conflicts:raise ValueError('Conflicting duplicate ride IDs; inspect Bronze before publishing')
    clean=clean.dropDuplicates(['ride_id']);accepted=clean.count();rejected=total-raw.where(condition).count()
    if not accepted or rejected/total>cfg.get('quality',{}).get('maximum_rejection_fraction',.35):raise ValueError(f'Quality gate rejected publication: {accepted} accepted, {rejected} rejected / {total}')
    candidate=f'{catalog}.silver.candidate_{hashlib.sha256(a.ingestion_run_id.encode()).hexdigest()[:16]}'
    clean.writeTo(candidate).using('iceberg').partitionedBy('source_year','source_month').createOrReplace()
    validation_checkpoint(cfg,'silver_before_publish')
    if not spark.catalog.tableExists(target):clean.limit(0).writeTo(target).using('iceberg').partitionedBy('source_year','source_month').create()
    add_columns(spark,target,{'source_revision':'INT','supersedes_sha256':'STRING'})
    # Complete-month source; atomic partition replacement also removes records removed in a corrected archive.
    spark.table(candidate).writeTo(target).overwritePartitions()
    validation_checkpoint(cfg,'silver_after_publish')
    print('PUBLICATION_JSON:'+json.dumps({**incoming,'decision':'publish'}))
    print(json.dumps({'source_rows':total,'accepted_unique_rides':accepted,'rejected_rows':rejected,'candidate_table':candidate,'published_table':target}))


def gold(spark,catalog,year,month,stage,cfg=None):
    cfg=cfg or load_config()
    with month_lock(warehouse_path(cfg),year,month):return _gold(spark,catalog,year,month,stage,cfg)

def _gold(spark,catalog,year,month,stage,cfg):
    from pyspark.sql import functions as F
    source=f'{catalog}.silver.citibike_rides';frame=spark.table(source).where((F.col('source_year')==year)&(F.col('source_month')==month))
    if not frame.limit(1).count():raise ValueError('No published source month; refuse to manufacture zero observations')
    state=publication_state(frame)
    name='station_hourly_utilization' if stage=='bi' else 'station_hourly_observations'
    table=f'{catalog}.gold.{name}'
    if spark.catalog.tableExists(table):
        existing=publication_state(spark.table(table).where((F.col('target_year')==year)&(F.col('target_month')==month)))
        if existing and existing['sha256']==state['sha256'] and existing['source_revision']==state['source_revision']:
            print('PUBLICATION_JSON:'+json.dumps({**state,'decision':'replay'}));return
    counts=frame.groupBy(F.col('start_station_id').alias('station_id'),F.date_trunc('hour','started_at').alias('station_hour_ts')).agg(F.count('*').alias('ride_count'),F.avg('duration_seconds').alias('avg_duration_seconds'),F.sum(F.when(F.col('member_casual')=='member',1).otherwise(0)).alias('member_rides'),F.sum(F.when(F.col('rideable_type')=='electric_bike',1).otherwise(0)).alias('electric_rides'))
    if stage=='bi':out=counts;name='station_hourly_utilization'
    else:
        start,end=month_bounds(year,month)
        if cfg and sample_validation(cfg):
            import pandas as pd
            bounds=frame.agg(F.min('started_at').alias('lo'),F.max('started_at').alias('hi')).first()
            start=pd.Timestamp(bounds['lo']).floor('h');end=pd.Timestamp(bounds['hi']).floor('h')+pd.Timedelta(hours=1)
        start=start.strftime('%Y-%m-%d %H:%M:%S');end=end.strftime('%Y-%m-%d %H:%M:%S')
        hours=spark.sql(f"SELECT explode(sequence(timestamp('{start}'),timestamp('{end}')-interval 1 hour,interval 1 hour)) station_hour_ts")
        stations=frame.select(F.col('start_station_id').alias('station_id')).distinct()
        out=stations.crossJoin(hours).join(counts.select('station_id','station_hour_ts','ride_count'),['station_id','station_hour_ts'],'left').fillna({'ride_count':0});name='station_hourly_observations'
    out=out.withColumn('target_year',F.lit(year)).withColumn('target_month',F.lit(month)).withColumn('source_checksum',F.lit(state['sha256'])).withColumn('source_revision',F.lit(state['source_revision']))
    table=f'{catalog}.gold.{name}'
    if not spark.catalog.tableExists(table):out.limit(0).writeTo(table).using('iceberg').partitionedBy('target_year','target_month').create()
    add_columns(spark,table,{'source_revision':'INT','source_checksum':'STRING'})
    out.writeTo(table).overwritePartitions();print('Published '+table)
    print('PUBLICATION_JSON:'+json.dumps({**state,'decision':'publish'}))
