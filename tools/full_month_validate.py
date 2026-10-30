"""Measured, resumable full-month ETL/audit/replay; never fits a model."""
import argparse,collections,gc,json,os,threading,time,zipfile
from pathlib import Path
from types import SimpleNamespace
import numpy as np,pandas as pd,yaml
from jobs.citibike import COLUMNS,read_chunks,clean_rides,checksum,month_bounds
from jobs.ml_utils import project_root,load_config,create_spark_session
from jobs.lakehouse import bronze,silver,gold,publication_state
from jobs.ml_features import REQUIRED_FEATURES,build_training_features

class MemoryMonitor:
    def __init__(self,path):self.path=path;self.stop=threading.Event();self.peak_working=0;self.peak_current=0
    def run(self):
        base=Path('/sys/fs/cgroup')
        with self.path.open('w') as stream:
            stream.write('elapsed_seconds,current_bytes,working_set_bytes\n');start=time.monotonic()
            while not self.stop.is_set():
                try:
                    current=int((base/'memory.current').read_text());stats=dict(line.split() for line in (base/'memory.stat').read_text().splitlines());working=max(0,current-int(stats.get('inactive_file',0)))
                    self.peak_current=max(self.peak_current,current);self.peak_working=max(self.peak_working,working)
                    stream.write(f'{time.monotonic()-start:.3f},{current},{working}\n');stream.flush()
                except (OSError,ValueError):pass
                self.stop.wait(.5)
    def finish(self):
        base=Path('/sys/fs/cgroup');result={'sample_interval_seconds':.5,'peak_sampled_working_set_bytes':self.peak_working,'peak_sampled_current_bytes':self.peak_current}
        for name in ['memory.peak','memory.max']:
            if (base/name).exists():result[name]=int((base/name).read_text().strip())
        if (base/'memory.events').exists():result['memory.events']={k:int(v) for k,v in (line.split() for line in (base/'memory.events').read_text().splitlines())}
        return result

def reference(source,year,month):
    counts=collections.Counter();reasons=collections.Counter();ids=set();stations=set();total=valid=rejected=0
    for chunk in read_chunks(source):
        total+=len(chunk);counts.update(chunk.source_member.value_counts().to_dict());good,bad=clean_rides(chunk,year,month)
        valid+=len(good);rejected+=len(bad);reasons.update(bad.rejection_reason.value_counts().to_dict());ids.update(good.ride_id);stations.update(good.start_station_id)
        if total%250000==0:print(f'REFERENCE rows={total}',flush=True)
    result={'source_rows':total,'member_rows':dict(counts),'accepted_before_dedup':valid,'accepted_unique':len(ids),'accepted_duplicate_rows':valid-len(ids),'rejected':rejected,'reasons':dict(reasons),'stations':len(stations)}
    del ids,stations;gc.collect();return result

def table_state(spark,catalog):
    result={}
    for suffix in ['bronze.citibike_rides_raw','silver.citibike_rides','quarantine.citibike_rides','gold.station_hourly_utilization','gold.station_hourly_observations','audit.ingestion_ledger']:
        table=f'{catalog}.{suffix}'
        snapshot=spark.sql(f'SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1').first()
        result[suffix]={'rows':spark.table(table).count(),'snapshot_id':snapshot[0] if snapshot else None,'schema':spark.table(table).schema.jsonValue()}
    return result

def audit(spark,catalog,year,month,expected,month_only=False):
    from pyspark.sql import functions as F,Window
    b=spark.table(f'{catalog}.bronze.citibike_rides_raw');s=spark.table(f'{catalog}.silver.citibike_rides');q=spark.table(f'{catalog}.quarantine.citibike_rides');bi=spark.table(f'{catalog}.gold.station_hourly_utilization');ml=spark.table(f'{catalog}.gold.station_hourly_observations')
    if month_only:
        b=b.where((F.col('source_year')==year)&(F.col('source_month')==month));s=s.where((F.col('source_year')==year)&(F.col('source_month')==month));q=q.where((F.col('source_year')==year)&(F.col('source_month')==month));bi=bi.where((F.col('target_year')==year)&(F.col('target_month')==month));ml=ml.where((F.col('target_year')==year)&(F.col('target_month')==month))
    assert all(b.schema[name].dataType.simpleString()=='string' for name in COLUMNS)
    assert s.schema['started_at'].dataType.simpleString()=='timestamp' and s.schema['duration_seconds'].dataType.simpleString()=='double'
    assert s.schema['start_station_id'].dataType.simpleString()=='string' and ml.schema['station_id'].dataType.simpleString()=='string'
    actual_members={r['source_member']:r['count'] for r in b.groupBy('source_member').count().collect()};assert actual_members==expected['member_rows']
    silver_rows=s.count();q_rows=q.count();assert silver_rows==expected['accepted_unique'] and q_rows==expected['rejected']
    assert s.select('ride_id').distinct().count()==silver_rows
    assert s.where(F.col('ride_id').isNull()|F.col('start_station_id').isNull()|F.col('duration_seconds').isNull()|F.isnan('duration_seconds')|(F.col('duration_seconds')<=0)).count()==0
    reasons={r['rejection_reason']:r['count'] for r in q.groupBy('rejection_reason').count().collect()};assert reasons==expected['reasons']
    start,end=month_bounds(year,month);hours=int((end-start)/pd.Timedelta(hours=1))
    assert s.where((F.col('started_at')<F.lit(start.to_pydatetime()))|(F.col('started_at')>=F.lit(end.to_pydatetime()))).count()==0
    station_count=s.select('start_station_id').distinct().count();assert station_count==expected['stations']
    assert ml.count()==station_count*hours
    assert ml.groupBy('station_id','station_hour_ts').count().where('count != 1').count()==0
    assert bi.groupBy('station_id','station_hour_ts').count().where('count != 1').count()==0
    station_hours=ml.groupBy('station_id').agg(F.count('*').alias('n'),F.countDistinct('station_hour_ts').alias('unique_hours'),F.min('station_hour_ts').alias('first'),F.max('station_hour_ts').alias('last'))
    assert station_hours.where((F.col('n')!=hours)|(F.col('unique_hours')!=hours)|(F.col('first')!=F.lit(start.to_pydatetime()))|(F.col('last')!=F.lit((end-pd.Timedelta(hours=1)).to_pydatetime()))).count()==0
    assert ml.where(F.col('ride_count').isNull()|(F.col('ride_count')<0)).count()==0
    bi_sum=bi.agg(F.sum('ride_count')).first()[0];ml_sum=ml.agg(F.sum('ride_count')).first()[0];assert bi_sum==ml_sum==silver_rows
    assert bi.count()==ml.where('ride_count > 0').count()
    mismatch=bi.select('station_id','station_hour_ts',F.col('ride_count').alias('b')).join(ml.select('station_id','station_hour_ts',F.col('ride_count').alias('m')),['station_id','station_hour_ts'],'full').where(F.coalesce(F.col('b'),F.lit(0))!=F.coalesce(F.col('m'),F.lit(0))).count();assert mismatch==0
    identities=[publication_state(frame) for frame in [s,bi,ml]];assert len({(x['sha256'],x['source_revision']) for x in identities})==1
    raw_nonnull=b.where(F.col('ride_id').isNotNull());raw_duplicates=raw_nonnull.count()-raw_nonnull.select('ride_id').distinct().count()
    print('PASS layer schemas, counts, quarantine, duplicates and dense station/hour coverage',flush=True)
    # Independent all-station feature oracle; Gold ML remains observations, not trained features.
    w=Window.partitionBy('station_id').orderBy('station_hour_ts');features=ml.withColumn('_row',F.row_number().over(w))
    for lag in [1,2,24,168]:features=features.withColumn(f'lag_{lag}h',F.lag('ride_count',lag).over(w))
    for width in [6,24]:
        previous=w.rowsBetween(-width,-1);features=features.withColumn(f'rolling_{width}h_mean',F.avg('ride_count').over(previous)).withColumn(f'rolling_{width}h_std',F.stddev_samp('ride_count').over(previous))
    local=F.from_utc_timestamp(F.col('station_hour_ts'),'America/New_York');hour=F.hour(local);day=(F.dayofweek(local)+5)%7
    for name,value,period in [('hour_of_day',hour,24),('day_of_week',day,7)]:
        features=features.withColumn(name+'_sin',F.sin(value*F.lit(2*np.pi/period))).withColumn(name+'_cos',F.cos(value*F.lit(2*np.pi/period)))
    features=features.withColumn('is_weekend',F.when(day.isin(5,6),1).otherwise(0));complete=features.where(F.col('_row')>168)
    invalid=[F.sum(F.when(F.col(c).isNull()|F.isnan(c)|(F.abs(F.col(c))==float('inf')),1).otherwise(0)).alias(c) for c in REQUIRED_FEATURES]
    stats=complete.agg(F.count('*').alias('rows'),*invalid).first().asDict();assert stats['rows']==station_count*(hours-168) and all(stats[c]==0 for c in REQUIRED_FEATURES)
    first=s.select(F.col('start_station_id').alias('station_id')).distinct().orderBy('station_id').first()[0]
    last=s.select(F.col('start_station_id').alias('station_id')).distinct().orderBy(F.col('station_id').desc()).first()[0]
    busiest=bi.groupBy('station_id').agg(F.sum('ride_count').alias('n')).orderBy(F.col('n').desc()).first()['station_id'];parity={}
    for station in dict.fromkeys([first,last,busiest]):
        history=ml.where(F.col('station_id')==station).select('station_id','station_hour_ts','ride_count').orderBy('station_hour_ts').toPandas();reference_frame=build_training_features(history)
        observed=complete.where(F.col('station_id')==station).select('station_hour_ts',*REQUIRED_FEATURES).orderBy('station_hour_ts').toPandas()
        assert np.allclose(reference_frame[REQUIRED_FEATURES].values,observed[REQUIRED_FEATURES].values,rtol=1e-6,atol=1e-6)
        parity[station]={'rows':len(observed),'max_absolute_delta':float(np.max(np.abs(reference_frame[REQUIRED_FEATURES].values-observed[REQUIRED_FEATURES].values)))}
    print('PASS all-station feature completeness and shared NumPy parity',flush=True)
    return {'silver_rows':silver_rows,'quarantine_rows':q_rows,'quarantine_reasons':reasons,'bronze_duplicate_nonnull_ids':raw_duplicates,'stations':station_count,'hours_per_station':hours,'gold_bi_rows':bi.count(),'gold_ml_rows':station_count*hours,'gold_bi_ride_sum':int(bi_sum),'gold_ml_ride_sum':int(ml_sum),'feature_rows_after_warmup':stats['rows'],'intentional_warmup_rows':station_count*168,'feature_invalid_counts':{c:stats[c] for c in REQUIRED_FEATURES},'numpy_parity_stations':parity}

def main():
    p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--output',required=True);p.add_argument('--config',default='config/full_month_validation.yml');p.add_argument('--year',type=int,required=True);p.add_argument('--month',type=int,required=True);p.add_argument('--resume',action='store_true');a=p.parse_args()
    root=project_root().resolve();out=Path(a.output).resolve();source=Path(a.source).resolve()
    if not out.is_relative_to(root/'artifacts'):raise ValueError('Output must be isolated under artifacts/')
    meta=json.loads(source.with_suffix(source.suffix+'.manifest.json').read_text());digest=checksum(source)
    if meta.get('dataset')!='citibike' or meta.get('sample') is not False or meta['sha256']!=digest or (meta['source_year'],meta['source_month'])!=(a.year,a.month):raise ValueError('Requires verified complete monthly source manifest')
    cfg=load_config(a.config)
    if cfg['catalog']['spark_catalog_name']!='citibike_fullmonth' or cfg.get('validation',{}).get('sample_mode'):raise ValueError('Isolated full-month profile required')
    cfg['catalog']['warehouse_path']=str(out/'warehouse')
    if not a.resume:out.mkdir(parents=True,exist_ok=False)
    elif not (out/'validation.json').exists():raise ValueError('No resumable validation report')
    report=json.loads((out/'validation.json').read_text()) if a.resume else {'source_sha256':digest,'source_url':meta['source_url'],'source_bytes':source.stat().st_size,'year':a.year,'month':a.month,'stages':{},'attempts':[],'model_training':False,'complete':False}
    if report['source_sha256']!=digest:raise ValueError('Resume source changed')
    (out/'config.yml').write_text(yaml.safe_dump(cfg));attempt=len(report['attempts'])+1;monitor=MemoryMonitor(out/f'memory-attempt-{attempt}.csv');thread=threading.Thread(target=monitor.run,daemon=True);thread.start();started=time.monotonic();spark=None
    def save():
        temporary=out/'validation.json.tmp';temporary.write_text(json.dumps(report,indent=2,default=str));temporary.replace(out/'validation.json')
    def phase(name,fn):
        print('BEGIN '+name,flush=True);t=time.monotonic();value=fn();report['stages'][name]={'seconds':round(time.monotonic()-t,3),'status':'complete'};save();print('END '+name+' '+str(report['stages'][name]),flush=True);return value
    save()
    try:
        if 'reference' not in report:report['reference']=phase('source_reference',lambda:reference(source,a.year,a.month));save()
        with zipfile.ZipFile(source) as archive:
            csv_info={item.filename:{'uncompressed_bytes':item.file_size,'compressed_bytes':item.compress_size} for item in archive.infolist() if item.filename.endswith('.csv') and '__MACOSX' not in item.filename}
        assert set(csv_info)==set(report['reference']['member_rows']);report['archive_members']=csv_info
        spark=create_spark_session(cfg,'ControlledFullMonthValidation');catalog=cfg['catalog']['spark_catalog_name']
        for ns in ['audit','bronze','silver','quarantine','gold']:spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}')
        run_id=f'full-month-{a.year}{a.month:02}-01'
        args=SimpleNamespace(source_filepath=str(source),source_filename=source.name,run_id=run_id,ingestion_run_id=run_id,source_revision=meta.get('source_revision',1),supersedes_sha256=meta.get('supersedes_sha256'),expected_checksum=digest)
        stages={'bronze':lambda:bronze(spark,catalog,cfg,args,a.year,a.month),'silver':lambda:silver(spark,catalog,cfg,args,a.year,a.month),'gold_bi':lambda:gold(spark,catalog,a.year,a.month,'bi',cfg),'gold_ml':lambda:gold(spark,catalog,a.year,a.month,'ml',cfg)}
        for name,fn in stages.items():
            if report['stages'].get(name,{}).get('status')!='complete':
                info=phase(name,fn)
                if name=='bronze' and info:report['bronze_ingest']=info;save()
        report['audit']=phase('audit',lambda:audit(spark,catalog,a.year,a.month,report['reference']));save()
        before=table_state(spark,catalog);report['before_replay']=before;save()
        for name,fn in stages.items():phase('replay_'+name,fn)
        after=table_state(spark,catalog);assert before==after,'Replay changed counts/snapshots/schemas';report['after_replay']=after
        report['idempotent_replay']=True;report['data_volumes']={}
        warehouse=out/'warehouse'
        for directory in sorted(warehouse.iterdir()):
            if directory.is_dir() and not directory.name.startswith('.'):
                report['data_volumes'][directory.name]=sum(path.stat().st_size for path in directory.rglob('*') if path.is_file())
        report['warehouse_bytes']=sum(report['data_volumes'].values());report['complete']=True;report.pop('error',None)
        print('PASS full-month ETL, audit and exact replay',flush=True)
    except Exception as error:report['error']=repr(error);report['complete']=False;raise
    finally:
        if spark:
            try:spark.stop()
            except Exception:pass
        monitor.stop.set();thread.join(timeout=3)
        report['attempts'].append({'attempt':attempt,'seconds':round(time.monotonic()-started,3),'memory':monitor.finish()});save()

if __name__=='__main__':main()
