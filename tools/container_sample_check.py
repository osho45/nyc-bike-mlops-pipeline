"""Bounded genuine-source integration through real Spark, Hadoop Iceberg and PostgreSQL.

Never publishes production tables/models; invoked manually or by the validation DAG.
"""
import argparse,json,os,time
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import yaml
from jobs.ml_utils import project_root,load_config,create_spark_session,write_json
from jobs.lakehouse import bronze,silver,gold,sample_validation
from jobs.citibike import checksum


def prepare_config(output):
    output=Path(output).resolve();root=project_root().resolve()
    if not output.is_relative_to(root/'artifacts'):raise ValueError('Validation output must be under repository artifacts/')
    cfg=load_config();cfg['catalog'].update(spark_catalog_name='citibike_validation',warehouse_path=str(output/'warehouse'))
    cfg['validation']={'sample_mode':True,'counts_are_population_demand':False}
    cfg['ml']['models_dir']=str(output/'models')
    cfg['spark'].update({'spark.master':'local[1]','spark.driver.memory':'512m','spark.sql.shuffle.partitions':2,'spark.default.parallelism':1,'spark.sql.adaptive.enabled':'false'})
    assert sample_validation(cfg)
    return cfg


def validate(source,output):
    import joblib,numpy as np,psycopg2
    from psycopg2 import sql
    from jobs.persistence import postgres_dsn
    from jobs.ml_features import prepare_model_frames
    from jobs.ml_experiments import make_logistic_regression
    source=Path(source).resolve();output=Path(output).resolve();cfg=prepare_config(output)
    manifest=json.loads(source.with_suffix(source.suffix+'.manifest.json').read_text())
    if manifest.get('dataset')!='citibike' or manifest.get('sample') is not True or checksum(source)!=manifest['sha256']:raise ValueError('Requires genuine-source sample manifest with matching checksum')
    year,month=int(manifest['source_year']),int(manifest['source_month'])
    output.mkdir(parents=True,exist_ok=False);config=output/'config.yml';config.write_text(yaml.safe_dump(cfg))
    started=time.monotonic();report={'scope':'isolated genuine-source sample; not population/model accuracy','source_sha256':manifest['sha256'],'source_url':manifest['source_url'],'checks':[],'complete':False}
    def passed(label):report['checks'].append(label);print('PASS '+label,flush=True)
    spark=None
    try:
        spark=create_spark_session(cfg,'CitiBikeBoundedContainerValidation');catalog='citibike_validation'
        for ns in ['audit','bronze','silver','quarantine','gold','predictions']:spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}')
        args=SimpleNamespace(source_filepath=str(source),source_filename=source.name,run_id='bounded-source',ingestion_run_id='bounded-source')
        bronze(spark,catalog,cfg,args,year,month)
        raw=spark.table(f'{catalog}.bronze.citibike_rides_raw').count()
        bronze(spark,catalog,cfg,args,year,month)
        assert spark.table(f'{catalog}.bronze.citibike_rides_raw').count()==raw
        passed('Spark Bronze ingestion and checksum retry idempotency')
        silver(spark,catalog,cfg,args,year,month)
        gold(spark,catalog,year,month,'bi',cfg);gold(spark,catalog,year,month,'ml',cfg)
        accepted=spark.table(f'{catalog}.silver.citibike_rides').count();rejected=spark.table(f'{catalog}.quarantine.citibike_rides').count()
        bi=spark.sql(f'SELECT sum(ride_count) n FROM {catalog}.gold.station_hourly_utilization').first()['n']
        ml=spark.table(f'{catalog}.gold.station_hourly_observations')
        assert raw==accepted+rejected and accepted==bi==ml.agg({'ride_count':'sum'}).first()[0]
        passed('Real Spark Silver/quarantine and both Iceberg Gold marts reconcile')
        from jobs.citibike import read_chunks,clean_rides
        reference,reference_bad=clean_rides(pd.concat(read_chunks(source),ignore_index=True),year,month)
        actual=spark.table(f'{catalog}.silver.citibike_rides').select('ride_id','duration_seconds').toPandas().set_index('ride_id')
        expected=reference.set_index('ride_id').duration_seconds
        reasons={row['rejection_reason']:row['count'] for row in spark.table(f'{catalog}.quarantine.citibike_rides').groupBy('rejection_reason').count().collect()}
        assert set(actual.index)==set(expected.index)
        assert reasons==reference_bad.rejection_reason.value_counts().to_dict()
        assert np.allclose(actual.duration_seconds.reindex(expected.index),expected,rtol=0,atol=0.000001)
        passed('Spark/reference accepted IDs, rejection reasons and fractional duration parity')
        report.update(source_rows=raw,accepted_rides=accepted,rejected_rows=rejected,gold_ml_rows=ml.count())
        station=spark.sql(f'SELECT station_id,sum(ride_count) n FROM {catalog}.gold.station_hourly_observations GROUP BY station_id ORDER BY n DESC LIMIT 1').first()['station_id']
        from jobs.ml_features import load_station_history
        history=load_station_history(ml,str(station));train,val,test,thresholds,features=prepare_model_frames(history)
        model=make_logistic_regression();model.fit(train[features],train.demand_class)
        cutoff=test.station_hour_ts.iloc[0]-pd.Timedelta(hours=1)
        models=Path(cfg['ml']['models_dir']);models.mkdir()
        joblib.dump(model,models/'champion_model.pkl')
        write_json(models/'champion_metadata.json',{'dataset':'citibike','validation_only':True,'target':'sample-count-smoke-only','station_id':str(station),'features':features,'q25':thresholds.q25,'q75':thresholds.q75,'training_cutoff_utc':str(train.station_hour_ts.max()),'model_name':'validation-only-logistic','model_version':'not-for-deployment','penalty_version':'asymmetric-v1','mlflow_run_id':'bounded-validation'})
        report.update(station_id=str(station),feature_window_end_utc=str(cutoff),prediction_hour_utc=str(cutoff+pd.Timedelta(hours=1)),training_rows=len(train))
        passed('Bounded training-only logistic smoke fit; no model quality claim')
        spark.stop();spark=None
        from jobs.ml_batch_score import run_batch_score
        run_batch_score(str(config),str(cutoff),0)
        passed('Actual batch scoring writes prediction through Spark/Iceberg MERGE')
        database='citibike_validation_'+output.name.replace('-','_').lower()
        if not database.replace('_','').isalnum() or len(database)>63:raise ValueError('Unsafe validation database name')
        connection=psycopg2.connect(postgres_dsn())
        try:
            connection.autocommit=True
            with connection.cursor() as cur:cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        finally:connection.close()
        os.environ['POSTGRES_CACHE_DB']=database
        from jobs.sync_to_postgres import sync_predictions_to_postgres
        sync_predictions_to_postgres(str(config),str(cutoff),0)
        sync_predictions_to_postgres(str(config),str(cutoff),0)
        with psycopg2.connect(postgres_dsn()) as connection:
            with connection.cursor() as cur:
                cur.execute('SELECT count(*),min(station_id),sum(coalesce(prob_class_0,0)+coalesce(prob_class_1,0)+coalesce(prob_class_2,0)) FROM station_hourly_demand_predictions')
                count,stored,probabilities=cur.fetchone()
                assert count==1 and stored==str(station)
                assert np.isfinite(probabilities) and np.isclose(probabilities,1)
        report['postgres_database']=database
        passed('Actual Hadoop metadata read and PostgreSQL repeated upsert preserve one text-key prediction')
        report['complete']=True
    except Exception as error:
        report['error']=repr(error);raise
    finally:
        if spark:spark.stop()
        report['duration_seconds']=round(time.monotonic()-started,2)
        (output/'validation.json').write_text(json.dumps(report,indent=2,default=str))
    return report


def main():
    p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    print(json.dumps(validate(a.source,a.output),indent=2),flush=True)

if __name__=='__main__':main()
