"""One-month data audit plus real previous-month feature-boundary checks; no fit."""
import argparse,json,time
from pathlib import Path
import numpy as np,pandas as pd
from tools.monthly_ingestion import ROOT
from tools.full_month_queue import isolated_config
from tools.full_month_validate import reference,audit
from jobs.ml_utils import create_spark_session
from jobs.citibike import checksum,month_bounds
from jobs.monthly_archives import atomic_json
from jobs.ml_features import build_training_features,REQUIRED_FEATURES
from jobs.year_features import spark_past_features

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--year',type=int,required=True);p.add_argument('--month',type=int,required=True);a=p.parse_args();cfg=isolated_config(ROOT)
    source=ROOT/'incoming'/f'{a.year}{a.month:02d}-citibike-tripdata.zip';started=time.monotonic();expected=reference(source,a.year,a.month)
    spark=create_spark_session(cfg,'MonthlyPublicationAudit');catalog=cfg['catalog']['spark_catalog_name']
    try:
        metrics=audit(spark,catalog,a.year,a.month,expected,month_only=True)
        from pyspark.sql import functions as F
        # For the pilot only, read the prior verified January warehouse without writing it.
        current=spark.table(f'{catalog}.gold.station_hourly_observations');start,end=month_bounds(a.year,a.month)
        history=current.where((F.col('station_hour_ts')>=F.lit((start-pd.Timedelta(hours=168)).to_pydatetime()))&(F.col('station_hour_ts')<F.lit(end.to_pydatetime()))).select('station_id','station_hour_ts','ride_count')
        prior_path=cfg['ingestion'].get('history_warehouse')
        if (a.year,a.month)==(2024,2) and prior_path and Path(prior_path).exists() and not current.where(F.col('station_hour_ts')<F.lit(start.to_pydatetime())).limit(1).count():
            spark.conf.set('spark.sql.catalog.prior_verified','org.apache.iceberg.spark.SparkCatalog');spark.conf.set('spark.sql.catalog.prior_verified.type','hadoop');spark.conf.set('spark.sql.catalog.prior_verified.warehouse',prior_path)
            prior=spark.table('prior_verified.gold.station_hourly_observations').where((F.col('station_hour_ts')>=F.lit((start-pd.Timedelta(hours=168)).to_pydatetime()))&(F.col('station_hour_ts')<F.lit(start.to_pydatetime())))
            history=history.where(F.col('station_hour_ts')>=F.lit(start.to_pydatetime())).unionByName(prior.select('station_id','station_hour_ts','ride_count'))
        station='6140.05'
        if (a.year,a.month)!=(2024,1):
            eligible=history.where(F.col('station_hour_ts')<F.lit(start.to_pydatetime())).groupBy('station_id').count().where('count = 168').join(history.where(F.col('station_hour_ts')==F.lit(start.to_pydatetime())).select('station_id'),['station_id'])
            candidate=eligible.where(F.col('station_id')==station).first() or eligible.orderBy('station_id').first()
            if candidate is None:raise ValueError('No complete previous-month station history for continuity audit')
            station=candidate['station_id']
        sample=history.where(F.col('station_id')==station).orderBy('station_hour_ts')
        frame=sample.toPandas();prepared=build_training_features(frame);features=spark_past_features(sample).orderBy('station_hour_ts').toPandas()
        np.testing.assert_allclose(prepared[REQUIRED_FEATURES].values,features[REQUIRED_FEATURES].values,rtol=1e-6,atol=1e-6)
        first=prepared.loc[prepared.station_hour_ts==start]
        continuity={'station':station,'cross_month_first_hour_available':not first.empty,'spark_numpy_parity':True,'feature_rows':len(features)}
        if (a.year,a.month)!=(2024,1):
            assert not first.empty,'Previous-month context missing at boundary'
            by_time=frame.set_index('station_hour_ts').ride_count
            for lag in [1,2,24,168]:assert first.iloc[0][f'lag_{lag}h']==by_time[start-pd.Timedelta(hours=lag)]
            changed=frame.copy();changed.loc[changed.station_hour_ts>=start,'ride_count']=999999
            alt=build_training_features(changed).loc[lambda f:f.station_hour_ts==start]
            np.testing.assert_allclose(first[REQUIRED_FEATURES].values,alt[REQUIRED_FEATURES].values)
            continuity['current_and_future_target_mutation_unchanged']=True
        result={'verified':True,'source_sha256':checksum(source),'reference':expected,'metrics':metrics,'continuity':continuity,'seconds':round(time.monotonic()-started,3),'model_training':False}
        atomic_json(ROOT/'checkpoints'/f'{a.year}-{a.month:02d}.audit.json',result);print('MONTHLY_AUDIT_JSON:'+json.dumps(result),flush=True)
    finally:spark.stop()

if __name__=='__main__':main()
