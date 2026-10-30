"""Calendar-year roles and past-only features across monthly partitions."""
import numpy as np
import pandas as pd
from jobs.ml_features import REQUIRED_FEATURES,build_training_features,fit_thresholds,apply_thresholds

def prepare_year_model_frames(history,years=(2024,2025,2026),test_end_month=9):
    """Training-only fitted thresholds, validation selection, Jan-Sep final test."""
    if len(set(years))!=3 or tuple(sorted(years))!=tuple(years):raise ValueError('Strictly ordered distinct split years required')
    if not 1<=test_end_month<=12:raise ValueError('Invalid final-test end month')
    if history.station_id.astype(str).nunique()!=1:raise ValueError('One station history required')
    if history.station_hour_ts.duplicated().any():raise ValueError('Duplicate station hours')
    features=build_training_features(history)
    local=pd.to_datetime(features.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
    frames=[features.loc[local.dt.year==year].copy() for year in years]
    frames[2]=features.loc[(local.dt.year==years[2])&(local.dt.month<=test_end_month)].copy()
    if any(frame.empty for frame in frames):raise ValueError('All configured years need genuine complete feature rows')
    thresholds=fit_thresholds(frames[0])
    return (*[apply_thresholds(frame,thresholds) for frame in frames],thresholds,REQUIRED_FEATURES.copy())

def spark_past_features(observations):
    """No partition-by-month/year: history crosses boundaries, gaps reset validity.

    Input must have unique, hour-aligned station/hour keys. A 168-row span of
    exactly 168 hours proves all intermediate rows are consecutive.
    """
    from pyspark.sql import functions as F,Window
    w=Window.partitionBy('station_id').orderBy('station_hour_ts');out=observations
    for lag in [1,2,24,168]:out=out.withColumn(f'lag_{lag}h',F.lag('ride_count',lag).over(w))
    for width in [6,24]:
        previous=w.rowsBetween(-width,-1);out=out.withColumn(f'rolling_{width}h_mean',F.avg('ride_count').over(previous)).withColumn(f'rolling_{width}h_std',F.stddev_samp('ride_count').over(previous))
    out=out.withColumn('_oldest',F.lag('station_hour_ts',168).over(w))
    local=F.from_utc_timestamp('station_hour_ts','America/New_York');day=(F.dayofweek(local)+5)%7
    for name,value,period in [('hour_of_day',F.hour(local),24),('day_of_week',day,7)]:
        out=out.withColumn(name+'_sin',F.sin(value*F.lit(2*np.pi/period))).withColumn(name+'_cos',F.cos(value*F.lit(2*np.pi/period)))
    return out.withColumn('is_weekend',F.when(day.isin(5,6),1).otherwise(0)).withColumn('data_split',F.when(F.year(local)==2024,'train').when(F.year(local)==2025,'validation').when(F.year(local)==2026,'test').otherwise('unassigned')).where(F.col('station_hour_ts').cast('long')-F.col('_oldest').cast('long')==168*3600).drop('_oldest')
