"""Bounded reference + real Iceberg-client + DuckDB/API validation, NOT a Spark/Airflow production run."""
import argparse,json,os,sys,time,platform,shutil,importlib.util
from pathlib import Path
import numpy as np
import pandas as pd
from jobs.citibike import read_chunks,clean_rides,hourly_marts,checksum,deduplicate_rides
from jobs.ml_features import prepare_model_frames,build_one_prediction_feature_row,LABEL_MAP,calculate_penalty


def validate(source,output,year,month,allow_sample=False):
    import duckdb,joblib,pyarrow as pa
    from sklearn.linear_model import LogisticRegression
    from pyiceberg.catalog.sql import SqlCatalog
    from fastapi.testclient import TestClient
    output=Path(output);output.mkdir(parents=True,exist_ok=False);source=Path(source)
    metadata_path=source.with_suffix(source.suffix+'.manifest.json')
    metadata=json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
    sample=bool(metadata.get('sample')) or '.sample.' in source.name
    if sample and not allow_sample:raise ValueError('Explicit --allow-sample required; sampled counts are not population demand')
    report={'mode':'bounded pandas-reference + actual PyIceberg SQL-catalog + DuckDB API','sampled_source':sample,'source_sha256':checksum(source),'source_url':metadata.get('source_url'),'source_rows':0,'rejected_rows':0,'checks':[], 'production_spark_validated':False,'airflow_validated':False,'postgres_validated':False,'model_quality_claim':None}
    def check(label,ok=True):
        assert ok,label
        report['checks'].append(label);print('PASS '+label,flush=True)
    try:
        bronze=pd.concat(list(read_chunks(source)),ignore_index=True);report['source_rows']=len(bronze)
        clean,bad=clean_rides(bronze,year,month);report['rejected_rows']=len(bad);report['rejection_reasons']=bad.rejection_reason.value_counts().to_dict()
        accepted_before_dedup=len(clean)
        clean=deduplicate_rides(clean);report['accepted_unique_rides']=len(clean)
        report['exact_duplicates_removed']=accepted_before_dedup-len(clean)
        check('Real source schema and opaque station IDs retained',len(clean)>0 and str(clean.start_station_id.dtype)=='string')
        check('Accepted/rejected rows reconcile before duplicate removal',accepted_before_dedup+len(bad)==len(bronze))
        bi,ml=hourly_marts(clean,year,month,complete_month=not sample)
        check('BI ride totals reconcile with Silver',int(bi.ride_count.sum())==len(clean))
        check('Gold ML is unique station/hour grid',not ml.duplicated(['station_id','station_hour_ts']).any())
        report['station_count']=ml.station_id.nunique();report['gold_ml_rows']=len(ml)
        if sample:report['warning']='Bounded source sample only: zero-filled/sample counts are NOT full-month population demand. No accuracy metrics are claimed; production ingestion rejects this sample.'
        for name,frame in [('bronze',bronze),('silver',clean),('quarantine',bad),('gold_bi',bi),('gold_ml',ml)]:frame.to_parquet(output/(name+'.parquet'),index=False)
        # Exercise real Iceberg writes, metadata and reads without mislabeling it as Spark/Hadoop validation.
        catalog=SqlCatalog('audit',uri='sqlite:///'+str((output/'catalog.db').resolve()).replace('\\','/'),warehouse=(output/'iceberg').resolve().as_uri(),**{'py-io-impl':'jobs.local_iceberg_io.LocalFileIO'})
        catalog.create_namespace('audit')
        report['iceberg_tables']={}
        for name,frame in [('bronze',bronze),('silver',clean),('gold_bi',bi),('gold_ml',ml)]:
            arrow=pa.Table.from_pandas(frame,preserve_index=False)
            fields=[pa.field(f.name,pa.timestamp('us') if pa.types.is_timestamp(f.type) else f.type,nullable=f.nullable) for f in arrow.schema]
            arrow=arrow.cast(pa.schema(fields))
            table=catalog.create_table('audit.'+name,schema=arrow.schema);table.append(arrow)
            loaded=catalog.load_table('audit.'+name).scan().to_arrow();check('Actual Iceberg append/scan '+name,loaded.num_rows==len(frame));report['iceberg_tables'][name]={'rows':loaded.num_rows,'metadata':table.metadata_location}
        station=bi.groupby('station_id').ride_count.sum().idxmax();history=ml[ml.station_id==station].copy()
        train,val,test,thresholds,features=prepare_model_frames(history)
        if train.demand_class.nunique()<2:raise ValueError('Not enough class variation for a meaningful training smoke test')
        model=LogisticRegression(max_iter=400,random_state=42);model.fit(train[features],train.demand_class)
        cutoff=test.station_hour_ts.iloc[0]-pd.Timedelta(hours=1)
        X,meta=build_one_prediction_feature_row(history,str(station),cutoff,thresholds)
        probs=np.zeros(3);probs[np.asarray(model.classes_,dtype=int)]=model.predict_proba(X)[0];prediction=int(np.argmax(probs))
        check('Bounded station model fit and finite next-hour inference',np.isfinite(probs).all() and np.isclose(probs.sum(),1))
        check('Model features precede target hour',meta['feature_window_end_ts']<meta['prediction_hour_ts'])
        joblib.dump(model,output/'smoke_model_not_for_deployment.pkl')
        report['smoke_training']={'station_id':str(station),'train_hours':len(train),'validation_hours':len(val),'test_hours':len(test),'classes':[int(x) for x in model.classes_],'deployable':False}
        database=output/'api_cache.duckdb';connection=duckdb.connect(str(database));connection.execute(Path('sql/citibike_cache.sql').read_text(encoding='utf-8-sig'))
        record={'station_id':str(station),'prediction_hour_ts':meta['prediction_hour_ts'],'scored_at_ts':pd.Timestamp.now(tz='UTC'),'model_name':'bounded-audit-logistic','model_version':'smoke-not-production','predicted_class':prediction,'predicted_label':LABEL_MAP[prediction],'prob_class_0':probs[0],'prob_class_1':probs[1],'prob_class_2':probs[2],'baseline_predicted_class':meta['baseline_predicted_class'],'feature_window_end_ts':cutoff,'threshold_q25':thresholds.q25,'threshold_q75':thresholds.q75,'actual_class':meta['actual_class'],'actual_ride_count':meta['actual_ride_count'],'model_penalty':calculate_penalty(meta['actual_class'],prediction),'baseline_penalty':calculate_penalty(meta['actual_class'],meta['baseline_predicted_class'])}
        columns=list(record);connection.execute('INSERT INTO station_hourly_demand_predictions ('+','.join(columns)+') VALUES ('+','.join(['?']*len(columns))+')',list(record.values()));connection.close()
        os.environ['PREDICTION_CACHE_BACKEND']='duckdb';os.environ['DUCKDB_CACHE_PATH']=str(database.resolve())
        spec=importlib.util.spec_from_file_location('citibike_audit_api',Path('fastapi/main.py'));module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with TestClient(module.app) as client:
            check('Actual local cache/API readiness',client.get('/ready').status_code==200)
            data=client.get('/predictions/'+str(station));check('Actual local cache/API prediction read',data.status_code==200 and data.json()['count']==1 and data.json()['station_id']==str(station))
            check('Station discovery/API',str(station) in client.get('/stations').json()['stations'])
            check('Bounded API query parameters',client.get('/predictions/'+str(station)+'?limit=0').status_code==422)
        report['complete']=True;return report
    except Exception as e:report.update(complete=False,error=repr(e));raise
    finally:(output/'validation.json').write_text(json.dumps(report,indent=2,default=str),encoding='utf-8')


def main():
    p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--output',required=True);p.add_argument('--year',type=int,required=True);p.add_argument('--month',type=int,required=True);p.add_argument('--allow-sample',action='store_true');a=p.parse_args();validate(a.source,a.output,a.year,a.month,a.allow_sample)
if __name__=='__main__':main()
