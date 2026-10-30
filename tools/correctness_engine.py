"""Inspect or resume isolated month/version checks; never trains a model."""
import argparse,json
from pathlib import Path
from types import SimpleNamespace
from pyiceberg.table import StaticTable
from jobs.ml_utils import load_config,create_spark_session
from jobs.lakehouse import bronze,silver,gold,sample_validation
ROOT=Path('/opt/airflow/artifacts/correctness-01')

def inspect():
    result={}
    for name in ['bronze/citibike_rides_raw','silver/citibike_rides','quarantine/citibike_rides','gold/station_hourly_utilization','gold/station_hourly_observations','audit/ingestion_ledger']:
        directory=ROOT/'warehouse'/name/'metadata';hint=directory/'version-hint.text'
        if not hint.exists():continue
        version=hint.read_text().strip();table=StaticTable.from_metadata(str((directory/f'v{version}.metadata.json').resolve()));frame=table.scan().to_arrow().to_pandas()
        data={'rows':len(frame),'metadata_version':version,'snapshot_id':table.current_snapshot().snapshot_id}
        year='source_year' if 'source_year' in frame else 'target_year';month='source_month' if 'source_month' in frame else 'target_month'
        if year in frame:
            data['months']={}
            for (y,m),group in frame.groupby([year,month]):
                key=f'{int(y)}-{int(m):02d}';entry={'rows':len(group)}
                if 'source_revision' in group:entry['revisions']=sorted(int(v) for v in group.source_revision.dropna().unique())
                if 'source_checksum' in group:entry['checksums']=sorted(group.source_checksum.dropna().unique().tolist())
                if 'ride_count' in group:entry['rides']=int(group.ride_count.sum())
                data['months'][key]=entry
        result[name]=data
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['inspect','resume']);p.add_argument('--config',default=str(ROOT/'config.yml'));p.add_argument('--source',default='jan_r3');a=p.parse_args()
    if a.action=='inspect':print(json.dumps(inspect(),indent=2));return
    cfg=load_config(a.config);assert sample_validation(cfg)
    meta=json.loads((ROOT/'sources.json').read_text())[a.source]
    spark=create_spark_session(cfg,'ControlledCorrectnessWorker')
    try:
        args=SimpleNamespace(source_filepath=meta['source_filepath'],source_filename=meta['source_filename'],run_id='controlled-'+a.source,ingestion_run_id=None,source_revision=meta['source_revision'],supersedes_sha256=meta.get('supersedes_sha256'),expected_checksum=meta['sha256'])
        bronze(spark,'citibike_validation',cfg,args,2024,1)
        from pyspark.sql import functions as F
        row=spark.table('citibike_validation.bronze.citibike_rides_raw').where((F.col('source_checksum')==meta['sha256'])&(F.col('source_revision')==meta['source_revision'])).select('ingestion_run_id').first();args.ingestion_run_id=row['ingestion_run_id']
        silver(spark,'citibike_validation',cfg,args,2024,1)
        if not cfg.get('validation',{}).get('pause_at'):
            gold(spark,'citibike_validation',2024,1,'bi',cfg);gold(spark,'citibike_validation',2024,1,'ml',cfg)
    finally:spark.stop()
    print(json.dumps({'resume_complete':True,'source':a.source}))

if __name__=='__main__':main()
