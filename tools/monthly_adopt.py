"""Adopt the previously verified January partitions without repeating ETL."""
import argparse,json,time
from tools.monthly_ingestion import ROOT,snapshots
from tools.full_month_queue import isolated_config
from jobs.ml_utils import create_spark_session
from jobs.monthly_archives import verified_local,atomic_json

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config');parser.add_argument('--year',type=int);parser.add_argument('--month',type=int);a=parser.parse_args()
    if (a.year,a.month)!=(2024,1):raise ValueError('Only the pinned, independently verified January donor may be adopted')
    cfg=isolated_config(ROOT);baseline=json.loads((ROOT.parent/'full-month-202401-01'/'validation.json').read_text());assert baseline['complete'] and baseline['idempotent_replay']
    source=verified_local('/opt/airflow/data/citibike/monthly/archives/202401-citibike-tripdata.zip',2024,1);assert source['sha256']==baseline['source_sha256']
    spark=create_spark_session(cfg,'AdoptVerifiedJanuary');catalog=cfg['catalog']['spark_catalog_name'];started=time.time()
    try:
        from pyspark.sql import functions as F
        spark.conf.set('spark.sql.catalog.verified_donor','org.apache.iceberg.spark.SparkCatalog');spark.conf.set('spark.sql.catalog.verified_donor.type','hadoop');spark.conf.set('spark.sql.catalog.verified_donor.warehouse',str(ROOT.parent/'full-month-202401-01'/'warehouse'))
        evidence={}
        for name,expected in baseline['after_replay'].items():
            donor=f'verified_donor.{name}';target=f'{catalog}.{name}'
            snapshot=spark.sql(f'SELECT snapshot_id FROM {donor}.snapshots ORDER BY committed_at DESC LIMIT 1').first()[0]
            assert snapshot==expected['snapshot_id'],'Donor changed since independent verification'
            frame=spark.table(donor);present=spark.table(target)
            if name.startswith('audit.'):
                present=present.where(F.col('file_checksum')==source['sha256'])
            else:
                prefix='target' if name.startswith('gold.') else 'source';present=present.where((F.col(prefix+'_year')==2024)&(F.col(prefix+'_month')==1))
            count=present.count()
            if count:
                assert count==expected['rows'],'Partial or conflicting adoption; preserve for inspection'
                if not name.startswith('audit.'):assert present.select('source_checksum').distinct().first()[0]==source['sha256']
            else:frame.select(*spark.table(target).columns).writeTo(target).append()
            evidence[name]={'donor_snapshot_id':snapshot,'rows':expected['rows']}
        result={'year':2024,'month':1,'split':'train','phase':'verified','verified':True,'source':source,'started_at':started,'completed_at':time.time(),'reuse_method':'Copied already-verified Iceberg partitions; no download/normalization/Silver transformation rerun','donor_evidence':evidence,'audit':{'verified':True,'source_sha256':source['sha256'],'metrics':baseline['audit'],'reference':baseline['reference'],'provenance':'Independent January full-month audit; copied counts checked against its immutable snapshots'},'tables':snapshots()}
        atomic_json(ROOT/'checkpoints/2024-01.json',result);print('ADOPTED_VERIFIED_JANUARY '+json.dumps(evidence),flush=True)
    finally:spark.stop()

if __name__=='__main__':main()
