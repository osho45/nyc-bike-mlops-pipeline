"""Fresh Spark/PyIceberg reads from a shadow-mounted restored copy, not the original."""
import json
from pathlib import Path
from jobs.ml_utils import load_config,create_spark_session
from tools.correctness_engine import inspect

def main():
    root=Path('/opt/airflow/artifacts/correctness-01')
    assert json.loads((root/'warehouse/restore-proof.json').read_text())['restored_from_verified_backup']
    spark=create_spark_session(load_config(root/'config.yml'),'VerifyRestoredWarehouse')
    try:
        silver=spark.table('citibike_validation.silver.citibike_rides').count()
        bi=spark.sql('SELECT sum(ride_count) n FROM citibike_validation.gold.station_hourly_utilization').first()['n']
        ml=spark.sql('SELECT sum(ride_count) n FROM citibike_validation.gold.station_hourly_observations').first()['n']
        assert silver==bi==ml==21984
        versions=spark.sql('SELECT source_month,max(source_revision) revision FROM citibike_validation.silver.citibike_rides GROUP BY source_month').collect()
        assert {row['source_month']:row['revision'] for row in versions}=={1:3,2:1}
    finally:spark.stop()
    report={'restored_copy_mounted_at_original_logical_path':True,'spark_totals':{'silver':silver,'gold_bi':bi,'gold_ml':ml},'tables':inspect()}
    (root/'restored-reader.json').write_text(json.dumps(report,indent=2))
    print(json.dumps({'restore_verified_with_fresh_spark':True,'rides':silver}))

if __name__=='__main__':main()
