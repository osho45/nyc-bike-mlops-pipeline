from jobs.ml_utils import load_config,create_spark_session,get_gold_ml_table

def main():
    cfg=load_config();spark=create_spark_session(cfg,'CitiBikeCoverage')
    try:spark.table(get_gold_ml_table(cfg)).groupBy('target_year','target_month').count().orderBy('target_year','target_month').show()
    finally:spark.stop()
if __name__=='__main__':main()
