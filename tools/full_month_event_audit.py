"""Read-only Spark reconciliation of the asset-produced month against baseline."""
import json,threading,time
from tools.full_month_events import ROOT
from tools.full_month_queue import isolated_config
from tools.full_month_validate import audit,table_state,MemoryMonitor
from jobs.ml_utils import create_spark_session

def main():
    baseline=json.loads((ROOT/'baseline.json').read_text());cfg=isolated_config(ROOT);started=time.monotonic()
    monitor=MemoryMonitor(ROOT/'audit-memory.csv');thread=threading.Thread(target=monitor.run,daemon=True);thread.start();spark=None;result={}
    try:
        spark=create_spark_session(cfg,'FullMonthAssetAudit')
        actual=audit(spark,cfg['catalog']['spark_catalog_name'],2024,1,baseline['reference'])
        assert actual==baseline['audit'],'Asset-produced data/feature metrics differ from verified baseline'
        tables=table_state(spark,cfg['catalog']['spark_catalog_name'])
        assert all(tables[name]['rows']==expected['rows'] and tables[name]['schema']==expected['schema'] for name,expected in baseline['after_replay'].items())
        result={'matches_full_month_baseline':True,'schema_matches_baseline':True,'audit':actual,'tables':tables}
    except Exception as error:
        result={'matches_full_month_baseline':False,'error':repr(error)};raise
    finally:
        if spark:spark.stop()
        monitor.stop.set();thread.join(timeout=3);result.update(seconds=round(time.monotonic()-started,3),memory=monitor.finish())
        (ROOT/'audit.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))

if __name__=='__main__':main()
