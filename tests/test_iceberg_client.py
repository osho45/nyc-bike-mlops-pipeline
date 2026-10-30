"""Actual local table IO, distinct from unexecuted Spark/Hadoop integration."""
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

def test_actual_iceberg_append_reload_scan(tmp_path):
    catalog=SqlCatalog('test',uri='sqlite:///'+str(tmp_path/'catalog.db').replace('\\','/'),warehouse=(tmp_path/'warehouse').as_uri(),**{'py-io-impl':'jobs.local_iceberg_io.LocalFileIO'})
    catalog.create_namespace('audit')
    rows=pa.table({'station_id':['001.20','A-2'],'ride_count':pa.array([1,2],type=pa.int64())})
    table=catalog.create_table('audit.observations',schema=rows.schema)
    table.append(rows)
    loaded=catalog.load_table('audit.observations').scan().to_arrow()
    assert sorted(loaded['station_id'].to_pylist())==['001.20','A-2']
    assert sum(loaded['ride_count'].to_pylist())==3
