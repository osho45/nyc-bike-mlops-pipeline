"""Read-only persistence evidence. Run in the Airflow scheduler container."""
import hashlib,json
from pathlib import Path
import psycopg2
import psycopg2.extensions
from psycopg2 import sql
from jobs.persistence import postgres_dsn
from jobs.citibike import checksum
from pyiceberg.table import StaticTable

def snapshot():
    root=Path('artifacts/container-sample-03');result={'files':{},'iceberg':{},'postgres':{}}
    for path in [Path('data/citibike/audit-source-cached/202401-citibike-tripdata.sample.csv'),*sorted((root/'models').glob('*'))]:
        if path.is_file():result['files'][str(path)]=checksum(path)
    for name in ['bronze/citibike_rides_raw','silver/citibike_rides','quarantine/citibike_rides','gold/station_hourly_utilization','gold/station_hourly_observations','predictions/station_hourly_demand_predictions']:
        directory=root/'warehouse'/name/'metadata';version=(directory/'version-hint.text').read_text().strip();metadata=directory/f'v{version}.metadata.json'
        table=StaticTable.from_metadata(str(metadata.resolve()))
        result['iceberg'][name]={'version':version,'metadata_sha256':checksum(metadata),'rows':table.scan().to_arrow().num_rows,'snapshot_id':table.current_snapshot().snapshot_id}
    connection=psycopg2.connect(postgres_dsn())
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database WHERE datname IN ('citibike_cache','citibike_validation_container_sample_03') ORDER BY datname")
            databases=[row[0] for row in cur.fetchall()]
    finally:connection.close()
    for database in databases:
        dsn=psycopg2.extensions.make_dsn(postgres_dsn(),dbname=database)
        with psycopg2.connect(dsn) as connection:
            with connection.cursor() as cur:
                cur.execute('SELECT * FROM station_hourly_demand_predictions ORDER BY station_id,prediction_hour_ts')
                rows=cur.fetchall();result['postgres'][database]={'rows':len(rows),'sha256':hashlib.sha256(json.dumps(rows,default=str).encode()).hexdigest()}
    return result

if __name__=='__main__':print(json.dumps(snapshot(),indent=2))
