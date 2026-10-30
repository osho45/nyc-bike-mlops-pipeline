"""Shared non-destructive cache/metadata adapters. Station IDs are opaque text."""
import os
from pathlib import Path
from jobs.ml_utils import project_root

def postgres_dsn():
    import psycopg2.extensions
    return psycopg2.extensions.make_dsn(host=os.getenv('POSTGRES_CACHE_HOST','localhost'),port=os.getenv('POSTGRES_CACHE_PORT','5543'),dbname=os.getenv('POSTGRES_CACHE_DB','citibike_cache'),user=os.getenv('POSTGRES_CACHE_USER','cache_user'),password=os.getenv('POSTGRES_CACHE_PASSWORD'),connect_timeout=5)

def load_prediction_table(cfg):
    from pyiceberg.table import StaticTable
    warehouse=Path(cfg['catalog']['warehouse_path'])
    if not warehouse.is_absolute():warehouse=project_root()/warehouse
    metadata=warehouse/'predictions/station_hourly_demand_predictions/metadata'
    hint=metadata/'version-hint.text'
    if not hint.is_file():raise FileNotFoundError('No Hadoop catalog version hint: '+str(hint))
    version=int(hint.read_text().strip());path=metadata/f'v{version}.metadata.json'
    if not path.is_file():raise FileNotFoundError('Version hint references missing metadata')
    return StaticTable.from_metadata(str(path.resolve()))

