"""Read actual isolated asset-chain Iceberg results and duplicate invariants."""
import json
from pathlib import Path
from pyiceberg.table import StaticTable

def counts():
    root=Path('artifacts/asset-validation-01/warehouse');result={}
    for name in ['bronze/citibike_rides_raw','silver/citibike_rides','quarantine/citibike_rides','gold/station_hourly_utilization','gold/station_hourly_observations','audit/ingestion_ledger']:
        directory=root/name/'metadata';version=(directory/'version-hint.text').read_text().strip()
        table=StaticTable.from_metadata(str((directory/f'v{version}.metadata.json').resolve()))
        frame=table.scan().to_arrow().to_pandas();data={'rows':len(frame)}
        if 'ride_count' in frame:data['ride_count_sum']=int(frame.ride_count.sum())
        if 'station_hour_ts' in frame:data['duplicate_station_hours']=int(frame.duplicated(['station_id','station_hour_ts']).sum())
        if 'ride_id' in frame:data['duplicate_ride_ids']=int(frame.ride_id.duplicated().sum())
        if name.startswith('bronze'):data['snapshot_ids']=[snapshot.snapshot_id for snapshot in table.snapshots()]
        if 'rejection_reason' in frame:data['reasons']=frame.rejection_reason.value_counts().to_dict()
        result[name]=data
    assert result['bronze/citibike_rides_raw']['rows']==20000
    assert result['silver/citibike_rides']['rows']==19986
    assert result['quarantine/citibike_rides']['rows']==14
    assert result['audit/ingestion_ledger']['rows']==1
    assert all(result[name]['ride_count_sum']==19986 for name in ['gold/station_hourly_utilization','gold/station_hourly_observations'])
    assert all(data.get('duplicate_station_hours',0)==data.get('duplicate_ride_ids',0)==0 for data in result.values())
    return result

if __name__=='__main__':print(json.dumps(counts(),indent=2))
