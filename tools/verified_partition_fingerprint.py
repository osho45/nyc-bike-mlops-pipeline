"""Read-only evidence that completed-month files/checkpoints are not reprocessed."""
import argparse,json
from pathlib import Path
from urllib.parse import urlparse,unquote
from pyiceberg.table import StaticTable
from pyiceberg.expressions import And,EqualTo,In
from jobs.citibike import checksum
from tools.monthly_ingestion import ROOT
from jobs.monthly_archives import atomic_json

def fingerprint(year=2024,through=6):
    cps={m:json.loads((ROOT/'checkpoints'/f'{year}-{m:02d}.json').read_text()) for m in range(1,through+1)}
    assert all(c['verified'] for c in cps.values())
    result={'year':year,'months':list(cps),'checkpoint_sha256':{str(m):checksum(ROOT/'checkpoints'/f'{year}-{m:02d}.json') for m in cps},'tables':{}}
    names=['bronze.citibike_rides_raw','silver.citibike_rides','quarantine.citibike_rides','gold.station_hourly_utilization','gold.station_hourly_observations','audit.ingestion_ledger']
    for name in names:
        directory=ROOT/'warehouse'/name.replace('.','/')/'metadata';version=(directory/'version-hint.text').read_text().strip()
        table=StaticTable.from_metadata(str(directory/f'v{version}.metadata.json'))
        if name.startswith('audit.'):
            condition=In('file_checksum',{c['source']['sha256'] for c in cps.values()})
        else:
            prefix='target' if name.startswith('gold.') else 'source'
            condition=And(EqualTo(prefix+'_year',year),In(prefix+'_month',set(cps)))
        files={}
        for task in table.scan(row_filter=condition).plan_files():
            data=task.file;uri=str(data.file_path);path=Path(unquote(urlparse(uri).path)) if uri.startswith('file:') else Path(uri)
            if uri in files:continue
            files[uri]={'bytes':path.stat().st_size,'sha256':checksum(path),'file_record_count':data.record_count}
        result['tables'][name]={'table_uuid':str(table.metadata.table_uuid),'files':files}
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();target=Path(a.output).resolve()
    if not target.is_relative_to(ROOT):raise ValueError('Evidence output must remain in monthly artifacts')
    result=fingerprint();atomic_json(target,result);print('Fingerprinted six verified months:',{k:len(v['files']) for k,v in result['tables'].items()})
