"""Shared asset-event contracts and Spark job invocation, with no Airflow imports."""
import hashlib,json,subprocess,sys
from pathlib import Path
from jobs.source_versions import revision

def unique_lineages(events):
    """Accept legacy single lineage or explicit batches; deduplicate by content.

    Different versions of the same month require an explicit operator decision,
    not an arbitrary event-order overwrite of published data.
    """
    found={};versions={}
    for event in events:
        extra=event.extra if hasattr(event,'extra') else event
        if not isinstance(extra,dict):raise ValueError('Asset event metadata must be an object')
        items=extra.get('lineages',[extra])
        if not isinstance(items,list) or not items:raise ValueError('Empty or malformed lineage batch')
        for original in items:
            item=dict(original)
            required=['source_year','source_month','sha256','source_filepath','source_filename']
            if item.get('dataset')!='citibike' or any(key not in item for key in required):raise ValueError('Incomplete Citi Bike asset lineage')
            year,month=int(item['source_year']),int(item['source_month'])
            if not 2013<=year<=2100 or not 1<=month<=12:raise ValueError('Invalid source year/month')
            digest=str(item['sha256'])
            if len(digest)!=64 or any(char not in '0123456789abcdef' for char in digest):raise ValueError('Invalid source SHA-256')
            rev=revision(item.get('source_revision',1));key=(year,month,rev,digest)
            version_key=(year,month,rev)
            if version_key in versions and versions[version_key]!=digest:raise ValueError('Conflicting source versions for one month/revision')
            if key in found and found[key].get('ingestion_run_id')!=item.get('ingestion_run_id'):raise ValueError('Conflicting canonical ingestion lineage')
            versions[version_key]=digest;found.setdefault(key,item)
    if not found:raise ValueError('Missing upstream asset event metadata')
    ordered=[found[key] for key in sorted(found)]
    by_month={}
    for item in ordered:by_month.setdefault((int(item['source_year']),int(item['source_month'])),[]).append(item)
    for chain in by_month.values():
        for previous,current in zip(chain,chain[1:]):
            if revision(current.get('source_revision',1))!=revision(previous.get('source_revision',1))+1 or current.get('supersedes_sha256')!=previous['sha256']:
                raise ValueError('Coalesced corrected sources require a contiguous explicit checksum chain')
    return ordered

def event_payload(lineages):
    return lineages[0] if len(lineages)==1 else {'dataset':'citibike','lineages':lineages}

def run_asset_stage(stage,events,config,run_id,runner=None):
    """Process every unique source; only emit downstream metadata after success."""
    runner=runner or subprocess.run
    items=unique_lineages(events);completed=[]
    if stage in ('bi','ml'):
        # Gold reads the latest atomically published Silver month, once per month.
        items=list({(int(item['source_year']),int(item['source_month'])):item for item in items}.values())
    modules={'bronze':'jobs.bronze_ingest','silver':'jobs.silver_wap','bi':'jobs.gold_bi','ml':'jobs.gold_ml'}
    for item in items:
        cmd=[sys.executable,'-m',modules[stage],'--config',str(config)]
        year,month=str(item['source_year']),str(item['source_month'])
        if stage=='bronze':
            source_run=f"{run_id}:{year}{month}:r{item.get('source_revision',1)}:{item['sha256'][:12]}"
            cmd+=['--source_filepath',item['source_filepath'],'--source_filename',item['source_filename'],'--source_year',year,'--source_month',month,'--run_id',source_run]
            cmd+=['--source_revision',str(item.get('source_revision',1)),'--expected_checksum',item['sha256']]
            if item.get('supersedes_sha256'):cmd+=['--supersedes_sha256',item['supersedes_sha256']]
        elif stage=='silver':
            if not item.get('ingestion_run_id'):raise ValueError('Missing canonical ingestion run')
            cmd+=['--ingestion_run_id',item['ingestion_run_id'],'--target_year',year,'--target_month',month]
        else:cmd+=['--year',year,'--month',month]
        result=runner(cmd,capture_output=True,text=True)
        print(result.stdout,flush=True)
        if result.returncode:
            print(result.stderr,flush=True)
            raise RuntimeError(f'{stage} Spark job failed with code {result.returncode}')
        if stage=='bronze':
            records=[json.loads(line.split('LINEAGE_JSON:',1)[1]) for line in result.stdout.splitlines() if line.startswith('LINEAGE_JSON:')]
            if not records:raise ValueError('Bronze did not emit canonical lineage')
            item={**item,'ingestion_run_id':records[-1]['ingestion_run_id']}
        publications=[json.loads(line.split('PUBLICATION_JSON:',1)[1]) for line in result.stdout.splitlines() if line.startswith('PUBLICATION_JSON:')]
        if publications:item={**item,'publication':publications[-1]}
        completed.append(item)
    return event_payload(completed)
