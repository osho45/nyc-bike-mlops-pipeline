"""New isolated genuine-month samples and explicitly synthetic test fixtures."""
import json,secrets
from pathlib import Path
import pandas as pd,yaml
from jobs.ml_utils import project_root
from jobs.citibike import COLUMNS,read_chunks,clean_rides,checksum
from tools.container_sample_check import prepare_config

def main():
    root=project_root()/'artifacts/correctness-01';root.mkdir(parents=True,exist_ok=False)
    cfg=prepare_config(root);(root/'config.yml').write_text(yaml.safe_dump(cfg))
    for hook in ['silver_before_publish','silver_after_publish']:
        profile={**cfg,'validation':{**cfg['validation'],'pause_at':hook}}
        (root/(hook+'.yml')).write_text(yaml.safe_dump(profile))
    january=project_root()/'data/citibike/audit-source-cached/202401-citibike-tripdata.sample.csv'
    february=project_root()/'data/citibike/correctness-feb-sample/202402-citibike-tripdata.sample.csv'
    sources={}
    for name,path in [('jan',january),('feb',february)]:
        meta=json.loads(path.with_suffix(path.suffix+'.manifest.json').read_text())
        assert meta['sample'] and checksum(path)==meta['sha256']
        sources[name]={**meta,'source_filepath':str(path),'source_revision':1}
    raw=pd.concat(read_chunks(january),ignore_index=True);clean,_=clean_rides(raw,2024,1);drop_ids=list(clean.ride_id.iloc[:2])
    parent=sources['jan']['sha256']
    for number in [2,3]:
        revised=raw[~raw.ride_id.isin(drop_ids[:number-1])][COLUMNS]
        path=root/f'202401-citibike-tripdata.r{number}.sample.csv';revised.to_csv(path,index=False)
        meta={**sources['jan'],'source_filepath':str(path),'source_filename':path.name,'sha256':checksum(path),'sample_rows':len(revised),'file_size_bytes':path.stat().st_size,'source_revision':number,'supersedes_sha256':parent,'fixture_kind':'controlled correction; NOT an official republished archive'}
        path.with_suffix(path.suffix+'.manifest.json').write_text(json.dumps(meta,indent=2));sources[f'jan_r{number}']=meta;parent=meta['sha256']
    (root/'sources.json').write_text(json.dumps(sources,indent=2))
    password=secrets.token_urlsafe(24)
    values={'POSTGRES_DB':'correctness','POSTGRES_USER':'correctness','POSTGRES_PASSWORD':password,'POSTGRES_CACHE_DB':'correctness','POSTGRES_CACHE_USER':'correctness','POSTGRES_CACHE_PASSWORD':password,'POSTGRES_CACHE_HOST':'citibike_correctness_postgres','POSTGRES_CACHE_PORT':'5432'}
    (root/'postgres.env').write_text(''.join(f'{key}={value}\n' for key,value in values.items()))
    print(json.dumps({'root':str(root),'jan_rows':20000,'feb_rows':2000,'corrections_are_controlled_fixtures':True,'secrets_printed':False}))

if __name__=='__main__':main()
