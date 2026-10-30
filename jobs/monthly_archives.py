"""Official object discovery and restart-safe monthly downloads; no import-time I/O."""
import csv,hashlib,io,json,os,re,time,zipfile
from datetime import datetime,timezone
from pathlib import Path
import xml.etree.ElementTree as ET
import requests
from jobs.citibike import COLUMNS,checksum,csv_members,validate_year_month
from jobs.source_versions import month_lock

BASE='https://s3.amazonaws.com/tripdata/'
BUCKET=BASE.rstrip('/')

def atomic_json(path,value):
    path=Path(path);temp=path.with_suffix(path.suffix+'.tmp');temp.write_text(json.dumps(value,indent=2),encoding='utf-8');temp.replace(path)

def available_months(years=(2024,2025,2026)):
    ns={'s':'http://s3.amazonaws.com/doc/2006-03-01/'};objects=[]
    for year in years:
        token=None
        while True:
            params={'list-type':'2','prefix':str(year)}
            if token:params['continuation-token']=token
            response=requests.get(BUCKET,params=params,timeout=(10,60));response.raise_for_status();root=ET.fromstring(response.content)
            for entry in root.findall('s:Contents',ns):
                key=entry.findtext('s:Key',namespaces=ns);match=re.fullmatch(r'(\d{4})(\d{2})-citibike-tripdata.zip',key or '')
                if match and int(match[1])==year and 1<=int(match[2])<=12:
                    objects.append({'year':year,'month':int(match[2]),'key':key,'bytes':int(entry.findtext('s:Size',namespaces=ns)),'etag':entry.findtext('s:ETag',namespaces=ns),'last_modified':entry.findtext('s:LastModified',namespaces=ns),'url':BASE+key,'split':{2024:'train',2025:'validation',2026:'test'}.get(year,'unassigned')})
            if root.findtext('s:IsTruncated',namespaces=ns)!='true':break
            token=root.findtext('s:NextContinuationToken',namespaces=ns)
            if not token:raise ValueError('Truncated official listing omitted continuation token')
    return sorted(objects,key=lambda item:(item['year'],item['month']))

def validate_archive(path):
    with zipfile.ZipFile(path) as archive:
        bad=archive.testzip()
        if bad:raise ValueError('ZIP CRC failure: '+bad)
    members=[]
    for name,stream in csv_members(path):
        with stream:
            header=next(csv.reader(io.TextIOWrapper(stream,encoding='utf-8-sig')))
            if not set(COLUMNS)<=set(header):raise ValueError('Unsupported Citi Bike schema in '+name)
        members.append(name)
    if not members:raise ValueError('No modern CSV members')
    return members

def verified_local(path,year,month):
    path=Path(path);meta=json.loads(path.with_suffix(path.suffix+'.manifest.json').read_text(encoding='utf-8-sig'))
    expected=f'{year}{month:02d}-citibike-tripdata.zip'
    if (path.name!=expected or meta.get('dataset')!='citibike' or meta.get('sample') is not False
        or (meta.get('source_year'),meta.get('source_month'))!=(year,month)
        or meta.get('source_url')!=BASE+expected or meta.get('file_size_bytes')!=path.stat().st_size
        or meta.get('sha256')!=checksum(path)):
        raise ValueError('Existing monthly archive is not verified: '+str(path))
    validate_archive(path)
    # Manifests may have been created on Windows and reused inside Docker.
    return {**meta,'source_filepath':str(path.resolve())}

def download_month(year,month,output,max_mb=1536,retries=3,resume=True,reuse=True,source_revision=1,supersedes_sha256=None,resource_check=None):
    validate_year_month(year,month)
    if max_mb<=0 or retries<0:raise ValueError('Invalid transfer budget/retries')
    if source_revision!=1 or supersedes_sha256:raise ValueError('Automated downloader preserves initial versions; corrections need explicit source-version workflow')
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    target=output/f'{year}{month:02d}-citibike-tripdata.zip';url=BASE+target.name
    with month_lock(output,year,month):
        manifest=target.with_suffix('.zip.manifest.json');partial=target.with_suffix('.zip.partial');state=partial.with_suffix('.partial.json')
        if target.exists():
            if not manifest.exists() and state.exists():
                completed=json.loads(state.read_text()).get('complete_manifest')
                if completed and checksum(target)==completed['sha256']:atomic_json(manifest,completed)
            if not reuse:raise FileExistsError(str(target))
            return {**verified_local(target,year,month),'reused':True,'bytes_transferred_this_call':0}
        if manifest.exists():raise FileExistsError('Orphan manifest preserved: '+str(manifest))
        last=None;transferred=0;resumed_at=partial.stat().st_size if partial.exists() else 0
        if partial.exists() and not resume:raise FileExistsError('Partial preserved; use resume')
        for attempt in range(retries+1):
            try:
                if resource_check:resource_check()
                if target.exists():
                    completed=json.loads(state.read_text()).get('complete_manifest')
                    if not completed or checksum(target)!=completed['sha256']:raise ValueError('Unverified final file preserved')
                    atomic_json(manifest,completed);return completed
                head=requests.head(url,headers={'Accept-Encoding':'identity'},timeout=(10,60));head.raise_for_status()
                total=int(head.headers['Content-Length']);etag=head.headers.get('ETag');head.close()
                if total>max_mb*1024**2:raise ValueError('Archive exceeds configured per-month budget')
                if not etag:raise ValueError('Origin omitted ETag required for safe resumable transfer')
                if state.exists():
                    saved=json.loads(state.read_text())
                    if (saved['url'],saved['etag'],saved['total_bytes'])!=(url,etag,total):raise ValueError('Origin changed; partial preserved for operator review')
                elif partial.exists():raise ValueError('Partial has no origin checkpoint; preserved')
                atomic_json(state,{'url':url,'etag':etag,'total_bytes':total})
                offset=partial.stat().st_size if partial.exists() else 0
                if offset>total:raise ValueError('Partial exceeds origin length; preserved')
                if offset<total:
                    headers={'Accept-Encoding':'identity','If-Match':etag}
                    if offset:headers['Range']=f'bytes={offset}-'
                    with requests.get(url,headers=headers,stream=True,timeout=(10,120)) as response:
                        response.raise_for_status()
                        if response.headers.get('ETag')!=etag:raise ValueError('Origin version changed during download')
                        if offset and (response.status_code!=206 or response.headers.get('Content-Range')!=f'bytes {offset}-{total-1}/{total}'):raise ValueError('Origin ignored/mismatched resume range; partial preserved')
                        if not offset and response.status_code!=200:raise ValueError('Unexpected full-download status')
                        announced=int(response.headers.get('Content-Length',total-offset))
                        if announced!=total-offset:raise ValueError('Transfer length mismatch')
                        with partial.open('ab' if offset else 'wb') as stream:
                            next_log=offset+16*1024**2
                            for block in response.iter_content(1024**2):
                                if not block:continue
                                if stream.tell()+len(block)>total:raise ValueError('Oversized response; partial preserved')
                                stream.write(block);transferred+=len(block)
                                if stream.tell()>=next_log:print(f'DOWNLOAD {year}-{month:02d} {stream.tell()}/{total}',flush=True);next_log+=16*1024**2
                                if resource_check and stream.tell()>=next_log-1024**2:resource_check()
                    if partial.stat().st_size!=total:raise requests.ConnectionError('Incomplete transfer; resume will continue')
                members=validate_archive(partial);digest=checksum(partial)
                meta={'dataset':'citibike','source_url':url,'source_filepath':str(target.resolve()),'source_filename':target.name,'source_year':year,'source_month':month,'file_size_bytes':total,'sha256':digest,'discovered_at_utc':datetime.now(timezone.utc).isoformat(),'sample':False,'sample_rows':None,'bytes_transferred':transferred,'bytes_transferred_this_call':transferred,'members':members,'source_revision':1,'supersedes_sha256':None,'origin_etag':etag,'resumed_from_bytes':resumed_at,'attempts_this_call':attempt+1,'reused':False}
                # Final data becomes visible before the atomic watcher manifest.
                atomic_json(state,{'url':url,'etag':etag,'total_bytes':total,'complete_manifest':meta})
                if target.exists():raise FileExistsError('Final target appeared during download; preserved')
                partial.replace(target);atomic_json(manifest,meta);return meta
            except (requests.RequestException,OSError) as error:
                last=error
                if isinstance(error,requests.HTTPError) and error.response is not None and error.response.status_code not in [408,429,500,502,503,504]:raise
                if attempt==retries:raise
                time.sleep(min(2**attempt,30))
        raise RuntimeError(str(last))
