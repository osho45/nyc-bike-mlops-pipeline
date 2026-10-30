"""Explicit official download. No network or month loop runs on import."""
import argparse,csv,hashlib,io,json,os,re,zipfile
from pathlib import Path
from datetime import datetime,timezone
import requests
from jobs.citibike import COLUMNS,validate_year_month,checksum
from jobs.source_versions import revision

BASE='https://s3.amazonaws.com/tripdata/'
def archive_url(year,month):
    validate_year_month(year,month);return BASE+f'{year}{month:02d}-citibike-tripdata.zip'

class HTTPRangeFile(io.RawIOBase):
    """Seekable public S3 object with an explicit transfer budget; no access bypass."""
    def __init__(self,url,budget):
        self.url=url;self.pos=0;self.transferred=0;self.budget=budget;self.cache=b'';self.cache_start=0
        r=requests.get(url,headers={'Range':'bytes=0-0'},timeout=(10,60),stream=True);r.raise_for_status()
        if r.status_code!=206:r.close();raise ValueError('Server does not support bounded range requests')
        self.size=int(r.headers['Content-Range'].split('/')[-1]);r.close()
    def readable(self):return True
    def seekable(self):return True
    def tell(self):return self.pos
    def seek(self,offset,whence=0):
        self.pos=offset if whence==0 else self.pos+offset if whence==1 else self.size+offset
        if not 0<=self.pos<=self.size:raise ValueError('Invalid archive offset')
        return self.pos
    def read(self,n=-1):
        n=self.size-self.pos if n<0 else min(n,self.size-self.pos)
        if not n:return b''
        if self.cache_start<=self.pos and self.pos+n<=self.cache_start+len(self.cache):
            start=self.pos-self.cache_start;self.pos+=n;return self.cache[start:start+n]
        fetch=min(max(n,1024**2),self.size-self.pos)
        if self.transferred+fetch>self.budget:raise ValueError('Sample transfer budget exceeded')
        r=requests.get(self.url,headers={'Range':f'bytes={self.pos}-{self.pos+fetch-1}'},timeout=(10,60),stream=True);r.raise_for_status()
        expected=f'bytes {self.pos}-{self.pos+fetch-1}/{self.size}'
        if r.status_code!=206 or r.headers.get('Content-Range')!=expected:r.close();raise ValueError('Range request ignored/mismatched')
        data=r.raw.read(fetch+1);r.close()
        if len(data)!=fetch:raise ValueError('Truncated or oversized range response')
        self.cache_start=self.pos;self.cache=data;self.transferred+=fetch;self.pos+=n;return data[:n]

def download(year,month,output,sample_rows=None,max_mb=512,source_revision=1,supersedes_sha256=None,resume=False,reuse=False,retries=3):
    source_revision=revision(source_revision)
    if (source_revision==1 and supersedes_sha256) or (source_revision>1 and (not supersedes_sha256 or len(supersedes_sha256)!=64 or any(c not in '0123456789abcdef' for c in supersedes_sha256))):raise ValueError('Corrections require an explicit predecessor SHA-256; initial sources have no predecessor')
    if resume or reuse:
        if sample_rows:raise ValueError('Resumption/reuse is supported for complete archives only')
        from jobs.monthly_archives import download_month
        return download_month(year,month,output,max_mb=max_mb,retries=retries,resume=resume,reuse=reuse,source_revision=source_revision,supersedes_sha256=supersedes_sha256)
    url=archive_url(year,month);output=Path(output);output.mkdir(parents=True,exist_ok=True)
    name=f'{year}{month:02d}-citibike-tripdata'+(f'.r{source_revision}' if source_revision>1 else '')+('.sample.csv' if sample_rows else '.zip');target=output/name
    if target.exists():raise FileExistsError(f'Preserve existing {target}; choose another directory')
    tmp=target.with_suffix(target.suffix+'.partial');members=[];rows=0
    if tmp.exists() or target.with_suffix(target.suffix+'.manifest.json').exists():
        raise FileExistsError('Existing partial download or manifest preserved; choose a new output directory')
    if max_mb<=0:raise ValueError('Download budget must be positive')
    try:
        if sample_rows:
            if sample_rows<1:raise ValueError('sample_rows must be positive')
            remote=HTTPRangeFile(url,max_mb*1024**2)
            with zipfile.ZipFile(remote) as z,tmp.open('w',newline='',encoding='utf-8') as f:
                writer=csv.DictWriter(f,fieldnames=COLUMNS);writer.writeheader()
                for info in sorted(z.infolist(),key=lambda i:i.filename):
                    if not info.filename.lower().endswith('.csv') or '__MACOSX' in info.filename:continue
                    members.append(info.filename)
                    with z.open(info) as stream:
                        reader=csv.DictReader(io.TextIOWrapper(stream,encoding='utf-8-sig'))
                        if set(COLUMNS)-set(reader.fieldnames or []):raise ValueError('Not the modern Citi Bike schema')
                        for row in reader:
                            writer.writerow({c:row[c] for c in COLUMNS});rows+=1
                            if rows>=sample_rows:break
                    if rows>=sample_rows:break
                if not rows:raise ValueError('No modern CSV member found')
            transferred=remote.transferred
        else:
            transferred=0
            with requests.get(url,stream=True,timeout=(10,120)) as r:
                r.raise_for_status()
                if int(r.headers.get('Content-Length',0))>max_mb*1024**2:raise ValueError('Archive exceeds transfer budget')
                with tmp.open('wb') as f:
                    for block in r.iter_content(1024**2):
                        transferred+=len(block)
                        if transferred>max_mb*1024**2:raise ValueError('Transfer budget exceeded')
                        f.write(block)
            with zipfile.ZipFile(tmp) as z:
                if z.testzip():raise ValueError('Archive CRC validation failed')
                members=[i.filename for i in z.infolist() if i.filename.lower().endswith('.csv') and '__MACOSX' not in i.filename]
                if not members:raise ValueError('No CSV data in archive')
        tmp.replace(target)
        meta={'dataset':'citibike','source_url':url,'source_filepath':str(target.resolve()),'source_filename':name,'source_year':year,'source_month':month,'file_size_bytes':target.stat().st_size,'sha256':checksum(target),'discovered_at_utc':datetime.now(timezone.utc).isoformat(),'sample':bool(sample_rows),'sample_rows':rows if sample_rows else None,'bytes_transferred':transferred,'members':members}
        meta.update(source_revision=source_revision,supersedes_sha256=supersedes_sha256)
        manifest=target.with_suffix(target.suffix+'.manifest.json');manifest.write_text(json.dumps(meta,indent=2));return meta
    finally:
        if tmp.exists():tmp.unlink() # Only this invocation's incomplete download, never existing user data.

def main():
    p=argparse.ArgumentParser();p.add_argument('--year',type=int,required=True);p.add_argument('--month',type=int,required=True);p.add_argument('--output',default='data/citibike/incoming');p.add_argument('--sample-rows',type=int);p.add_argument('--max-download-mb',type=int,default=512);p.add_argument('--source-revision',type=int,default=1);p.add_argument('--supersedes-sha256');p.add_argument('--resume',action='store_true');p.add_argument('--reuse',action='store_true');p.add_argument('--retries',type=int,default=3);a=p.parse_args();print(json.dumps(download(a.year,a.month,a.output,a.sample_rows,a.max_download_mb,a.source_revision,a.supersedes_sha256,a.resume,a.reuse,a.retries),indent=2))
if __name__=='__main__':main()
