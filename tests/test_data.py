import io,json,zipfile,sys
import pandas as pd
import pytest
from jobs.citibike import COLUMNS,read_chunks,clean_rides,hourly_marts,month_bounds
from jobs.download_raw_data import archive_url,download,HTTPRangeFile

def test_official_url():
    assert archive_url(2024,1)=='https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.zip'
    with pytest.raises(ValueError):archive_url(2024,13)

def test_all_archive_members_and_string_ids(tmp_path,ride):
    path=tmp_path/'month.zip'
    with zipfile.ZipFile(path,'w') as z:
        z.writestr('a.csv',pd.DataFrame([ride]).to_csv(index=False))
        z.writestr('b.csv',pd.DataFrame([{**ride,'ride_id':'ride-2'}]).to_csv(index=False))
        z.writestr('__MACOSX/._a.csv','garbage')
    raw=pd.concat(read_chunks(path),ignore_index=True)
    assert len(raw)==2 and raw.start_station_id.tolist()==['001.20','001.20']
    clean,bad=clean_rides(raw,2024,1)
    assert bad.empty and clean.started_at.iloc[0]==pd.Timestamp('2024-01-01 17:00')

def test_taxi_schema_rejected(tmp_path):
    path=tmp_path/'taxi.csv';path.write_text('PULocationID,fare_amount\n12,8\n')
    with pytest.raises(ValueError,match='Unsupported source schema'):list(read_chunks(path))

def test_nested_archive_members(tmp_path,ride):
    inner=io.BytesIO()
    with zipfile.ZipFile(inner,'w') as z:z.writestr('rides.csv',pd.DataFrame([ride]).to_csv(index=False))
    path=tmp_path/'outer.zip'
    with zipfile.ZipFile(path,'w') as z:z.writestr('part.zip',inner.getvalue())
    assert sum(len(frame) for frame in read_chunks(path))==1

@pytest.mark.parametrize('change,reason',[
    ({'ride_id':None},'missing_ride_id'),({'start_station_id':''},'missing_start_station'),
    ({'member_casual':'other'},'invalid_membership'),({'rideable_type':'taxi'},'unsupported_bike_type'),
    ({'ended_at':'2024-01-01 11:59'},'invalid_duration'),({'start_lat':'NaN'},'invalid_coordinates'),
    ({'started_at':'2023-12-31 23:59'},'outside_source_month')])
def test_reasoned_quarantine(ride,change,reason):
    clean,bad=clean_rides(pd.DataFrame([{**ride,**change}]),2024,1)
    assert clean.empty and bad.rejection_reason.tolist()==[reason]

def test_optional_end_station_is_retained(ride):
    clean,bad=clean_rides(pd.DataFrame([{**ride,'end_station_id':None,'end_lat':None,'end_lng':None}]),2024,1)
    assert len(clean)==1 and bad.empty

@pytest.mark.parametrize('month,start,end',[(3,'2024-03-10 02:20','2024-03-10 03:20'),(11,'2024-11-03 01:20','2024-11-03 02:20')])
def test_dst_ambiguity_is_quarantined(ride,month,start,end):
    clean,bad=clean_rides(pd.DataFrame([{**ride,'started_at':start,'ended_at':end}]),2024,month)
    assert clean.empty and bad.rejection_reason.iloc[0]=='invalid_or_ambiguous_time'

@pytest.mark.parametrize('month,hours',[(1,744),(3,743),(11,721)])
def test_month_grid_uses_real_utc_hours(ride,month,hours):
    start=f'2024-{month:02}-01 12:00';end=f'2024-{month:02}-01 12:10'
    clean,_=clean_rides(pd.DataFrame([{**ride,'started_at':start,'ended_at':end}]),2024,month)
    bi,ml=hourly_marts(clean,2024,month)
    assert len(ml)==hours and bi.ride_count.sum()==ml.ride_count.sum()==1
    assert ml.station_id.unique().tolist()==['001.20']

def test_preserve_existing_source(tmp_path):
    path=tmp_path/'202401-citibike-tripdata.zip';path.write_bytes(b'existing')
    with pytest.raises(FileExistsError):download(2024,1,tmp_path)
    assert path.read_bytes()==b'existing'

def test_sample_cannot_enter_production_without_starting_spark(tmp_path,monkeypatch):
    from jobs.lakehouse import main
    path=tmp_path/'202401.sample.csv';path.write_text('unused')
    monkeypatch.setattr(sys,'argv',['bronze','--config','config/local.yml','--source_filepath',str(path),'--source_year','2024','--source_month','1'])
    with pytest.raises(ValueError,match='Sample sources'):main('bronze')

def test_range_reader_validates_response_and_budget(monkeypatch):
    import jobs.download_raw_data as module
    body=b'x'*(2*1024**2)
    class Response:
        status_code=206
        def __init__(self,lo,hi):self.headers={'Content-Range':f'bytes {lo}-{hi}/{len(body)}'};self.raw=io.BytesIO(body[lo:hi+1])
        def close(self):pass
        def raise_for_status(self):pass
    def get(url,headers,**kwargs):
        lo,hi=map(int,headers['Range'][6:].split('-'));return Response(lo,hi)
    monkeypatch.setattr(module.requests,'get',get)
    f=HTTPRangeFile('official',1024**2)
    assert f.read(20)==b'x'*20
    assert f.read(20)==b'x'*20 and f.transferred==1024**2
    f.seek(1024**2)
    with pytest.raises(ValueError,match='budget'):f.read(1)

def test_watcher_manifest_bounds_checksum_and_month(tmp_path,monkeypatch):
    import sftp_mock_daemon as watcher
    from jobs.citibike import checksum
    monkeypatch.setattr(watcher,'WATCH_DIRECTORY',str(tmp_path))
    p=tmp_path/'202401-citibike-tripdata.zip';p.write_bytes(b'archive')
    meta={'dataset':'citibike','source_filepath':'/host/path','source_filename':p.name,'source_year':2024,'source_month':1,'file_size_bytes':p.stat().st_size,'discovered_at_utc':'now','sha256':checksum(p)}
    assert watcher.validate_manifest('manifest',dict(meta))
    for change in [{'sample':True},{'source_month':2},{'sha256':'bad'},{'source_filename':'../outside.zip'}]:
        assert not watcher.validate_manifest('manifest',{**meta,**change})

def test_corrected_archive_names_preserve_original_and_require_revision_chain(tmp_path,monkeypatch):
    import sftp_mock_daemon as watcher
    from jobs.citibike import checksum
    monkeypatch.setattr(watcher,'WATCH_DIRECTORY',str(tmp_path))
    original=tmp_path/'202401-citibike-tripdata.zip';original.write_bytes(b'original source')
    revised=tmp_path/'202401-citibike-tripdata.r2.zip';revised.write_bytes(b'controlled revision')
    meta={'dataset':'citibike','source_filepath':str(revised),'source_filename':revised.name,'source_year':2024,'source_month':1,'file_size_bytes':revised.stat().st_size,'discovered_at_utc':'now','sha256':checksum(revised),'source_revision':2,'supersedes_sha256':checksum(original)}
    assert watcher.validate_manifest('manifest',dict(meta))
    assert not watcher.validate_manifest('manifest',{**meta,'source_revision':1})
    assert not watcher.validate_manifest('manifest',{**meta,'supersedes_sha256':None})
    with pytest.raises(FileExistsError,match='r2.zip'):
        download(2024,1,tmp_path,source_revision=2,supersedes_sha256=checksum(original))
    assert original.read_bytes()==b'original source' and revised.read_bytes()==b'controlled revision'
