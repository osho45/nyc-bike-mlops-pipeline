import io,json,zipfile
from pathlib import Path
from types import SimpleNamespace
import numpy as np,pandas as pd,pytest,requests
from jobs import monthly_archives as archive
from jobs.citibike import COLUMNS
from jobs.ml_features import build_training_features,REQUIRED_FEATURES
from jobs.year_features import prepare_year_model_frames

def payload():
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w') as z:z.writestr('data.csv',','.join(COLUMNS)+'\n')
    return stream.getvalue()

class Response:
    def __init__(self,data=b'',status=200,headers=None):self.data=data;self.status_code=status;self.headers=headers or {}
    def raise_for_status(self):pass
    def close(self):pass
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,size):yield self.data

def fake_origin(monkeypatch,data,offset=0):
    etag='"version-1"'
    monkeypatch.setattr(archive.requests,'head',lambda *a,**kw:Response(headers={'Content-Length':str(len(data)),'ETag':etag}))
    def get(url,headers,**kwargs):
        assert headers['If-Match']==etag
        actual=int(headers['Range'].split('=')[1].split('-')[0]) if 'Range' in headers else 0
        assert actual==offset
        return Response(data[actual:],206 if actual else 200,{'Content-Length':str(len(data)-actual),'ETag':etag,**({'Content-Range':f'bytes {actual}-{len(data)-1}/{len(data)}'} if actual else {})})
    monkeypatch.setattr(archive.requests,'get',get)
    return etag

def test_real_zip_range_resume_and_verified_reuse(tmp_path,monkeypatch):
    data=payload();offset=40;etag=fake_origin(monkeypatch,data,offset)
    partial=tmp_path/'202402-citibike-tripdata.zip.partial';partial.write_bytes(data[:offset]);archive.atomic_json(partial.with_suffix('.partial.json'),{'url':archive.BASE+'202402-citibike-tripdata.zip','etag':etag,'total_bytes':len(data)})
    meta=archive.download_month(2024,2,tmp_path,max_mb=1)
    assert meta['resumed_from_bytes']==offset and meta['bytes_transferred_this_call']==len(data)-offset
    assert (tmp_path/meta['source_filename']).read_bytes()==data
    manifest=tmp_path/(meta['source_filename']+'.manifest.json');saved=json.loads(manifest.read_text());saved['source_filepath']='C:\\different-host\\archive.zip';archive.atomic_json(manifest,saved)
    monkeypatch.setattr(archive.requests,'head',lambda *a,**k:pytest.fail('Verified reuse must not redownload'))
    reused=archive.download_month(2024,2,tmp_path,max_mb=1)
    assert reused['reused'] and reused['source_filepath']==str((tmp_path/meta['source_filename']).resolve())

def test_network_failure_preserves_and_resumes_prefix(tmp_path,monkeypatch):
    data=payload();etag=fake_origin(monkeypatch,data,40);good=archive.requests.get;calls=[]
    class Broken(Response):
        def iter_content(self,size):yield data[:40];raise requests.ConnectionError('controlled connection loss')
    def get(*args,**kwargs):
        calls.append(kwargs['headers'])
        return Broken(headers={'Content-Length':str(len(data)),'ETag':etag}) if len(calls)==1 else good(*args,**kwargs)
    monkeypatch.setattr(archive.requests,'get',get);monkeypatch.setattr(archive.time,'sleep',lambda _:None)
    result=archive.download_month(2024,2,tmp_path,max_mb=1,retries=1)
    assert result['attempts_this_call']==2 and calls[1]['Range']=='bytes=40-'
    assert result['bytes_transferred_this_call']==len(data)

def test_changed_origin_preserves_partial(tmp_path,monkeypatch):
    data=payload();fake_origin(monkeypatch,data,40);partial=tmp_path/'202402-citibike-tripdata.zip.partial';partial.write_bytes(data[:40]);archive.atomic_json(partial.with_suffix('.partial.json'),{'url':archive.BASE+'202402-citibike-tripdata.zip','etag':'old-version','total_bytes':len(data)})
    with pytest.raises(ValueError,match='Origin changed'):archive.download_month(2024,2,tmp_path)
    assert partial.read_bytes()==data[:40]

def test_bad_existing_archive_not_overwritten(tmp_path):
    target=tmp_path/'202402-citibike-tripdata.zip';target.write_bytes(b'preserve me');archive.atomic_json(target.with_suffix('.zip.manifest.json'),{})
    with pytest.raises(ValueError):archive.download_month(2024,2,tmp_path)
    assert target.read_bytes()==b'preserve me'

def test_crash_after_data_rename_repairs_manifest_without_redownload(tmp_path,monkeypatch):
    data=payload();target=tmp_path/'202402-citibike-tripdata.zip';target.write_bytes(data)
    meta={'dataset':'citibike','sample':False,'source_year':2024,'source_month':2,'source_filename':target.name,'source_url':archive.BASE+target.name,'file_size_bytes':len(data),'sha256':archive.checksum(target)}
    archive.atomic_json(target.with_suffix('.zip.partial.json'),{'complete_manifest':meta})
    monkeypatch.setattr(archive.requests,'head',lambda *a,**k:pytest.fail('Committed data must not be redownloaded'))
    assert archive.download_month(2024,2,tmp_path)['reused']
    assert target.with_suffix('.zip.manifest.json').exists()

def test_multi_year_execution_gate_precedes_any_ingestion(tmp_path,monkeypatch):
    import tools.monthly_ingestion as controller
    monkeypatch.setattr(controller,'ROOT',tmp_path);monkeypatch.setattr(controller,'inventory',lambda:[])
    monkeypatch.setattr('sys.argv',['monthly_ingestion','run','--all'])
    monkeypatch.setattr(controller,'activate',lambda:pytest.fail('Must not activate ingestion before pilot passes'))
    with pytest.raises(ValueError,match='pilot'):controller.main()

def test_official_listing_paginates_and_ignores_non_monthly_keys(monkeypatch):
    calls=[]
    def get(url,params,**kwargs):
        calls.append(dict(params));second='continuation-token' in params
        keys=['202602-citibike-tripdata.zip'] if second else ['202601-citibike-tripdata.zip','JC-202601-citibike-tripdata.csv.zip','202601-readme.txt']
        body='<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>'+('false' if second else 'true')+'</IsTruncated>'
        if not second:body+='<NextContinuationToken>next-page</NextContinuationToken>'
        body+=''.join('<Contents><Key>'+key+'</Key><Size>123</Size><ETag>version</ETag></Contents>' for key in keys)+'</ListBucketResult>'
        return SimpleNamespace(content=body.encode(),raise_for_status=lambda:None)
    monkeypatch.setattr(archive.requests,'get',get)
    items=archive.available_months([2026])
    assert [x['month'] for x in items]==[1,2] and {x['split'] for x in items}=={'test'}
    assert calls[1]['continuation-token']=='next-page'

def test_worker_resume_preserves_telemetry_and_completed_responses(tmp_path,monkeypatch):
    import tools.full_month_queue as queue
    monkeypatch.setattr(queue,'project_root',lambda:tmp_path)
    root=tmp_path/'artifacts'/'monthly';root.mkdir(parents=True);(root/'queue').mkdir()
    (root/'worker-memory.csv').write_text('old memory');(root/'stop-worker').write_text('old stop')
    response=root/'queue'/'complete.result.json';response.write_text('keep completed result')
    with pytest.raises(ValueError,match='resume'):queue.preserve_worker_attempt(root,False)
    queue.preserve_worker_attempt(root,True)
    assert not (root/'stop-worker').exists() and response.read_text()=='keep completed result'
    assert next((root/'worker-attempts').glob('*/worker-memory.csv')).read_text()=='old memory'

def test_gap_does_not_create_false_hourly_lags():
    times=pd.date_range('2024-01-01',periods=220,freq='h').append(pd.date_range('2024-03-01',periods=220,freq='h'))
    frame=pd.DataFrame({'station_id':'s','station_hour_ts':times,'ride_count':np.arange(440)})
    result=build_training_features(frame)
    assert len(result)==104 and result.loc[result.station_hour_ts>='2024-03-01'].station_hour_ts.min()==pd.Timestamp('2024-03-08')

def test_calendar_year_roles_keep_history_and_train_only_thresholds():
    times=pd.date_range('2024-12-20', '2026-01-10',freq='h')
    frame=pd.DataFrame({'station_id':'s','station_hour_ts':times,'ride_count':np.arange(len(times))%20})
    train,val,test,thresholds,_=prepare_year_model_frames(frame)
    for part,year in [(train,2024),(val,2025),(test,2026)]:assert set(pd.to_datetime(part.station_hour_ts,utc=True).dt.tz_convert('America/New_York').dt.year)=={year}
    target=val.station_hour_ts.min();assert val.iloc[0].lag_168h==frame.set_index('station_hour_ts').loc[target-pd.Timedelta(hours=168),'ride_count']
    changed=frame.copy();changed.loc[changed.station_hour_ts>=target,'ride_count']=99999
    changed_train,_,_,changed_thresholds,_=prepare_year_model_frames(changed)
    assert thresholds==changed_thresholds
    np.testing.assert_allclose(train[REQUIRED_FEATURES],changed_train[REQUIRED_FEATURES])

def test_strict_year_finalization_does_not_fit_validation_rows(tmp_path,monkeypatch):
    import jobs.ml_experiments as experiments
    seen=[]
    class SpyModel:
        def fit(self,x,y):seen.extend(x.marker.tolist());return self
    monkeypatch.setattr(experiments,'build_model',lambda *args:SpyModel())
    monkeypatch.setattr(experiments,'evaluate_classifier',lambda *args,**kwargs:{})
    monkeypatch.setattr(experiments,'save_confusion_matrix',lambda *args,**kwargs:None)
    champion=SimpleNamespace(model_family='spy',params={},run_name='unit fixture',run_id='test-only')
    experiments.log_test_metrics_to_champion_run(champion,pd.DataFrame({'marker':[2024]}),pd.Series([0]),pd.DataFrame({'marker':[2025]}),pd.Series([1]),pd.DataFrame({'marker':[2026]}),pd.Series([1]),tmp_path,refit_validation=False)
    assert seen==[2024]
