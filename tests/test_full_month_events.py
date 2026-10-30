import hashlib,json
import signal
from types import SimpleNamespace
import pytest
import sftp_mock_daemon as watcher
import tools.full_month_queue as queue

def test_docker_sigterm_uses_clean_keyboard_interrupt_shutdown():
    with pytest.raises(KeyboardInterrupt):watcher.request_shutdown(signal.SIGTERM,None)

def test_observer_waits_for_new_delivery_instead_of_previous_success():
    from tools.full_month_events import chain_complete
    states={stage:[{'state':'success'}] for stage in ['bronze','silver','bi','ml']}
    assert chain_complete(states,1) and not chain_complete(states,2)
    for runs in states.values():runs.append({'state':'running'})
    assert not chain_complete(states,2)
    for runs in states.values():runs[-1]['state']='success'
    assert chain_complete(states,2)

@pytest.fixture
def isolated(tmp_path,monkeypatch):
    root=tmp_path/'artifacts'/'events';root.mkdir(parents=True)
    cfg={'catalog':{'spark_catalog_name':'citibike_fullmonth_events','warehouse_path':str(root/'warehouse')},'spark':{'spark.master':'local[1]','spark.driver.memory':'1g'}}
    monkeypatch.setattr(queue,'project_root',lambda:tmp_path)
    monkeypatch.setattr(queue,'load_config',lambda path:cfg)
    return root,cfg

def test_worker_transport_allows_only_isolated_etl(isolated):
    root,cfg=isolated
    command=['python','-m','jobs.bronze_ingest','--config',str(root/'config.yml'),'--source_filepath',str(root/'incoming'/'month.zip')]
    assert queue.validate_command(root,command)[1:]==command[1:]
    for bad in [command[:2]+['jobs.ml_train_pipeline']+command[3:],command[:-1]+[str(root.parent/'production.zip')],command[:3]+['--config','config/local.yml'],command+['--shell','anything']]:
        with pytest.raises(ValueError):queue.validate_command(root,bad)
    cfg['spark']['spark.master']='local[2]'
    with pytest.raises(ValueError):queue.validate_command(root,command)

def test_completed_queue_response_is_reused_without_subprocess(isolated,monkeypatch):
    root,_=isolated;command=['python','-m','jobs.gold_ml','--config',str(root/'config.yml')]
    key=hashlib.sha256(json.dumps(['ml','asset-run',2,command]).encode()).hexdigest()[:24]
    directory=root/'queue';directory.mkdir();(directory/(key+'.result.json')).write_text(json.dumps({'returncode':0,'stdout':'actual saved result','stderr':''}))
    def forbidden(*args,**kwargs):raise AssertionError('Queue client must not launch Spark')
    monkeypatch.setattr(queue.subprocess,'run',forbidden)
    assert queue.FileQueueRunner(root,'ml','asset-run',2)(command).stdout=='actual saved result'
    assert len(list(directory.glob('*.request.json')))==1

def test_atomic_manifest_rename_reaches_existing_watcher(monkeypatch):
    seen=[];monkeypatch.setattr(watcher,'process_manifest',seen.append)
    handler=watcher.InboundManifestStagingHandler()
    handler.on_moved(SimpleNamespace(is_directory=False,dest_path='incoming/month.zip.manifest.json'))
    handler.on_moved(SimpleNamespace(is_directory=False,dest_path='incoming/unfinished.pending'))
    assert seen==['incoming/month.zip.manifest.json']

def test_periodic_reconciliation_discovers_a_missed_bind_mount_event(tmp_path,monkeypatch):
    processed=tmp_path/'.processed';processed.mkdir()
    lost=tmp_path/'month.zip.manifest.json';lost.write_text('{}')
    (processed/'previous.manifest.json').write_text('{}')
    seen=[];monkeypatch.setattr(watcher,'WATCH_DIRECTORY',str(tmp_path));monkeypatch.setattr(watcher,'PROCESSED_DIRECTORY',str(processed))
    monkeypatch.setattr(watcher,'process_manifest',seen.append)
    monkeypatch.setattr(watcher,'pending_manifests',set())
    watcher.reconcile_manifests()
    assert seen==[str(lost)]

def test_duplicate_delivery_retains_both_archived_receipts(tmp_path,monkeypatch):
    processed=tmp_path/'.processed';processed.mkdir();monkeypatch.setattr(watcher,'PROCESSED_DIRECTORY',str(processed))
    source=tmp_path/'month.zip.manifest.json'
    source.write_text('first');watcher.archive_processed_manifest(str(source))
    source.write_text('second');watcher.archive_processed_manifest(str(source))
    assert sorted(path.read_text() for path in processed.iterdir())==['first','second']

def test_isolated_consumer_override_leaves_default_unchanged(monkeypatch):
    seen=[];defaults=watcher.CONSUMER_DAG_IDS
    monkeypatch.setenv('CONSUMER_DAG_IDS','validation_fullmonth_bronze,validation_fullmonth_silver')
    def get(url,**kwargs):
        seen.append(url.rsplit('/',1)[-1]);return SimpleNamespace(status_code=200,json=lambda:{'is_paused':False,'is_stale':False})
    monkeypatch.setattr(watcher.requests,'get',get)
    assert watcher.consumers_ready({}) and seen==['validation_fullmonth_bronze','validation_fullmonth_silver']
    assert watcher.CONSUMER_DAG_IDS==defaults
    monkeypatch.setenv('CONSUMER_DAG_IDS',' ')
    assert not watcher.consumers_ready({})
