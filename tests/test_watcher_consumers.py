import pytest
import sftp_mock_daemon as watcher

@pytest.mark.parametrize('paused,stale,status,expected',[(False,False,200,True),(True,False,200,False),(False,True,200,False),(False,False,404,False)])
def test_consumer_activation_checked_before_manifest_ack(monkeypatch,paused,stale,status,expected):
    class Response:
        status_code=status
        def json(self):return {'is_paused':paused,'is_stale':stale}
    monkeypatch.setattr(watcher.requests,'get',lambda *args,**kwargs:Response())
    assert watcher.consumers_ready({}) is expected

def test_paused_consumer_retains_manifest_without_emission_or_archive(tmp_path,monkeypatch):
    path=str(tmp_path/'new.manifest.json')
    monkeypatch.setattr(watcher,'load_manifest',lambda path:{'dataset':'citibike'})
    monkeypatch.setattr(watcher,'validate_manifest',lambda *args:True)
    monkeypatch.setattr(watcher,'fetch_authenticated_jwt',lambda:'not-printed')
    monkeypatch.setattr(watcher,'consumers_ready',lambda headers:False)
    def forbidden(*args,**kwargs):raise AssertionError('Paused consumer must not emit or archive')
    monkeypatch.setattr(watcher.requests,'post',forbidden)
    monkeypatch.setattr(watcher,'archive_processed_manifest',forbidden)
    watcher.process_manifest(path)
    assert path in watcher.pending_manifests
    watcher.pending_manifests.discard(path)
