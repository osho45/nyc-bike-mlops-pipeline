import json
from pathlib import Path
import pytest
from tools.training_transfer import sha,safe_path,source_closure,verify,copy_files,full_network_plan


def fixture(tmp_path):
 source=tmp_path/'source';source.mkdir();f=source/'jobs/a.py';f.parent.mkdir();f.write_bytes(b'content-preserved'*100)
 record={'path':'jobs/a.py','category':'source','bytes':f.stat().st_size,'sha256':sha(f)}
 return source,{'files':[record],'development_rows':None}


def test_resumable_copy_and_identical_destination_reuse(tmp_path):
 source,m=fixture(tmp_path);dest=tmp_path/'dest';partial=dest/'jobs/a.py.transfer-part';partial.parent.mkdir(parents=True);partial.write_bytes((source/'jobs/a.py').read_bytes()[:31])
 result=copy_files(source,dest,m);assert result['resumed']==1 and result['copied']==1
 assert verify(dest,m) and not partial.exists()
 assert copy_files(source,dest,m)['skipped_identical']==1


def test_conflicting_destination_and_partial_are_preserved(tmp_path):
 source,m=fixture(tmp_path);dest=tmp_path/'dest';target=dest/'jobs/a.py';target.parent.mkdir(parents=True);target.write_bytes(b'user data')
 with pytest.raises(FileExistsError):copy_files(source,dest,m)
 assert target.read_bytes()==b'user data'
 dest2=tmp_path/'dest2';partial=dest2/'jobs/a.py.transfer-part';partial.parent.mkdir(parents=True);partial.write_bytes(b'wrong')
 with pytest.raises(ValueError,match='prefix'):copy_files(source,dest2,m)
 assert partial.read_bytes()==b'wrong'


def test_path_traversal_and_source_tampering_are_rejected(tmp_path):
 for relative in ['../secret','/absolute','C:/private','jobs\\escape.py']:
  with pytest.raises(ValueError):safe_path(tmp_path,relative)
 source,m=fixture(tmp_path);(source/'jobs/a.py').write_bytes(b'changed')
 with pytest.raises(ValueError):copy_files(source,tmp_path/'dest',m)
 with pytest.raises(ValueError):verify(source,m)


def test_source_closure_captures_uncommitted_transitive_code_without_importing(tmp_path):
 (tmp_path/'tools').mkdir();(tmp_path/'jobs').mkdir()
 (tmp_path/'tools/a.py').write_text('from jobs.b import value\nraise RuntimeError("must not execute")\n')
 (tmp_path/'jobs/b.py').write_text('value=1\n')
 assert source_closure(tmp_path,['tools/a.py'])=={'tools/a.py','jobs/b.py'}


def test_full_network_planning_never_enables_or_trains(tmp_path):
 source,m=fixture(tmp_path);result=full_network_plan(source,m)
 assert result['status']=='blocked' and not result['full_network_training_enabled'] and not result['model_invoked']
 assert result['stations']==1898 and '384' in result['blockers'][1]


def test_linux_host_memory_and_wsl_host_confusion(tmp_path,monkeypatch):
 from tools import native_4g_preflight as p
 file=tmp_path/'meminfo';file.write_text('MemTotal: 16777216 kB\nMemAvailable: 8388608 kB\n')
 monkeypatch.setattr(p.sys,'platform','linux');monkeypatch.setattr(p.platform,'release',lambda:'6.8.0-generic');monkeypatch.setattr(p,'Path',lambda value:file)
 assert p.host_memory()==(16*1024**3,8*1024**3)
 monkeypatch.setattr(p.platform,'release',lambda:'6.6-microsoft-standard-WSL2')
 with pytest.raises(RuntimeError,match='native Windows'):p.host_memory()


def test_portable_probe_and_uid_configuration():
 import yaml
 c=yaml.safe_load(Path('docker-compose.native-4g.yml').read_text());probe=c['services']['resource-probe'];trainer=c['services']['trainer']
 assert probe['mem_limit']==probe['memswap_limit']=='64m' and probe['network_mode']=='none'
 assert 'NATIVE_UID' in trainer['user'] and trainer['restart']=='no'


def test_remote_docker_context_is_rejected_before_host_memory_is_mixed(monkeypatch):
 from tools import native_4g_preflight as p
 monkeypatch.setenv('DOCKER_HOST','ssh://other-machine')
 def forbidden():raise AssertionError('Local RAM must not be used for remote daemon admission')
 monkeypatch.setattr(p,'host_memory',forbidden)
 with pytest.raises(RuntimeError,match='daemon host'):p.collect('fixture',0)
