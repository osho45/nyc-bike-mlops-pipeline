import sys
import pytest
from tools import warehouse_backup

def test_corrupt_backup_rejected_before_original_is_touched(tmp_path,monkeypatch):
    root=tmp_path/'artifacts/correctness-01';warehouse=root/'warehouse';warehouse.mkdir(parents=True)
    (warehouse/'metadata.json').write_text('original metadata')
    monkeypatch.setattr(warehouse_backup,'project_root',lambda:tmp_path)
    monkeypatch.setattr(sys,'argv',['warehouse_backup','backup']);warehouse_backup.main()
    (root/'warehouse-backup/metadata.json').write_text('corrupted backup')
    monkeypatch.setattr(sys,'argv',['warehouse_backup','restore'])
    with pytest.raises(ValueError,match='integrity'):warehouse_backup.main()
    assert (warehouse/'metadata.json').read_text()=='original metadata'
    assert not (root/'warehouse-restored').exists()

def test_restore_copies_backup_and_preserves_original(tmp_path,monkeypatch):
    root=tmp_path/'artifacts/correctness-01';warehouse=root/'warehouse';warehouse.mkdir(parents=True)
    (warehouse/'data.parquet').write_bytes(b'test-backup-payload')
    monkeypatch.setattr(warehouse_backup,'project_root',lambda:tmp_path)
    monkeypatch.setattr(sys,'argv',['warehouse_backup','backup']);warehouse_backup.main()
    monkeypatch.setattr(sys,'argv',['warehouse_backup','restore']);warehouse_backup.main()
    assert (root/'warehouse-restored/data.parquet').read_bytes()==(warehouse/'data.parquet').read_bytes()
    with pytest.raises(FileExistsError):warehouse_backup.main()
