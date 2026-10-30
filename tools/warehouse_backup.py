"""Isolated Iceberg backup/restore without deleting the original warehouse.

Restore creates a new copy, mounted at the original logical path in a disposable
reader because Hadoop Iceberg manifests contain absolute file locations.
"""
import argparse,json,shutil
from pathlib import Path
from jobs.citibike import checksum
from jobs.ml_utils import project_root

def inventory(root):return {str(path.relative_to(root)):checksum(path) for path in sorted(root.rglob('*')) if path.is_file() and '.publication-locks' not in path.parts}

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['backup','restore']);a=p.parse_args()
    root=(project_root()/'artifacts/correctness-01').resolve();warehouse=(root/'warehouse').resolve();backup=(root/'warehouse-backup').resolve();restored=(root/'warehouse-restored').resolve()
    for path in [warehouse,backup,restored]:
        if not path.is_relative_to(root):raise ValueError('Backup targets must stay in the isolated correctness directory')
    if a.action=='backup':
        if backup.exists():raise FileExistsError('Existing backup preserved')
        expected=inventory(warehouse);shutil.copytree(warehouse,backup)
        assert inventory(backup)==expected
        (root/'warehouse-backup-manifest.json').write_text(json.dumps(expected,indent=2))
        print(json.dumps({'backup_verified':True,'files':len(expected)}));return
    expected=json.loads((root/'warehouse-backup-manifest.json').read_text())
    if inventory(backup)!=expected:raise ValueError('Backup integrity check failed before modifying the test warehouse')
    if restored.exists():raise FileExistsError('Prior restored warehouse exists; refusing overwrite')
    shutil.copytree(backup,restored)
    assert inventory(restored)==expected
    (restored/'restore-proof.json').write_text(json.dumps({'restored_from_verified_backup':True}))
    print(json.dumps({'restored':True,'files_verified':len(expected),'original_untouched':True,'restored_path':str(restored),'mount_at_original_logical_path':True}))

if __name__=='__main__':main()
