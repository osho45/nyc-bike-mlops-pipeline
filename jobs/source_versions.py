"""Explicit source-revision policy and crash-safe local publication locking."""
from contextlib import contextmanager
from pathlib import Path

def revision(value):
    if isinstance(value,bool):raise ValueError('Source revision must be a positive integer')
    number=int(value)
    if number<1 or str(number)!=str(value):raise ValueError('Source revision must be a positive integer')
    return number

def publication_decision(current,incoming):
    new=revision(incoming['source_revision'])
    if current is None:
        if new!=1 or incoming.get('supersedes_sha256'):raise ValueError('Initial publication must start at revision 1')
        return 'publish'
    old=revision(current['source_revision'])
    if new<old:return 'stale'
    if new==old:
        if incoming['sha256']!=current['sha256']:raise ValueError('Conflicting checksum for the same source revision')
        return 'replay'
    if new!=old+1 or incoming.get('supersedes_sha256')!=current['sha256']:
        raise ValueError('Correction must be the next revision and supersede the currently published checksum')
    return 'publish'

@contextmanager
def month_lock(warehouse,year,month):
    path=Path(warehouse)/'.publication-locks'/f'{year}-{month:02}.lock';path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as lock:
        if path.stat().st_size==0:lock.write(b'0');lock.flush()
        lock.seek(0)
        if __import__('os').name=='nt':
            import msvcrt
            msvcrt.locking(lock.fileno(),msvcrt.LK_LOCK,1)
            try:yield
            finally:lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_UNLCK,1)
        else:
            import fcntl
            fcntl.flock(lock,fcntl.LOCK_EX)
            try:yield
            finally:fcntl.flock(lock,fcntl.LOCK_UN)
