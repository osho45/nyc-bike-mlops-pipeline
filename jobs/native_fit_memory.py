"""Opt-in fitting controls for fixed-budget native capacity experiments."""
import ctypes,gc

def fit_settings(base,variant):
    choices={'baseline','compact','hist8','one_thread','deferred'}
    if variant not in choices:raise ValueError('Unauthorized fitting variant')
    params=dict(base)
    if variant=='hist8':params['histogram_pool_size']=8
    if variant=='one_thread':params['num_threads']=1
    return params

def validate_deferred(rounds,early_stopping):
    if rounds!=100 or early_stopping:raise ValueError('Deferred validation requires the fixed 100-round capacity protocol without early stopping')

def release_python_labels(dataset):
    """Discard only the Python cache; never call native set_label(None)."""
    old=dataset.label
    count=0 if old is None else old.nbytes
    dataset.label=None
    return count

def trim_allocator():
    gc.collect()
    libc=ctypes.CDLL('libc.so.6')
    return int(libc.malloc_trim(0))
