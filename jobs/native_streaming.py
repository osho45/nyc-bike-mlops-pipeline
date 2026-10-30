"""Opt-in bounded input adapter; unchanged features and original matrix manifests."""
import os
import numpy as np
from jobs.native_lgbm import DiskSequence

class BufferedDiskSequence(DiskSequence):
    """Borrowed float32 batches, independent float64 samples; synchronous consumers only."""
    def __init__(self,root,evict=True):
        if evict and not hasattr(os,'posix_fadvise'):raise RuntimeError('Cache advice requires Linux posix_fadvise')
        super().__init__(root)
        self.matrix._mmap.close();del self.matrix
        self.reader=open(self.root/'X.f32','rb',buffering=0)
        self.buffer=np.empty((self.batch_size,len(self.manifest['features'])),dtype='float32')
        self.evict=evict;self.advice_calls=0;self.bytes_read=0
    def release_cache(self):
        if self.evict:
            os.posix_fadvise(self.reader.fileno(),0,0,os.POSIX_FADV_DONTNEED);self.advice_calls+=1
    def __getitem__(self,index):
        if isinstance(index,(int,np.integer)):
            if not 0<=index<self.rows:raise IndexError(index)
            start=index;n=1;scalar=True;self.scalar_reads+=1
        elif isinstance(index,slice):
            if index.step not in [None,1]:raise ValueError('Contiguous batches required')
            start,stop,_=index.indices(self.rows);n=max(0,stop-start);scalar=False
            if n>self.batch_size:raise ValueError('Native row batch exceeds limit')
            self.batch_reads+=1;self.max_batch_seen=max(self.max_batch_seen,n)
        else:raise TypeError('Only scalar or contiguous batch access supported')
        view=self.buffer[:n];self.reader.seek(start*view.shape[1]*4)
        if not n:return view
        raw=memoryview(view).cast('B');read=0
        while read<len(raw):
            count=self.reader.readinto(raw[read:])
            if not count:raise ValueError('Truncated feature matrix')
            read+=count
        self.bytes_read+=read
        result=view[0].astype('float64') if scalar else view
        if not scalar or self.scalar_reads%512==0:self.release_cache()
        return result
    def close(self):
        if self.reader.closed:return
        self.release_cache();self.reader.close()
        if hasattr(self.labels,'_mmap'):self.labels._mmap.close()


class BlockBufferedDiskSequence(BufferedDiskSequence):
    """Cache one 4096-row sample window; preserve independent float64 sample rows."""
    def __init__(self,root,evict=True):
        super().__init__(root,evict=evict)
        self.sample_first=self.sample_last=-1;self.sample_block_reads=0
    def __getitem__(self,index):
        if not isinstance(index,(int,np.integer)):
            self.sample_first=self.sample_last=-1
            return super().__getitem__(index)
        if not 0<=index<self.rows:raise IndexError(index)
        self.scalar_reads+=1
        if not self.sample_first<=index<self.sample_last:
            first=index//self.batch_size*self.batch_size;last=min(first+self.batch_size,self.rows)
            super().__getitem__(slice(first,last));self.batch_reads-=1
            self.sample_first,self.sample_last=first,last;self.sample_block_reads+=1
        return self.buffer[index-self.sample_first].astype('float64')
