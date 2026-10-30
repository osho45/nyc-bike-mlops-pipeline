"""Virtual concatenation of immutable station matrices; one reusable reader buffer."""
import bisect,json,os
from pathlib import Path
import numpy as np
import lightgbm as lgb

class StationMatrixSequence(lgb.Sequence):
    batch_size=4096
    def __init__(self,manifest_path):
        self.root=Path(manifest_path).parent;self.manifest=json.loads(Path(manifest_path).read_text());self.parts=self.manifest['parts'];self.ends=[];n=0
        for part in self.parts:n+=part['rows'];self.ends.append(n)
        self.rows=n;self.columns=len(self.manifest['features']);self.buffer=np.empty((self.batch_size,self.columns),dtype='float32');self.labels=np.memmap(self.root/self.manifest['label_file'],mode='r',dtype='float32',shape=(n,));self.reader=None;self.open_path=None;self.sample_first=self.sample_last=-1;self.scalar_reads=self.batch_reads=0
    def __len__(self):return self.rows
    def _read(self,start,stop):
        n=stop-start
        if n>self.batch_size:raise ValueError('Bounded native batch exceeded')
        written=0
        while start<stop:
            idx=bisect.bisect_right(self.ends,start);part=self.parts[idx];base=0 if idx==0 else self.ends[idx-1];count=min(stop-start,self.ends[idx]-start);path=self.root/part['x']
            if path!=self.open_path:
                if self.reader:self.reader.close()
                self.reader=open(path,'rb',buffering=0);self.open_path=path
            self.reader.seek((part.get('offset_rows',0)+start-base)*self.columns*4);view=memoryview(self.buffer[written:written+count]).cast('B');offset=0
            while offset<len(view):
                got=self.reader.readinto(view[offset:])
                if not got:raise ValueError('Truncated station feature matrix')
                offset+=got
            if hasattr(os,'posix_fadvise'):os.posix_fadvise(self.reader.fileno(),0,0,os.POSIX_FADV_DONTNEED)
            start+=count;written+=count
        return self.buffer[:n]
    def __getitem__(self,index):
        if isinstance(index,(int,np.integer)):
            if not 0<=index<self.rows:raise IndexError(index)
            self.scalar_reads+=1
            if not self.sample_first<=index<self.sample_last:
                self.sample_first=index//self.batch_size*self.batch_size;self.sample_last=min(self.sample_first+self.batch_size,self.rows);self._read(self.sample_first,self.sample_last)
            return self.buffer[index-self.sample_first].astype('float64')
        if not isinstance(index,slice) or index.step not in [None,1]:raise ValueError('Contiguous native input required')
        self.sample_first=self.sample_last=-1;self.batch_reads+=1;start,stop,_=index.indices(self.rows)
        return self._read(start,max(start,stop))
    def close(self):
        if self.reader:self.reader.close();self.reader=None
        self.labels._mmap.close()
