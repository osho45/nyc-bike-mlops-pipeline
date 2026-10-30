import numpy as np
import pytest
from jobs.native_streaming import BufferedDiskSequence
from jobs.native_lgbm import MatrixWriter
from tests.test_native_capacity import data


def test_buffered_adapter_preserves_values_and_independent_sampling(tmp_path):
 frame=data(20);writer=MatrixWriter(tmp_path/'train','train');writer.append(frame);writer.seal()
 seq=BufferedDiskSequence(tmp_path/'train',evict=False)
 try:
  assert seq.verify()
  a=seq[2];b=seq[3]
  assert a.dtype==np.float64 and not np.shares_memory(a,b)
  np.testing.assert_array_equal(a,frame[seq.manifest['features']].iloc[2])
  first=seq[:10];assert first.dtype==np.float32
  np.testing.assert_array_equal(first,frame[seq.manifest['features']].iloc[:10])
  second=seq[10:20];assert np.shares_memory(first,second)
  np.testing.assert_array_equal(second,frame[seq.manifest['features']].iloc[10:20])
  assert seq[0:0].shape==(0,len(seq.manifest['features']))
  with pytest.raises(IndexError):seq[20]
  with pytest.raises(ValueError):seq[::2]
 finally:seq.close();seq.close()


def test_buffered_adapter_rejects_truncated_matrix(tmp_path):
 writer=MatrixWriter(tmp_path/'train','train');writer.append(data(5));writer.seal()
 seq=BufferedDiskSequence(tmp_path/'train',evict=False)
 try:
  # A dedicated fixture is truncated; no project artifact is modified.
  with (tmp_path/'train/X.f32').open('r+b') as f:f.truncate(4)
  with pytest.raises(ValueError,match='Truncated'):seq[:5]
 finally:seq.close()


def test_shards_keep_global_row_order_and_targets(tmp_path):
 from tools.profile_native_construction import SequenceShard
 writer=MatrixWriter(tmp_path/'train','train');frame=data(12);writer.append(frame);writer.seal()
 seq=BufferedDiskSequence(tmp_path/'train',evict=False)
 try:
  shard=SequenceShard(seq,5,7)
  np.testing.assert_array_equal(shard[0],frame[seq.manifest['features']].iloc[5])
  np.testing.assert_array_equal(shard[1:7],frame[seq.manifest['features']].iloc[6:12])
  with pytest.raises(IndexError):shard[7]
 finally:seq.close()


def test_advisory_cache_release_does_not_change_values(tmp_path,monkeypatch):
 import os
 calls=[]
 monkeypatch.setattr(os,'posix_fadvise',lambda *args:calls.append(args),raising=False)
 monkeypatch.setattr(os,'POSIX_FADV_DONTNEED',4,raising=False)
 writer=MatrixWriter(tmp_path/'train','train');frame=data(4);writer.append(frame);writer.seal()
 seq=BufferedDiskSequence(tmp_path/'train')
 try:
  np.testing.assert_array_equal(seq[:4],frame[seq.manifest['features']])
  assert calls and seq.verify()
 finally:seq.close()


def test_sample_window_reuse_and_batch_invalidation(tmp_path):
 from jobs.native_streaming import BlockBufferedDiskSequence
 writer=MatrixWriter(tmp_path/'train','train');frame=data(5000);writer.append(frame);writer.seal()
 seq=BlockBufferedDiskSequence(tmp_path/'train',evict=False)
 try:
  a=seq[2];b=seq[3];assert seq.sample_block_reads==1 and seq.scalar_reads==2
  np.testing.assert_array_equal(a,frame[seq.manifest['features']].iloc[2])
  seq[4096:5000];c=seq[3];assert seq.sample_block_reads==2
  np.testing.assert_array_equal(c,b)
  tail=seq[4999];assert seq.sample_block_reads==3
  np.testing.assert_array_equal(tail,frame[seq.manifest['features']].iloc[4999])
 finally:seq.close()
