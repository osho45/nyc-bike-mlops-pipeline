"""Citi Bike contracts and a bounded pandas reference implementation used by validation."""
from __future__ import annotations
import calendar,hashlib,io,json,zipfile
from pathlib import Path
import numpy as np
import pandas as pd

COLUMNS=['ride_id','rideable_type','started_at','ended_at','start_station_name','start_station_id','end_station_name','end_station_id','start_lat','start_lng','end_lat','end_lng','member_casual']
STRINGS=[c for c in COLUMNS if c not in ['start_lat','start_lng','end_lat','end_lng']]
TIMEZONE='America/New_York'

def validate_year_month(year,month):
    if not 2013<=int(year)<=2100 or not 1<=int(month)<=12:raise ValueError('Invalid Citi Bike year/month')

def month_bounds(year,month):
    validate_year_month(year,month)
    start=pd.Timestamp(year=year,month=month,day=1,tz=TIMEZONE)
    end=start+pd.offsets.MonthBegin(1)
    return start.tz_convert('UTC').tz_localize(None),end.tz_convert('UTC').tz_localize(None)

def checksum(path):
    with open(path,'rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def csv_members(path):
    """Read every CSV in an official monthly archive; never extract arbitrary paths."""
    path=Path(path)
    if path.suffix.lower()=='.csv':
        yield path.name,path.open('rb');return
    if path.suffix.lower()!='.zip' and not (path.name.endswith('.zip.partial') and zipfile.is_zipfile(path)):raise ValueError('Expected modern Citi Bike CSV or ZIP, not taxi Parquet')
    def members(z,prefix='',depth=0):
        if depth>2:raise ValueError('Excessively nested archive')
        for info in sorted(z.infolist(),key=lambda i:i.filename):
            if info.is_dir() or '__MACOSX' in info.filename or Path(info.filename).name.startswith('.'):continue
            if info.file_size>4*1024**3:raise ValueError('Oversized archive member')
            if info.filename.lower().endswith('.csv'):yield prefix+info.filename,z.open(info)
            elif info.filename.lower().endswith('.zip'):
                if info.file_size>512*1024**2:raise ValueError('Nested ZIP exceeds memory guard')
                with z.open(info) as f,zipfile.ZipFile(io.BytesIO(f.read())) as inner:yield from members(inner,prefix+info.filename+'/',depth+1)
    with zipfile.ZipFile(path) as z:yield from members(z)

def read_chunks(path,chunksize=50000):
    found=False
    for member,stream in csv_members(path):
        found=True
        with stream:
            for frame in pd.read_csv(stream,dtype='string',chunksize=chunksize):
                missing=set(COLUMNS)-set(frame.columns)
                if missing:raise ValueError(f'Unsupported source schema in {member}; missing {sorted(missing)}')
                frame=frame[COLUMNS].copy();frame['source_member']=member;yield frame
    if not found:raise ValueError('Archive contains no usable CSV members')

def clean_rides(raw,year,month):
    """Return accepted and reason-coded rejected rows. IDs remain opaque strings."""
    missing=set(COLUMNS)-set(raw.columns)
    if missing:raise ValueError(f'Missing source fields: {sorted(missing)}')
    out=raw.copy();reason=pd.Series('',index=out.index,dtype='string')
    def reject(mask,label):
        nonlocal reason
        mask=mask.fillna(True);reason=reason.mask(mask & reason.eq(''),label)
    for c in STRINGS:out[c]=out[c].astype('string').str.strip()
    for c in ['started_at','ended_at']:
        naive=pd.to_datetime(out[c],errors='coerce',format='mixed')
        # Public source has local wall times without offsets: quarantine DST ambiguity rather than guess.
        if getattr(naive.dt,'tz',None) is not None:raise ValueError('Expected official offset-free local source timestamps')
        local=naive.dt.tz_localize(TIMEZONE,ambiguous='NaT',nonexistent='NaT')
        reject(local.isna(),'invalid_or_ambiguous_time')
        if c=='started_at':reject((local.dt.year!=year)|(local.dt.month!=month),'outside_source_month')
        out[c]=local.dt.tz_convert('UTC').dt.tz_localize(None)
    reject(out.ride_id.isna()|out.ride_id.eq(''),'missing_ride_id')
    reject(out.start_station_id.isna()|out.start_station_id.eq(''),'missing_start_station')
    reject(~out.member_casual.isin(['member','casual']),'invalid_membership')
    reject(~out.rideable_type.isin(['classic_bike','electric_bike','docked_bike']),'unsupported_bike_type')
    duration=(out.ended_at-out.started_at).dt.total_seconds();reject((duration<=0)|(duration>86400),'invalid_duration')
    for c in ['start_lat','start_lng','end_lat','end_lng']:
        value=pd.to_numeric(out[c],errors='coerce');limit=90 if c.endswith('lat') else 180
        reject(out[c].notna() & (value.isna()|value.abs().gt(limit)),'invalid_coordinates');out[c]=value
    out['duration_seconds']=duration;out['source_year']=year;out['source_month']=month
    out['rejection_reason']=reason
    return out[reason.eq('')].drop(columns='rejection_reason'),out[reason.ne('')]

def hourly_marts(clean,year,month,complete_month=True):
    """Reference gold layers; zero filling requires a complete monthly source."""
    if clean.empty:raise ValueError('No accepted rides; refuse to manufacture observations')
    frame=clean.copy();frame['station_hour_ts']=frame.started_at.dt.floor('h');frame['station_id']=frame.start_station_id.astype('string')
    bi=frame.groupby(['station_id','station_hour_ts'],as_index=False).agg(ride_count=('ride_id','size'),avg_duration_seconds=('duration_seconds','mean'),member_rides=('member_casual',lambda s:int(s.eq('member').sum())),electric_rides=('rideable_type',lambda s:int(s.eq('electric_bike').sum())))
    start,end=month_bounds(year,month)
    if not complete_month:start=frame.station_hour_ts.min();end=frame.station_hour_ts.max()+pd.Timedelta(hours=1)
    grid=pd.MultiIndex.from_product([sorted(frame.station_id.dropna().unique()),pd.date_range(start,end,freq='h',inclusive='left')],names=['station_id','station_hour_ts']).to_frame(index=False)
    ml=grid.merge(bi[['station_id','station_hour_ts','ride_count']],on=['station_id','station_hour_ts'],how='left');ml['ride_count']=ml.ride_count.fillna(0).astype('int64')
    for f in (bi,ml):f['target_year']=year;f['target_month']=month
    return bi,ml


def deduplicate_rides(clean):
    """Collapse identical source payloads; never choose an arbitrary conflicting ride."""
    distinct=clean.drop_duplicates(subset=COLUMNS)
    if distinct.ride_id.duplicated().any():
        raise ValueError('Conflicting duplicate ride IDs; inspect source before publishing')
    return clean.drop_duplicates('ride_id')
