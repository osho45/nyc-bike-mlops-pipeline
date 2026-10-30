import zipfile
import pandas as pd
from tools.full_month_validate import reference

def test_streamed_month_audit_counts_duplicate_ids_across_members(tmp_path,ride):
    source=tmp_path/'month.zip'
    with zipfile.ZipFile(source,'w') as archive:
        archive.writestr('first.csv',pd.DataFrame([ride]).to_csv(index=False))
        archive.writestr('second.csv',pd.DataFrame([ride,{**ride,'ride_id':'bad','start_station_id':None}]).to_csv(index=False))
    result=reference(source,2024,1)
    assert result['source_rows']==3 and result['member_rows']=={'first.csv':1,'second.csv':2}
    assert result['accepted_before_dedup']==2 and result['accepted_unique']==1 and result['accepted_duplicate_rows']==1
    assert result['rejected']==1 and result['reasons']=={'missing_start_station':1}
