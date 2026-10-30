import pandas as pd
import pytest
from jobs.cohort_selection import select_stations


def population():
    rows=[]
    for s in range(144):
        for year in [2024,2025]:
            for month in range(1,13):
                rows.append({'station_id':f's{s:03d}','target_year':year,'target_month':month,'hours':720,'rides':int((s+1)*(20+month*(s%5+1))),'positive_hours':20+s})
    return pd.DataFrame(rows)


def test_balanced_representative_cohort_deterministic_and_within_cap():
    data=population();cohort,coverage=select_stations(data,50)
    assert len(cohort)==48 and coverage['selected_stations']==48 and not coverage['selection_uses_test']
    selected=pd.DataFrame(cohort)
    assert set(selected.demand_band)=={'low','medium','high'}
    assert selected.groupby(['demand_band','seasonality_quartile']).size().eq(4).all()
    shuffled,_=select_stations(data.sample(frac=1,random_state=7),50)
    assert [r['station_id'] for r in cohort]==[r['station_id'] for r in shuffled]


def test_eligibility_requires_two_years_and_seasonal_positive_support():
    data=population();data=data.loc[~((data.station_id=='s000')&(data.target_year==2025)&(data.target_month==12))]
    data.loc[data.station_id=='s001','positive_hours']=0
    selected,coverage=select_stations(data)
    assert coverage['eligible_stations']==142
    assert not {'s000','s001'}&{s['station_id'] for s in selected}


def test_test_year_rejected_and_calibration_demand_does_not_rank_stations():
    data=population();a,_=select_stations(data)
    changed=data.copy();changed.loc[changed.target_year==2025,'rides']*=100
    b,_=select_stations(changed)
    assert [r['station_id'] for r in a]==[r['station_id'] for r in b]
    bad=data.copy();bad.loc[0,'target_year']=2026
    with pytest.raises(ValueError,match='test years'):select_stations(bad)


def test_small_supported_population_reduces_cohort_without_duplicates():
    data=population();data=data.loc[data.station_id.isin([f's{s:03d}' for s in range(36)])]
    selected,coverage=select_stations(data,48)
    assert len(selected)==36 and len({s['station_id'] for s in selected})==36
    with pytest.raises(ValueError):select_stations(data,51)

def test_data_only_stage_planner_does_not_remove_pilot_training_cap():
    from jobs.cohort_selection import select_development_cohort
    data=population();cohort,coverage=select_development_cohort(data,96)
    assert len(cohort)==96 and coverage['selected_stations']==96
    with pytest.raises(ValueError):select_stations(data,96)
