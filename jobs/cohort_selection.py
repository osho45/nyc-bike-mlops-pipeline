"""Training/development-only representative station selection for bounded experiments."""
import numpy as np
import pandas as pd

SEASONS={12:'winter',1:'winter',2:'winter',3:'spring',4:'spring',5:'spring',6:'summer',7:'summer',8:'summer',9:'fall',10:'fall',11:'fall'}


def eligible_stations(monthly):
    """All development-supported stations; no cohort cap or outcome-based exclusion."""
    if not set(monthly.target_year).issubset({2024,2025}):raise ValueError('Cohort selection forbids test years')
    rows=[]
    for station,g in monthly.groupby('station_id'):
        annual={};seasonal={};eligible=True
        for year in [2024,2025]:
            y=g.loc[g.target_year==year].copy();y['season']=y.target_month.map(SEASONS)
            if set(y.target_month)!=set(range(1,13)) or y.hours.sum()<8000 or y.positive_hours.sum()<50:eligible=False;break
            s=y.groupby('season')[['hours','rides','positive_hours']].sum()
            if len(s)!=4 or (s.hours<500).any() or (s.positive_hours<10).any():eligible=False;break
            annual[year]={'hours':int(y.hours.sum()),'rides':int(y.rides.sum()),'positive_hours':int(y.positive_hours.sum()),'zero_fraction':float(1-y.positive_hours.sum()/y.hours.sum()),'mean_demand':float(y.rides.sum()/y.hours.sum())}
            seasonal[year]=(s.rides/s.hours).to_dict()
        if not eligible:continue
        means=np.array(list(seasonal[2024].values()))
        rows.append({'station_id':str(station),'training_mean':annual[2024]['mean_demand'],'training_zero_fraction':annual[2024]['zero_fraction'],'training_seasonal_cv':float(means.std()/means.mean()),'training':annual[2024],'calibration':annual[2025],'training_season_means':seasonal[2024]})
    eligible=pd.DataFrame(rows)
    if eligible.empty:return eligible
    return eligible.sort_values(['training_mean','station_id']).reset_index(drop=True)


def select_development_cohort(monthly,desired):
    """Uncapped data-only stage planning; does not authorize training."""
    if desired<12:raise ValueError('Need at least 12 stations for balanced strata')
    eligible=eligible_stations(monthly)
    if len(eligible)<12:raise ValueError('Not enough adequately supported stations')
    eligible=eligible.sort_values(['training_mean','station_id']).reset_index(drop=True)
    eligible['demand_band']=np.array(['low','medium','high'])[np.minimum(np.arange(len(eligible))*3//len(eligible),2)]
    pieces=[]
    for band,g in eligible.groupby('demand_band'):
        g=g.sort_values(['training_seasonal_cv','station_id']).copy();g['seasonality_quartile']=np.minimum(np.arange(len(g))*4//len(g),3)
        pieces.append(g)
    eligible=pd.concat(pieces,ignore_index=True);groups=list(eligible.groupby(['demand_band','seasonality_quartile'],sort=True))
    if len(groups)!=12:raise ValueError('Insufficient strata for representative selection')
    # A multiple of 12 keeps equal coverage of all three demand bands and four CV strata.
    per_cell=min(desired//12,min(len(g) for _,g in groups));n=12*per_cell
    if n<12:raise ValueError('Insufficient stratum support')
    selected=[]
    for _,g in groups:
        g=g.sort_values(['training_zero_fraction','station_id']).reset_index(drop=True)
        selected.extend(g.iloc[np.linspace(0,len(g)-1,per_cell).round().astype(int)].to_dict('records'))
    selected=sorted(selected,key=lambda x:x['station_id'])
    assert len(selected)==len({s['station_id'] for s in selected})==n
    coverage={'observed_stations':int(monthly.station_id.nunique()),'eligible_stations':len(eligible),'selected_stations':n,'requested_maximum':desired,'strata':12,'per_stratum':per_cell,'selection_uses_test':False,'reduction_reason':'Equal allocation across 3 demand bands x 4 training-seasonality strata','eligible_training_mean_range':[float(eligible.training_mean.min()),float(eligible.training_mean.max())],'selected_training_mean_range':[float(min(s['training_mean'] for s in selected)),float(max(s['training_mean'] for s in selected))],'selected_zero_fraction_range':[float(min(s['training_zero_fraction'] for s in selected)),float(max(s['training_zero_fraction'] for s in selected))]}
    return selected,coverage


def select_stations(monthly,desired=48):
    if not 12<=desired<=50:raise ValueError('Cohort must contain 12 to 50 stations')
    selected,coverage=select_development_cohort(monthly,desired)
    coverage['reduction_reason']='Equal allocation across 3 demand bands x 4 training-seasonality strata; at most 50 stations'
    return selected,coverage
