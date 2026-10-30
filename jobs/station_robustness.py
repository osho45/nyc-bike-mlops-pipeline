"""Station/season non-inferiority checks and baseline-shrunk action probabilities."""
import numpy as np
import pandas as pd
from scipy.stats import t
from jobs.ml_features import COST_MATRIX
from jobs.decision_policies import actual_classes,expected_cost_action

CONSTRAINTS={'reference':'last-hour class action','absolute_tolerance':.05,'relative_tolerance':.02,'one_sided_confidence':.95,'annual_min_hours':2000,'annual_min_week_blocks':26,'annual_min_positive_hours':50,'season_min_hours':500,'season_min_week_blocks':8,'season_min_positive_hours':10}


def calendar(frame):
    local=pd.to_datetime(frame.station_hour_ts,utc=True).dt.tz_convert('America/New_York')
    season=local.dt.month.map({12:'winter',1:'winter',2:'winter',3:'spring',4:'spring',5:'spring',6:'summer',7:'summer',8:'summer',9:'fall',10:'fall',11:'fall'})
    week=local.dt.tz_localize(None).dt.to_period('W-SUN').astype(str)
    return season.to_numpy(),week.to_numpy()


def empirical_probabilities(policy,frame,mu):
    stations=frame.station_id.astype(str).to_numpy();mu=np.asarray(mu,dtype=float);p=np.zeros((len(frame),3))
    for station in sorted(set(stations)):
        state=policy['states'][station];idx=stations==station
        p[idx]=np.asarray(state['probabilities'])[np.searchsorted(state['edges'],mu[idx],side='right')]
    return p


def blended_actions(probabilities,baseline,alpha):
    if not 0<=alpha<=1:raise ValueError('Shrinkage weight must be in [0,1]')
    baseline=np.asarray(baseline)
    if not np.isin(baseline,[0,1,2]).all():raise ValueError('Invalid baseline actions')
    return expected_cost_action((1-alpha)*np.asarray(probabilities)+alpha*np.eye(3)[baseline])


def apply_policy(spec,frame,probabilities,baseline):
    action=blended_actions(probabilities,baseline,spec.get('alpha',0));baseline=np.asarray(baseline)
    station=frame.station_id.astype(str).to_numpy();season,_=calendar(frame)
    fallback=np.isin(station,spec.get('fallback_stations',[]))
    cells={tuple(x) for x in spec.get('fallback_cells',[])}
    fallback|=np.array([(s,c) in cells for s,c in zip(station,season)])
    action[fallback]=baseline[fallback]
    return action


def checks(frame,decisions,baseline,thresholds,constraints=None):
    c=constraints or CONSTRAINTS;frame=frame.reset_index(drop=True)
    y=actual_classes(frame,thresholds);d=np.asarray(decisions);b=np.asarray(baseline)
    costs=COST_MATRIX[y,d];reference=COST_MATRIX[y,b];delta=costs-reference
    season,week=calendar(frame);groups=frame[['station_id']].copy();groups['season']=season
    result=[]
    for dimension,keys in [('annual',['station_id']),('season',['station_id','season'])]:
        for key,idx in groups.groupby(keys).groups.items():
            key=key if isinstance(key,tuple) else (key,);idx=np.asarray(idx)
            z=pd.DataFrame({'delta':delta[idx],'week':week[idx]}).groupby('week').delta.agg(['sum','count'])
            mean=float(delta[idx].mean());g=len(z);n=len(idx)
            # Paired cluster-robust SE for the hourly mean, with NY calendar-week blocks.
            se=float(np.sqrt(g/(g-1)*np.square(z['sum']-mean*z['count']).sum())/n) if g>1 else None
            upper=mean+float(t.ppf(c['one_sided_confidence'],g-1))*se if se is not None else None
            tol=max(c['absolute_tolerance'],c['relative_tolerance']*float(reference[idx].mean()))
            prefix='annual' if dimension=='annual' else 'season';positive=int((frame.ride_count.to_numpy()[idx]>0).sum())
            support=n>=c[prefix+'_min_hours'] and g>=c[prefix+'_min_week_blocks'] and positive>=c[prefix+'_min_positive_hours']
            identical=bool(np.array_equal(d[idx],b[idx]))
            passed=identical or (support and upper is not None and upper<=tol)
            result.append({'station_id':str(key[0]),'season':str(key[1]) if len(key)>1 else 'all','dimension':dimension,'hours':n,'positive_hours':positive,'zero_fraction':float((frame.ride_count.to_numpy()[idx]==0).mean()),'week_blocks':g,'policy_penalty':float(costs[idx].mean()),'baseline_penalty':float(reference[idx].mean()),'mean_delta':mean,'cluster_standard_error':se,'upper_one_sided_bound':upper,'tolerance':tol,'support_sufficient':bool(support),'identical_to_baseline':identical,'passes':bool(passed)})
    return result


def guarded_policy(frame,probabilities,baseline,thresholds,seasonal=False):
    original=blended_actions(probabilities,baseline,0);failed=[r for r in checks(frame,original,baseline,thresholds) if not r['passes']]
    stations=sorted({r['station_id'] for r in failed if not seasonal or r['dimension']=='annual'})
    cells=sorted({(r['station_id'],r['season']) for r in failed if seasonal and r['dimension']=='season'})
    spec={'kind':'guarded_seasonal' if seasonal else 'guarded_station','alpha':0,'fallback_stations':stations,'fallback_cells':[list(x) for x in cells]}
    # Partial seasonal fallback may change the annual CI: escalate failures to a full-station fallback.
    post=checks(frame,apply_policy(spec,frame,probabilities,baseline),baseline,thresholds)
    spec['fallback_stations']=sorted(set(stations)|{r['station_id'] for r in post if not r['passes']})
    return spec
