"""Local count-to-action policies. They never change count forecasts or target classes."""
import numpy as np
import pandas as pd
from scipy.stats import poisson
from jobs.ml_features import COST_MATRIX
from tools.bounded_ml_readiness import count_classes,metrics


def actual_classes(frame,thresholds):
    q25=frame.station_id.map(lambda s:thresholds[str(s)][0]).to_numpy()
    q75=frame.station_id.map(lambda s:thresholds[str(s)][1]).to_numpy()
    return count_classes(frame.ride_count,q25,q75)


def expected_cost_action(probabilities):
    p=np.asarray(probabilities,dtype=float)
    if p.ndim!=2 or p.shape[1]!=3 or not np.isfinite(p).all() or (p<0).any() or not np.allclose(p.sum(axis=1),1):raise ValueError('Invalid class probabilities')
    # Matrix rows are ACTUAL classes, columns are ACTIONS. No transpose.
    return (p@COST_MATRIX).argmin(axis=1)


def fit_empirical(frame,forecasts,thresholds,bins=8,strength=20):
    """2024-only station-conditional class frequencies given frozen count forecast."""
    y=actual_classes(frame,thresholds);mu=np.asarray(forecasts);stations=frame.station_id.astype(str).to_numpy();states={}
    for station in sorted(set(stations)):
        select=stations==station;values=mu[select];labels=y[select]
        edges=np.unique(np.quantile(values,np.linspace(0,1,bins+1)[1:-1]))
        prior=np.bincount(labels,minlength=3)/len(labels)
        bucket=np.searchsorted(edges,values,side='right');counts=np.zeros((len(edges)+1,3))
        np.add.at(counts,(bucket,labels),1)
        probabilities=(counts+strength*prior)/(counts.sum(axis=1,keepdims=True)+strength)
        states[station]={'edges':edges.tolist(),'probabilities':probabilities.tolist(),'training_class_prior':prior.tolist(),'rows':len(labels)}
    return {'kind':'empirical_expected_cost','bins':bins,'strength':strength,'states':states}


def actions(policy,frame,forecasts,thresholds):
    mu=np.asarray(forecasts,dtype=float)
    if mu.shape!=(len(frame),) or not np.isfinite(mu).all() or (mu<0).any():raise ValueError('Invalid frozen count forecasts')
    q25=frame.station_id.map(lambda s:thresholds[str(s)][0]).to_numpy()
    q75=frame.station_id.map(lambda s:thresholds[str(s)][1]).to_numpy()
    kind=policy['kind']
    if kind=='original':return count_classes(mu,q25,q75)
    if kind=='threshold_shift':
        low=np.maximum(q25+policy['low_shift'],0);high=np.maximum(q75+policy['high_shift'],low)
        return count_classes(mu,low,high)
    if kind=='asymmetric_scale':return count_classes(mu*policy['factor'],q25,q75)
    if kind=='poisson_expected_cost':
        # Counts are integers: Low < q25 and Normal <= q75, including fractional quantiles.
        low=poisson.cdf(np.ceil(q25)-1,mu*policy['scale'])
        through_normal=poisson.cdf(np.floor(q75),mu*policy['scale'])
        p=np.column_stack([low,np.maximum(through_normal-low,0),np.maximum(1-through_normal,0)])
        return expected_cost_action(p)
    if kind=='empirical_expected_cost':
        p=np.zeros((len(frame),3));stations=frame.station_id.astype(str).to_numpy()
        for station in sorted(set(stations)):
            state=policy['states'][station];select=stations==station
            bucket=np.searchsorted(state['edges'],mu[select],side='right')
            p[select]=np.asarray(state['probabilities'])[bucket]
        return expected_cost_action(p)
    raise ValueError('Unknown decision policy')


def evaluate(frame,forecasts,decisions,thresholds):
    d=np.asarray(decisions)
    if d.shape!=(len(frame),) or not np.isin(d,[0,1,2]).all():raise ValueError('Invalid decisions')
    out=metrics(frame,forecasts,thresholds);y=actual_classes(frame,thresholds);cost=COST_MATRIX[y,d]
    zero=frame.ride_count.to_numpy()==0
    out.update(mean_operational_penalty=float(cost.mean()),total_operational_penalty=float(cost.sum()),
               decision_accuracy=float((d==y).mean()),zero_demand_rows=int(zero.sum()),
               zero_demand_high_action_rate=float((d[zero]==2).mean()) if zero.any() else None,
               zero_demand_mean_penalty=float(cost[zero].mean()) if zero.any() else None,
               confusion_actual_rows_action_columns=np.bincount(y*3+d,minlength=9).reshape(3,3).tolist())
    return out


def station_metrics(frame,forecasts,decisions,thresholds):
    frame=frame.reset_index(drop=True)
    return {str(s):evaluate(frame.loc[idx],np.asarray(forecasts)[idx],np.asarray(decisions)[idx],thresholds) for s,idx in frame.groupby('station_id').groups.items()}
