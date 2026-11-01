"""Compare alerts with the TRUE labels: how many frauds did we catch, how many alerts were wrong?"""
import pandas as pd, sys
lab = pd.read_csv("data/labels.csv"); al = pd.read_csv("data/alerts.csv")
flagged = set(al.txn_id)
lab["flagged"] = lab.txn_id.isin(flagged)
tp = int(((lab.is_fraud==1)&lab.flagged).sum()); fn = int(((lab.is_fraud==1)&~lab.flagged).sum())
fp = int(((lab.is_fraud==0)&lab.flagged).sum()); tn = int(((lab.is_fraud==0)&~lab.flagged).sum())
print(f"flagged transactions: {tp+fp}   caught fraud: {tp}/{tp+fn} (recall {tp/(tp+fn):.1%})   precision {tp/max(tp+fp,1):.1%}   false alarms: {fp} ({fp/(fp+tn):.2%} of normal)")
print("\nBy fraud pattern (transaction level):")
print(lab[lab.is_fraud==1].groupby("fraud_pattern").agg(total=("txn_id","count"), caught=("flagged","sum")).assign(rate=lambda d:(d.caught/d.total).round(2)))
print("\nAlerts by rule vs truth:")
m = al.merge(lab, on="txn_id")
print(pd.crosstab(m.rule, m.is_fraud.map({0:"false alarm",1:"real fraud"})))

# Event level: did we raise at least one alert during each fraud event? (what a fraud team cares about)
fe = lab[lab.is_fraud==1].groupby(["fraud_event","fraud_pattern"]).flagged.any().reset_index()
print("\nFraud EVENTS caught (at least one alert):")
print(fe.groupby("fraud_pattern").flagged.agg(events="count", caught="sum"))
print(f"Total: {int(fe.flagged.sum())}/{len(fe)} events ({fe.flagged.mean():.0%})")
