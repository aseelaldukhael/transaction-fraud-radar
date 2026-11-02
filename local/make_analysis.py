"""Aggregate the week of results into a small JSON for the analysis dashboard (analysis.html)."""
import json, csv, itertools
from datetime import datetime
from collections import defaultdict
import pandas as pd
import rules

t = pd.read_csv("data/transactions.csv"); lab = pd.read_csv("data/labels.csv"); al = pd.read_csv("data/alerts.csv")
t = t.merge(lab, on="txn_id", how="left"); t["is_fraud"] = t.is_fraud.fillna(0).astype(int)
t["hour"] = t.event_time.str[11:13].astype(int); t["day"] = t.event_time.str[:10]
flagged = set(al.txn_id); t["flagged"] = t.txn_id.isin(flagged)

def pack(g):  # per group: all txns, fraud, flagged, true flagged
    return dict(n=int(len(g)), fraud=int(g.is_fraud.sum()), flagged=int(g.flagged.sum()),
                caught=int((g.flagged & (g.is_fraud == 1)).sum()), fp=int((g.flagged & (g.is_fraud == 0)).sum()))
out = {"week": pack(t) | {"events": int(lab.fraud_event.nunique())}}
out["hour"] = [pack(t[t.hour == h]) | {"h": h} for h in range(24)]
out["city"] = sorted([pack(g) | {"k": k} for k, g in t.groupby("city")], key=lambda d: -d["fraud"])
out["cat"] = sorted([pack(g) | {"k": k} for k, g in t.groupby("category")], key=lambda d: -d["fraud"])
out["chan"] = [pack(g) | {"k": k} for k, g in t.groupby("channel")]
out["day"] = [pack(g) | {"k": k} for k, g in t.groupby("day")]
# amount bands: where does fraud sit
bands = [(0, 10), (10, 100), (100, 500), (500, 1000), (1000, 3000), (3000, 1e9)]
names = ["≤10", "10–100", "100–500", "500–1,000", "1,000–3,000", "3,000+"]
out["amount"] = [pack(t[(t.amount_sar > a) & (t.amount_sar <= b)] if a else t[t.amount_sar <= b]) | {"k": n} for (a, b), n in zip(bands, names)]
# pattern x caught
fr = t[t.is_fraud == 1]
out["pattern"] = [dict(k=k, n=int(len(g)), caught=int(g.flagged.sum()),
                       events=int(g.fraud_event.nunique()), ev_caught=int(g.groupby("fraud_event").flagged.any().sum()))
                  for k, g in fr.groupby("fraud_pattern")]
m = al.merge(lab, on="txn_id")
out["rule"] = [dict(k=k, alerts=int(len(g)), real=int((g.is_fraud == 1).sum()), false=int((g.is_fraud == 0).sum()))
               for k, g in m.groupby("rule")]
missed = fr[~fr.flagged]
out["missed"] = [dict(k=k, n=int(len(g))) for k, g in missed.groupby("fraud_pattern")]

# --- threshold sweep: re-run the rules with different high-amount settings ---
rows = list(t.sort_values(["event_time", "txn_id"])[["txn_id", "event_time", "card_id", "lat", "lon", "amount_sar", "is_fraud", "fraud_event"]].itertuples(index=False))
fraud_events = set(lab.fraud_event.dropna())
def run(min_sar, mult):
    rules.HIGH_MIN_SAR = min_sar; rules.NEW_CARD_LIMIT = 3000; rules.HIGH_MULT = mult
    states = defaultdict(rules.CardState); flg = set()
    for r in rows:
        if rules.check(states[r.card_id], datetime.fromisoformat(r.event_time), r.lat, r.lon, r.amount_sar):
            flg.add(r.txn_id)
    tp = sum(1 for r in rows if r.is_fraud == 1 and r.txn_id in flg)
    fp = sum(1 for r in rows if r.is_fraud == 0 and r.txn_id in flg)
    ev = len({r.fraud_event for r in rows if r.is_fraud == 1 and r.txn_id in flg and isinstance(r.fraud_event, str)})
    return dict(min=min_sar, mult=mult, tp=tp, fp=fp, ev=ev)
mins = [1000, 1500, 2000, 2500, 3000, 4000, 5000]; mults = [4, 6, 8, 10, 12]
out["sweep"] = {"mins": mins, "mults": mults, "grid": [run(a, b) for a in mins for b in mults]}
out["sweep"]["fraud_txns"] = int(len(fr)); out["sweep"]["events"] = len(fraud_events)
json.dump(out, open("analysis_data.json", "w"), separators=(",", ":"))
print("ok", len(json.dumps(out)) // 1024, "KB")
print([ (g['min'],g['mult'],g['tp'],g['fp'],g['ev']) for g in out["sweep"]["grid"] if g['mult']==8])
print(out["week"], out["rule"], out["missed"])
