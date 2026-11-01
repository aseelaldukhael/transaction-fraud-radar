# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Evaluate and export for the website
# MAGIC Uses the secret truth labels (`raw/labels.csv`) ONLY here, to measure the rules.
# MAGIC Then writes `exports/data.json`, the file the website reads.

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "fraud_radar"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print("Working in:", CATALOG, SCHEMA, "| files at:", BASE)
# COMMAND ----------

import json, pandas as pd
lab = pd.read_csv(f"{BASE}/raw/labels.csv").fillna({"fraud_pattern": "", "fraud_event": ""})
silver = spark.table("silver_transactions").toPandas()
alerts = spark.table("gold_alerts").toPandas()
silver["event_time"] = pd.to_datetime(silver["event_time"]); alerts["event_time"] = pd.to_datetime(alerts["event_time"])
print(f"payments in Silver: {len(silver):,} | alerts: {len(alerts):,}")

# COMMAND ----------

lab = lab[lab.txn_id.isin(silver.txn_id)].copy()          # only payments the pipeline has already seen
lab["flagged"] = lab.txn_id.isin(set(alerts.txn_id))
tp = int(((lab.is_fraud == 1) & lab.flagged).sum()); fn = int(((lab.is_fraud == 1) & ~lab.flagged).sum())
fp = int(((lab.is_fraud == 0) & lab.flagged).sum())
fe = lab[lab.is_fraud == 1].groupby(["fraud_event", "fraud_pattern"]).flagged.any().reset_index().groupby("fraud_pattern").flagged.agg(["count", "sum"])
print(f"fraud payments caught: {tp}/{tp+fn} | false alarms: {fp} | precision: {tp/max(tp+fp,1):.1%}")
print(fe)

# COMMAND ----------

# build data.json for the website (same structure as the demo data)
cities = sorted(silver.city.unique()); cats = sorted(silver.category.unique()); chans = ["POS", "online", "ATM"]
coords = {c: [float(silver[silver.city == c].lat.mean()), float(silver[silver.city == c].lon.mean())] for c in cities}
a_by = alerts.groupby("txn_id").apply(lambda g: [[r.rule, json.loads(r.details)] for r in g.itertuples()]).to_dict()
d = silver.merge(lab[["txn_id", "is_fraud", "fraud_pattern"]], on="txn_id", how="left").fillna({"is_fraud": 0, "fraud_pattern": ""})
d["day"] = d.event_time.dt.strftime("%Y-%m-%d")
best_day = d[d.is_fraud == 1].groupby("day").size().idxmax()                  # replay the day with the most fraud
day = d[d.day == best_day].sort_values("event_time")
rows = [[int(r.event_time.hour*3600 + r.event_time.minute*60 + r.event_time.second), r.txn_id[-5:], r.card_id[-5:], cities.index(r.city),
         cats.index(r.category), float(r.amount_sar), chans.index(r.channel), a_by.get(r.txn_id, []), int(r.is_fraud), r.fraud_pattern] for r in day.itertuples()]
m = alerts.merge(lab, on="txn_id")
by_rule = {r: [int((m[m.rule == r].is_fraud == 1).sum()), int((m[m.rule == r].is_fraud == 0).sum())] for r in m.rule.unique()}
ev = {k: [int(v["count"]), int(v["sum"])] for k, v in fe.iterrows()}
week = dict(txns=len(silver), fraud_txns=int(lab.is_fraud.sum()), events=sum(v[0] for v in ev.values()), events_caught=sum(v[1] for v in ev.values()),
            tp=tp, fn=fn, fp=fp, by_pattern=ev, by_rule=by_rule, normal=len(silver) - int(lab.is_fraud.sum()))
out = dict(cities=cities, cats=cats, chans=chans, coords=coords, rows=rows, week=week)
path = f"{BASE}/exports/data.json"
json.dump(out, open(path, "w"), separators=(",", ":"))
print("saved", path, "| replay day:", best_day, "| rows:", len(rows))
