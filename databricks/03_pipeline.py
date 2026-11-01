# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Pipeline: Bronze -> Silver -> Gold
# MAGIC * **Bronze**: Auto Loader reads the new JSON files exactly as they are.
# MAGIC * **Silver**: clean the payments, remove duplicates, count what was rejected.
# MAGIC * **Gold**: run the 4 fraud rules on each micro-batch and save the alerts.
# MAGIC
# MAGIC Each stream uses `availableNow`: it processes everything that arrived since the last run, then stops.
# MAGIC (Databricks Free Edition has serverless compute only, which does not support always-on triggers.
# MAGIC Run this notebook again, or schedule it as a Job every few minutes.)

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "fraud_radar"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print("Working in:", CATALOG, SCHEMA, "| files at:", BASE)
# COMMAND ----------

# MAGIC %md
# MAGIC ### Fraud rules (same code as rules.py) and micro-batch scoring

# COMMAND ----------

"""
Fraud rules. Written as a small stateful function: it sees ONE transaction at a time
(in time order) plus a little memory per card. This is exactly how a streaming job works,
so the same logic can run inside Spark Structured Streaming later.
"""
import math
from collections import deque
from datetime import datetime

# ---- thresholds (easy to tune, and to explain in an interview) ----
HIGH_MULT = 8          # amount > 8x the card's usual amount ...
HIGH_MIN_SAR = 3000    # ... and at least this much (tuned: 1500 gave 335 false alarms)
NEW_CARD_LIMIT = 3000  # card with little history: flag above this amount
VELOCITY_N, VELOCITY_SEC = 5, 120      # 5 transactions within 2 minutes
TRAVEL_KMH = 900       # faster than a plane = impossible
TRAVEL_MIN_KM = 300
TEST_SMALL_SAR, TEST_N, TEST_SEC = 10, 3, 300   # 3 tiny payments within 5 minutes
TEST_BIG_SAR, TEST_FOLLOW_SEC = 1000, 600       # then a big one within 10 minutes

def km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    h = math.sin((p2-p1)/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(math.radians(lon2-lon1)/2)**2
    return 6371*2*math.asin(math.sqrt(h))

class CardState:
    __slots__ = ("amounts", "recent", "last", "small", "last_small_alert")
    def __init__(self):
        self.amounts = deque(maxlen=30)   # last 30 legit-looking amounts
        self.recent = deque()             # timestamps of the last few minutes
        self.last = None                  # (ts, lat, lon)
        self.small = deque()              # timestamps of tiny payments
        self.last_small_alert = None

def check(state, ts: datetime, lat, lon, amount):
    """Return a list of (rule, details-dict) for this transaction, and update the card memory."""
    hits = []
    t = ts.timestamp()

    # 1) unusually large amount
    if len(state.amounts) >= 3:
        usual = sorted(state.amounts)[len(state.amounts)//2]
        if amount >= HIGH_MIN_SAR and amount > HIGH_MULT*usual:
            hits.append(("high_amount", {"amount": amount, "usual": round(usual, 1)}))
    elif amount >= NEW_CARD_LIMIT:
        hits.append(("high_amount", {"amount": amount, "usual": None}))

    # 2) many transactions in a short time
    state.recent.append(t)
    while state.recent and t - state.recent[0] > VELOCITY_SEC:
        state.recent.popleft()
    if len(state.recent) >= VELOCITY_N:
        hits.append(("velocity", {"count": len(state.recent), "seconds": VELOCITY_SEC}))

    # 3) impossible travel
    if state.last:
        t0, la0, lo0 = state.last
        d = km(la0, lo0, lat, lon); hrs = max((t-t0)/3600, 1/3600)
        if d >= TRAVEL_MIN_KM and d/hrs > TRAVEL_KMH:
            hits.append(("impossible_travel", {"km": round(d), "minutes": round((t-t0)/60, 1)}))
    state.last = (t, lat, lon)

    # 4) card testing: tiny payments, then a big one
    if amount <= TEST_SMALL_SAR:
        state.small.append(t)
        while state.small and t - state.small[0] > TEST_SEC:
            state.small.popleft()
        if len(state.small) >= TEST_N:
            hits.append(("card_testing", {"small_count": len(state.small)}))
            state.last_small_alert = t
    elif amount >= TEST_BIG_SAR and state.last_small_alert and t - state.last_small_alert <= TEST_FOLLOW_SEC:
        hits.append(("card_testing", {"followed_by_big": amount}))

    # remember normal-looking amounts only (so fraud does not teach the card to be "normal")
    if not hits:
        state.amounts.append(amount)
    return hits

# COMMAND ----------

# ---- core logic for one micro-batch (pure pandas, so it can be tested without Spark) ----
import json
import pandas as pd

HISTORY_PER_CARD = 60   # how many earlier payments of the card we replay to rebuild its "memory"

def score_batch(batch: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    """
    batch   : new payments of this micro-batch  (columns: txn_id,event_time,card_id,lat,lon,amount_sar)
    history : earlier payments of the same cards (same columns), already in Silver
    returns : one row per alert (txn_id, event_time, card_id, rule, details)
    The card memory is rebuilt by replaying history first, then the new payments, in time order.
    """
    out = []
    if batch.empty:
        return pd.DataFrame(columns=["txn_id", "event_time", "card_id", "rule", "details"])
    new_ids = set(batch.txn_id)
    allrows = pd.concat([history, batch], ignore_index=True).sort_values(["event_time", "txn_id"])
    for card, g in allrows.groupby("card_id", sort=False):
        state = CardState()
        for r in g.itertuples(index=False):
            hits = check(state, r.event_time.to_pydatetime(), float(r.lat), float(r.lon), float(r.amount_sar))
            if r.txn_id in new_ids:
                for rule, det in hits:
                    out.append((r.txn_id, r.event_time, card, rule, json.dumps(det)))
    return pd.DataFrame(out, columns=["txn_id", "event_time", "card_id", "rule", "details"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS silver_transactions (
  txn_id STRING, event_time TIMESTAMP, card_id STRING, customer_id STRING, merchant_id STRING,
  category STRING, city STRING, lat DOUBLE, lon DOUBLE, amount_sar DOUBLE, channel STRING, ingested_at TIMESTAMP)""")
spark.sql("""CREATE TABLE IF NOT EXISTS gold_alerts (
  txn_id STRING, event_time TIMESTAMP, card_id STRING, rule STRING, details STRING,
  city STRING, category STRING, channel STRING, amount_sar DOUBLE)""")
spark.sql("""CREATE TABLE IF NOT EXISTS quality_log (
  run_time TIMESTAMP, layer STRING, check_name STRING, rows_checked BIGINT, rows_failed BIGINT)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze: raw files -> Delta (Auto Loader)

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([StructField(c, StringType()) for c in ["event_time","txn_id","card_id","customer_id","merchant_id","category","city"]]
                    + [StructField("lat", DoubleType()), StructField("lon", DoubleType()), StructField("amount_sar", DoubleType()),
                       StructField("channel", StringType())])

bronze = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
          .schema(schema).load(f"{BASE}/landing")
          .withColumn("ingested_at", F.current_timestamp())
          .withColumn("source_file", F.col("_metadata.file_path")))
q = (bronze.writeStream.option("checkpointLocation", f"{BASE}/checkpoints/bronze")
     .trigger(availableNow=True).toTable("bronze_transactions"))
q.awaitTermination()
print("Bronze rows:", spark.table("bronze_transactions").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver: clean + remove duplicates + quality checks

# COMMAND ----------

def to_silver(batch_df, batch_id):
    s = batch_df.sparkSession
    b = batch_df.withColumn("event_time", F.to_timestamp("event_time"))
    ok = (F.col("txn_id").isNotNull() & F.col("card_id").isNotNull() & F.col("event_time").isNotNull()
          & (F.col("amount_sar") > 0) & F.col("lat").between(15, 33) & F.col("lon").between(34, 56))
    total = b.count()
    good = b.filter(ok).dropDuplicates(["txn_id"])
    failed = total - b.filter(ok).count()
    dups = b.filter(ok).count() - good.count()
    good.select("txn_id","event_time","card_id","customer_id","merchant_id","category","city","lat","lon","amount_sar","channel","ingested_at") \
        .createOrReplaceTempView("silver_batch")
    s.sql("""MERGE INTO silver_transactions t USING silver_batch s ON t.txn_id = s.txn_id
             WHEN NOT MATCHED THEN INSERT *""")
    log = [(None, "silver", "valid_row (id, time, amount > 0, coordinates in Saudi Arabia)", total, failed),
           (None, "silver", "duplicate_txn_id", total, dups)]
    s.createDataFrame(log, "run_time TIMESTAMP, layer STRING, check_name STRING, rows_checked BIGINT, rows_failed BIGINT") \
     .withColumn("run_time", F.current_timestamp()).write.mode("append").saveAsTable("quality_log")

q = (spark.readStream.table("bronze_transactions").writeStream.foreachBatch(to_silver)
     .option("checkpointLocation", f"{BASE}/checkpoints/silver").trigger(availableNow=True).start())
q.awaitTermination()
print("Silver rows:", spark.table("silver_transactions").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: fraud rules on every micro-batch -> alerts
# MAGIC For each card in the batch we load its last 60 earlier payments from Silver, rebuild the card memory,
# MAGIC then score the new payments. Alerts are saved with MERGE, so running twice never duplicates.

# COMMAND ----------

def to_gold(batch_df, batch_id):
    s = batch_df.sparkSession
    b = batch_df.select("txn_id","event_time","card_id","lat","lon","amount_sar","city","category","channel").toPandas()
    if b.empty:
        return
    b["event_time"] = pd.to_datetime(b["event_time"])
    cards = b.card_id.unique().tolist()
    tmin = b.event_time.min().to_pydatetime()
    w = Window.partitionBy("card_id").orderBy(F.col("event_time").desc())
    hist = (s.table("silver_transactions").filter(F.col("card_id").isin(cards)).filter(F.col("event_time") < F.lit(tmin))
            .withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= HISTORY_PER_CARD)
            .select("txn_id","event_time","card_id","lat","lon","amount_sar").toPandas())
    hist["event_time"] = pd.to_datetime(hist["event_time"])
    alerts = score_batch(b[["txn_id","event_time","card_id","lat","lon","amount_sar"]], hist)
    if alerts.empty:
        return
    alerts = alerts.merge(b[["txn_id","city","category","channel","amount_sar"]], on="txn_id")
    s.createDataFrame(alerts).createOrReplaceTempView("gold_batch")
    s.sql("""MERGE INTO gold_alerts t USING gold_batch s ON t.txn_id = s.txn_id AND t.rule = s.rule
             WHEN NOT MATCHED THEN INSERT *""")

q = (spark.readStream.option("skipChangeCommits", "true").table("silver_transactions")
     .writeStream.foreachBatch(to_gold)
     .option("checkpointLocation", f"{BASE}/checkpoints/gold").trigger(availableNow=True).start())
q.awaitTermination()
print("Alerts so far:", spark.table("gold_alerts").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily summary + a look at the results

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE gold_daily_summary AS
  SELECT DATE(event_time) AS day, rule, COUNT(*) AS alerts, ROUND(SUM(amount_sar), 2) AS flagged_sar
  FROM gold_alerts GROUP BY 1, 2""")
display(spark.table("gold_daily_summary").orderBy("day", "rule"))
display(spark.table("quality_log").orderBy(F.col("run_time").desc()).limit(10))
