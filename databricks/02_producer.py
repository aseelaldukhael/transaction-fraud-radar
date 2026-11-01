# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Producer (simulates payments arriving)
# MAGIC Reads `raw/transactions.csv` and drops small JSON files into the **landing** volume,
# MAGIC a few seconds apart, like a payment system sending events.
# MAGIC It remembers where it stopped, so every run continues with the NEXT payments.
# MAGIC (Later, a Kafka producer can replace this notebook. The rest of the pipeline stays the same.)

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "fraud_radar"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print("Working in:", CATALOG, SCHEMA, "| files at:", BASE)
# COMMAND ----------

dbutils.widgets.text("files", "10", "How many files to drop")
dbutils.widgets.text("rows_per_file", "300", "Payments per file")
dbutils.widgets.text("pause_seconds", "2", "Seconds between files")

# COMMAND ----------

import os, time, pandas as pd
n_files = int(dbutils.widgets.get("files"))
rows_per_file = int(dbutils.widgets.get("rows_per_file"))
pause = float(dbutils.widgets.get("pause_seconds"))

df = pd.read_csv(f"{BASE}/raw/transactions.csv", dtype={"event_time": str}).sort_values(["event_time", "txn_id"]).reset_index(drop=True)
offset_file = f"{BASE}/checkpoints/producer_offset.txt"
offset = int(open(offset_file).read()) if os.path.exists(offset_file) else 0
print(f"{len(df):,} payments in total, starting at row {offset:,}")

for i in range(n_files):
    chunk = df.iloc[offset: offset + rows_per_file]
    if chunk.empty:
        print("No more payments. Everything was sent."); break
    path = f"{BASE}/landing/batch_{offset:08d}.json"
    chunk.to_json(path, orient="records", lines=True)
    offset += len(chunk)
    open(offset_file, "w").write(str(offset))
    print(f"wrote {path.split('/')[-1]}  ({len(chunk)} payments, up to {chunk.event_time.iloc[-1]})")
    time.sleep(pause)
print(f"Done. Next run continues from row {offset:,} of {len(df):,}.")
