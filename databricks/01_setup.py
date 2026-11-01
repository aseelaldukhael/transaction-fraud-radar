# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup
# MAGIC Creates the schema and 4 volumes (folders) for the fraud pipeline.
# MAGIC After running, upload `transactions.csv` and `labels.csv` into the **raw** volume:
# MAGIC Catalog -> your catalog -> fraud_radar -> raw -> *Upload to this volume*.

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "fraud_radar"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print("Working in:", CATALOG, SCHEMA, "| files at:", BASE)
# COMMAND ----------

for v in ["raw", "landing", "checkpoints", "exports"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {v}")
print([r.volume_name for r in spark.sql("SHOW VOLUMES").collect()])
