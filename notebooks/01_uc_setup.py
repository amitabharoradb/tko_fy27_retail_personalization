# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Unity Catalog Setup
# MAGIC Creates catalog, schema, and volume for the TKO27 retail demo.

# COMMAND ----------

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"
VOLUME = "raw_data"

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    if "already exists" in str(e).lower() or "INVALID_STATE" in str(e):
        print(f"Catalog {CATALOG} already exists, skipping creation")
    else:
        raise
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# Verify
display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {CATALOG}.{SCHEMA}"))
