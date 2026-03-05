# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Load CSV to Delta Tables
# MAGIC Uses `read_files()` to load CSVs from Volume into UC Delta tables with Liquid Clustering.

# COMMAND ----------

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"
VOLUME_PATH = "/Volumes/amitabh_arora_catalog/tko27_retail/raw_data"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE products AS
SELECT
  product_id,
  name,
  category,
  subcategory,
  CAST(price AS DOUBLE) AS price,
  brand,
  description,
  split(tags, '\\\\|') AS tags,
  CAST(created_at AS TIMESTAMP) AS created_at
FROM read_files(
  '{VOLUME_PATH}/products/',
  format => 'csv',
  header => true
)
""")

spark.sql("ALTER TABLE products CLUSTER BY (category, brand)")
print(f"products: {spark.table('products').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Profiles

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE customer_profiles AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  credit_card_last4,
  age_bucket,
  gender,
  loyalty_tier,
  CAST(loyalty_points AS INT) AS loyalty_points,
  CAST(ltv_score AS DOUBLE) AS ltv_score,
  split(preferred_categories, '\\\\|') AS preferred_categories,
  CAST(last_purchase_date AS DATE) AS last_purchase_date,
  CAST(created_at AS TIMESTAMP) AS created_at
FROM read_files(
  '{VOLUME_PATH}/customer_profiles/',
  format => 'csv',
  header => true
)
""")

spark.sql("ALTER TABLE customer_profiles CLUSTER BY (loyalty_tier, age_bucket)")
print(f"customer_profiles: {spark.table('customer_profiles').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Purchase History

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE purchase_history AS
SELECT
  transaction_id,
  customer_id,
  product_id,
  CAST(quantity AS INT) AS quantity,
  CAST(unit_price AS DOUBLE) AS unit_price,
  CAST(total_amount AS DOUBLE) AS total_amount,
  CAST(purchase_date AS TIMESTAMP) AS purchase_date,
  channel
FROM read_files(
  '{VOLUME_PATH}/purchase_history/',
  format => 'csv',
  header => true
)
""")

spark.sql("ALTER TABLE purchase_history CLUSTER BY (customer_id, purchase_date)")
print(f"purchase_history: {spark.table('purchase_history').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clickstream Events

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE clickstream_events AS
SELECT
  event_id,
  session_id,
  NULLIF(customer_id, '') AS customer_id,
  NULLIF(product_id, '') AS product_id,
  event_type,
  NULLIF(search_term, '') AS search_term,
  category,
  CAST(timestamp AS TIMESTAMP) AS timestamp,
  device_type,
  referrer
FROM read_files(
  '{VOLUME_PATH}/clickstream_events/',
  format => 'csv',
  header => true
)
""")

spark.sql("ALTER TABLE clickstream_events CLUSTER BY (customer_id, event_type)")
print(f"clickstream_events: {spark.table('clickstream_events').count()} rows")

# COMMAND ----------

# Summary
for t in ["products", "customer_profiles", "purchase_history", "clickstream_events"]:
    cnt = spark.table(t).count()
    print(f"  {t}: {cnt}")
print("All 4 tables loaded.")
