# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — UC Governance
# MAGIC PII tags, high-value segment column, and dynamic column masking.

# COMMAND ----------

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII Tags

# COMMAND ----------

pii_columns = [
    ("customer_profiles", "email"),
    ("customer_profiles", "first_name"),
    ("customer_profiles", "last_name"),
    ("customer_profiles", "credit_card_last4"),
]

for table, col in pii_columns:
    try:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} SET TAGS ('pii' = 'true')")
        print(f"  Tagged {table}.{col} pii=true")
    except Exception as e:
        if "PERMISSION_DENIED" in str(e) or "UNAUTHORIZED" in str(e).upper():
            print(f"  WARN: no permission to set pii tag on {table}.{col}, skipping")
        else:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## High-Value Segment (boolean column)

# COMMAND ----------

# Add is_high_value column if not exists
try:
    spark.sql("ALTER TABLE customer_profiles ADD COLUMN is_high_value BOOLEAN")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Column is_high_value already exists, skipping ADD.")
    else:
        raise

# Set top 10% by ltv_score
spark.sql("""
UPDATE customer_profiles
SET is_high_value = (ltv_score >= (
  SELECT percentile_approx(ltv_score, 0.9) FROM customer_profiles
))
""")

hv_count = spark.sql("SELECT count(*) FROM customer_profiles WHERE is_high_value").collect()[0][0]
print(f"High-value customers: {hv_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Column Masking

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION mask_pii(val STRING)
  RETURNS STRING
  RETURN CASE WHEN is_member('marketing_role') THEN '****' ELSE val END
""")

masked_columns = ["email", "first_name", "last_name", "credit_card_last4"]
for col in masked_columns:
    spark.sql(f"ALTER TABLE customer_profiles ALTER COLUMN {col} SET MASK mask_pii")
    print(f"  Masked customer_profiles.{col}")

# COMMAND ----------

print("Governance setup complete.")
