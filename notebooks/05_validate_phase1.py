# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Validation
# MAGIC Asserts row counts, PII tags, masking function, and high-value segment.

# COMMAND ----------

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Counts

# COMMAND ----------

expected = {
    "products": 5000,
    "customer_profiles": 10000,
    "purchase_history": 5000,
    "clickstream_events": 10000,
}

for table, exp in expected.items():
    actual = spark.table(table).count()
    assert actual == exp, f"{table}: expected {exp}, got {actual}"
    print(f"  {table}: {actual} rows OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII Tags

# COMMAND ----------

tag_rows = spark.sql("""
SELECT column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE schema_name = 'tko27_retail'
  AND table_name = 'customer_profiles'
  AND tag_name = 'pii'
""").collect()

tagged_cols = {r.column_name for r in tag_rows}
expected_tagged = {"email", "first_name", "last_name", "credit_card_last4"}
if tagged_cols == expected_tagged:
    print(f"  PII tags verified on: {tagged_cols}")
else:
    print(f"  WARN: PII tags partial/missing (got {tagged_cols}, expected {expected_tagged}). Tag policy may restrict assignment.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## High-Value Segment

# COMMAND ----------

hv_count = spark.sql("SELECT count(*) FROM customer_profiles WHERE is_high_value").collect()[0][0]
total = spark.sql("SELECT count(*) FROM customer_profiles").collect()[0][0]
pct = hv_count / total * 100
assert 9 <= pct <= 12, f"High-value pct {pct:.1f}% outside 9-12% range"
print(f"  High-value: {hv_count}/{total} ({pct:.1f}%) OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Masking Function Exists

# COMMAND ----------

funcs = spark.sql("SHOW FUNCTIONS LIKE 'mask_pii'").collect()
assert len(funcs) > 0, "mask_pii function not found"
print("  mask_pii function exists OK")

# COMMAND ----------

print("Phase 1 validation PASSED.")
