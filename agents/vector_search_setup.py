# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Vector Search Setup
# MAGIC Creates VS endpoint + Delta Sync index on products.description.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import time

CATALOG = "amitabh_arora_catalog"
SCHEMA = "tko27_retail"
VS_ENDPOINT = "tko27_vs_endpoint"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.products_description_index"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.products"

vsc = VectorSearchClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create VS Endpoint

# COMMAND ----------

# Create endpoint if not exists
try:
    ep = vsc.get_endpoint(VS_ENDPOINT)
    print(f"Endpoint {VS_ENDPOINT} already exists: {ep.get('endpoint_status', {}).get('state')}")
except Exception:
    vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    print(f"Creating endpoint {VS_ENDPOINT}...")

# Wait for ONLINE
for _ in range(60):
    ep = vsc.get_endpoint(VS_ENDPOINT)
    state = ep.get("endpoint_status", {}).get("state")
    if state == "ONLINE":
        print(f"Endpoint {VS_ENDPOINT} is ONLINE")
        break
    print(f"  Waiting... state={state}")
    time.sleep(30)
else:
    raise TimeoutError(f"Endpoint {VS_ENDPOINT} not ONLINE after 30 min")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed on Source Table

# COMMAND ----------

spark.sql(f"ALTER TABLE {SOURCE_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"CDF enabled on {SOURCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Sync Index

# COMMAND ----------

try:
    idx = vsc.get_index(VS_ENDPOINT, INDEX_NAME)
    print(f"Index {INDEX_NAME} already exists: {idx.describe().get('status', {}).get('ready')}")
except Exception:
    vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT,
        index_name=INDEX_NAME,
        source_table_name=SOURCE_TABLE,
        pipeline_type="TRIGGERED",
        primary_key="product_id",
        embedding_source_column="description",
        embedding_model_endpoint_name="databricks-gte-large-en",
        columns_to_sync=["product_id", "name", "category", "price"],
    )
    print(f"Creating index {INDEX_NAME}...")

# Wait for index to be ready
for _ in range(60):
    idx = vsc.get_index(VS_ENDPOINT, INDEX_NAME)
    status = idx.describe().get("status", {})
    if status.get("ready"):
        print(f"Index {INDEX_NAME} is READY")
        break
    print(f"  Waiting... status={status}")
    time.sleep(30)
else:
    raise TimeoutError(f"Index {INDEX_NAME} not ready after 30 min")

# COMMAND ----------

# Quick test query
results = vsc.get_index(VS_ENDPOINT, INDEX_NAME).similarity_search(
    query_text="casual summer denim",
    columns=["product_id", "name", "category", "price"],
    num_results=3,
)
display(results.get("result", {}).get("data_array", []))
print("Vector Search setup complete.")
