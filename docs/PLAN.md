# Implementation Plan: Hyper-Personalized Loyalty App

**Version:** 1.0
**Date:** 2026-03-05
**Workspace profile:** `fevm`
**UC Catalog:** `amitabh_arora_catalog`, schema: `tko27_retail`

---

## Build Order

```
Phase 0 (DAB scaffold) → Phase 1 (data + UC) → Phase 2 (pipelines + AI) → Phase 3 (serving)
```

Dependencies are strict: each phase requires the prior phase's tables/endpoints to exist.

---

## Phase 0 — DAB Scaffold

Create the project skeleton so all subsequent work deploys via Asset Bundles.

### Files to create

| File | Purpose |
|------|---------|
| `databricks.yml` | Bundle root; variables for catalog/schema/volume |
| `resources/jobs/phase1_setup.yml` | Sequential job: 5 notebook tasks |
| `resources/jobs/phase2_ai.yml` | Sequential job: VS setup + agent deploy |
| `resources/pipeline.yml` | Lakeflow SDP pipeline resource |

### `databricks.yml` pattern
```yaml
bundle:
  name: tko27-retail-personalization

variables:
  catalog:
    default: amitabh_arora_catalog
  schema:
    default: tko27_retail
  volume_path:
    default: /Volumes/amitabh_arora_catalog/tko27_retail/raw_data

targets:
  dev:
    workspace:
      profile: fevm
    default: true

include:
  - resources/jobs/*.yml
  - resources/pipeline.yml
```

### Job resource pattern (`phase1_setup.yml`)
```yaml
resources:
  jobs:
    phase1_setup:
      name: "[TKO27] Phase 1 - UC Setup"
      tasks:
        - task_key: uc_setup
          notebook_task:
            notebook_path: ${workspace.root_path}/notebooks/01_uc_setup
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 0
            data_security_mode: SINGLE_USER
            runtime_engine: SERVERLESS
        - task_key: generate_data
          depends_on: [{task_key: uc_setup}]
          notebook_task:
            notebook_path: ${workspace.root_path}/notebooks/02_generate_data
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 0
            runtime_engine: SERVERLESS
        # ... tasks 03-05 follow same pattern
```

### Verification
```bash
databricks bundle validate --profile fevm
```

---

## Phase 1 — Mock Data + Unity Catalog

### Notebook 01: `notebooks/01_uc_setup.py`

```python
# Databricks notebook source
# MAGIC %sql
CREATE CATALOG IF NOT EXISTS amitabh_arora_catalog;
CREATE SCHEMA IF NOT EXISTS amitabh_arora_catalog.tko27_retail;
CREATE VOLUME IF NOT EXISTS amitabh_arora_catalog.tko27_retail.raw_data;
```

### Notebook 02: `notebooks/02_generate_data.py`

Use `databricks-synthetic-data-gen` skill pattern: Faker + Spark on Serverless.

Key pattern:
```python
from faker import Faker
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

fake = Faker()
spark = SparkSession.builder.getOrCreate()

VOLUME_PATH = "/Volumes/amitabh_arora_catalog/tko27_retail/raw_data"

# Generate as list of dicts → spark.createDataFrame → write CSV
products_data = [generate_product(i) for i in range(5000)]
df = spark.createDataFrame(products_data)
df.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/products/")
```

Datasets and sizes:
- `products`: 5,000 rows — categories: Denim, Tops, Footwear, Accessories, Outerwear
- `customer_profiles`: 10,000 rows — loyalty tiers, LTV scores, preferred categories
- `purchase_history`: 5,000 rows — FK to customers + products
- `clickstream_events`: 10,000 rows — event types with weights

### Notebook 03: `notebooks/03_load_data.py`

```python
# MAGIC %sql
CREATE TABLE IF NOT EXISTS amitabh_arora_catalog.tko27_retail.products
USING DELTA
AS SELECT * FROM read_files(
  '/Volumes/amitabh_arora_catalog/tko27_retail/raw_data/products/',
  format => 'csv', header => true, inferSchema => true
);
```
Repeat for all 4 tables.

### Notebook 04: `notebooks/04_governance.py`

```sql
-- PII tags
ALTER TABLE customer_profiles ALTER COLUMN email SET TAGS ('pii' = 'true');
ALTER TABLE customer_profiles ALTER COLUMN first_name SET TAGS ('pii' = 'true');
ALTER TABLE customer_profiles ALTER COLUMN last_name SET TAGS ('pii' = 'true');
ALTER TABLE customer_profiles ALTER COLUMN credit_card_last4 SET TAGS ('pii' = 'true');

-- High-value segment tag (top 10% by ltv_score)
-- Compute threshold first, then tag rows via UPDATE on a tagging helper table

-- Column masking policy
CREATE FUNCTION amitabh_arora_catalog.tko27_retail.mask_pii(col STRING)
  RETURN CASE WHEN is_member('marketing_role') THEN '****' ELSE col END;

ALTER TABLE customer_profiles ALTER COLUMN email
  SET MASK amitabh_arora_catalog.tko27_retail.mask_pii;
-- Repeat for first_name, last_name, credit_card_last4
```

### Notebook 05: `notebooks/05_validate_phase1.py`

```python
# Smoke tests
assert spark.table("amitabh_arora_catalog.tko27_retail.products").count() == 5000
assert spark.table("amitabh_arora_catalog.tko27_retail.customer_profiles").count() == 10000
assert spark.table("amitabh_arora_catalog.tko27_retail.purchase_history").count() == 5000
assert spark.table("amitabh_arora_catalog.tko27_retail.clickstream_events").count() == 10000
# Check tags
tag_rows = spark.sql("SHOW TAGS ON TABLE customer_profiles COLUMN email").collect()
assert any(r["tag_name"] == "pii" for r in tag_rows)
```

---

## Phase 2 — Pipelines + AI Layer

### Lakeflow SDP: `pipelines/retail_intent_pipeline.py`

Use `databricks-spark-declarative-pipelines` skill patterns.

```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math

VOLUME_PATH = "/Volumes/amitabh_arora_catalog/tko27_retail/raw_data"

@dlt.table(name="clickstream_bronze", comment="Raw clickstream events from Volume")
def clickstream_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schema/clickstream")
            .option("header", "true")
            .load(f"{VOLUME_PATH}/clickstream_events/")
    )

@dlt.table(name="clickstream_silver", comment="Enriched with intent scores")
def clickstream_silver():
    event_weights = F.when(F.col("event_type") == "add_to_cart", 5) \
                     .when(F.col("event_type") == "search", 3) \
                     .when(F.col("event_type") == "product_view", 2) \
                     .otherwise(1)

    age_hours = (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("timestamp")) / 3600

    decay = F.exp(-F.log(F.lit(2.0)) * age_hours / 48)

    return (
        dlt.read_stream("clickstream_bronze")
            .join(spark.table("amitabh_arora_catalog.tko27_retail.products"),
                  "product_id", "left")
            .withColumn("event_weight", event_weights)
            .withColumn("age_hours", age_hours)
            .withColumn("intent_score", event_weights * decay)
    )

@dlt.table(name="customer_current_interests", comment="Top 3 category interests per customer")
def customer_current_interests():
    window = Window.partitionBy("customer_id").orderBy(F.desc("total_score"))
    return (
        dlt.read("clickstream_silver")
            .filter(F.col("timestamp") > F.date_sub(F.current_date(), 7))
            .groupBy("customer_id", "category")
            .agg(F.sum("intent_score").alias("intent_score"))
            .withColumn("rank", F.rank().over(window))
            .filter(F.col("rank") <= 3)
            .withColumn("updated_at", F.current_timestamp())
    )
```

### Pipeline DAB resource: `resources/pipeline.yml`

```yaml
resources:
  pipelines:
    retail_intent_pipeline:
      name: tko27-retail-intent-pipeline
      serverless: true
      continuous: true
      catalog: amitabh_arora_catalog
      target: tko27_retail
      libraries:
        - notebook:
            path: ${workspace.root_path}/pipelines/retail_intent_pipeline
```

### Vector Search: `agents/vector_search_setup.py`

Use `databricks-vector-search` skill patterns.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    EndpointType, VectorIndexType, DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn, PipelineType
)

w = WorkspaceClient()

# 1. Create endpoint
w.vector_search_endpoints.create_endpoint(
    name="tko27-retail-vs-endpoint",
    endpoint_type=EndpointType.STANDARD
)

# 2. Create Delta Sync index
w.vector_search_indexes.create_index(
    name="amitabh_arora_catalog.tko27_retail.products_description_index",
    endpoint_name="tko27-retail-vs-endpoint",
    primary_key="product_id",
    index_type=VectorIndexType.DELTA_SYNC,
    delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
        source_table="amitabh_arora_catalog.tko27_retail.products",
        pipeline_type=PipelineType.TRIGGERED,
        embedding_source_columns=[
            EmbeddingSourceColumn(
                name="description",
                embedding_model_endpoint_name="databricks-gte-large-en"
            )
        ]
    )
)
```

### Style Assistant Agent: `agents/style_assistant/agent.py`

Use `databricks-model-serving` skill patterns (MLflow ChatAgent).

```python
import mlflow
from mlflow.pyfunc import ChatModel
from databricks.sdk import WorkspaceClient

class StyleAssistantAgent(ChatModel):
    def predict(self, context, messages, params=None):
        customer_id = self._extract_customer_id(messages)
        w = WorkspaceClient()

        # Fetch interests
        interests = spark.sql(f"""
            SELECT category, intent_score FROM amitabh_arora_catalog.tko27_retail.customer_current_interests
            WHERE customer_id = '{customer_id}' ORDER BY rank LIMIT 3
        """).collect()

        # Fetch purchases
        purchases = spark.sql(f"""
            SELECT p.name, p.category, ph.purchase_date
            FROM amitabh_arora_catalog.tko27_retail.purchase_history ph
            JOIN amitabh_arora_catalog.tko27_retail.products p USING (product_id)
            WHERE ph.customer_id = '{customer_id}'
            ORDER BY ph.purchase_date DESC LIMIT 5
        """).collect()

        # Vector Search
        query_text = " ".join([r.category for r in interests])
        vs_results = w.vector_search_indexes.query_index(
            index_name="amitabh_arora_catalog.tko27_retail.products_description_index",
            columns=["product_id", "name", "category", "price"],
            query_text=query_text,
            num_results=5
        )

        # Call Claude via AI Gateway
        import openai
        client = openai.OpenAI(
            base_url=f"{w.config.host}/serving-endpoints/claude-opus-4-6-gateway/v1",
            api_key=w.config.token
        )
        response = client.chat.completions.create(
            model="claude-opus-4-6",
            messages=[
                {"role": "system", "content": RETAIL_PERSONA_PROMPT},
                {"role": "user", "content": self._build_prompt(customer_id, interests, purchases, vs_results)}
            ]
        )
        return {"messages": [{"role": "assistant", "content": response.choices[0].message.content}]}
```

Deploy with:
```python
mlflow.set_registry_uri("databricks-uc")
with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="style_assistant",
        python_model=StyleAssistantAgent(),
        registered_model_name="amitabh_arora_catalog.tko27_retail.style_assistant"
    )
# Then deploy via SDK or CLI
```

---

## Phase 3 — Serving Layer

### Lakebase: `lakebase/schema.sql`

Standard PostgreSQL DDL:
```sql
CREATE TABLE IF NOT EXISTS personalized_offers (
    offer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(50) NOT NULL,
    offer_code VARCHAR(50) NOT NULL,
    product_id VARCHAR(50),
    relevance_score DOUBLE PRECISION DEFAULT 0.0,
    offer_type VARCHAR(20) CHECK (offer_type IN ('discount','bundle','loyalty_bonus')),
    discount_pct INTEGER DEFAULT 0,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    session_id VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS active_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    current_category VARCHAR(50),
    cart_items JSONB DEFAULT '[]',
    intent_snapshot JSONB DEFAULT '{}'
);
```

Provision with Databricks SDK:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
db = w.lakebase.create_or_update_database(name="retail_state")
```

### Genie Space: `genie/certified_sql.sql`

Contains certified SQL definitions from PRD D3.2. Genie Space created via `databricks-genie` skill / MCP tool.

### Streamlit App: `app/app.py`

Use `databricks-app-python` skill patterns.

Key patterns:
- OAuth OBO: use `databricks-sdk` with `oauth_service_principal` or `oauth_u2m`
- Lakebase connection: `psycopg2` with credentials from `generate_lakebase_credential`
- Intent polling: `time.sleep(10); st.rerun()`

```python
import streamlit as st
from databricks.sdk import WorkspaceClient

# OAuth OBO
w = WorkspaceClient()  # picks up Databricks App OAuth automatically

st.set_page_config(page_title="Customer Portal", layout="wide")
# Pages configured via app/pages/ directory
```

`app/app.yaml`:
```yaml
command: ["streamlit", "run", "app.py", "--server.port", "8080"]
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: "sql_warehouse_id"
```

---

## Critical File Checklist

| File | Phase | Notes |
|------|-------|-------|
| `databricks.yml` | 0 | Bundle root |
| `resources/jobs/phase1_setup.yml` | 0 | Notebooks 01→05 |
| `resources/jobs/phase2_ai.yml` | 0 | VS + agent deploy |
| `resources/pipeline.yml` | 0 | SDP pipeline |
| `notebooks/01_uc_setup.py` | 1 | DDL: schema, volume |
| `notebooks/02_generate_data.py` | 1 | Faker+Spark data gen |
| `notebooks/03_load_data.py` | 1 | CSV → Delta via `read_files` |
| `notebooks/04_governance.py` | 1 | Tags + masking DDL |
| `notebooks/05_validate_phase1.py` | 1 | Smoke tests |
| `pipelines/retail_intent_pipeline.py` | 2 | DLT/SDP pipeline |
| `agents/vector_search_setup.py` | 2 | VS endpoint + index |
| `agents/style_assistant/agent.py` | 2 | MLflow ChatAgent |
| `agents/style_assistant/requirements.txt` | 2 | mlflow, databricks-sdk, openai |
| `lakebase/schema.sql` | 3 | PostgreSQL DDL |
| `genie/certified_sql.sql` | 3 | Certified SQL for Genie |
| `app/app.py` | 3 | Streamlit entry point |
| `app/app.yaml` | 3 | Databricks App manifest |
| `app/pages/01_lookup.py` | 3 | Customer lookup page |
| `app/pages/02_profile.py` | 3 | Profile + loyalty page |
| `app/pages/03_recommendations.py` | 3 | Style assistant page |
| `app/pages/04_offers.py` | 3 | Active offers (Lakebase) |
| `app/pages/05_intent.py` | 3 | Intent signals (polls 10s) |
| `app/requirements.txt` | 3 | streamlit, databricks-sdk, psycopg2 |

---

## Verification by Phase

| Phase | Command / Check |
|-------|----------------|
| 0 | `databricks bundle validate --profile fevm` |
| 1 | Run `05_validate_phase1.py`; check masking as marketing_role |
| 2 | Query `customer_current_interests`; call VS REST API; POST to `style-assistant-endpoint` |
| 3 | Open app URL; run Genie sample queries; verify Lakebase reads <1s |
