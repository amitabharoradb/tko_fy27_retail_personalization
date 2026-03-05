# Task List: Hyper-Personalized Loyalty App

**Version:** 1.0
**Date:** 2026-03-05
**Execution order:** Phase 0 → 1 → 2 → 3. Within each phase, tasks are sequential unless noted.

Legend: `[ ]` pending · `[x]` done · `[~]` in-progress · `[!]` blocked

---

## Phase 0 — DAB Scaffold

### T0.1 Create `databricks.yml`
- [ ] Create `databricks.yml` at repo root
- [ ] Set bundle name: `tko27-retail-personalization`
- [ ] Define variables: `catalog`, `schema`, `volume_path`
- [ ] Define target `dev` with `profile: fevm`
- [ ] Add `include` for `resources/jobs/*.yml` and `resources/pipeline.yml`
- **AC:** `databricks bundle validate --profile fevm` exits 0

### T0.2 Create `resources/jobs/phase1_setup.yml`
- [ ] Create `resources/jobs/` directory
- [ ] Define job `phase1_setup` with 5 sequential notebook tasks (01→05)
- [ ] All tasks use Serverless compute (`runtime_engine: SERVERLESS`)
- [ ] Each task depends on the prior via `depends_on`
- **AC:** `databricks bundle validate` shows job resource; no errors

### T0.3 Create `resources/jobs/phase2_ai.yml`
- [ ] Define job `phase2_ai` with 2 sequential tasks:
  1. Run `agents/vector_search_setup.py`
  2. Run `agents/style_assistant/agent.py` (deploys to Model Serving)
- [ ] Both tasks use Serverless compute
- **AC:** `databricks bundle validate` shows `phase2_ai` job

### T0.4 Create `resources/pipeline.yml`
- [ ] Define pipeline resource `retail_intent_pipeline`
- [ ] Set `serverless: true`, `continuous: true`
- [ ] Set `catalog: amitabh_arora_catalog`, `target: tko27_retail`
- [ ] Point library to `pipelines/retail_intent_pipeline` notebook path
- **AC:** `databricks bundle validate` shows pipeline resource

---

## Phase 1 — Mock Data + Unity Catalog

> **Prereq:** Phase 0 complete; `databricks bundle deploy --profile fevm` run.

### T1.1 Create `notebooks/01_uc_setup.py`
- [ ] Create catalog `amitabh_arora_catalog` (IF NOT EXISTS)
- [ ] Create schema `tko27_retail` (IF NOT EXISTS)
- [ ] Create volume `raw_data` under schema (IF NOT EXISTS)
- [ ] Grant `CREATE TABLE` on schema to current user
- **AC:** `SHOW SCHEMAS IN amitabh_arora_catalog` shows `tko27_retail`; volume path exists

### T1.2 Create `notebooks/02_generate_data.py`
- [ ] Install `faker` via `%pip install faker`
- [ ] Generate `products` (5,000 rows): `product_id`, `name`, `category`, `subcategory`, `price`, `brand`, `description`, `tags`, `created_at`
  - Categories: Denim, Tops, Footwear, Accessories, Outerwear
  - Description: 2–3 sentence free text (used for VS embeddings)
- [ ] Generate `customer_profiles` (10,000 rows): full schema per PRD
  - `loyalty_tier`: Bronze/Silver/Gold/Platinum weighted distribution
  - `ltv_score`: 0–100 continuous
  - `preferred_categories`: 1–3 random categories as array
- [ ] Generate `purchase_history` (5,000 rows): valid FK refs to customer + product IDs
- [ ] Generate `clickstream_events` (10,000 rows): mix of event types, ~20% anonymous
- [ ] Write each dataset as CSV to Volume: `/Volumes/amitabh_arora_catalog/tko27_retail/raw_data/<table>/`
- **AC:** All 4 CSV directories exist in volume; file counts non-zero

### T1.3 Create `notebooks/03_load_data.py`
- [ ] Load `products` from Volume CSV → Delta table using `read_files()`
- [ ] Load `customer_profiles` → Delta table
- [ ] Load `purchase_history` → Delta table
- [ ] Load `clickstream_events` → Delta table
- [ ] Add `OPTIMIZE` + `ZORDER` on key columns (e.g., `customer_id`)
- **AC:** All 4 tables queryable; row counts match generated data

### T1.4 Create `notebooks/04_governance.py`
- [ ] Tag `pii=true` on: `email`, `first_name`, `last_name`, `credit_card_last4` (customer_profiles)
- [ ] Tag `segment=high_value` on top 10% of customers by `ltv_score`
  - Compute 90th percentile threshold; update table tags via `ALTER TABLE ... ALTER COLUMN ... SET TAGS`
- [ ] Create row group `marketing_role` (group or role)
- [ ] Create masking function `mask_pii` returning `****` for `marketing_role` members
- [ ] Apply masking to all 4 PII columns
- **AC:** `DESCRIBE TABLE EXTENDED customer_profiles` shows tags; SELECT as marketing_role returns `****`

### T1.5 Create `notebooks/05_validate_phase1.py`
- [ ] Assert row counts: products=5000, customer_profiles=10000, purchase_history=5000, clickstream_events=10000
- [ ] Assert `pii=true` tag on `customer_profiles.email`
- [ ] Assert top 10% customers have `segment=high_value` tag
- [ ] Assert masking function exists: `SHOW FUNCTIONS LIKE 'mask_pii'`
- [ ] Print PASS/FAIL summary
- **AC:** Notebook runs to completion with all assertions passing

---

## Phase 2 — Pipelines + AI Layer

> **Prereq:** Phase 1 complete; all 4 UC tables populated.

### T2.1 Create `pipelines/retail_intent_pipeline.py`
- [ ] Bronze table `clickstream_bronze`: Auto Loader streaming from Volume CSV path
  - Schema location: `${VOLUME_PATH}/_schema/clickstream`
- [ ] Silver table `clickstream_silver`:
  - Join with `products` on `product_id`
  - Compute `event_weight` (add_to_cart=5, search=3, product_view=2, page_view=1)
  - Compute `age_hours` from event timestamp to now
  - Compute `intent_score = event_weight * exp(-ln(2) * age_hours / 48)`
  - Filter to rolling 7-day window
- [ ] Gold table `customer_current_interests`:
  - Group by `customer_id`, `category`; SUM `intent_score`
  - Window rank per customer; keep `rank <= 3`
  - Add `updated_at = current_timestamp()`
- **AC:** Pipeline starts; Bronze table receives rows within 30s of trigger; Gold table populated

### T2.2 Deploy SDP pipeline via bundle
- [ ] `databricks bundle deploy --profile fevm` (includes pipeline resource)
- [ ] Start pipeline: `databricks pipelines start tko27-retail-intent-pipeline --profile fevm`
- **AC:** Pipeline visible in Databricks UI; status = Running (continuous)

### T2.3 Create `agents/vector_search_setup.py`
- [ ] Create VS endpoint `tko27-retail-vs-endpoint` (type: STANDARD)
- [ ] Wait for endpoint to be ONLINE (poll SDK)
- [ ] Create Delta Sync index `products_description_index`:
  - Source: `amitabh_arora_catalog.tko27_retail.products`
  - Primary key: `product_id`
  - Embedding column: `description`
  - Embedding model: `databricks-gte-large-en`
  - Metadata: `product_id`, `name`, `category`, `price`
- [ ] Trigger initial sync; wait for ONLINE status
- **AC:** `databricks vector-search indexes get --name products_description_index --profile fevm` shows `ONLINE`; test query returns ≥1 result

### T2.4 Create `agents/style_assistant/agent.py`
- [ ] Subclass `mlflow.pyfunc.ChatModel`
- [ ] `predict()` method:
  1. Extract `customer_id` from input messages
  2. SQL query → top 3 interests from `customer_current_interests`
  3. SQL query → last 5 purchases from `purchase_history` JOIN `products`
  4. VS query using interest categories → top 5 products
  5. Build prompt with interests + purchase history + VS results
  6. Call Claude Opus 4.6 via AI Gateway endpoint
  7. Return structured JSON (products list + reasoning)
- [ ] Log model to UC model registry: `amitabh_arora_catalog.tko27_retail.style_assistant`
- [ ] Deploy to endpoint `style-assistant-endpoint` (Serverless)
- **AC:** POST to endpoint with `{"messages":[{"role":"user","content":"customer_id: CUST-00001"}]}` returns valid JSON with `recommended_products` array

### T2.5 Create `agents/style_assistant/requirements.txt`
- [ ] `mlflow>=2.14`
- [ ] `databricks-sdk`
- [ ] `openai`
- [ ] `pyspark`

---

## Phase 3 — Serving Layer

> **Prereq:** Phase 2 complete; `customer_current_interests` populated; `style-assistant-endpoint` live.

### T3.1 Provision Lakebase database
- [ ] Create Lakebase DB `retail_state` via Databricks SDK:
  ```python
  w.lakebase.create_or_update_database(name="retail_state")
  ```
- [ ] Generate credential for connection
- **AC:** DB visible in Unity Catalog; can connect via psycopg2

### T3.2 Create Lakebase tables (`lakebase/schema.sql`)
- [ ] Create `personalized_offers` table (full schema per PRD D3.1)
- [ ] Create `active_sessions` table (full schema per PRD D3.1)
- [ ] Run schema file against Lakebase
- **AC:** `\dt` in psql shows both tables; INSERT + SELECT round-trips work

### T3.3 Seed Lakebase with sample data
- [ ] Insert 50 sample offers for customers `CUST-00001` through `CUST-00050`
  - Mix of offer types: discount (DENIM20), bundle, loyalty_bonus
  - `expires_at` = NOW() + 7 days
- [ ] Insert 10 active sessions
- **AC:** `SELECT COUNT(*) FROM personalized_offers` returns 50

### T3.4 Create Genie Space (`genie/certified_sql.sql`)
- [ ] File contains 3 certified SQL definitions from PRD D3.2 (LTV, churn risk, recent interests)
- [ ] Create Genie Space `Retail Marketing Intelligence` via `databricks-genie` skill / MCP:
  - Tables: `customer_profiles`, `purchase_history`, `clickstream_silver`, `customer_current_interests`
  - Load certified SQL from file
  - Add 3 pre-loaded sample queries from PRD
- **AC:** Genie Space visible in workspace; "top LTV customers who browsed denim" returns results

### T3.5 Create Streamlit app (`app/app.py` + pages)
- [ ] `app/app.py`: entry point with OAuth OBO via `databricks-sdk`
- [ ] `app/pages/01_lookup.py`: search by `customer_id`; SQL Warehouse query to `customer_profiles`
- [ ] `app/pages/02_profile.py`: display tier, points, LTV, preferred categories
- [ ] `app/pages/03_recommendations.py`: call `style-assistant-endpoint`; render product cards
- [ ] `app/pages/04_offers.py`: read `personalized_offers` from Lakebase via psycopg2
- [ ] `app/pages/05_intent.py`: poll `customer_current_interests` every 10s via `st.rerun()`
- **AC:** All pages render without error; recommendations load in <3s; intent tab updates without full reload

### T3.6 Create `app/requirements.txt`
- [ ] `streamlit>=1.32`
- [ ] `databricks-sdk`
- [ ] `psycopg2-binary`
- [ ] `openai`

### T3.7 Create `app/app.yaml`
- [ ] Command: `["streamlit", "run", "app.py", "--server.port", "8080"]`
- [ ] Env vars: `DATABRICKS_WAREHOUSE_ID`, `LAKEBASE_HOST`, `LAKEBASE_PORT`, `LAKEBASE_DB`
- **AC:** File validates; `databricks apps deploy` accepts it

### T3.8 Deploy Databricks App
- [ ] `databricks apps deploy retail-customer-portal --source-code-path ./app --profile fevm`
- [ ] Open app URL; verify all 5 pages load
- [ ] Verify OAuth OBO: marketing user sees masked PII in profile page
- **AC:** App URL accessible; intent signals poll correctly; recommendations return in <3s

---

## Final Verification Checklist

- [ ] `databricks bundle validate --profile fevm` → exit 0
- [ ] `databricks bundle deploy --profile fevm` → all resources created
- [ ] Phase 1 notebook `05_validate_phase1.py` → all assertions PASS
- [ ] Pipeline `tko27-retail-intent-pipeline` → status RUNNING; Gold table has rows
- [ ] VS index `products_description_index` → ONLINE; test query <500ms
- [ ] `style-assistant-endpoint` → returns valid JSON recommendations
- [ ] Genie Space → "denim + 30-day" query returns correct cohort
- [ ] Streamlit app → all 5 pages functional; intent polls every 10s
- [ ] OAuth OBO → marketing_role sees `****` for PII columns
