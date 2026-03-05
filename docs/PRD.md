# Product Requirements Document: Hyper-Personalized Loyalty App

**Version:** 1.0
**Date:** 2026-03-05
**Status:** Draft
**Workspace profile:** `fevm`
**UC Catalog:** `amitabh_arora_catalog`

---

## Problem Statement

Retailers suffer from "Relevance Fatigue": generic marketing erodes brand value, static loyalty programs miss real-time shopper intent, and conversion opportunities are lost because the right offer arrives too late. This demo shows Databricks transforming a retailer's data estate into a hyper-personalized loyalty engine — unifying clickstream, purchase history, and product catalog into a real-time AI layer that surfaces the right offer to the right shopper at the right moment.

---

## Personas

| Persona | Role | Primary interaction |
|---------|------|---------------------|
| **SE (Builder)** | Databricks SE running the TKO demo | Builds and deploys all assets; queries UC directly |
| **Marketer** | Retail marketing analyst | Uses Genie Space for cohort queries and campaign targeting |
| **Shopper** | End customer | Uses the Databricks App (Customer Portal) to see offers and loyalty status |

---

## Technical Constraints

- All compute: **Serverless** (no classic clusters unless unavoidable)
- All resources in UC catalog: `amitabh_arora_catalog`, schema: `tko27_retail`
- UC Volume path: `/Volumes/amitabh_arora_catalog/tko27_retail/raw_data/`
- Workspace profile: `fevm`

---

## Phase 1: Foundation — Mock Data + Unity Catalog

**Target:** Week 1
**Goal:** All raw data loaded into UC; SE can query tables and observe governance controls.

### Deliverables

#### D1.1 — Mock Data Generation Script
Python script generates 4 synthetic datasets and writes CSVs to the UC Volume.

#### D1.2 — UC Catalog / Schema / Volume
Create `amitabh_arora_catalog.tko27_retail` schema and `/Volumes/amitabh_arora_catalog/tko27_retail/raw_data/` volume.

#### D1.3 — UC Tags
- Tag `pii=true` on: `customer_profiles.email`, `customer_profiles.first_name`, `customer_profiles.last_name`, `customer_profiles.credit_card_last4`
- Tag `segment=high_value` on top 10% of customers by `ltv_score`

#### D1.4 — Column-level Permissions
- Create role `marketing_role`
- Dynamic data masking: `email`, `first_name`, `last_name`, `credit_card_last4` return `****` for `marketing_role`

---

### Data Schemas

#### `products` — 5,000 rows

| Column | Type | Notes |
|--------|------|-------|
| `product_id` | STRING | PK, e.g. `PROD-00001` |
| `name` | STRING | Product display name |
| `category` | STRING | Denim, Tops, Footwear, Accessories, Outerwear |
| `subcategory` | STRING | e.g. Skinny Jeans, Ankle Boots |
| `price` | DOUBLE | 9.99 – 299.99 |
| `brand` | STRING | e.g. UrbanEdge, NorthStyle |
| `description` | STRING | Free-text, used for Vector Search embeddings |
| `tags` | ARRAY\<STRING\> | e.g. ["casual","summer","trending"] |
| `created_at` | TIMESTAMP | |

#### `customer_profiles` — 10,000 rows

| Column | Type | Notes |
|--------|------|-------|
| `customer_id` | STRING | PK, e.g. `CUST-00001` |
| `first_name` | STRING | PII — masked for `marketing_role` |
| `last_name` | STRING | PII — masked |
| `email` | STRING | PII — masked; tag `pii=true` |
| `credit_card_last4` | STRING | PII — masked; tag `pii=true` |
| `age_bucket` | STRING | 18-24, 25-34, 35-44, 45+ |
| `gender` | STRING | M, F, Non-binary, Prefer not to say |
| `loyalty_tier` | STRING | Bronze, Silver, Gold, Platinum |
| `loyalty_points` | INT | 0 – 50,000 |
| `ltv_score` | DOUBLE | 0–100; top 10% tagged `segment=high_value` |
| `preferred_categories` | ARRAY\<STRING\> | e.g. ["Denim","Footwear"] |
| `last_purchase_date` | DATE | |
| `created_at` | TIMESTAMP | |

#### `purchase_history` — 5,000 rows

| Column | Type | Notes |
|--------|------|-------|
| `transaction_id` | STRING | PK |
| `customer_id` | STRING | FK → `customer_profiles` |
| `product_id` | STRING | FK → `products` |
| `quantity` | INT | 1–5 |
| `unit_price` | DOUBLE | |
| `total_amount` | DOUBLE | `quantity * unit_price` |
| `purchase_date` | TIMESTAMP | |
| `channel` | STRING | web, mobile, in-store |

#### `clickstream_events` — 10,000 rows

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | STRING | PK |
| `session_id` | STRING | Groups events in a browsing session |
| `customer_id` | STRING | Nullable — anonymous browsing allowed |
| `product_id` | STRING | Nullable |
| `event_type` | STRING | page_view, product_view, add_to_cart, search, purchase |
| `search_term` | STRING | Nullable — populated for `search` events |
| `category` | STRING | Category context of the event |
| `timestamp` | TIMESTAMP | |
| `device_type` | STRING | mobile, desktop, tablet |
| `referrer` | STRING | e.g. google, instagram, direct |

---

### Phase 1 Acceptance Criteria

- [ ] All 4 tables queryable in `amitabh_arora_catalog.tko27_retail`
- [ ] `DESCRIBE TABLE EXTENDED customer_profiles` shows `pii=true` on sensitive columns
- [ ] Top 10% LTV customers have `segment=high_value` tag
- [ ] `marketing_role` SELECT on `customer_profiles` returns masked values for PII columns

---

## Phase 2: Pipelines + AI Layer

**Target:** Week 2
**Goal:** Streaming intent scores computed; RAG agent deployed and queryable.

### D2.1 — Lakeflow Spark Declarative Pipeline

**Pipeline name:** `tko27-retail-intent-pipeline`
**Mode:** Serverless, continuous streaming

| Layer | Table | Logic |
|-------|-------|-------|
| Bronze | `clickstream_bronze` | Auto Loader from UC Volume, streaming table |
| Silver | `clickstream_silver` | Join with `products`; compute `intent_score` per customer per category |
| Gold | `customer_current_interests` | Materialized view — top 3 categories per customer with score |

**Intent score formula:**

```
intent_score = SUM(event_weight * exp(-ln(2) * age_hours / 48))
```

Event weights: `add_to_cart=5`, `search=3`, `product_view=2`, `page_view=1`
Decay: exponential with 48-hour half-life
Window: rolling 7 days

**`customer_current_interests` schema:**

| Column | Type |
|--------|------|
| `customer_id` | STRING |
| `category` | STRING |
| `intent_score` | DOUBLE |
| `rank` | INT (1=top interest) |
| `updated_at` | TIMESTAMP |

---

### D2.2 — Vector Search Index

| Setting | Value |
|---------|-------|
| Source table | `amitabh_arora_catalog.tko27_retail.products` |
| Embedding column | `description` |
| Embedding model | `databricks-gte-large-en` (managed) |
| Index type | Delta Sync |
| Online serving | Enabled |
| Metadata columns | `product_id`, `name`, `category`, `price` |
| Index name | `products_description_index` |

---

### D2.3 — Style Assistant Agent (Model Serving)

| Setting | Value |
|---------|-------|
| LLM | Claude Opus 4.6 (`claude-opus-4-6`) via Databricks AI Gateway external model endpoint |
| Framework | RAG agent (MLflow ChatAgent) |
| Endpoint name | `style-assistant-endpoint` |
| Serving mode | Serverless |

**Agent logic:**
1. Accept `customer_id` as input
2. Fetch top 3 interests from `customer_current_interests`
3. Fetch last 5 purchases from `purchase_history`
4. Query Vector Search index using interest categories as query text → top 5 products
5. Pass context + customer history to Claude Opus 4.6 with retail persona system prompt

**Output schema:**
```json
{
  "recommended_products": [
    {
      "product_id": "PROD-00123",
      "name": "Slim Fit Dark Wash Jeans",
      "category": "Denim",
      "price": 89.99,
      "reasoning": "Matches recent Denim browsing and past purchase pattern"
    }
  ],
  "reasoning": "Based on your recent interest in Denim and Footwear..."
}
```

---

### Phase 2 Acceptance Criteria

- [ ] Pipeline runs in Serverless mode; no classic cluster config
- [ ] Intent scores update within 60s of new clickstream events ingested
- [ ] Vector Search returns results in <500ms (p95)
- [ ] `style-assistant-endpoint` returns valid JSON recommendations for any valid `customer_id`

---

## Phase 3: Serving Layer

**Target:** Week 3
**Goal:** Real-time app live; Genie Space answers marketing queries end-to-end.

### D3.1 — Lakebase State Store (Serverless PostgreSQL)

**Database:** `retail_state`

#### Table: `personalized_offers`

| Column | Type | Notes |
|--------|------|-------|
| `offer_id` | UUID | PK, auto-generated |
| `customer_id` | VARCHAR(50) | |
| `offer_code` | VARCHAR(50) | e.g. `DENIM20` |
| `product_id` | VARCHAR(50) | FK to UC products |
| `relevance_score` | DOUBLE PRECISION | 0.0–1.0 |
| `offer_type` | VARCHAR(20) | discount, bundle, loyalty_bonus |
| `discount_pct` | INTEGER | 0–50 |
| `expires_at` | TIMESTAMPTZ | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() |
| `session_id` | VARCHAR(100) | Links offer to triggering session |

#### Table: `active_sessions`

| Column | Type | Notes |
|--------|------|-------|
| `session_id` | VARCHAR(100) | PK |
| `customer_id` | VARCHAR(50) | |
| `last_seen` | TIMESTAMPTZ | Updated on each event |
| `current_category` | VARCHAR(50) | Most recently browsed category |
| `cart_items` | JSONB | Array of `{product_id, quantity, price}` |
| `intent_snapshot` | JSONB | Top 3 interests at session start |

---

### D3.2 — Genie Space

**Space name:** `Retail Marketing Intelligence`

**Tables exposed:**
- `amitabh_arora_catalog.tko27_retail.customer_profiles`
- `amitabh_arora_catalog.tko27_retail.purchase_history`
- `amitabh_arora_catalog.tko27_retail.clickstream_silver`
- `amitabh_arora_catalog.tko27_retail.customer_current_interests`

**Certified SQL definitions:**

```sql
-- LTV calculation
-- ltv_score is a pre-computed column (0-100); for raw spend:
SELECT customer_id, SUM(total_amount) AS lifetime_value
FROM purchase_history GROUP BY customer_id;

-- Churn risk: high LTV, no purchase in 30d, recent browsing activity
SELECT cp.customer_id, cp.ltv_score, cp.last_purchase_date
FROM customer_profiles cp
WHERE cp.last_purchase_date < CURRENT_DATE - 30
  AND cp.ltv_score > 70
  AND EXISTS (
    SELECT 1 FROM clickstream_silver cs
    WHERE cs.customer_id = cp.customer_id
      AND cs.timestamp > NOW() - INTERVAL 7 DAYS
  );

-- Recent interest window (48h)
SELECT customer_id, category, SUM(intent_score) AS recent_score
FROM customer_current_interests
WHERE updated_at > NOW() - INTERVAL 48 HOURS
GROUP BY customer_id, category;
```

**Pre-loaded sample queries:**
1. "Top 10% shoppers by LTV who haven't bought in 30 days but browsed denim recently"
2. "Category interest trend by loyalty tier this week"
3. "Customers at churn risk with high LTV"

---

### D3.3 — Databricks App (Customer Portal)

**Framework:** Streamlit (Python)
**Auth:** OAuth OBO — app uses logged-in user's identity for all Databricks resource calls
**App name:** `retail-customer-portal`

**Pages:**

| Page | Description |
|------|-------------|
| 1. Customer Lookup | Search by `customer_id` or name; returns matching profile row |
| 2. Profile & Loyalty | Tier, points, LTV score, preferred categories |
| 3. Recommendations | Calls `style-assistant-endpoint`; displays top 5 products with reasoning |
| 4. Active Offers | Reads `personalized_offers` from Lakebase for current customer |
| 5. Intent Signals | Browsing history; polls `customer_current_interests` every 10s → shows "Currently interested in: Denim" |

**Data connections:**

| Data source | Access method |
|-------------|---------------|
| Customer profile | SQL Warehouse → UC `customer_profiles` |
| Recommendations | REST call → `style-assistant-endpoint` |
| Active offers | Lakebase `personalized_offers` via psycopg2 |
| Intent signals | SQL Warehouse → `customer_current_interests` (polled every 10s) |

---

### Phase 3 Acceptance Criteria

- [ ] App loads customer profile in <1s from Lakebase / SQL Warehouse
- [ ] Recommendations panel loads in <3s from Model Serving endpoint
- [ ] Genie returns correct customer cohort for the "denim + 30-day" query
- [ ] Intent signal refreshes every 10s without page reload
- [ ] OAuth OBO auth enforces UC column masking (marketing user sees masked PII)
- [ ] `databricks bundle deploy` completes without errors for `dev` target
- [ ] All DAB jobs visible in workspace Jobs UI

---

## Project Structure

```
tko_fy27_retail_personalization/
├── databricks.yml              # DAB root config
├── resources/                  # DAB resource definitions
│   ├── jobs/
│   │   ├── phase1_setup.yml    # Runs notebooks 01-05 (sequential)
│   │   └── phase2_ai.yml       # Runs VS setup + agent deploy
│   └── pipeline.yml            # SDP pipeline resource
├── docs/                       # BRD, PRD, PLAN, TASKS
├── notebooks/                  # Phase 1 notebooks
│   ├── 01_uc_setup.py
│   ├── 02_generate_data.py
│   ├── 03_load_data.py
│   ├── 04_governance.py
│   └── 05_validate_phase1.py
├── pipelines/
│   └── retail_intent_pipeline.py
├── agents/
│   ├── vector_search_setup.py
│   └── style_assistant/
│       ├── agent.py
│       └── requirements.txt
├── lakebase/
│   └── schema.sql
├── genie/
│   └── certified_sql.sql
└── app/
    ├── app.py
    ├── pages/
    │   ├── 01_lookup.py
    │   ├── 02_profile.py
    │   ├── 03_recommendations.py
    │   ├── 04_offers.py
    │   └── 05_intent.py
    ├── requirements.txt
    └── app.yaml
```

---

## Deployment — Databricks Asset Bundles

**Bundle name:** `tko27-retail-personalization`
**Root file:** `databricks.yml`

### `databricks.yml` structure

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

### DAB Resources

| File | Resource | Phase |
|------|----------|-------|
| `resources/jobs/phase1_setup.yml` | Job: runs notebooks 01→05 on Serverless, sequential tasks | 1 |
| `resources/jobs/phase2_ai.yml` | Job: runs `vector_search_setup.py` + `style_assistant/agent.py` deploy | 2 |
| `resources/pipeline.yml` | Lakeflow SDP: `tko27-retail-intent-pipeline`, Serverless, continuous | 2 |

The Databricks App (`retail-customer-portal`) is deployed via `databricks apps deploy` using `app/app.yaml`; it is **not** a DAB resource.

### Deployment Commands

```bash
# Initial deploy (all targets)
databricks bundle deploy --profile fevm

# Run Phase 1 setup job
databricks bundle run phase1_setup --profile fevm

# Run Phase 2 AI deploy job
databricks bundle run phase2_ai --profile fevm

# Deploy Databricks App separately
databricks apps deploy retail-customer-portal --source-code-path ./app --profile fevm
```

---

## Non-Goals (Out of Scope)

- Real payment processing or PII
- Production SLA guarantees or DR planning
- A/B testing framework
- Multi-catalog or multi-workspace setup

---

## Milestone Summary

| Phase | Week | Key output | Done when |
|-------|------|-----------|-----------|
| 1 — Foundation | 1 | 4 UC tables + governance | SE can query all tables; masking verified |
| 2 — AI Layer | 2 | Pipeline + VS + Agent | Recommendations return in <3s |
| 3 — Serving | 3 | App + Genie + Lakebase | Marketer cohort query works; app live |
