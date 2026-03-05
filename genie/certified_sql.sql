-- Certified SQL for Genie Space: Retail Marketing Intelligence
-- These definitions help Genie understand retail metrics and answer marketing queries.

-- =============================================================================
-- LTV Calculation: lifetime value from purchase history
-- Use when asked about customer lifetime value, total spend, or top spenders
-- =============================================================================
SELECT
    customer_id,
    SUM(total_amount) AS lifetime_value,
    COUNT(*) AS total_orders
FROM amitabh_arora_catalog.tko27_retail.purchase_history
GROUP BY customer_id;

-- =============================================================================
-- Churn Risk: high-LTV customers with no purchase in 30 days but recent browsing
-- Use when asked about churn risk, at-risk customers, or retention targets
-- =============================================================================
SELECT
    cp.customer_id,
    cp.first_name,
    cp.last_name,
    cp.loyalty_tier,
    cp.ltv_score,
    cp.last_purchase_date
FROM amitabh_arora_catalog.tko27_retail.customer_profiles cp
WHERE cp.last_purchase_date < CURRENT_DATE - INTERVAL 30 DAYS
  AND cp.ltv_score > 70
  AND EXISTS (
    SELECT 1
    FROM amitabh_arora_catalog.tko27_retail.clickstream_events ce
    WHERE ce.customer_id = cp.customer_id
      AND ce.timestamp > NOW() - INTERVAL 7 DAYS
  );

-- =============================================================================
-- Recent Category Interest: browsing activity by category in last 7 days
-- Use when asked about trending categories, what customers are browsing,
-- or recent interest patterns
-- =============================================================================
SELECT
    ce.customer_id,
    ce.category,
    COUNT(*) AS browse_count,
    COUNT(DISTINCT ce.session_id) AS sessions
FROM amitabh_arora_catalog.tko27_retail.clickstream_events ce
WHERE ce.timestamp > NOW() - INTERVAL 7 DAYS
GROUP BY ce.customer_id, ce.category;

-- =============================================================================
-- BRD KEY QUERY: Top 10% shoppers who haven't bought in 30 days but browsed denim
-- Use when asked about top shoppers who lapsed but browsed denim, re-engagement
-- targets, high-value denim prospects, or the exact BRD ask:
-- "Show me our top 10% of shoppers who haven't bought in 30 days but have
--  browsed denim recently"
-- =============================================================================
WITH ltv_ranked AS (
  SELECT customer_id, ltv_score,
         PERCENT_RANK() OVER (ORDER BY ltv_score) AS ltv_pct
  FROM amitabh_arora_catalog.tko27_retail.customer_profiles
),
denim_browsers AS (
  SELECT DISTINCT customer_id
  FROM amitabh_arora_catalog.tko27_retail.clickstream_events
  WHERE category = 'Denim'
    AND timestamp > NOW() - INTERVAL 30 DAYS
)
SELECT cp.customer_id, cp.first_name, cp.last_name,
       cp.loyalty_tier, cp.ltv_score, cp.last_purchase_date
FROM amitabh_arora_catalog.tko27_retail.customer_profiles cp
JOIN ltv_ranked lr ON cp.customer_id = lr.customer_id
JOIN denim_browsers db ON cp.customer_id = db.customer_id
WHERE lr.ltv_pct >= 0.90
  AND cp.last_purchase_date < CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY cp.ltv_score DESC;

-- =============================================================================
-- Category breakdown by loyalty tier
-- Use when asked about category preferences by tier, loyalty tier analysis,
-- or "what do Gold/Platinum customers browse"
-- =============================================================================
SELECT
    cp.loyalty_tier,
    ce.category,
    COUNT(*) AS event_count,
    COUNT(DISTINCT ce.customer_id) AS unique_customers
FROM amitabh_arora_catalog.tko27_retail.clickstream_events ce
JOIN amitabh_arora_catalog.tko27_retail.customer_profiles cp
  ON ce.customer_id = cp.customer_id
WHERE ce.timestamp > NOW() - INTERVAL 7 DAYS
GROUP BY cp.loyalty_tier, ce.category
ORDER BY cp.loyalty_tier, event_count DESC;
