-- Lakebase State Store: retail_state
-- Run after provisioning database via SDK or MCP

CREATE TABLE IF NOT EXISTS personalized_offers (
    offer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id VARCHAR(50) NOT NULL,
    offer_code VARCHAR(50) NOT NULL,
    product_id VARCHAR(50),
    relevance_score DOUBLE PRECISION DEFAULT 0.0,
    offer_type VARCHAR(20) NOT NULL CHECK (offer_type IN ('discount', 'bundle', 'loyalty_bonus')),
    discount_pct INTEGER DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 50),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    session_id VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_offers_customer ON personalized_offers (customer_id);
CREATE INDEX IF NOT EXISTS idx_offers_session ON personalized_offers (session_id);

CREATE TABLE IF NOT EXISTS active_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    current_category VARCHAR(50),
    cart_items JSONB DEFAULT '[]'::jsonb,
    intent_snapshot JSONB DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_sessions_customer ON active_sessions (customer_id);
