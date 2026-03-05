# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Generate Mock Data
# MAGIC Uses Faker to generate 4 synthetic retail datasets → CSV to UC Volume.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta

from faker import Faker
from pyspark.sql import Row

fake = Faker()
Faker.seed(42)
random.seed(42)

VOLUME_PATH = "/Volumes/amitabh_arora_catalog/tko27_retail/raw_data"

CATEGORIES = ["Denim", "Tops", "Footwear", "Accessories", "Outerwear"]
SUBCATEGORIES = {
    "Denim": ["Skinny Jeans", "Straight Leg", "Bootcut", "Wide Leg", "Shorts"],
    "Tops": ["T-Shirt", "Blouse", "Sweater", "Tank Top", "Hoodie"],
    "Footwear": ["Ankle Boots", "Sneakers", "Loafers", "Sandals", "Heels"],
    "Accessories": ["Belt", "Watch", "Sunglasses", "Handbag", "Scarf"],
    "Outerwear": ["Jacket", "Coat", "Blazer", "Vest", "Parka"],
}
BRANDS = ["UrbanEdge", "NorthStyle", "CoastalThread", "PeakWear", "StreetLux",
           "BlueHorizon", "MetroFit", "WildTrail", "SilkRoute", "IronThread"]
TAGS_POOL = ["casual", "summer", "trending", "formal", "winter", "sporty",
             "vintage", "sustainable", "limited-edition", "basics"]
AGE_BUCKETS = ["18-24", "25-34", "35-44", "45+"]
GENDERS = ["M", "F", "Non-binary", "Prefer not to say"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "search", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
REFERRERS = ["google", "instagram", "direct", "facebook", "tiktok", "email"]
CHANNELS = ["web", "mobile", "in-store"]
SEARCH_TERMS = ["summer dresses", "black boots", "denim jacket", "casual wear",
                "running shoes", "leather belt", "winter coat", "silk scarf",
                "vintage tee", "slim fit jeans"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products — 5,000 rows

# COMMAND ----------

products = []
for i in range(1, 5001):
    cat = random.choice(CATEGORIES)
    subcat = random.choice(SUBCATEGORIES[cat])
    tags = random.sample(TAGS_POOL, k=random.randint(1, 4))
    products.append(Row(
        product_id=f"PROD-{i:05d}",
        name=f"{fake.word().title()} {subcat}",
        category=cat,
        subcategory=subcat,
        price=round(random.uniform(9.99, 299.99), 2),
        brand=random.choice(BRANDS),
        description=fake.paragraph(nb_sentences=3),
        tags="|".join(tags),
        created_at=fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
    ))

df_products = spark.createDataFrame(products)
df_products.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/products")
print(f"Products: {df_products.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Profiles — 10,000 rows

# COMMAND ----------

customers = []
for i in range(1, 10001):
    pref_cats = random.sample(CATEGORIES, k=random.randint(1, 3))
    customers.append(Row(
        customer_id=f"CUST-{i:05d}",
        first_name=fake.first_name(),
        last_name=fake.last_name(),
        email=fake.email(),
        credit_card_last4=fake.credit_card_number()[-4:],
        age_bucket=random.choice(AGE_BUCKETS),
        gender=random.choice(GENDERS),
        loyalty_tier=random.choices(LOYALTY_TIERS, weights=[40, 30, 20, 10])[0],
        loyalty_points=random.randint(0, 50000),
        ltv_score=round(random.uniform(0, 100), 2),
        preferred_categories="|".join(pref_cats),
        last_purchase_date=fake.date_between(start_date="-90d", end_date="today").isoformat(),
        created_at=fake.date_time_between(start_date="-2y", end_date="now").isoformat(),
    ))

df_customers = spark.createDataFrame(customers)
df_customers.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/customer_profiles")
print(f"Customer profiles: {df_customers.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Purchase History — 5,000 rows

# COMMAND ----------

purchases = []
for i in range(1, 5001):
    cust_id = f"CUST-{random.randint(1, 10000):05d}"
    prod_id = f"PROD-{random.randint(1, 5000):05d}"
    qty = random.randint(1, 5)
    unit_price = round(random.uniform(9.99, 299.99), 2)
    purchases.append(Row(
        transaction_id=f"TXN-{uuid.uuid4().hex[:12].upper()}",
        customer_id=cust_id,
        product_id=prod_id,
        quantity=qty,
        unit_price=unit_price,
        total_amount=round(qty * unit_price, 2),
        purchase_date=fake.date_time_between(start_date="-90d", end_date="now").isoformat(),
        channel=random.choice(CHANNELS),
    ))

df_purchases = spark.createDataFrame(purchases)
df_purchases.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/purchase_history")
print(f"Purchase history: {df_purchases.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clickstream Events — 10,000 rows

# COMMAND ----------

events = []
for i in range(1, 10001):
    evt_type = random.choices(EVENT_TYPES, weights=[35, 30, 15, 10, 10])[0]
    # ~20% anonymous browsing
    cust_id = f"CUST-{random.randint(1, 10000):05d}" if random.random() > 0.2 else ""
    prod_id = f"PROD-{random.randint(1, 5000):05d}" if evt_type != "page_view" else ""
    search_term = random.choice(SEARCH_TERMS) if evt_type == "search" else ""
    events.append(Row(
        event_id=f"EVT-{uuid.uuid4().hex[:12].upper()}",
        session_id=f"SESS-{uuid.uuid4().hex[:8].upper()}",
        customer_id=cust_id,
        product_id=prod_id,
        event_type=evt_type,
        search_term=search_term,
        category=random.choice(CATEGORIES),
        timestamp=fake.date_time_between(start_date="-7d", end_date="now").isoformat(),
        device_type=random.choice(DEVICES),
        referrer=random.choice(REFERRERS),
    ))

df_events = spark.createDataFrame(events)
df_events.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/clickstream_events")
print(f"Clickstream events: {df_events.count()} rows")

# COMMAND ----------

# Verify all files written
dbutils.fs.ls(f"dbfs:{VOLUME_PATH}/products")
dbutils.fs.ls(f"dbfs:{VOLUME_PATH}/customer_profiles")
dbutils.fs.ls(f"dbfs:{VOLUME_PATH}/purchase_history")
dbutils.fs.ls(f"dbfs:{VOLUME_PATH}/clickstream_events")
print("All 4 datasets written to Volume.")
