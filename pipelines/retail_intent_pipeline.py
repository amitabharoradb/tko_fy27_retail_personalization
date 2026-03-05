# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow SDP — Retail Intent Pipeline
# MAGIC Bronze → Silver → Gold intent scoring with exponential decay.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

volume_path = spark.conf.get("volume_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Auto Loader from Volume

# COMMAND ----------

@dp.table(name="clickstream_bronze", cluster_by=["customer_id"])
def clickstream_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{volume_path}/_schema/clickstream")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{volume_path}/clickstream_events/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Join products, compute intent scores

# COMMAND ----------

@dp.table(name="clickstream_silver", cluster_by=["customer_id", "category"])
def clickstream_silver():
    events = spark.readStream.table("clickstream_bronze")
    products = spark.read.table("amitabh_arora_catalog.tko27_retail.products")

    event_weights = (
        F.when(F.col("event_type") == "add_to_cart", 5)
        .when(F.col("event_type") == "search", 3)
        .when(F.col("event_type") == "product_view", 2)
        .otherwise(1)
    )

    age_hours = (
        F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("timestamp"))
    ) / 3600

    decay = F.exp(-F.log(F.lit(2.0)) * age_hours / 48.0)

    return (
        events
        .join(
            products.select("product_id", "category", "name").alias("p"),
            events.product_id == F.col("p.product_id"),
            "left",
        )
        .withColumn("event_weight", event_weights)
        .withColumn("intent_score", event_weights * decay)
        .select(
            events.event_id,
            events.session_id,
            events.customer_id,
            events.event_type,
            events.search_term,
            F.coalesce(F.col("p.category"), events.category).alias("category"),
            F.col("p.name").alias("product_name"),
            events.product_id,
            events.timestamp,
            events.device_type,
            events.referrer,
            "event_weight",
            "intent_score",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Top 3 interests per customer (rolling 7 days)

# COMMAND ----------

@dp.materialized_view(name="customer_current_interests")
def customer_current_interests():
    w = Window.partitionBy("customer_id").orderBy(F.desc("intent_score"))
    return (
        spark.read.table("clickstream_silver")
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("timestamp") > F.date_sub(F.current_date(), 7))
        .groupBy("customer_id", "category")
        .agg(F.sum("intent_score").alias("intent_score"))
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 3)
        .withColumn("updated_at", F.current_timestamp())
    )
