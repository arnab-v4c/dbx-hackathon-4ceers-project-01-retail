# Databricks notebook source
# =============================================================================
# GlobalMart — MDM (Master Data Management) Layer Pipeline
# Target: globalmart.mdm
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, row_number, when
from pyspark.sql.window import Window

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA mdm")

@dp.table(name="customers", comment="Master customer record")
def mdm_customers():
    silver = spark.read.table("globalmart.silver.customers")

    ranked = silver.withColumn(
        "_source_rank",
        when(col("_source_file").contains("Region_1"), 1)
        .when(col("_source_file").contains("Region_2"), 2)
        .when(col("_source_file").contains("Region_3"), 3)
        .when(col("_source_file").contains("Region_4"), 4)
        .when(col("_source_file").contains("Region_5"), 5)
        .when(col("_source_file").contains("Region_6"), 6)
        .otherwise(7)
    )

    w_priority = Window.partitionBy("customer_id").orderBy("_source_rank")
    base = ranked.withColumn("_rn", row_number().over(w_priority)).filter(col("_rn") == 1).drop("_rn", "_source_rank")

    w_email = Window.partitionBy("customer_id").orderBy(col("_load_timestamp").desc())
    emails = silver.filter(col("customer_email").isNotNull()).withColumn("_rn", row_number().over(w_email)).filter(col("_rn") == 1).select(
        col("customer_id").alias("_eid"), col("customer_email").alias("_best_email")
    )

    w_recent = Window.partitionBy("customer_id").orderBy(col("_load_timestamp").desc())
    addresses = silver.filter(col("city").isNotNull()).withColumn("_rn", row_number().over(w_recent)).filter(col("_rn") == 1).select(
        col("customer_id").alias("_aid"), col("city").alias("_best_city"), col("state").alias("_best_state"),
        col("postal_code").alias("_best_postal"), col("region").alias("_best_region"), col("_load_timestamp").alias("_best_load_ts")
    )

    return (
        base
        .join(emails, base.customer_id == emails._eid, "left")
        .join(addresses, base.customer_id == addresses._aid, "left")
        .select(
            base.customer_id,
            coalesce(col("_best_email"), base.customer_email).alias("customer_email"),
            base.customer_name, base.segment, base.country,
            coalesce(col("_best_city"), base.city).alias("city"),
            coalesce(col("_best_state"), base.state).alias("state"),
            coalesce(col("_best_postal"), base.postal_code).alias("postal_code"),
            coalesce(col("_best_region"), base.region).alias("region"),
            coalesce(col("_best_load_ts"), base._load_timestamp).alias("_load_timestamp") # Passed through for SCD2 sequencing
        )
    )

@dp.table(name="products")
def mdm_products():
    silver = spark.read.table("globalmart.silver.products")
    w = Window.partitionBy("product_id").orderBy(col("_load_timestamp").desc())
    return silver.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).select(
        "product_id", "product_name", "brand", "categories", "colors", "manufacturer", "dimension", "sizes",
        "upc", "weight", "product_photos_qty", "date_added", "date_updated", "_load_timestamp"
    )

@dp.table(name="vendors")
def mdm_vendors():
    silver = spark.read.table("globalmart.silver.vendors")
    w = Window.partitionBy("vendor_id").orderBy(col("_load_timestamp").desc())
    return silver.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).select("vendor_id", "vendor_name", "_load_timestamp")