# Databricks notebook source
# =============================================================================
# GlobalMart — MDM (Master Data Management) Layer Pipeline
# Pipeline Type: Spark Declarative Pipelines (SDP)
# Target: globalmart.mdm
#
# Produces golden records — one authoritative row per entity.
# Customers are the primary MDM target (6 regional sources with overlaps).
# Products and vendors are single-source passthroughs with dedup safeguards.
#
# Survivorship strategy (from globalmart.ingestion_control.mdm_rules):
#   customer_email:  most_complete (pick first non-null across all sources)
#   customer_name:   source_priority (Region 1 > 2 > 3 > 4 > 5 > 6)
#   segment:         source_priority
#   city/state/zip:  most_recent (latest _load_timestamp wins)
#   region/country:  source_priority
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, coalesce, row_number, when, monotonically_increasing_id
)
from pyspark.sql.window import Window

spark.sql("USE CATALOG globalmart_new")
spark.sql("USE SCHEMA mdm")

# =============================================================================
# MDM CUSTOMERS — Golden Record
# =============================================================================

@dp.table(name="customers", comment="Master customer record — deduplicated across 6 regional sources. Survivorship rules applied per field.")
def mdm_customers():
    silver = spark.read.table("globalmart.silver.customers")

    # Assign source priority rank based on regional file path
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

    # BASE RECORD: highest priority source per customer_id
    w_priority = Window.partitionBy("customer_id").orderBy("_source_rank")
    base = (
        ranked
        .withColumn("_rn", row_number().over(w_priority))
        .filter(col("_rn") == 1)
        .drop("_rn", "_source_rank")
    )

    # MOST COMPLETE EMAIL: first non-null email, preferring most recent
    w_email = Window.partitionBy("customer_id").orderBy(col("_load_timestamp").desc())
    emails = (
        silver
        .filter(col("customer_email").isNotNull())
        .withColumn("_rn", row_number().over(w_email))
        .filter(col("_rn") == 1)
        .select(
            col("customer_id").alias("_eid"),
            col("customer_email").alias("_best_email")
        )
    )

    # MOST RECENT ADDRESS: latest loaded address per customer_id
    w_recent = Window.partitionBy("customer_id").orderBy(col("_load_timestamp").desc())
    addresses = (
        silver
        .filter(col("city").isNotNull())
        .withColumn("_rn", row_number().over(w_recent))
        .filter(col("_rn") == 1)
        .select(
            col("customer_id").alias("_aid"),
            col("city").alias("_best_city"),
            col("state").alias("_best_state"),
            col("postal_code").alias("_best_postal"),
            col("region").alias("_best_region")
        )
    )

    # ASSEMBLE GOLDEN RECORD
    return (
        base
        .join(emails, base.customer_id == emails._eid, "left")
        .join(addresses, base.customer_id == addresses._aid, "left")
        .select(
            base.customer_id,
            coalesce(col("_best_email"), base.customer_email).alias("customer_email"),
            base.customer_name,
            base.segment,
            base.country,
            coalesce(col("_best_city"), base.city).alias("city"),
            coalesce(col("_best_state"), base.state).alias("state"),
            coalesce(col("_best_postal"), base.postal_code).alias("postal_code"),
            coalesce(col("_best_region"), base.region).alias("region")
        )
    )

# =============================================================================
# MDM PRODUCTS — Golden Record
# =============================================================================

@dp.table(name="products", comment="Master product record — single source passthrough with dedup safeguard.")
def mdm_products():
    silver = spark.read.table("globalmart.silver.products")
    w = Window.partitionBy("product_id").orderBy(col("_load_timestamp").desc())
    return (
        silver
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .select(
            "product_id", "product_name", "brand", "categories",
            "colors", "manufacturer", "dimension", "sizes",
            "upc", "weight", "product_photos_qty",
            "date_added", "date_updated"
        )
    )

# =============================================================================
# MDM VENDORS — Golden Record
# =============================================================================

@dp.table(name="vendors", comment="Master vendor record — single source passthrough with dedup safeguard.")
def mdm_vendors():
    silver = spark.read.table("globalmart.silver.vendors")
    w = Window.partitionBy("vendor_id").orderBy(col("_load_timestamp").desc())
    return (
        silver
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .select("vendor_id", "vendor_name")
    )