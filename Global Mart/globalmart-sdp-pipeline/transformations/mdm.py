# =============================================================================
# GlobalMart — MDM (Master Data Management) Layer Pipeline
# Target: globalmart.mdm
#
# SURVIVORSHIP STRATEGIES (from globalmart.config.mdm_rules):
#   most_complete   — pick the non-null value across duplicate rows
#   most_recent     — pick from the row with the latest _load_timestamp
#   source_priority — pick from the highest-priority source file
#
# For customers, each customer_id has exactly 2 Silver rows (one from
# customers_1-5, one from customers_6). The MDM layer merges these
# into a single golden record using per-column survivorship rules.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, row_number, first, coalesce, lit, when, regexp_extract
)
from pyspark.sql.window import Window

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA mdm")

# Source file priority: customers_1 = highest, customers_6 = lowest
_SOURCE_PRIORITY = {
    "customers_1": 1, "customers_2": 2, "customers_3": 3,
    "customers_4": 4, "customers_5": 5, "customers_6": 6,
}


@dp.table(name="customers", comment="Master customer record — per-column survivorship from config.mdm_rules.")
def mdm_customers():
    """
    Applies per-column survivorship rules from config.mdm_rules:
      - customer_email: most_complete (pick non-null)
      - customer_name:  source_priority (prefer customers_1 > ... > customers_6)
      - segment:        source_priority
      - city:           most_recent (_load_timestamp DESC)
      - state:          most_recent
      - postal_code:    most_recent
      - region:         source_priority
      - country:        source_priority

    Implementation: build 3 ranked views (one per strategy), then join on
    customer_id and pick the winning column from each. No groupBy/agg —
    avoids the first(when()) null issue.
    """
    silver = spark.read.table("globalmart.silver.customers")

    # Extract source file number for priority ordering
    enriched = (
        silver
        .withColumn(
            "_source_name",
            regexp_extract("_source_file", r"(customers_\d)", 1)
        )
        .withColumn(
            "_source_rank",
            when(col("_source_name") == "customers_1", lit(1))
            .when(col("_source_name") == "customers_2", lit(2))
            .when(col("_source_name") == "customers_3", lit(3))
            .when(col("_source_name") == "customers_4", lit(4))
            .when(col("_source_name") == "customers_5", lit(5))
            .when(col("_source_name") == "customers_6", lit(6))
            .otherwise(lit(99))
        )
    )

    # --- source_priority winner: name, segment, region, country ---
    w_priority = Window.partitionBy("customer_id").orderBy(col("_source_rank").asc())
    priority_winner = (
        enriched
        .withColumn("_rn", row_number().over(w_priority))
        .filter(col("_rn") == 1)
        .select(
            col("customer_id"),
            col("customer_name").alias("customer_name"),
            col("segment").alias("segment"),
            col("region").alias("region"),
            col("country").alias("country"),
        )
    )

    # --- most_recent winner: city, state, postal_code, _load_timestamp ---
    w_recent = Window.partitionBy("customer_id").orderBy(
        col("_load_timestamp").desc(), col("_source_rank").asc()
    )
    recent_winner = (
        enriched
        .withColumn("_rn", row_number().over(w_recent))
        .filter(col("_rn") == 1)
        .select(
            col("customer_id"),
            col("city").alias("city"),
            col("state").alias("state"),
            col("postal_code").alias("postal_code"),
            col("_load_timestamp"),
        )
    )

    # --- most_complete winner: customer_email (non-null first, tiebreak by priority) ---
    w_email = Window.partitionBy("customer_id").orderBy(
        when(col("customer_email").isNotNull(), lit(0)).otherwise(lit(1)).asc(),
        col("_source_rank").asc()
    )
    email_winner = (
        enriched
        .withColumn("_rn", row_number().over(w_email))
        .filter(col("_rn") == 1)
        .select(
            col("customer_id"),
            col("customer_email").alias("customer_email"),
        )
    )

    # --- Join the three winners into one golden record ---
    return (
        priority_winner
        .join(recent_winner, on="customer_id", how="inner")
        .join(email_winner, on="customer_id", how="inner")
        .select(
            "customer_id", "customer_email", "customer_name",
            "segment", "country", "city", "state", "postal_code", "region",
            "_load_timestamp"
        )
    )


@dp.table(name="products", comment="Master product catalog — deduped by product_id.")
def mdm_products():
    silver = spark.read.table("globalmart.silver.products")
    w = Window.partitionBy("product_id").orderBy(col("_load_timestamp").desc())
    return (
        silver
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .select(
            "product_id", "product_name", "brand", "categories", "colors",
            "manufacturer", "dimension", "sizes", "upc", "weight",
            "product_photos_qty", "date_added", "date_updated", "_load_timestamp"
        )
    )


@dp.table(name="vendors", comment="Master vendor registry — deduped by vendor_id.")
def mdm_vendors():
    silver = spark.read.table("globalmart.silver.vendors")
    w = Window.partitionBy("vendor_id").orderBy(col("_load_timestamp").desc())
    return (
        silver
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .select("vendor_id", "vendor_name", "_load_timestamp")
    )