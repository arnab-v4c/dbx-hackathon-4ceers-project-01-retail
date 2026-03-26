# Databricks notebook source
# =============================================================================
# GlobalMart — Silver Layer Pipeline
# Pipeline Type: Spark Declarative Pipelines (SDP)
# Target Schema: globalmart.silver
#
# Metadata-driven transformation pipeline. All column mappings, type casts,
# value cleanups, and DQ rules are read from globalmart.ingestion_control at
# runtime. No source column names or DQ expressions are hardcoded.
#
# Expectation Strategy:
#   @dp.expect_or_drop  → CRITICAL rules (null PKs/FKs) — removed from main table
#   @dp.expect          → WARNING rules (missing optional fields) — stays in main table
#   Both CRITICAL and WARNING failures are captured in quarantine tables.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, coalesce, expr, lit, current_timestamp, when,
    monotonically_increasing_id
)
from pyspark.sql import DataFrame
import uuid

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA silver")

# =============================================================================
# PRE-LOAD CONFIG at module level (before any @dp.table)
# =============================================================================

RUN_ID = str(uuid.uuid4())

# Notice: Removed effective_to filtering, schema updated to globalmart.ingestion_control
_mappings_df = spark.read.table("globalmart.ingestion_control.column_mapping")
_rules_df = spark.read.table("globalmart.ingestion_control.dq_rules")

CONFIG_MAPPINGS = {}
CONFIG_RULES = {}
for entity in ["customers", "orders", "transactions", "returns", "products", "vendors"]:
    CONFIG_MAPPINGS[entity] = _mappings_df.filter(col("entity") == entity).select("source_column", "canonical_column", "transformation").collect()
    CONFIG_RULES[entity] = _rules_df.filter(col("entity") == entity).collect()

# =============================================================================
# HARMONIZATION ENGINE 
# =============================================================================

def harmonize(df: DataFrame, entity: str) -> DataFrame:
    mappings = CONFIG_MAPPINGS[entity]

    canonical_groups = {}
    for m in mappings:
        canon = m["canonical_column"]
        src = m["source_column"]
        xform = m["transformation"]
        if canon not in canonical_groups:
            canonical_groups[canon] = []
        canonical_groups[canon].append((src, xform))

    select_exprs = []
    for canon, sources in canonical_groups.items():
        coalesce_parts = []
        for src, xform in sources:
            if src in [c.lower() for c in df.columns]:
                if xform:
                    sql_expr = xform.replace("{col}", src)
                    coalesce_parts.append(expr(sql_expr))
                else:
                    coalesce_parts.append(col(src))

        if len(coalesce_parts) == 1:
            select_exprs.append(coalesce_parts[0].alias(canon))
        elif len(coalesce_parts) > 1:
            select_exprs.append(coalesce(*coalesce_parts).alias(canon))
        else:
            select_exprs.append(lit(None).alias(canon))

    for meta_col in ["source_file", "load_timestamp", "_source_file", "_load_timestamp"]:
        if meta_col in df.columns:
            target_name = meta_col if meta_col.startswith("_") else f"_{meta_col}"
            select_exprs.append(col(meta_col).alias(target_name))

    return df.select(*select_exprs)

# =============================================================================
# QUARANTINE ENGINE 
# =============================================================================

def quarantine(df: DataFrame, entity: str) -> DataFrame:
    rules = CONFIG_RULES[entity]
    frames = []

    for rule in rules:
        tagged = (
            df
            .filter(expr(f"NOT ({rule['rule_expression']})"))
            .withColumn("_run_id", lit(RUN_ID))
            .withColumn("_run_timestamp", current_timestamp())
            .withColumn("_expectation_name", lit(rule["rule_name"]))
            .withColumn("_severity", lit(rule["severity"]))
            .withColumn("_issue_type", lit(rule["issue_type"]))
            .withColumn("_failure_reason", lit(rule["failure_reason"]))
            .withColumn("_quarantine_timestamp", current_timestamp())
        )
        frames.append(tagged)

    if frames:
        result = frames[0]
        for frame in frames[1:]:
            result = result.unionByName(frame, allowMissingColumns=True)
        return result
    else:
        return (
            df.limit(0)
            .withColumn("_run_id", lit(None).cast("string"))
            .withColumn("_run_timestamp", current_timestamp())
            .withColumn("_expectation_name", lit(None).cast("string"))
            .withColumn("_severity", lit(None).cast("string"))
            .withColumn("_issue_type", lit(None).cast("string"))
            .withColumn("_failure_reason", lit(None).cast("string"))
            .withColumn("_quarantine_timestamp", current_timestamp())
        )

# =============================================================================
# SILVER CUSTOMERS
# =============================================================================

@dp.table(name="customers")
@dp.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect("valid_customer_email", "customer_email IS NOT NULL")
@dp.expect("valid_customer_name", "customer_name IS NOT NULL")
@dp.expect("valid_segment", "segment IN ('Consumer', 'Corporate', 'Home Office')")
def silver_customers():
    return harmonize(spark.readStream.table("globalmart.bronze.customers"), "customers")

@dp.table(name="customers_quarantine")
def silver_customers_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.customers"), "customers"), "customers")

# =============================================================================
# SILVER ORDERS
# =============================================================================

@dp.table(name="orders")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect("valid_purchase_date", "order_purchase_timestamp IS NOT NULL")
@dp.expect("valid_ship_mode", "ship_mode IS NOT NULL")
@dp.expect("valid_delivery_customer", "order_delivered_customer_timestamp IS NOT NULL")
@dp.expect("valid_delivery_carrier", "order_delivered_carrier_timestamp IS NOT NULL")
def silver_orders():
    return harmonize(spark.readStream.table("globalmart.bronze.orders"), "orders")

@dp.table(name="orders_quarantine")
def silver_orders_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.orders"), "orders"), "orders")

# =============================================================================
# SILVER TRANSACTIONS
# =============================================================================

@dp.table(name="transactions")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dp.expect("valid_sales", "sales IS NOT NULL")
@dp.expect("valid_profit", "profit IS NOT NULL")
@dp.expect("valid_payment_type", "payment_type IS NOT NULL")
def silver_transactions():
    return harmonize(spark.readStream.table("globalmart.bronze.transactions"), "transactions")

@dp.table(name="transactions_quarantine")
def silver_transactions_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.transactions"), "transactions"), "transactions")

# =============================================================================
# SILVER RETURNS
# =============================================================================

@dp.table(name="returns")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect("valid_return_status", "return_status IN ('Pending', 'Approved', 'Rejected')")
@dp.expect("valid_return_reason", "return_reason IS NOT NULL")
@dp.expect("valid_refund_amount", "refund_amount IS NOT NULL")
def silver_returns():
    return harmonize(spark.readStream.table("globalmart.bronze.returns"), "returns")

@dp.table(name="returns_quarantine")
def silver_returns_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.returns"), "returns"), "returns")

# =============================================================================
# SILVER PRODUCTS
# =============================================================================

@dp.table(name="products")
@dp.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dp.expect("valid_product_name", "product_name IS NOT NULL")
@dp.expect("valid_brand", "brand IS NOT NULL")
def silver_products():
    return harmonize(spark.readStream.table("globalmart.bronze.products"), "products")

@dp.table(name="products_quarantine")
def silver_products_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.products"), "products"), "products")

# =============================================================================
# SILVER VENDORS
# =============================================================================

@dp.table(name="vendors")
@dp.expect_or_drop("vendor_id_not_null", "vendor_id IS NOT NULL")
@dp.expect("valid_vendor_name", "vendor_name IS NOT NULL")
def silver_vendors():
    return harmonize(spark.readStream.table("globalmart.bronze.vendors"), "vendors")

@dp.table(name="vendors_quarantine")
def silver_vendors_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.vendors"), "vendors"), "vendors")