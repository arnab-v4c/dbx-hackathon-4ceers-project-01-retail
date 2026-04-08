# Databricks notebook source
# MAGIC %md
# MAGIC # GlobalMart — Silver Layer: Before & After Validation
# MAGIC
# MAGIC **Purpose:** Prove that every transformation in the Silver SDP pipeline worked correctly.
# MAGIC For each of the 6 entities, we compare raw Bronze values against cleaned Silver values,
# MAGIC then confirm quarantine tables captured the right rejected records.
# MAGIC
# MAGIC **Pre-requisite:** The Silver SDP pipeline must have completed at least one successful run.
# MAGIC
# MAGIC **Tables read:**
# MAGIC - `globalmart.bronze.*` — raw source data (the **before**)
# MAGIC - `globalmart.silver.*` — cleaned output (the **after**)
# MAGIC - `globalmart.silver.*_quarantine` — rejected records
# MAGIC - `globalmart.config.dq_rules` — the quality rules the SDP pipeline enforces

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType
)
from functools import reduce

spark.sql("USE CATALOG globalmart")

# -----------------------------------------------------------------------------
# Load DQ rules from the SAME config table the SDP pipeline reads.
# This means the validation notebook tests exactly the rules that ran —
# no hardcoding, no drift between pipeline and validation.
# -----------------------------------------------------------------------------
df_rules = spark.read.table("globalmart.config.dq_rules")

def get_rules(entity: str) -> list:
    """Return all DQ rules for a given entity as a list of Row objects."""
    return df_rules.filter(col("entity") == entity).collect()


# -----------------------------------------------------------------------------
# DQ summary helper
# Evaluates every rule expression from the config table against a Silver
# DataFrame and returns a pass/fail count table ready for display().
# -----------------------------------------------------------------------------
DQ_SUMMARY_SCHEMA = StructType([
    StructField("rule_id",       StringType(),  True),
    StructField("rule_name",     StringType(),  True),
    StructField("expression",    StringType(),  True),
    StructField("consequence",   StringType(),  True),
    StructField("passed",        IntegerType(), True),
    StructField("failed",        IntegerType(), True),
    StructField("pass_rate_pct", FloatType(),   True),
])

def dq_summary(df, entity: str):
    """
    For every rule in globalmart.config.dq_rules for this entity,
    count how many rows in the Silver table pass and fail.

    Args:
        df     : Silver DataFrame to evaluate
        entity : entity name matching the config table (e.g. 'customers')
    """
    total = df.count()
    rows  = []
    for rule in get_rules(entity):
        passed = df.filter(rule["rule_expression"]).count()
        failed = total - passed
        rows.append((
            rule["rule_id"],
            rule["rule_name"],
            rule["rule_expression"],
            rule["consequence"],
            passed,
            failed,
            round(passed / total * 100, 1),
        ))
    return spark.createDataFrame(rows, schema=DQ_SUMMARY_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 1. Customers
# MAGIC **Transformations applied by the SDP pipeline:**
# MAGIC - 4 regional ID column aliases unified → `customer_id`
# MAGIC - Segment abbreviations expanded: `CONS` → `Consumer`, `CORP` → `Corporate`, `HO` → `Home Office`
# MAGIC - Typo corrected: `Cosumer` → `Consumer`
# MAGIC - Missing emails (Region 5) retained as `NULL` and flagged

# COMMAND ----------

# MAGIC %md ### Before — Raw segment values in Bronze

# COMMAND ----------

df_bronze_customers = spark.read.table("globalmart.bronze.customers")

# Raw segment column — abbreviations (CONS, CORP, HO) and typos (Cosumer) visible here
display(
    df_bronze_customers
    .groupBy("segment")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md ### After — Cleaned segment values in Silver

# COMMAND ----------

df_silver_customers = spark.read.table("globalmart.silver.customers")

# Only the 3 canonical values should remain after cleaning
display(
    df_silver_customers
    .groupBy("segment")
    .count()
    .orderBy("count", ascending=False)
)

# Sample rows — confirms canonical column names and clean values
display(
    df_silver_customers
    .select("customer_id", "customer_name", "segment", "customer_email", "region", "_source_file")
    .limit(10)
)

# COMMAND ----------

df_silver_customers = spark.read.table("globalmart.silver.customers")

# Get value counts for region column
display(
    df_silver_customers
    .groupBy("region")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

display(spark.read.table("globalmart.config.column_mapping"))

# COMMAND ----------

df_bronze_customers = spark.read.table("globalmart.bronze.customers")

# COMMAND ----------

# MAGIC %md ### DQ Rule Results — loaded from `globalmart.config.dq_rules`

# COMMAND ----------

# Rules are read directly from the config table — same source the SDP pipeline used
display(dq_summary(df_silver_customers, "customers"))

# COMMAND ----------

# MAGIC %md ### Quarantine — Rejected records with failure context

# COMMAND ----------

df_q_customers = spark.read.table("globalmart.silver.customers_quarantine")

# Why were records rejected, and how many per rule?
display(
    df_q_customers
    .groupBy("_expectation_name", "_severity", "_issue_type", "_failure_reason")
    .count()
    .orderBy("count", ascending=False)
)

# Sample rejected rows so Finance can see the actual bad values
display(
    df_q_customers
    .select("customer_id", "customer_name", "segment",
            "_expectation_name", "_failure_reason", "_source_file")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 2. Orders
# MAGIC **Transformations applied by the SDP pipeline:**
# MAGIC - Two mixed date formats unified → `TIMESTAMP` via `COALESCE(TRY_TO_TIMESTAMP(...))`
# MAGIC - All 5 date columns cast from plain strings to proper `TIMESTAMP` type

# COMMAND ----------

# MAGIC %md ### Before — Raw date strings in Bronze

# COMMAND ----------

df_bronze_orders = spark.read.table("globalmart.bronze.orders")

# Raw string values — both date formats (MM/dd/yyyy and yyyy-MM-dd) visible here
display(
    df_bronze_orders
    .select("order_id", "order_purchase_date", "order_approved_at")
    .limit(15)
)

# COMMAND ----------

# MAGIC %md ### After — Proper TIMESTAMP type in Silver

# COMMAND ----------

df_silver_orders = spark.read.table("globalmart.silver.orders")

# Confirm date columns are now TIMESTAMP, not STRING
print("Timestamp column types in Silver orders:")
for field in df_silver_orders.schema.fields:
    if "timestamp" in field.name.lower():
        print(f"  {field.name:<45} → {field.dataType}")

# Sample rows with parsed timestamps
display(
    df_silver_orders
    .select("order_id", "order_purchase_timestamp", "order_approved_timestamp",
            "order_delivered_carrier_timestamp", "ship_mode", "order_status")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md ### DQ Rule Results — loaded from `globalmart.config.dq_rules`

# COMMAND ----------

display(dq_summary(df_silver_orders, "orders"))

# COMMAND ----------

# MAGIC %md ### Quarantine — Rejected records with failure context

# COMMAND ----------

df_q_orders = spark.read.table("globalmart.silver.orders_quarantine")

display(
    df_q_orders
    .groupBy("_expectation_name", "_severity", "_issue_type", "_failure_reason")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 3. Transactions
# MAGIC **Transformations applied by the SDP pipeline:**
# MAGIC - `$` prefix stripped from `sales`: `$450.00` → `450.00` via `REGEXP_REPLACE` + `DECIMAL` cast
# MAGIC - `?` placeholder replaced with `NULL` in `payment_type`
# MAGIC - `profit` column absent in some regions → kept as `NULL`, flagged

# COMMAND ----------

# MAGIC %md ### Before — `$` in sales, `?` in payment_type (Bronze)

# COMMAND ----------

df_bronze_tx = spark.read.table("globalmart.bronze.transactions")

# Raw sales values — "$" prefix visible
display(
    df_bronze_tx
    .select("order_id", "product_id", "sales", "payment_type", "discount", "profit")
    .limit(15)
)

# All distinct payment_type values — "?" will appear here
display(
    df_bronze_tx
    .groupBy("payment_type")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# Raw sales values — display only rows where sales has "$" sign
display(
    df_bronze_tx
    .filter(col("sales").cast("string").like("%$%"))
    .select("order_id", "product_id", "sales", "payment_type", "discount", "profit")
    .limit(50)
)

# COMMAND ----------

# MAGIC %md ### After — `$` stripped, `?` replaced (Silver)

# COMMAND ----------

df_silver_tx = spark.read.table("globalmart.silver.transactions")

# Verification: no "$" should remain in the sales column
dirty_count = df_silver_tx.filter("CAST(sales AS STRING) LIKE '%$%'").count()
print(f"Rows still containing '$' in sales: {dirty_count}  (expected: 0)")

# Verification: "?" should no longer appear in payment_type
display(
    df_silver_tx
    .groupBy("payment_type")
    .count()
    .orderBy("count", ascending=False)
)

# Sample clean rows
display(
    df_silver_tx
    .select("order_id", "product_id", "sales", "quantity", "discount", "profit", "payment_type")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md ### DQ Rule Results — loaded from `globalmart.config.dq_rules`

# COMMAND ----------

display(dq_summary(df_silver_tx, "transactions"))

# COMMAND ----------

# MAGIC %md ### Quarantine — Rejected records with failure context

# COMMAND ----------

df_q_tx = spark.read.table("globalmart.silver.transactions_quarantine")

display(
    df_q_tx
    .groupBy("_expectation_name", "_severity", "_issue_type", "_failure_reason")
    .count()
    .orderBy("count", ascending=False)
)

display(
    df_q_tx
    .select("order_id", "product_id", "sales", "payment_type",
            "_expectation_name", "_failure_reason", "_source_file")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 4. Returns
# MAGIC **Transformations applied by the SDP pipeline:**
# MAGIC - Status abbreviations expanded: `PENDG` → `Pending`, `APPRVD` → `Approved`, `RJCTD` → `Rejected`
# MAGIC - `$` prefix stripped from `refund_amount`
# MAGIC - `?` placeholder replaced with `NULL` in `return_reason`
# MAGIC - `return_date` cast from string → `DATE` type

# COMMAND ----------

# MAGIC %md ### Before — Status abbreviations and `$` in Bronze

# COMMAND ----------

df_bronze_returns = spark.read.table("globalmart.bronze.returns")

# Raw return_status values — abbreviations visible here
display(
    df_bronze_returns
    .groupBy("return_status")
    .count()
    .orderBy("count", ascending=False)
)

# Raw refund_amount — "$" prefix visible
display(
    df_bronze_returns
    #.select("order_id", "refund_amount", "return_status", "return_reason")
    #.limit(10)
)

# COMMAND ----------

# MAGIC %md ### After — Status expanded, `$` stripped (Silver)

# COMMAND ----------

df_silver_returns = spark.read.table("globalmart.silver.returns")

# Only 3 canonical values should remain
display(
    df_silver_returns
    .groupBy("return_status")
    .count()
    .orderBy("count", ascending=False)
)

# Verification: no raw abbreviations remain
abbrev_count = df_silver_returns.filter(
    col("return_status").isin(["PENDG", "APPRVD", "RJCTD"])
).count()
print(f"Rows with raw abbreviations still present: {abbrev_count}  (expected: 0)")

# Sample clean rows
display(
    df_silver_returns
    .select("order_id", "refund_amount", "return_date", "return_status", "return_reason")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md ### DQ Rule Results — loaded from `globalmart.config.dq_rules`

# COMMAND ----------

display(dq_summary(df_silver_returns, "returns"))

# COMMAND ----------

# MAGIC %md ### Quarantine — Rejected records with failure context

# COMMAND ----------

df_q_returns = spark.read.table("globalmart.silver.returns_quarantine")

display(
    df_q_returns
    .groupBy("_expectation_name", "_severity", "_issue_type", "_failure_reason")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 5. Products
# MAGIC **Transformations applied by the SDP pipeline:**
# MAGIC - `date_added` / `date_updated` cast from ISO 8601 strings → `TIMESTAMP` via `TRY_TO_TIMESTAMP()`
# MAGIC - Sparse optional fields (colors, sizes, weight …) retained as `NULL` — not dropped

# COMMAND ----------

# MAGIC %md ### Before — Date columns as strings in Bronze

# COMMAND ----------

df_bronze_products = spark.read.table("globalmart.bronze.products")

# Schema — date_added will be StringType before cleaning
print("Bronze products — date column types:")
for field in df_bronze_products.schema.fields:
    if "date" in field.name.lower():
        print(f"  {field.name:<30} → {field.dataType}")

# Raw ISO 8601 string values
display(
    df_bronze_products
    .select("product_id", "product_name", "dateadded", "dateupdated")
    .filter(col("dateadded").isNotNull())
    .limit(10)
)

# COMMAND ----------

df_bronze_products = spark.read.table("globalmart.bronze.products")
display(df_bronze_products)

# COMMAND ----------

# MAGIC %md ### After — Proper TIMESTAMP type in Silver

# COMMAND ----------

df_silver_products = spark.read.table("globalmart.silver.products")

# Confirm date columns are now TIMESTAMP type
print("Silver products — date column types:")
for field in df_silver_products.schema.fields:
    if "date" in field.name.lower():
        print(f"  {field.name:<30} → {field.dataType}")

# Sample clean rows
display(
    df_silver_products
    .select("product_id", "product_name", "brand", "categories", "date_added", "date_updated")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md ### DQ Rule Results — loaded from `globalmart.config.dq_rules`

# COMMAND ----------

display(dq_summary(df_silver_products, "products"))

# COMMAND ----------

# MAGIC %md ### Quarantine — Rejected records with failure context

# COMMAND ----------

df_q_products = spark.read.table("globalmart.silver.products_quarantine")

display(
    df_q_products
    .groupBy("_expectation_name", "_severity", "_issue_type", "_failure_reason")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 6. Vendors
# MAGIC **Transformations applied by the SDP pipeline:**
# MAGIC - Column names mapped → canonical `vendor_id`, `vendor_name`
# MAGIC - Bronze schema is otherwise clean; direct passthrough after column standardization

# COMMAND ----------

# MAGIC %md ### Before — Raw Bronze values

# COMMAND ----------

df_bronze_vendors = spark.read.table("globalmart.bronze.vendors")

print(f"Bronze columns: {df_bronze_vendors.columns}")
display(df_bronze_vendors.limit(10))

# COMMAND ----------

# MAGIC %md ### After — Canonical column names in Silver

# COMMAND ----------

df_silver_vendors = spark.read.table("globalmart.silver.vendors")

display(
    df_silver_vendors
    .select("vendor_id", "vendor_name")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md ### DQ Rule Results — loaded from `globalmart.config.dq_rules`

# COMMAND ----------

display(dq_summary(df_silver_vendors, "vendors"))

# COMMAND ----------

# MAGIC %md ### Quarantine — Rejected records with failure context

# COMMAND ----------

df_q_vendors = spark.read.table("globalmart.silver.vendors_quarantine")

display(
    df_q_vendors
    .groupBy("_expectation_name", "_severity", "_issue_type", "_failure_reason")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 7. Master Summary
# MAGIC One view across all 6 entities. Screenshot this section for the submission.

# COMMAND ----------

# --- 7a. Clean vs quarantined row counts per entity ---

entity_pairs = [
    ("customers",    df_silver_customers, df_q_customers),
    ("orders",       df_silver_orders,    df_q_orders),
    ("transactions", df_silver_tx,        df_q_tx),
    ("returns",      df_silver_returns,   df_q_returns),
    ("products",     df_silver_products,  df_q_products),
    ("vendors",      df_silver_vendors,   df_q_vendors),
]

summary_rows = []
for entity_name, df_silver, df_quarantine in entity_pairs:
    clean       = df_silver.count()
    quarantined = df_quarantine.count()
    total       = clean + quarantined
    rate        = round(quarantined / total * 100, 1) if total > 0 else 0.0
    summary_rows.append((entity_name, total, clean, quarantined, rate))

summary_schema = StructType([
    StructField("entity",              StringType(),  True),
    StructField("total_bronze_rows",   IntegerType(), True),
    StructField("silver_clean_rows",   IntegerType(), True),
    StructField("quarantined_rows",    IntegerType(), True),
    StructField("quarantine_rate_pct", DoubleType(),  True),
])

display(
    spark.createDataFrame(summary_rows, schema=summary_schema)
    .orderBy("entity")
)

# COMMAND ----------

# --- 7b. Quarantine breakdown — all entities in one table ---

def to_quarantine_summary(df, entity_name):
    """Select only the columns needed for the cross-entity summary."""
    return df.select(
        lit(entity_name).alias("entity"),
        "_expectation_name",
        "_severity",
        "_issue_type",
        "_failure_reason",
    )

all_quarantine = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    [
        to_quarantine_summary(df_q_customers, "customers"),
        to_quarantine_summary(df_q_orders,    "orders"),
        to_quarantine_summary(df_q_tx,        "transactions"),
        to_quarantine_summary(df_q_returns,   "returns"),
        to_quarantine_summary(df_q_products,  "products"),
        to_quarantine_summary(df_q_vendors,   "vendors"),
    ]
)

display(
    all_quarantine
    .groupBy("entity", "_expectation_name", "_severity", "_issue_type", "_failure_reason")
    .count()
    .orderBy("entity", "count", ascending=[True, False])
)