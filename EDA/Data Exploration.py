# Databricks notebook source
import os

raw_path = "/Volumes/dbx_retail/bronze/raw"
files = dbutils.fs.ls(raw_path)

dataframes = {}
for file_info in files:
    file_path = file_info.path
    file_name = os.path.basename(file_path)
    name_no_ext = os.path.splitext(file_name)[0]
    if file_name.endswith(".csv"):
        df = spark.read.option("header", "true").csv(file_path)
        dataframes[name_no_ext] = df
    elif file_name.endswith(".json"):
        df = spark.read.option("multiLine", "true").json(file_path)
        dataframes[name_no_ext] = df

for name, df in dataframes.items():
    table_name = f"dbx_retail.bronze.{name}"
    df.write.mode("overwrite").saveAsTable(table_name)
    print(f"Done for: {name}")

# COMMAND ----------

# DBTITLE 1,EDA Header
# MAGIC %md
# MAGIC ## Exploratory Data Analysis — `dbx_retail.bronze`
# MAGIC This notebook explores **16 bronze tables** across 6 entity groups: Customers (1-6), Orders (1-3), Products, Returns (1-2), Transactions (1-3), and Vendors.

# COMMAND ----------

# DBTITLE 1,Row Counts and Schema Overview
from pyspark.sql import functions as F
from functools import reduce

# All tables in the bronze layer
table_names = [
    "customers_1", "customers_2", "customers_3", "customers_4", "customers_5", "customers_6",
    "orders_1", "orders_2", "orders_3",
    "products",
    "returns_1", "returns_2",
    "transactions_1", "transactions_2", "transactions_3",
    "vendors"
]

# Collect row counts and column counts
rows = []
for t in table_names:
    fqn = f"dbx_retail.bronze.{t}"
    df = spark.table(fqn)
    cnt = df.count()
    cols = len(df.columns)
    rows.append((t, fqn, cnt, cols, ", ".join(df.columns)))

schema_overview = spark.createDataFrame(rows, ["table", "fqn", "row_count", "col_count", "columns"])
display(schema_overview.orderBy("table"))

# COMMAND ----------

# DBTITLE 1,Schema Comparison Header
# MAGIC %md
# MAGIC ### Schema Comparison Across Related Tables
# MAGIC The tables below highlight **column naming inconsistencies** across related table groups that will need harmonization during silver-layer transformations.

# COMMAND ----------

# DBTITLE 1,Schema Comparison Across Related Tables
import pandas as pd

# Group related tables for schema comparison
groups = {
    "Customers": ["customers_1", "customers_2", "customers_3", "customers_4", "customers_5", "customers_6"],
    "Orders": ["orders_1", "orders_2", "orders_3"],
    "Returns": ["returns_1", "returns_2"],
    "Transactions": ["transactions_1", "transactions_2", "transactions_3"],
}

for group_name, tables in groups.items():
    print(f"\n{'='*60}")
    print(f"  {group_name} — Column Comparison")
    print(f"{'='*60}")
    
    all_cols = {}
    for t in tables:
        df = spark.table(f"dbx_retail.bronze.{t}")
        all_cols[t] = df.columns
    
    # Build comparison matrix
    all_unique_cols = []
    for cols in all_cols.values():
        for c in cols:
            c_lower = c.lower()
            if c_lower not in [x.lower() for x in all_unique_cols]:
                all_unique_cols.append(c)
    
    comparison_rows = []
    for col in all_unique_cols:
        row = {"column": col}
        for t in tables:
            # Check if column exists (case-insensitive match)
            matched = [c for c in all_cols[t] if c.lower() == col.lower()]
            if matched:
                row[t] = matched[0]  # Show actual name
            else:
                row[t] = "MISSING"
        comparison_rows.append(row)
    
    comp_df = pd.DataFrame(comparison_rows)
    display(comp_df)

# COMMAND ----------

# DBTITLE 1,Null and Duplicate Header
# MAGIC %md
# MAGIC ### Null & Duplicate Analysis
# MAGIC Checking for **null values** across all columns and **duplicate primary keys** in each table.

# COMMAND ----------

# DBTITLE 1,Null Value Analysis
# --- Null Analysis ---
null_results = []
for t in table_names:
    fqn = f"dbx_retail.bronze.{t}"
    df = spark.table(fqn)
    total = df.count()
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")).count()
        if null_count > 0:
            null_results.append((t, col_name, null_count, total, round(100.0 * null_count / total, 2)))

if null_results:
    null_df = spark.createDataFrame(null_results, ["table", "column", "null_or_empty_count", "total_rows", "pct_missing"])
    print("Columns with Null or Empty Values:")
    display(null_df.orderBy(F.desc("pct_missing")))
else:
    print("No null or empty values found in any table.")

# COMMAND ----------

# DBTITLE 1,Duplicate Key Analysis
# --- Duplicate Analysis on Key Columns ---
key_cols_map = {
    "customers_1": "customer_id",
    "customers_2": "CustomerID",
    "customers_3": "cust_id",
    "customers_4": "customer_id",
    "customers_5": "customer_id",
    "customers_6": "customer_identifier",
    "orders_1": "order_id",
    "orders_2": "order_id",
    "orders_3": "order_id",
    "products": "product_id",
    "returns_1": "order_id",
    "returns_2": "OrderId",
    "transactions_1": "Order_id",
    "transactions_2": "Order_id",
    "transactions_3": "Order_ID",
    "vendors": "vendor_id",
}

dup_results = []
for t, key_col in key_cols_map.items():
    fqn = f"dbx_retail.bronze.{t}"
    df = spark.table(fqn)
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    dup_count = total - distinct
    dup_results.append((t, key_col, total, distinct, dup_count, round(100.0 * dup_count / total, 2) if total > 0 else 0))

dup_df = spark.createDataFrame(dup_results, ["table", "key_column", "total_rows", "distinct_keys", "duplicate_rows", "pct_duplicates"])
print("Duplicate Key Analysis:")
display(dup_df.orderBy("table"))

# COMMAND ----------

# DBTITLE 1,Data Quality Header
# MAGIC %md
# MAGIC ### Data Quality Issues
# MAGIC Checking for **format inconsistencies** in numeric string columns (e.g., `$` prefixes in Sales/refund amounts) and **inconsistent categorical values**.

# COMMAND ----------

# DBTITLE 1,Data Quality and Format Analysis
# --- Data Quality: Format Inconsistencies in Numeric String Columns ---
print("=" * 60)
print("  Format Inconsistencies in Numeric Columns")
print("=" * 60)

numeric_checks = [
    ("transactions_1", "Sales"),
    ("transactions_1", "profit"),
    ("transactions_2", "Sales"),
    ("transactions_3", "Sales"),
    ("transactions_3", "profit"),
    ("returns_1", "refund_amount"),
    ("returns_2", "amount"),
]

format_results = []
for t, col in numeric_checks:
    fqn = f"dbx_retail.bronze.{t}"
    df = spark.table(fqn)
    total = df.count()
    dollar_prefix = df.filter(F.col(col).startswith("$")).count()
    no_prefix = total - dollar_prefix
    format_results.append((t, col, total, dollar_prefix, no_prefix, f"{round(100.0 * dollar_prefix / total, 1)}% have $ prefix" if dollar_prefix > 0 else "Clean"))

format_df = spark.createDataFrame(format_results, ["table", "column", "total_rows", "with_dollar_sign", "without_dollar_sign", "status"])
display(format_df)

# --- Inconsistent Categorical Values ---
print("\n" + "=" * 60)
print("  Categorical Value Distributions")
print("=" * 60)

categorical_checks = [
    ("returns_1", "return_status"),
    ("returns_2", "status"),
    ("returns_1", "return_reason"),
    ("returns_2", "reason"),
    ("orders_1", "order_status"),
    ("orders_1", "ship_mode"),
    ("customers_1", "segment"),
    ("customers_1", "region"),
    ("transactions_1", "payment_type"),
]

for t, col in categorical_checks:
    fqn = f"dbx_retail.bronze.{t}"
    df = spark.table(fqn)
    print(f"\n--- {t}.{col} ---")
    display(df.groupBy(col).count().orderBy(F.desc("count")))

# COMMAND ----------

# DBTITLE 1,Cross-Table Key Overlap Header
# MAGIC %md
# MAGIC ### Cross-Table Key Overlap & Referential Integrity
# MAGIC Checking how keys relate **across entity groups**: do customer IDs in orders match those in customer tables? Do order IDs in transactions appear in orders?

# COMMAND ----------

# DBTITLE 1,Cross-Table Key Overlap Analysis
# --- Cross-Table Key Overlap ---
print("=" * 60)
print("  Cross-Table Key Overlap Analysis")
print("=" * 60)

# Collect all customer IDs
cust_ids = (
    spark.table("dbx_retail.bronze.customers_1").select(F.col("customer_id").alias("id"))
    .union(spark.table("dbx_retail.bronze.customers_2").select(F.col("CustomerID").alias("id")))
    .union(spark.table("dbx_retail.bronze.customers_3").select(F.col("cust_id").alias("id")))
    .union(spark.table("dbx_retail.bronze.customers_4").select(F.col("customer_id").alias("id")))
    .union(spark.table("dbx_retail.bronze.customers_5").select(F.col("customer_id").alias("id")))
    .union(spark.table("dbx_retail.bronze.customers_6").select(F.col("customer_identifier").alias("id")))
).distinct()

# Collect all order IDs
order_ids = (
    spark.table("dbx_retail.bronze.orders_1").select(F.col("order_id").alias("id"))
    .union(spark.table("dbx_retail.bronze.orders_2").select(F.col("order_id").alias("id")))
    .union(spark.table("dbx_retail.bronze.orders_3").select(F.col("order_id").alias("id")))
).distinct()

# Collect all product IDs from transactions
txn_product_ids = (
    spark.table("dbx_retail.bronze.transactions_1").select(F.col("Product_id").alias("id"))
    .union(spark.table("dbx_retail.bronze.transactions_2").select(F.col("Product_id").alias("id")))
    .union(spark.table("dbx_retail.bronze.transactions_3").select(F.col("Product_ID").alias("id")))
).distinct()

product_ids = spark.table("dbx_retail.bronze.products").select(F.col("product_id").alias("id")).distinct()
vendor_ids = spark.table("dbx_retail.bronze.vendors").select(F.col("vendor_id").alias("id")).distinct()

# Customer IDs in orders vs customer tables
order_cust_ids = (
    spark.table("dbx_retail.bronze.orders_1").select(F.col("customer_id").alias("id"))
    .union(spark.table("dbx_retail.bronze.orders_2").select(F.col("customer_id").alias("id")))
    .union(spark.table("dbx_retail.bronze.orders_3").select(F.col("customer_id").alias("id")))
).distinct()

# Vendor IDs in orders
order_vendor_ids = (
    spark.table("dbx_retail.bronze.orders_1").select(F.col("vendor_id").alias("id"))
    .union(spark.table("dbx_retail.bronze.orders_2").select(F.col("vendor_id").alias("id")))
    .union(spark.table("dbx_retail.bronze.orders_3").select(F.col("vendor_id").alias("id")))
).distinct()

# Transaction order IDs
txn_order_ids = (
    spark.table("dbx_retail.bronze.transactions_1").select(F.col("Order_id").alias("id"))
    .union(spark.table("dbx_retail.bronze.transactions_2").select(F.col("Order_id").alias("id")))
    .union(spark.table("dbx_retail.bronze.transactions_3").select(F.col("Order_ID").alias("id")))
).distinct()

# Return order IDs
return_order_ids = (
    spark.table("dbx_retail.bronze.returns_1").select(F.col("order_id").alias("id"))
    .union(spark.table("dbx_retail.bronze.returns_2").select(F.col("OrderId").alias("id")))
).distinct()

# Compute overlaps
overlap_results = []

# Customers in orders vs customer tables
cust_in_orders = order_cust_ids.count()
cust_matched = order_cust_ids.join(cust_ids, "id", "inner").count()
overlap_results.append(("Orders.customer_id → Customers", cust_in_orders, cust_matched, cust_in_orders - cust_matched))

# Vendors in orders vs vendor table
vend_in_orders = order_vendor_ids.count()
vend_matched = order_vendor_ids.join(vendor_ids, "id", "inner").count()
overlap_results.append(("Orders.vendor_id → Vendors", vend_in_orders, vend_matched, vend_in_orders - vend_matched))

# Transaction order IDs vs order tables
txn_oids = txn_order_ids.count()
txn_matched = txn_order_ids.join(order_ids, "id", "inner").count()
overlap_results.append(("Transactions.order_id → Orders", txn_oids, txn_matched, txn_oids - txn_matched))

# Return order IDs vs order tables
ret_oids = return_order_ids.count()
ret_matched = return_order_ids.join(order_ids, "id", "inner").count()
overlap_results.append(("Returns.order_id → Orders", ret_oids, ret_matched, ret_oids - ret_matched))

# Transaction product IDs vs products table
txn_pids = txn_product_ids.count()
prod_matched = txn_product_ids.join(product_ids, "id", "inner").count()
overlap_results.append(("Transactions.product_id → Products", txn_pids, prod_matched, txn_pids - prod_matched))

overlap_df = spark.createDataFrame(overlap_results, ["relationship", "source_distinct_keys", "matched_in_target", "orphaned_keys"])
print("Referential Integrity Check:")
display(overlap_df)