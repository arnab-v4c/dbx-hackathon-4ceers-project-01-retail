# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Validations
# MAGIC
# MAGIC Post-ingestion sanity checks for all 6 bronze tables.
# MAGIC This notebook is in the `validations/` folder — **excluded from the SDP pipeline**.
# MAGIC Run it manually after a pipeline update to verify data integrity.
# MAGIC
# MAGIC ## Checks Performed
# MAGIC 1. **Row counts** — match expected totals from source files
# MAGIC 2. **Schema consistency** — column superset is correct per entity
# MAGIC 3. **Source file traceability** — every row has `_source_file` and `_load_timestamp`
# MAGIC 4. **Region/segment values** — detect abbreviations, typos, and inconsistencies
# MAGIC 5. **Format anomalies** — `$` prefixes, `%` discounts, `?` placeholders
# MAGIC 6. **Missing columns per file** — verify which files lack expected columns
# MAGIC 7. **Rescued data** — check for rows that didn't parse cleanly

# COMMAND ----------

from pyspark.sql.functions import col, count, when, collect_set, lit, substring_index, sum as _sum

CATALOG = "globalmart"
SCHEMA = "bronze"

def table(name):
    return spark.table(f"{CATALOG}.{SCHEMA}.{name}")

def file_name_col(df):
    """Extract just the filename from the full _source_file path."""
    return substring_index(df["source_file"], "/", -1)

results = []

def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    results.append({"check": name, "status": status, "detail": detail})
    print(f"[{status}] {name}" + (f" — {detail}" if detail else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row Count Validation

# COMMAND ----------

expected_counts = {
    "customers": 748,
    "orders": 5006,
    "transactions": 9816,
    "returns": 800,
    "products": 1774,
    "vendors": 7
}

print("=" * 60)
print("ROW COUNT VALIDATION")
print("=" * 60)
for tbl, expected in expected_counts.items():
    actual = table(tbl).count()
    check(
        f"{tbl}: row count",
        actual == expected,
        f"expected={expected}, actual={actual}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema Consistency — Customers

# COMMAND ----------

print("=" * 60)
print("SCHEMA CONSISTENCY — CUSTOMERS")
print("=" * 60)

cust = table("customers")
cust_cols = set(cust.columns)

# Expected superset columns from all 6 files + metadata
expected_cust_cols = {
    # From files 1, 4, 5 (standard naming)
    "customer_id", "customer_email", "customer_name", "segment",
    "country", "city", "state", "postal_code", "region",
    # From file 2 (PascalCase)
    "customerid",
    # From file 3 (abbreviated)
    "cust_id",
    # From file 6 (completely different naming)
    "customer_identifier", "email_address", "full_name", "customer_segment",
    # Auto Loader metadata
    "rescued_data",
    # Pipeline-added
    "source_file", "load_timestamp"
}

missing = expected_cust_cols - cust_cols
extra = cust_cols - expected_cust_cols

check("customers: expected superset columns present", len(missing) == 0,
      f"missing={missing}" if missing else "all 18 columns present")
check("customers: no unexpected columns", len(extra) == 0,
      f"extra={extra}" if extra else "no extra columns")

# Per-file column population check
print("\n  Per-file column population:")
cust_files = (
    cust.withColumn("file", file_name_col(cust))
    .groupBy("file")
    .agg(
        count("*").alias("rows"),
        count(when(col("customer_id").isNotNull(), 1)).alias("has_customer_id"),
        count(when(col("CustomerID").isNotNull(), 1)).alias("has_CustomerID"),
        count(when(col("cust_id").isNotNull(), 1)).alias("has_cust_id"),
        count(when(col("customer_identifier").isNotNull(), 1)).alias("has_customer_identifier"),
        count(when(col("customer_email").isNotNull(), 1)).alias("has_customer_email"),
        count(when(col("email_address").isNotNull(), 1)).alias("has_email_address"),
    )
    .orderBy("file")
)
display(cust_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Region & Segment Value Checks — Customers

# COMMAND ----------

print("=" * 60)
print("REGION & SEGMENT VALUES — CUSTOMERS")
print("=" * 60)

region_seg = (
    cust.withColumn("file", file_name_col(cust))
    .groupBy("file")
    .agg(
        collect_set("region").alias("regions"),
        collect_set("segment").alias("segments"),
        collect_set("customer_segment").alias("customer_segments"),
        count("*").alias("rows")
    )
    .orderBy("file")
)
display(region_seg)

# Known issues to flag
region_check = cust.select("region").distinct().collect()
all_regions = {r["region"] for r in region_check if r["region"]}
abbreviated_regions = {"W", "S", "E", "N", "C"} & all_regions
check("customers: abbreviated regions detected",
      len(abbreviated_regions) > 0,
      f"found={abbreviated_regions} — must map W→West, S→South in Silver")

seg_check = cust.select("segment").distinct().collect()
all_segments = {s["segment"] for s in seg_check if s["segment"]}
abbreviated_segs = {"CONS", "CORP", "HO"} & all_segments
typo_segs = {"Cosumer"} & all_segments
check("customers: abbreviated segments detected",
      len(abbreviated_segs) > 0,
      f"found={abbreviated_segs} — must map CONS→Consumer, CORP→Corporate, HO→Home Office")
check("customers: segment typos detected",
      len(typo_segs) > 0,
      f"found={typo_segs} — must fix Cosumer→Consumer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema Consistency — Orders

# COMMAND ----------

print("=" * 60)
print("SCHEMA CONSISTENCY — ORDERS")
print("=" * 60)

orders = table("orders")
orders_cols = set(orders.columns)

expected_order_cols = {
    "order_id", "customer_id", "vendor_id", "ship_mode", "order_status",
    "order_purchase_date", "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
    "rescued_data", "source_file", "load_timestamp"
}

missing_o = expected_order_cols - orders_cols
check("orders: expected columns present", len(missing_o) == 0,
      f"missing={missing_o}" if missing_o else "all 13 columns present")

# Check orders_3 missing estimated_delivery_date
orders_3_nulls = (
    orders.filter(col("source_file").contains("orders_3"))
    .select(count(when(col("order_estimated_delivery_date").isNull(), 1)).alias("null_est_date"),
            count("*").alias("total"))
    .collect()[0]
)
check("orders: orders_3 missing estimated_delivery_date",
      orders_3_nulls["null_est_date"] == orders_3_nulls["total"],
      f"null={orders_3_nulls['null_est_date']}/{orders_3_nulls['total']}")

# Ship mode inconsistency
ship_modes = (
    orders.withColumn("file", file_name_col(orders))
    .groupBy("file")
    .agg(collect_set("ship_mode").alias("ship_modes"))
    .orderBy("file")
)
print("\n  Ship mode values per file:")
display(ship_modes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema Consistency & Format Anomalies — Transactions

# COMMAND ----------

print("=" * 60)
print("TRANSACTIONS — FORMAT ANOMALIES")
print("=" * 60)

txn = table("transactions")

txn_checks = (
    txn.withColumn("file", file_name_col(txn))
    .groupBy("file")
    .agg(
        count("*").alias("rows"),
        count(when(col("Sales").startswith("$"), 1)).alias("dollar_prefix_sales"),
        count(when(col("discount").endswith("%"), 1)).alias("pct_discount"),
        count(when(col("profit").isNull(), 1)).alias("null_profit"),
        count(when(col("payment_type") == "?", 1)).alias("question_mark_payment"),
        count(when(col("payment_type").isNull(), 1)).alias("null_payment_type"),
        count(when(col("Quantity") == "?", 1)).alias("question_mark_quantity"),
    )
    .orderBy("file")
)
display(txn_checks)

# transactions_1: $ prefix in Sales
t1_dollar = txn.filter(col("source_file").contains("transactions_1")).filter(col("Sales").startswith("$")).count()
check("transactions_1: $ prefix in Sales",
      t1_dollar > 0, f"count={t1_dollar} — must strip $ before casting to numeric")

# transactions_2: missing profit column (all NULL)
t2_null_profit = txn.filter(col("source_file").contains("transactions_2")).filter(col("profit").isNull()).count()
t2_total = txn.filter(col("source_file").contains("transactions_2")).count()
check("transactions_2: profit column missing",
      t2_null_profit == t2_total, f"null_profit={t2_null_profit}/{t2_total}")

# transactions_2: % discount format
t2_pct = txn.filter(col("source_file").contains("transactions_2")).filter(col("discount").endswith("%")).count()
check("transactions_2: discount as percentage",
      t2_pct == t2_total, f"pct_format={t2_pct}/{t2_total} — must strip % and divide by 100")

# transactions_1: ? placeholder in payment_type
t1_q = txn.filter(col("source_file").contains("transactions_1")).filter(col("payment_type") == "?").count()
check("transactions_1: ? placeholder in payment_type",
      t1_q > 0, f"count={t1_q} — must map to NULL in Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema Consistency — Returns

# COMMAND ----------

print("=" * 60)
print("RETURNS — COLUMN SPLIT & STATUS ABBREVIATIONS")
print("=" * 60)

ret = table("returns")

# Verify returns_1 uses order_id/return_* columns, returns_2 uses OrderId/amount/etc.
ret_schema = (
    ret.withColumn("file", file_name_col(ret))
    .groupBy("file")
    .agg(
        count("*").alias("rows"),
        count(when(col("order_id").isNotNull(), 1)).alias("has_order_id"),
        count(when(col("OrderId").isNotNull(), 1)).alias("has_OrderId"),
        count(when(col("return_status").isNotNull(), 1)).alias("has_return_status"),
        count(when(col("status").isNotNull(), 1)).alias("has_status"),
        count(when(col("refund_amount").isNotNull(), 1)).alias("has_refund_amount"),
        count(when(col("amount").isNotNull(), 1)).alias("has_amount"),
    )
    .orderBy("file")
)
display(ret_schema)

# Status abbreviations in returns_1
r1_statuses = (
    ret.filter(col("source_file").contains("returns_1"))
    .groupBy("return_status").count().orderBy("return_status")
)
print("\n  returns_1 return_status distribution:")
display(r1_statuses)

abbrev_count = ret.filter(col("return_status").isin("PENDG", "APPRVD", "RJCTD")).count()
check("returns_1: abbreviated statuses detected",
      abbrev_count > 0,
      f"count={abbrev_count} — must map PENDG→Pending, APPRVD→Approved, RJCTD→Rejected")

# $ prefix in refund_amount
r1_dollar = ret.filter(col("refund_amount").startswith("$")).count()
check("returns_1: $ prefix in refund_amount",
      r1_dollar > 0, f"count={r1_dollar} — must strip before casting")

# ? placeholder in returns_2.reason
r2_q = ret.filter(col("reason") == "?").count()
check("returns_2: ? placeholder in reason",
      r2_q > 0, f"count={r2_q} — must map to NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Products — Sparsity Check

# COMMAND ----------

print("=" * 60)
print("PRODUCTS — SPARSITY CHECK")
print("=" * 60)

prod = table("products")
total_prod = prod.count()

sparse_cols = ["weight", "dimension", "sizes", "manufacturer", "upc"]
for c in sparse_cols:
    null_count = prod.filter(col(c).isNull() | (col(c) == "")).count()
    pct = round(100 * null_count / total_prod, 1)
    check(f"products: {c} sparsity", True, f"{null_count}/{total_prod} null ({pct}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Source File Traceability

# COMMAND ----------

print("=" * 60)
print("SOURCE FILE TRACEABILITY")
print("=" * 60)

for tbl in expected_counts.keys():
    df = table(tbl)
    null_source = df.filter(col("source_file").isNull()).count()
    null_ts = df.filter(col("load_timestamp").isNull()).count()
    total = df.count()
    check(f"{tbl}: source_file populated", null_source == 0,
          f"null={null_source}/{total}")
    check(f"{tbl}: load_timestamp populated", null_ts == 0,
          f"null={null_ts}/{total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Rescued Data Check
# MAGIC
# MAGIC `_rescued_data` captures rows/columns that didn't match the inferred schema.
# MAGIC Non-null values here indicate parsing issues that need investigation.

# COMMAND ----------

print("=" * 60)
print("RESCUED DATA CHECK")
print("=" * 60)

for tbl in expected_counts.keys():
    df = table(tbl)
    if "rescued_data" in df.columns:
        rescued = df.filter(col("rescued_data").isNotNull()).count()
        total = df.count()
        check(f"{tbl}: rescued data rows", rescued == 0,
              f"rescued={rescued}/{total}" + (" — investigate these rows" if rescued > 0 else ""))
    else:
        check(f"{tbl}: rescued data column", True, "not present (OK)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Referential Integrity Spot Check

# COMMAND ----------

print("=" * 60)
print("REFERENTIAL INTEGRITY SPOT CHECK")
print("=" * 60)

# All customer IDs in orders should exist in at least one customer file
# Coalesce all customer ID columns from the superset schema
cust_ids = (
    table("customers")
    .selectExpr("COALESCE(customer_id, CustomerID, cust_id, customer_identifier) as cid")
    .filter(col("cid").isNotNull())
    .select("cid").distinct()
)

order_cust_ids = table("orders").select("customer_id").distinct()
orphan_orders = order_cust_ids.join(cust_ids, order_cust_ids["customer_id"] == cust_ids["cid"], "left_anti")
orphan_count = orphan_orders.count()
check("orders→customers: referential integrity",
      orphan_count == 0,
      f"orphaned customer_ids in orders={orphan_count}")

# All order_ids in transactions should exist in orders
order_ids = table("orders").select("order_id").distinct()
# Transactions use Order_id (mixed case) — already stored as-is
txn_order_ids = table("transactions").select(col("Order_id").alias("order_id")).distinct()
orphan_txn = txn_order_ids.join(order_ids, "order_id", "left_anti")
orphan_txn_count = orphan_txn.count()
check("transactions→orders: referential integrity",
      orphan_txn_count == 0,
      f"orphaned Order_ids in transactions={orphan_txn_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)

pass_count = sum(1 for r in results if r["status"] == "PASS")
fail_count = sum(1 for r in results if r["status"] == "FAIL")
total_checks = len(results)

print(f"\nTotal checks: {total_checks}")
print(f"  PASS: {pass_count}")
print(f"  FAIL: {fail_count}")

if fail_count > 0:
    print(f"\nFailed checks:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"  [FAIL] {r['check']} — {r['detail']}")

print(f"\nAll known data quality issues are EXPECTED at bronze layer.")
print(f"They confirm the raw data was loaded faithfully and will be remediated in Silver.")

# Create summary DataFrame for display
summary_df = spark.createDataFrame(results)
display(summary_df)

# COMMAND ----------


