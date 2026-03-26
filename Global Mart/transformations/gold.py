# Databricks notebook source
# =============================================================================
# GlobalMart — Gold Layer Pipeline
# Pipeline Type: Spark Declarative Pipelines (SDP)
# Target: globalmart.gold
#
# 5 business-level DQ checks on facts and MVs:
#   1. fact_sales: no_orphan_customers (Revenue Audit)
#   2. fact_returns: refund_not_exceeding_5x_sales (Returns Fraud)
#   3. mv_revenue_by_region: region_not_null (Revenue Audit)
#   4. mv_return_rate_by_vendor: return_rate_in_range (Vendor Quality)
#   5. mv_slow_moving_products: valid_days_since_sale (Inventory Blindspot)
# =============================================================================

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, coalesce, row_number, count, sum as _sum, avg,
    round as _round, datediff, current_date, current_timestamp, max as _max,
    min as _min, countDistinct, when, month, year, quarter,
    date_format, dayofweek, expr, monotonically_increasing_id,
    to_date, abs as _abs
)
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA gold")

# COMMAND ----------

# =============================================================================
# DIMENSIONS
# =============================================================================

@dp.table(name="dim_customers", comment="Customer dimension — sourced from MDM golden records.")
def dim_customers():
    return (
        spark.read.table("globalmart.mdm.customers")
        .withColumn("customer_key", monotonically_increasing_id())
        .select("customer_key", "customer_id", "customer_name", "customer_email",
                "segment", "country", "city", "state", "postal_code", "region")
    )

# COMMAND ----------

@dp.table(name="dim_products", comment="Product dimension — sourced from MDM golden records.")
def dim_products():
    return (
        spark.read.table("globalmart.mdm.products")
        .withColumn("product_key", monotonically_increasing_id())
        .select("product_key", "product_id", "product_name", "brand",
                "categories", "colors", "manufacturer")
    )

# COMMAND ----------

@dp.table(name="dim_vendors", comment="Vendor dimension — sourced from MDM golden records.")
def dim_vendors():
    return (
        spark.read.table("globalmart.mdm.vendors")
        .withColumn("vendor_key", monotonically_increasing_id())
        .select("vendor_key", "vendor_id", "vendor_name")
    )

# COMMAND ----------

@dp.table(name="dim_dates", comment="Date dimension — generated calendar.")
def dim_dates():
    orders = spark.read.table("globalmart.silver.orders")
    min_max_df = orders.select(
        coalesce(_min(to_date("order_purchase_timestamp")), to_date(lit("2016-01-01"))).alias("min_date"),
        coalesce(_max(to_date("order_purchase_timestamp")), to_date(lit("2019-12-31"))).alias("max_date")
    )
    return (
        min_max_df
        .select(expr("explode(sequence(min_date, max_date, INTERVAL 1 DAY)) AS full_date"))
        .withColumn("date_key", expr("CAST(date_format(full_date, 'yyyyMMdd') AS INT)"))
        .withColumn("year", year("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("month_name", date_format("full_date", "MMMM"))
        .withColumn("day_of_week", dayofweek("full_date"))
        .withColumn("day_name", date_format("full_date", "EEEE"))
        .withColumn("is_weekend", when(dayofweek("full_date").isin(1, 7), True).otherwise(False))
        .select("date_key", "full_date", "year", "quarter", "month",
                "month_name", "day_of_week", "day_name", "is_weekend")
    )

# COMMAND ----------

# =============================================================================
# FACT TABLES
# =============================================================================

@dp.table(name="fact_orders", comment="Order fact — one row per order.")
def fact_orders():
    return (
        spark.read.table("globalmart.silver.orders")
        .withColumn("order_key", monotonically_increasing_id())
        .select("order_key", "order_id", "customer_id", "vendor_id",
                "ship_mode", "order_status", "order_purchase_timestamp",
                "order_approved_timestamp", "order_delivered_carrier_timestamp",
                "order_delivered_customer_timestamp", "order_estimated_delivery_timestamp")
    )

# COMMAND ----------

@dp.table(name="fact_sales", comment="Sales fact — grain: one row per order-product line item.")
@dp.expect("no_orphan_customers", "customer_key IS NOT NULL")
def fact_sales():
    txn = spark.read.table("globalmart.silver.transactions")
    orders = spark.read.table("globalmart.silver.orders")
    dim_cust = dp.read("dim_customers")
    dim_prod = dp.read("dim_products")
    dim_vend = dp.read("dim_vendors")
    dim_ord = dp.read("fact_orders")
    dim_dt = dp.read("dim_dates")

    base = txn.join(
        orders.select("order_id", "customer_id", "vendor_id", "order_purchase_timestamp"),
        on="order_id", how="inner"
    )

    return (
        base
        .join(dim_ord.select("order_key", "order_id"), on="order_id", how="left")
        .join(dim_cust.select("customer_key", "customer_id"), on="customer_id", how="left")
        .join(dim_prod.select("product_key", "product_id"), on="product_id", how="left")
        .join(dim_vend.select("vendor_key", "vendor_id"), on="vendor_id", how="left")
        .withColumn("order_date_key", expr("CAST(date_format(order_purchase_timestamp, 'yyyyMMdd') AS INT)"))
        .join(dim_dt.select("date_key"), dim_dt.date_key == col("order_date_key"), how="left")
        .withColumn("sales_key", monotonically_increasing_id())
        .select("sales_key", "order_key", "customer_key", "product_key", "vendor_key",
                col("date_key").alias("order_date_key"),
                "order_id", "product_id", "customer_id", "vendor_id",
                "sales", "quantity", "discount", "profit",
                "payment_type", "payment_installments")
    )

@dp.table(name="fact_sales_quarantine", comment="Revenue Audit — transactions with no customer attribution.")
def fact_sales_quarantine():
    return (
        dp.read("fact_sales")
        .filter("customer_key IS NULL")
        .withColumn("_expectation", lit("no_orphan_customers"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Revenue cannot be attributed to a customer segment or region — causes reconciliation failures"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

@dp.table(name="fact_returns", comment="Returns fact — grain: one row per return event.")
@dp.expect("refund_not_exceeding_5x_sales", "original_sales IS NULL OR refund_amount <= original_sales * 5")
def fact_returns():
    ret = spark.read.table("globalmart.silver.returns")
    orders = spark.read.table("globalmart.silver.orders")
    txn = spark.read.table("globalmart.silver.transactions")
    dim_cust = dp.read("dim_customers")
    dim_prod = dp.read("dim_products")
    dim_vend = dp.read("dim_vendors")
    dim_ord = dp.read("fact_orders")
    dim_dt = dp.read("dim_dates")

    txn_with_distance = txn.select(
        col("order_id").alias("_t_order_id"),
        col("product_id"),
        col("sales").alias("original_sales")
    )

    returns_x_txn = (
        ret
        .join(orders.select("order_id", "customer_id", "vendor_id", "order_purchase_timestamp"),
              on="order_id", how="left")
        .join(txn_with_distance, ret.order_id == txn_with_distance._t_order_id, how="left")
        .withColumn("_price_distance", expr("ABS(refund_amount - original_sales)"))
    )

    w_match = Window.partitionBy(ret.order_id).orderBy(col("_price_distance").asc(), col("product_id").asc())
    base = (
        returns_x_txn
        .withColumn("_match_rank", row_number().over(w_match))
        .filter(col("_match_rank") == 1)
        .drop("_match_rank", "_price_distance", "_t_order_id")
    )

    return (
        base
        .join(dim_ord.select("order_key", "order_id"), on="order_id", how="left")
        .join(dim_cust.select("customer_key", "customer_id"), on="customer_id", how="left")
        .join(dim_prod.select("product_key", "product_id"), on="product_id", how="left")
        .join(dim_vend.select("vendor_key", "vendor_id"), on="vendor_id", how="left")
        .withColumn("return_date_key", expr("CAST(date_format(return_date, 'yyyyMMdd') AS INT)"))
        .join(dim_dt.select("date_key"), dim_dt.date_key == col("return_date_key"), how="left")
        .withColumn("return_key", monotonically_increasing_id())
        .withColumn("return_to_sales_ratio",
                    when(col("original_sales") > 0,
                         _round(col("refund_amount") / col("original_sales"), 2))
                    .otherwise(lit(None)))
        .select("return_key", "order_key", "customer_key", "product_key", "vendor_key",
                col("date_key").alias("return_date_key"),
                "order_id", "product_id", "customer_id", "vendor_id",
                "refund_amount", "return_reason", "return_status",
                "return_date", "original_sales", "return_to_sales_ratio")
    )

@dp.table(name="fact_returns_quarantine", comment="Returns Fraud — refunds suspiciously exceeding product price.")
def fact_returns_quarantine():
    return (
        dp.read("fact_returns")
        .filter("original_sales IS NOT NULL AND refund_amount > original_sales * 5")
        .withColumn("_expectation", lit("refund_not_exceeding_5x_sales"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Refund exceeds 5x the product sale price — potential fraud or price-matching error"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

# =============================================================================
# MATERIALIZED VIEWS
# =============================================================================

@dp.table(name="mv_revenue_by_region", comment="Monthly revenue by region — addresses Revenue Audit.")
@dp.expect("region_not_null", "region IS NOT NULL")
def mv_revenue_by_region():
    fact = dp.read("fact_sales")
    dim_cust = dp.read("dim_customers")
    dim_dt = dp.read("dim_dates")
    return (
        fact
        .join(dim_cust.select("customer_key", "region"), on="customer_key", how="left")
        .join(dim_dt.select("date_key", "year", "month", "month_name", "quarter"),
              fact.order_date_key == dim_dt.date_key, how="left")
        .groupBy("year", "quarter", "month", "month_name", "region")
        .agg(
            _round(_sum("sales"), 2).alias("total_sales"),
            _round(_sum("profit"), 2).alias("total_profit"),
            count("*").alias("line_item_count"),
            countDistinct("order_id").alias("order_count"),
            _round(avg("sales"), 2).alias("avg_line_item_value"),
            _round(_sum("sales") / countDistinct("order_id"), 2).alias("avg_order_value")
        )
        .orderBy("year", "month", "region")
    )

@dp.table(name="mv_revenue_by_region_quarantine", comment="Revenue Audit — revenue with no region attribution.")
def mv_revenue_by_region_quarantine():
    return (
        dp.read("mv_revenue_by_region")
        .filter("region IS NULL")
        .withColumn("_expectation", lit("region_not_null"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Revenue cannot be attributed to a region — auditors cannot reconcile regional reports"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

@dp.table(name="mv_return_rate_by_vendor", comment="Return rate by vendor — addresses Returns Fraud and vendor quality.")
@dp.expect("return_rate_in_range", "return_rate_pct IS NULL OR (return_rate_pct >= 0 AND return_rate_pct <= 100)")
def mv_return_rate_by_vendor():
    fact_s = dp.read("fact_sales")
    fact_r = dp.read("fact_returns")
    dim_vend = dp.read("dim_vendors")

    sold = fact_s.groupBy("vendor_id").agg(
        countDistinct("order_id").alias("total_orders_sold"),
        _round(_sum("sales"), 2).alias("total_sales")
    )
    returned = fact_r.groupBy("vendor_id").agg(
        count("*").alias("total_returns"),
        _round(_sum("refund_amount"), 2).alias("total_refund_amount"),
        _round(avg("refund_amount"), 2).alias("avg_refund_amount")
    )
    return (
        sold
        .join(returned, on="vendor_id", how="left")
        .join(dim_vend.select("vendor_id", "vendor_name"), on="vendor_id", how="left")
        .withColumn("total_returns", coalesce(col("total_returns"), lit(0)))
        .withColumn("total_refund_amount", coalesce(col("total_refund_amount"), lit(0)))
        .withColumn("return_rate_pct",
                    _round((col("total_returns") / col("total_orders_sold")) * 100, 2))
        .select("vendor_id", "vendor_name", "total_orders_sold", "total_sales",
                "total_returns", "total_refund_amount", "avg_refund_amount", "return_rate_pct")
        .orderBy(col("return_rate_pct").desc())
    )

@dp.table(name="mv_return_rate_by_vendor_quarantine", comment="Vendor Quality — impossible return rate values.")
def mv_return_rate_by_vendor_quarantine():
    return (
        dp.read("mv_return_rate_by_vendor")
        .filter("return_rate_pct IS NOT NULL AND (return_rate_pct < 0 OR return_rate_pct > 100)")
        .withColumn("_expectation", lit("return_rate_in_range"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Mathematically impossible return rate — merchandising team would make wrong vendor contract decisions"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

@dp.table(name="mv_slow_moving_products", comment="Slow-moving products by region — addresses Inventory Blindspot.")
@dp.expect("valid_days_since_sale", "days_since_last_sale IS NOT NULL AND days_since_last_sale >= 0")
def mv_slow_moving_products():
    fact_s = dp.read("fact_sales")
    dim_prod = dp.read("dim_products")
    dim_cust = dp.read("dim_customers")
    dim_dt = dp.read("dim_dates")
    return (
        fact_s
        .join(dim_prod.select("product_key", "product_id", "product_name", "brand"),
              on="product_key", how="left")
        .join(dim_cust.select("customer_key", "region"), on="customer_key", how="left")
        .join(dim_dt.select("date_key", "full_date"),
              fact_s.order_date_key == dim_dt.date_key, how="left")
        .groupBy(fact_s.product_id, "product_name", "brand", "region")
        .agg(
            _sum("quantity").alias("total_quantity_sold"),
            _round(_sum("sales"), 2).alias("total_sales"),
            countDistinct("order_id").alias("order_count"),
            _max("full_date").alias("last_sale_date"),
            _min("full_date").alias("first_sale_date")
        )
        .withColumn("days_since_last_sale", datediff(current_date(), col("last_sale_date")))
        .withColumn("selling_period_days", datediff(col("last_sale_date"), col("first_sale_date")))
        .withColumn("avg_daily_quantity",
                    when(col("selling_period_days") > 0,
                         _round(col("total_quantity_sold") / col("selling_period_days"), 2))
                    .otherwise(col("total_quantity_sold")))
        .select("product_id", "product_name", "brand", "region",
                "total_quantity_sold", "total_sales", "order_count",
                "first_sale_date", "last_sale_date",
                "days_since_last_sale", "selling_period_days", "avg_daily_quantity")
        .orderBy(col("days_since_last_sale").desc(), col("total_quantity_sold").asc())
    )

@dp.table(name="mv_slow_moving_products_quarantine", comment="Inventory Blindspot — products with invalid velocity metrics.")
def mv_slow_moving_products_quarantine():
    return (
        dp.read("mv_slow_moving_products")
        .filter("days_since_last_sale IS NULL OR days_since_last_sale < 0")
        .withColumn("_expectation", lit("valid_days_since_sale"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Product with invalid days-since-last-sale goes undetected — 12-18% revenue loss from missed discount window"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

@dp.table(name="mv_customer_return_history", comment="Customer return history — addresses Returns Fraud with fraud risk scoring.")
def mv_customer_return_history():
    fact_r = dp.read("fact_returns")
    dim_cust = dp.read("dim_customers")
    return (
        fact_r
        .join(dim_cust.select("customer_key", "customer_id", "customer_name",
                              "segment", "region", "customer_email"),
              on="customer_key", how="left")
        .groupBy(
            dim_cust.customer_id, dim_cust.customer_name,
            dim_cust.customer_email, dim_cust.segment, dim_cust.region
        )
        .agg(
            count("*").alias("total_returns"),
            _round(_sum("refund_amount"), 2).alias("total_refund_amount"),
            _round(avg("refund_amount"), 2).alias("avg_refund_amount"),
            _round(_max("refund_amount"), 2).alias("max_single_refund"),
            _min("return_date").alias("first_return_date"),
            _max("return_date").alias("last_return_date"),
            countDistinct("return_reason").alias("distinct_reasons"),
            _round(avg("return_to_sales_ratio"), 2).alias("avg_return_to_sales_ratio")
        )
        .withColumn("days_between_first_last_return",
                    datediff(col("last_return_date"), col("first_return_date")))
        .withColumn("fraud_risk_score",
                    when((col("total_returns") >= 5) & (col("avg_return_to_sales_ratio") > 0.8), "HIGH")
                    .when((col("total_returns") >= 3) | (col("max_single_refund") > 400), "MEDIUM")
                    .otherwise("LOW"))
        .select("customer_id", "customer_name", "customer_email", "segment", "region",
                "total_returns", "total_refund_amount", "avg_refund_amount", "max_single_refund",
                "first_return_date", "last_return_date", "days_between_first_last_return",
                "distinct_reasons", "avg_return_to_sales_ratio", "fraud_risk_score")
        .orderBy(col("total_returns").desc())
    )