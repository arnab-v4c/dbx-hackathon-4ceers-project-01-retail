# =============================================================================
# GlobalMart — Gold Layer Pipeline
# Target: globalmart.gold
#   in dim_customers matches the customer_id on silver.orders.
#   All joins work directly — no bridge tables, no remapping.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, coalesce, count, sum as _sum, avg,
    round as _round, datediff, current_date, current_timestamp, max as _max,
    min as _min, countDistinct, when, month, year, quarter,
    date_format, dayofweek, expr, to_date, md5, concat_ws,
    lead, row_number, size, filter as _filter
)
from pyspark.sql.window import Window

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA gold")

# =============================================================================
# DIMENSIONS
# =============================================================================

@dp.table(name="dim_customers", comment="Customer dimension — 1:1 from MDM.")
def dim_customers():
    return (
        spark.read.table("globalmart.mdm.customers")
        .withColumn("customer_key", md5(col("customer_id")))
        .select("customer_key", "customer_id", "customer_name", "customer_email",
                "segment", "country", "city", "state", "postal_code", "region")
    )

@dp.table(name="dim_products", comment="Product dimension — from MDM.")
def dim_products():
    return (
        spark.read.table("globalmart.mdm.products")
        .withColumn("product_key", md5(col("product_id")))
        .select("product_key", "product_id", "product_name", "brand",
                "categories", "colors", "manufacturer","upc","sizes","weight","product_photos_qty","date_added","date_updated", "dimension"
    ))

@dp.table(name="dim_vendors", comment="Vendor dimension — from MDM.")
def dim_vendors():
    return (
        spark.read.table("globalmart.mdm.vendors")
        .withColumn("vendor_key", md5(col("vendor_id")))
        .select("vendor_key", "vendor_id", "vendor_name")
    )

@dp.table(name="dim_dates", comment="Date dimension — generated calendar.")
def dim_dates():
    orders = spark.read.table("globalmart.silver.orders")
    returns = spark.read.table("globalmart.silver.returns")
    
    # Combine date ranges from both orders and returns to ensure full coverage (2015-2022)
    min_max_df = (
        orders.select(to_date("order_purchase_timestamp").alias("d"))
        .union(returns.select(to_date("return_date").alias("d")))
        .select(
            coalesce(_min("d"), to_date(lit("2015-01-01"))).alias("min_date"),
            coalesce(_max("d"), to_date(lit("2022-12-31"))).alias("max_date")
        )
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

# =============================================================================
# FACT TABLES
# =============================================================================

@dp.table(name="fact_orders", comment="Order fact — streaming table.")
def fact_orders():
    return (
        spark.readStream.table("globalmart.silver.orders")
        .withColumn("order_key", md5(col("order_id")))
        .select("order_key", "order_id", "customer_id", "vendor_id",
                "ship_mode", "order_status", "order_purchase_timestamp",
                "order_approved_timestamp", "order_delivered_carrier_timestamp",
                "order_delivered_customer_timestamp", "order_estimated_delivery_timestamp")
    )

@dp.table(name="fact_sales", comment="Sales fact — stream-static joins to dimensions.")
@dp.expect("no_orphan_customers", "customer_id IS NOT NULL")
def fact_sales():
    txn = spark.readStream.table("globalmart.silver.transactions")

    orders = spark.read.table("globalmart.silver.orders")
    dim_cust = dp.read("dim_customers")
    dim_prod = dp.read("dim_products")
    dim_vend = dp.read("dim_vendors")
    dim_dt = dp.read("dim_dates")

    base = txn.join(
        orders.select("order_id", "customer_id", "vendor_id", "order_purchase_timestamp"),
        on="order_id", how="inner"
    )

    return (
        base
        .join(dim_cust, on="customer_id", how="left")
        .join(dim_prod, on="product_id", how="left")
        .join(dim_vend, on="vendor_id", how="left")
        .withColumn("order_date_key", expr("CAST(date_format(order_purchase_timestamp, 'yyyyMMdd') AS INT)"))
        .join(dim_dt.select("date_key"), dim_dt.date_key == col("order_date_key"), how="left")
        .withColumn("sales_key", md5(concat_ws("||", col("order_id"), col("product_id"))))
        .select(
            "sales_key", "customer_key", "customer_id", "region", "segment",
            "product_key", "product_id", "vendor_key", "vendor_id",
            col("date_key").alias("order_date_key"),
            "order_id", "sales", "quantity", "discount", "profit",
            "payment_type", "payment_installments"
        )
    )

@dp.table(name="fact_sales_quarantine", comment="Transactions with no customer attribution.")
def fact_sales_quarantine():
    return (
        dp.read("fact_sales")
        .filter("customer_id IS NULL")
        .withColumn("_expectation", lit("no_orphan_customers"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Revenue cannot be attributed to a customer segment or region"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

@dp.table(name="fact_returns", comment="Returns fact — array-sort product matching.")
@dp.expect("refund_not_exceeding_5x_sales", "original_sales IS NULL OR refund_amount <= original_sales * 5")
def fact_returns():
    ret_stream = spark.readStream.table("globalmart.silver.returns")

    orders = spark.read.table("globalmart.silver.orders")
    txn = spark.read.table("globalmart.silver.transactions")

    dim_cust = dp.read("dim_customers")
    dim_prod = dp.read("dim_products")
    dim_vend = dp.read("dim_vendors")
    dim_dt = dp.read("dim_dates")

    txn_agg = txn.groupBy("order_id").agg(
        expr("collect_list(named_struct('product_id', product_id, 'original_sales', sales))").alias("products_array")
    )

    joined_stream = (
        ret_stream
        .join(orders.select("order_id", "customer_id", "vendor_id", "order_purchase_timestamp"),
              on="order_id", how="left")
        .join(txn_agg, on="order_id", how="left")
    )

    match_expr = """
        array_sort(
            products_array, 
            (left, right) -> CAST(
                COALESCE(
                    SIGN(
                        ABS(COALESCE(left.original_sales, 0) - COALESCE(refund_amount, 0)) - 
                        ABS(COALESCE(right.original_sales, 0) - COALESCE(refund_amount, 0))
                    ), 0
                ) AS INT
            )
        )[0]
    """

    confidence_expr = """
        CASE 
            WHEN products_array IS NULL OR size(products_array) = 0 THEN 'ORPHAN'
            WHEN size(filter(products_array, x -> ABS(x.original_sales - refund_amount) < 0.01)) = 1 THEN 'EXACT'
            WHEN size(filter(products_array, x -> ABS(x.original_sales - refund_amount) < 0.01)) > 1 THEN 'AMBIGUOUS'
            ELSE 'ESTIMATED'
        END
    """

    processed_stream = (
        joined_stream
        .withColumn("best_match", expr(match_expr))
        .withColumn("product_id", col("best_match.product_id"))
        .withColumn("original_sales", col("best_match.original_sales"))
        .withColumn("match_confidence", expr(confidence_expr))
        .drop("products_array", "best_match")
    )

    return (
        processed_stream
        .join(dim_cust, on="customer_id", how="left")
        .join(dim_prod, on="product_id", how="left")
        .join(dim_vend, on="vendor_id", how="left")
        .withColumn("return_date_key", expr("CAST(date_format(return_date, 'yyyyMMdd') AS INT)"))
        .join(dim_dt.select("date_key"), dim_dt.date_key == col("return_date_key"), how="left")
        .withColumn("return_key", md5(concat_ws("||", col("order_id"), col("return_date"))))
        .withColumn("return_to_sales_ratio",
                    when(col("original_sales") > 0, _round(col("refund_amount") / col("original_sales"), 2))
                    .otherwise(lit(None)))
        .select(
            "return_key", "customer_key", "customer_id", "region", "segment",
            "product_key", "product_id", "vendor_key", "vendor_id",
            col("date_key").alias("return_date_key"),
            "order_id", "refund_amount", "return_reason", "return_status",
            "return_date", "original_sales", "return_to_sales_ratio", "match_confidence"
        )
    )

@dp.table(name="fact_returns_quarantine", comment="Refunds exceeding 5x product price or low-confidence matches.")
def fact_returns_quarantine():
    return (
        dp.read("fact_returns")
        .filter("(original_sales IS NOT NULL AND refund_amount > original_sales * 5) OR match_confidence IN ('AMBIGUOUS', 'ESTIMATED', 'ORPHAN')")
        .withColumn("_expectation", when(col("match_confidence") == 'EXACT', lit("refund_not_exceeding_5x_sales")).otherwise(concat_ws(": ", lit("low_confidence_match"), col("match_confidence"))))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", 
            when(col("match_confidence") == 'ORPHAN', lit("No transaction found for this return order"))
            .when(col("match_confidence") == 'AMBIGUOUS', lit("Multiple products share the same price, match is uncertain"))
            .when(col("match_confidence") == 'ESTIMATED', lit("No exact price match found, heuristic guess used"))
            .otherwise(lit("Refund exceeds 5x the product sale price — potential fraud")))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

# =============================================================================
# MATERIALIZED VIEWS
# =============================================================================
# Region and segment are denormalized onto fact_sales/fact_returns from
# dim_customers at join time. MVs read them directly from the fact tables.
# =============================================================================

@dp.table(name="mv_product_price_history", comment="SCD Type 2 history of product prices by vendor.")
def mv_product_price_history():
    txn = spark.read.table("globalmart.silver.transactions")
    orders = spark.read.table("globalmart.silver.orders")

    # 1. Calculate Base Unit Price (Handling the decimal discount format)
    pricing_base = (
        txn.join(orders.select("order_id", "vendor_id", "order_purchase_timestamp"), on="order_id")
        .filter(col("sales").isNotNull()) 
        .withColumn("base_unit_price", _round(col("sales") / (col("quantity") * (1 - col("discount")/100)), 2))
        .select("product_id", "vendor_id", "base_unit_price", "order_purchase_timestamp")
    )

    # 2. Consolidate to unique price "versions"
    price_versions = (
        pricing_base.groupBy("product_id", "vendor_id", "base_unit_price")
        .agg(_min("order_purchase_timestamp").alias("valid_from"))
    )

    # 3. Apply SCD Type 2 Logic (Timeline building)
    w_history = Window.partitionBy("product_id", "vendor_id").orderBy("valid_from")
    
    history_timeline = (
        price_versions
        .withColumn("valid_to", lead("valid_from").over(w_history))
        .withColumn("version_rank", row_number().over(w_history))
        .withColumn("is_current", col("valid_to").isNull())
    )

    # 4. Calculate "At-that-point" running metrics
    w_running = Window.partitionBy("product_id", "vendor_id").orderBy("valid_from").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    return (
        history_timeline
        .withColumn("avg_price_to_date", _round(avg("base_unit_price").over(w_running), 2))
        .withColumn("min_price_to_date", _min("base_unit_price").over(w_running))
        .withColumn("max_price_to_date", _max("base_unit_price").over(w_running))
        .select(
            "product_id", "vendor_id", "base_unit_price", 
            "valid_from", "valid_to", "is_current", 
            "version_rank", "avg_price_to_date", 
            "min_price_to_date", "max_price_to_date"
        )
    )

@dp.table(name="mv_revenue_by_region", comment="Monthly revenue by region.")
@dp.expect("region_not_null", "region IS NOT NULL")
def mv_revenue_by_region():
    fact = dp.read("fact_sales")
    dim_dt = dp.read("dim_dates")
    return (
        fact
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

@dp.table(name="mv_revenue_by_region_quarantine", comment="Revenue with no region attribution.")
def mv_revenue_by_region_quarantine():
    return (
        dp.read("mv_revenue_by_region")
        .filter("region IS NULL")
        .withColumn("_expectation", lit("region_not_null"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Revenue cannot be attributed to a region"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

@dp.table(name="mv_return_rate_by_vendor", comment="Return rate by vendor.")
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

@dp.table(name="mv_return_rate_by_vendor_quarantine", comment="Impossible return rate values.")
def mv_return_rate_by_vendor_quarantine():
    return (
        dp.read("mv_return_rate_by_vendor")
        .filter("return_rate_pct IS NOT NULL AND (return_rate_pct < 0 OR return_rate_pct > 100)")
        .withColumn("_expectation", lit("return_rate_in_range"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Mathematically impossible return rate"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

@dp.table(name="mv_slow_moving_products", comment="Slow-moving products by region.")
@dp.expect("valid_days_since_sale", "days_since_last_sale IS NOT NULL AND days_since_last_sale >= 0")
def mv_slow_moving_products():
    fact_s = dp.read("fact_sales")
    dim_prod = dp.read("dim_products")
    dim_dt = dp.read("dim_dates")
    return (
        fact_s
        .join(dim_prod.select("product_id", "product_name", "brand"), on="product_id", how="left")
        .join(dim_dt.select("date_key", "full_date"), fact_s.order_date_key == dim_dt.date_key, how="left")
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

@dp.table(name="mv_slow_moving_products_quarantine", comment="Invalid velocity metrics.")
def mv_slow_moving_products_quarantine():
    return (
        dp.read("mv_slow_moving_products")
        .filter("days_since_last_sale IS NULL OR days_since_last_sale < 0")
        .withColumn("_expectation", lit("valid_days_since_sale"))
        .withColumn("_severity", lit("WARNING"))
        .withColumn("_business_impact", lit("Product with invalid days-since-last-sale goes undetected"))
        .withColumn("_quarantine_timestamp", current_timestamp())
    )

@dp.table(name="mv_customer_return_history", comment="Customer return history — fraud risk scoring.")
def mv_customer_return_history():
    fact_r = dp.read("fact_returns")
    dim_cust = dp.read("dim_customers")

    return (
        fact_r
        .join(dim_cust.select("customer_id", "customer_name", "customer_email"),
              on="customer_id", how="left")
        .groupBy("customer_id", "customer_name", "customer_email", "region", "segment")
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
