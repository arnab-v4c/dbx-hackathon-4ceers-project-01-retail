# Databricks notebook source
# MAGIC %md
# MAGIC # GlobalMart — Genie Space Governance Setup
# MAGIC
# MAGIC Three governance layers for the Genie Space:
# MAGIC
# MAGIC **Metric Views** — centralised KPI definitions (named `metricview_*` to avoid confusion with existing `mv_*` Materialized Views)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Metric Views
# MAGIC
# MAGIC **Correct YAML join syntax (from Databricks docs):**
# MAGIC - Source table columns use the `source.` prefix in the `on` condition
# MAGIC - Joined table columns use the join `name` as prefix
# MAGIC - `on` must be quoted as `'on':` — YAML 1.1 parsers treat unquoted `on` as a boolean
# MAGIC - `using:` can be used instead when the column name is identical in both tables
# MAGIC
# MAGIC **Query syntax:** `SELECT Region, MEASURE(\`Total Revenue\`) FROM metricview_revenue GROUP BY ALL`

# COMMAND ----------

# ── Metric View 1: Revenue & Profit ──────────────────────────────────────
# fact_sales is the source. FK columns: customer_id (shared name), order_date_key != date_key
# customer_id is the same in both tables -> use "using"
# order_date_key != date_key -> must use "on" with source. prefix

spark.sql("""
CREATE OR REPLACE VIEW globalmart.gold.metricview_revenue
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Revenue and profit KPIs. Source: fact_sales. Use MEASURE() to query measures."
source: globalmart.gold.fact_sales
joins:
  - name: customers
    source: globalmart.gold.dim_customers
    using:
      - customer_id
  - name: dates
    source: globalmart.gold.dim_dates
    'on': source.order_date_key = dates.date_key
dimensions:
  - name: Region
    expr: customers.region
    comment: "Customer region: East, West, Central, South, North"
  - name: Segment
    expr: customers.segment
    comment: "Customer segment: Consumer, Corporate, Home Office"
  - name: Year
    expr: dates.year
  - name: Quarter
    expr: dates.quarter
  - name: Month
    expr: dates.month_name
  - name: Payment Type
    expr: payment_type
measures:
  - name: Total Revenue
    expr: SUM(sales)
    comment: "Sum of all transaction sales amounts"
  - name: Total Profit
    expr: SUM(profit)
    comment: "Sum of net profit across all transactions"
  - name: Profit Margin Pct
    expr: ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 1)
    comment: "Net profit as a percentage of revenue"
  - name: Order Count
    expr: COUNT(DISTINCT order_id)
    comment: "Number of distinct orders"
  - name: Units Sold
    expr: SUM(quantity)
    comment: "Total units sold"
  - name: Avg Order Value
    expr: ROUND(SUM(sales) / NULLIF(COUNT(DISTINCT order_id), 0), 2)
    comment: "Average revenue per order"
$$
""")
print("metricview_revenue created")


# COMMAND ----------

# ── Metric View 2: Vendor Return Rate ────────────────────────────────────
# fact_returns is the source. vendor_id is the same in both tables -> use "using"

spark.sql("""
CREATE OR REPLACE VIEW globalmart.gold.metricview_vendor_returns
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Vendor return rate KPIs. Source: fact_returns. return_rate_pct is 0-100 scale."
source: globalmart.gold.fact_returns
joins:
  - name: vendors
    source: globalmart.gold.dim_vendors
    using:
      - vendor_id
dimensions:
  - name: Vendor
    expr: vendors.vendor_name
    comment: "Vendor name"
  - name: Return Reason
    expr: return_reason
  - name: Return Status
    expr: return_status
    comment: "approved, pending, or rejected"
  - name: Return Year
    expr: YEAR(return_date)
measures:
  - name: Total Returns
    expr: COUNT(return_key)
    comment: "Number of return transactions"
  - name: Total Refund Amount
    expr: ROUND(SUM(refund_amount), 2)
    comment: "Total dollar value refunded"
  - name: Avg Refund Amount
    expr: ROUND(AVG(refund_amount), 2)
    comment: "Average refund per return"
  - name: Return Rate Pct
    expr: ROUND(COUNT(return_key) * 100.0 / NULLIF(COUNT(DISTINCT order_id), 0), 1)
    comment: "Returns as a percentage of orders. Above 20 is high."
$$
""")
print("metricview_vendor_returns created")


# COMMAND ----------

# ── Metric View 3: Slow-Moving Inventory ─────────────────────────────────
# mv_slow_moving_products is the source. product_id is the same in both tables -> use "using"

spark.sql("""
CREATE OR REPLACE VIEW globalmart.gold.metricview_inventory
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Inventory velocity KPIs. Source: mv_slow_moving_products. Above 90 days = slow-moving."
source: globalmart.gold.mv_slow_moving_products
joins:
  - name: products
    source: globalmart.gold.dim_products
    using:
      - product_id
dimensions:
  - name: Region
    expr: region
    comment: "Region where product velocity is measured"
  - name: Brand
    expr: products.brand
  - name: Category
    expr: products.categories
  - name: Slow Moving Flag
    expr: CASE WHEN days_since_last_sale > 90 THEN 'Yes' ELSE 'No' END
    comment: "Yes = not sold in over 90 days in this region"
measures:
  - name: Slow Product Count
    expr: COUNT(CASE WHEN days_since_last_sale > 90 THEN 1 END)
    comment: "Number of products with no sales in over 90 days"
  - name: Avg Days Since Last Sale
    expr: ROUND(AVG(days_since_last_sale), 0)
    comment: "Average days since last sale"
  - name: Total Quantity Sold
    expr: SUM(total_quantity_sold)
  - name: Total Sales
    expr: ROUND(SUM(total_sales), 2)
$$
""")
print("metricview_inventory created")

spark.sql("USE CATALOG globalmart")
print("\nAll metric views:")
display(spark.sql("SHOW VIEWS IN globalmart.gold").filter("viewName LIKE 'metricview_%'"))

# COMMAND ----------

# ── Sample queries to verify all three metric views ──────────────────────

print("Revenue by Region:")
display(spark.sql("""
    SELECT `Region`,
           MEASURE(`Total Revenue`),
           MEASURE(`Total Profit`),
           MEASURE(`Profit Margin Pct`)
    FROM globalmart.gold.metricview_revenue
    GROUP BY ALL
    ORDER BY MEASURE(`Total Revenue`) DESC
"""))

print("\nVendor Return Rates:")
display(spark.sql("""
    SELECT `Vendor`,
           MEASURE(`Total Returns`),
           MEASURE(`Return Rate Pct`),
           MEASURE(`Total Refund Amount`)
    FROM globalmart.gold.metricview_vendor_returns
    GROUP BY ALL
    ORDER BY MEASURE(`Return Rate Pct`) DESC
"""))

print("\nSlow-Moving by Region:")
display(spark.sql("""
    SELECT `Region`,
           MEASURE(`Slow Product Count`),
           MEASURE(`Avg Days Since Last Sale`)
    FROM globalmart.gold.metricview_inventory
    GROUP BY ALL
    ORDER BY MEASURE(`Slow Product Count`) DESC
"""))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Add metric views to the Genie Space
# MAGIC
# MAGIC Configure → Data → Add:
# MAGIC - `globalmart.gold.metricview_revenue`
# MAGIC - `globalmart.gold.metricview_vendor_returns`
# MAGIC - `globalmart.gold.metricview_inventory`