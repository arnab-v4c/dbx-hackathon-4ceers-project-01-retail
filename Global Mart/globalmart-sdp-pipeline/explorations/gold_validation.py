# Databricks notebook source
# DBTITLE 1,=== BRONZE LAYER CHECKS ===


# COMMAND ----------

# DBTITLE 1,B1: Bronze row counts — raw file ingestion completeness
# MAGIC %sql
# MAGIC SELECT 'customers' AS entity, COUNT(*) AS bronze_rows, 748 AS expected FROM globalmart.bronze.customers
# MAGIC UNION ALL SELECT 'orders', COUNT(*), 5006 FROM globalmart.bronze.orders
# MAGIC UNION ALL SELECT 'transactions', COUNT(*), 9816 FROM globalmart.bronze.transactions
# MAGIC UNION ALL SELECT 'returns', COUNT(*), 800 FROM globalmart.bronze.returns
# MAGIC UNION ALL SELECT 'products', COUNT(*), 1774 FROM globalmart.bronze.products
# MAGIC UNION ALL SELECT 'vendors', COUNT(*), 7 FROM globalmart.bronze.vendors

# COMMAND ----------

# DBTITLE 1,B2: Bronze null rate in key columns (before Silver harmonization)
# MAGIC %sql
# MAGIC SELECT 'customers' AS entity, 'customer_id variants' AS check_col,
# MAGIC   SUM(CASE WHEN customer_id IS NULL AND customerid IS NULL AND cust_id IS NULL AND customer_identifier IS NULL THEN 1 ELSE 0 END) AS all_id_null,
# MAGIC   COUNT(*) AS total
# MAGIC FROM globalmart.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders', 'order_id', SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END), COUNT(*)
# MAGIC FROM globalmart.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'transactions', 'order_id', SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END), COUNT(*)
# MAGIC FROM globalmart.bronze.transactions
# MAGIC UNION ALL
# MAGIC SELECT 'returns', 'order_id variants', SUM(CASE WHEN order_id IS NULL AND orderid IS NULL THEN 1 ELSE 0 END), COUNT(*)
# MAGIC FROM globalmart.bronze.returns

# COMMAND ----------

# DBTITLE 1,B3: Bronze schema validation results (pre-Bronze contract check)
# MAGIC %sql
# MAGIC SELECT entity, file_name, status, key_check, required_check, min_columns_check, issues
# MAGIC FROM globalmart.raw.schema_validation_bronze
# MAGIC ORDER BY entity, file_name

# COMMAND ----------

# DBTITLE 1,=== SILVER LAYER CHECKS ===


# COMMAND ----------

# DBTITLE 1,S1: Silver row counts — Bronze → Silver pass-through (no rows dropped by CRITICAL rules)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   'customers' AS entity,
# MAGIC   (SELECT COUNT(*) FROM globalmart.bronze.customers) AS bronze_rows,
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.customers) AS silver_rows,
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.customers_quarantine) AS quarantine_rows
# MAGIC UNION ALL SELECT 'orders',
# MAGIC   (SELECT COUNT(*) FROM globalmart.bronze.orders),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.orders),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.orders_quarantine)
# MAGIC UNION ALL SELECT 'transactions',
# MAGIC   (SELECT COUNT(*) FROM globalmart.bronze.transactions),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.transactions),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.transactions_quarantine)
# MAGIC UNION ALL SELECT 'returns',
# MAGIC   (SELECT COUNT(*) FROM globalmart.bronze.returns),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.returns),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.returns_quarantine)
# MAGIC UNION ALL SELECT 'products',
# MAGIC   (SELECT COUNT(*) FROM globalmart.bronze.products),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.products),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.products_quarantine)
# MAGIC UNION ALL SELECT 'vendors',
# MAGIC   (SELECT COUNT(*) FROM globalmart.bronze.vendors),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.vendors),
# MAGIC   (SELECT COUNT(*) FROM globalmart.silver.vendors_quarantine)

# COMMAND ----------

# DBTITLE 1,S2: Quarantine breakdown by rule — which DQ checks are catching what?
# MAGIC %sql
# MAGIC SELECT entity, rule, severity, cnt FROM (
# MAGIC   SELECT 'customers' AS entity, _expectation_name AS rule, _severity AS severity, COUNT(*) AS cnt FROM globalmart.silver.customers_quarantine GROUP BY _expectation_name, _severity
# MAGIC   UNION ALL SELECT 'orders', _expectation_name, _severity, COUNT(*) FROM globalmart.silver.orders_quarantine GROUP BY _expectation_name, _severity
# MAGIC   UNION ALL SELECT 'transactions', _expectation_name, _severity, COUNT(*) FROM globalmart.silver.transactions_quarantine GROUP BY _expectation_name, _severity
# MAGIC   UNION ALL SELECT 'returns', _expectation_name, _severity, COUNT(*) FROM globalmart.silver.returns_quarantine GROUP BY _expectation_name, _severity
# MAGIC ) ORDER BY entity, severity DESC, cnt DESC

# COMMAND ----------

# DBTITLE 1,S3: Silver customer dedup flow — 748 silver → 374 MDM (2:1 dedup ratio)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   'silver.customers total rows' AS metric, COUNT(*) AS value FROM globalmart.silver.customers
# MAGIC UNION ALL SELECT 'silver.customers distinct customer_id', COUNT(DISTINCT customer_id) FROM globalmart.silver.customers
# MAGIC UNION ALL SELECT 'mdm.customers (deduped)', COUNT(*) FROM globalmart.mdm.customers
# MAGIC UNION ALL SELECT 'customers with orders', COUNT(DISTINCT customer_id) FROM globalmart.silver.orders
# MAGIC UNION ALL SELECT 'order customers found in MDM', COUNT(DISTINCT o.customer_id)
# MAGIC   FROM globalmart.silver.orders o
# MAGIC   JOIN globalmart.mdm.customers m ON o.customer_id = m.customer_id

# COMMAND ----------

# DBTITLE 1,S4: Region distribution in MDM customers
# MAGIC %sql
# MAGIC SELECT region, COUNT(*) AS customers,
# MAGIC   SUM(CASE WHEN customer_id IN (SELECT DISTINCT customer_id FROM globalmart.silver.orders) THEN 1 ELSE 0 END) AS customers_with_orders
# MAGIC FROM globalmart.mdm.customers
# MAGIC GROUP BY region
# MAGIC ORDER BY region

# COMMAND ----------

# DBTITLE 1,S5: Mapping source audit — LLM vs static config
# MAGIC %sql
# MAGIC SELECT entity, mapping_source, cnt FROM (
# MAGIC   SELECT 'customers' AS entity, _mapping_source AS mapping_source, COUNT(*) AS cnt FROM globalmart.silver.customers GROUP BY _mapping_source
# MAGIC   UNION ALL SELECT 'orders', _mapping_source, COUNT(*) FROM globalmart.silver.orders GROUP BY _mapping_source
# MAGIC   UNION ALL SELECT 'transactions', _mapping_source, COUNT(*) FROM globalmart.silver.transactions GROUP BY _mapping_source
# MAGIC   UNION ALL SELECT 'returns', _mapping_source, COUNT(*) FROM globalmart.silver.returns GROUP BY _mapping_source
# MAGIC   UNION ALL SELECT 'products', _mapping_source, COUNT(*) FROM globalmart.silver.products GROUP BY _mapping_source
# MAGIC   UNION ALL SELECT 'vendors', _mapping_source, COUNT(*) FROM globalmart.silver.vendors GROUP BY _mapping_source
# MAGIC ) ORDER BY entity

# COMMAND ----------

# DBTITLE 1,S6: Orders with no transaction line items (expected gap)
# MAGIC %sql
# MAGIC SELECT o.order_status, COUNT(*) AS orders_without_transactions
# MAGIC FROM globalmart.silver.orders o
# MAGIC LEFT JOIN globalmart.silver.transactions t ON o.order_id = t.order_id
# MAGIC WHERE t.order_id IS NULL
# MAGIC GROUP BY o.order_status
# MAGIC ORDER BY orders_without_transactions DESC

# COMMAND ----------

# DBTITLE 1,S7: Referential integrity — orphan foreign keys
# MAGIC %sql
# MAGIC SELECT 'txn→orders (orphan transactions)' AS fk_check, COUNT(*) AS orphan_count
# MAGIC FROM globalmart.silver.transactions t LEFT ANTI JOIN globalmart.silver.orders o ON t.order_id = o.order_id
# MAGIC UNION ALL
# MAGIC SELECT 'txn→products (orphan transactions)', COUNT(*)
# MAGIC FROM globalmart.silver.transactions t LEFT ANTI JOIN globalmart.silver.products p ON t.product_id = p.product_id
# MAGIC UNION ALL
# MAGIC SELECT 'orders→customers (orphan orders)', COUNT(*)
# MAGIC FROM globalmart.silver.orders o LEFT ANTI JOIN globalmart.mdm.customers c ON o.customer_id = c.customer_id
# MAGIC UNION ALL
# MAGIC SELECT 'returns→orders (orphan returns)', COUNT(*)
# MAGIC FROM globalmart.silver.returns r LEFT ANTI JOIN globalmart.silver.orders o ON r.order_id = o.order_id

# COMMAND ----------

# DBTITLE 1,=== GOLD LAYER CHECKS ===


# COMMAND ----------

# DBTITLE 1,G1: fact_sales vs silver.transactions — row counts and financial totals
# MAGIC %sql
# MAGIC SELECT 'silver.transactions' AS source, COUNT(*) AS rows, ROUND(SUM(sales),2) AS total_sales, ROUND(SUM(profit),2) AS total_profit, ROUND(SUM(quantity),0) AS total_qty
# MAGIC FROM globalmart.silver.transactions
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fact_sales', COUNT(*), ROUND(SUM(sales),2), ROUND(SUM(profit),2), ROUND(SUM(quantity),0)
# MAGIC FROM globalmart.gold.fact_sales

# COMMAND ----------

# DBTITLE 1,G2: fact_orders vs silver.orders — row counts and distinct keys
# MAGIC %sql
# MAGIC SELECT 'silver.orders' AS source, COUNT(*) AS rows, COUNT(DISTINCT order_id) AS distinct_orders, COUNT(DISTINCT customer_id) AS distinct_customers
# MAGIC FROM globalmart.silver.orders
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fact_orders', COUNT(*), COUNT(DISTINCT order_id), COUNT(DISTINCT customer_id)
# MAGIC FROM globalmart.gold.fact_orders

# COMMAND ----------

# DBTITLE 1,G3: fact_returns vs silver.returns — row counts and refund totals
# MAGIC %sql
# MAGIC SELECT 'silver.returns' AS source, COUNT(*) AS rows, ROUND(SUM(refund_amount),2) AS total_refunds, COUNT(DISTINCT order_id) AS distinct_orders
# MAGIC FROM globalmart.silver.returns
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fact_returns', COUNT(*), ROUND(SUM(refund_amount),2), COUNT(DISTINCT order_id)
# MAGIC FROM globalmart.gold.fact_returns

# COMMAND ----------

# DBTITLE 1,G4: Dimension key coverage in fact_sales — should be zero NULLs
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key,
# MAGIC   SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_key,
# MAGIC   SUM(CASE WHEN vendor_key IS NULL THEN 1 ELSE 0 END) AS null_vendor_key,
# MAGIC   SUM(CASE WHEN order_date_key IS NULL THEN 1 ELSE 0 END) AS null_date_key,
# MAGIC   SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS null_region,
# MAGIC   SUM(CASE WHEN segment IS NULL THEN 1 ELSE 0 END) AS null_segment
# MAGIC FROM globalmart.gold.fact_sales

# COMMAND ----------

# DBTITLE 1,G5: Order-level integrity — silver.orders ↔ gold.fact_orders (should be 0 gaps)
# MAGIC %sql
# MAGIC SELECT 'in_silver_not_gold' AS gap, COUNT(*) AS cnt
# MAGIC FROM globalmart.silver.orders s LEFT ANTI JOIN globalmart.gold.fact_orders g ON s.order_id = g.order_id
# MAGIC UNION ALL
# MAGIC SELECT 'in_gold_not_silver', COUNT(*)
# MAGIC FROM globalmart.gold.fact_orders g LEFT ANTI JOIN globalmart.silver.orders s ON g.order_id = s.order_id

# COMMAND ----------

# DBTITLE 1,G6: Customer dimension integrity — all order customers in dim_customers?
# MAGIC %sql
# MAGIC SELECT 'orders_customer_not_in_dim' AS check_name, COUNT(DISTINCT o.customer_id) AS cnt
# MAGIC FROM globalmart.gold.fact_orders o
# MAGIC LEFT JOIN globalmart.gold.dim_customers d ON o.customer_id = d.customer_id
# MAGIC WHERE d.customer_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'fact_sales_customer_not_in_dim', COUNT(DISTINCT f.customer_id)
# MAGIC FROM globalmart.gold.fact_sales f
# MAGIC WHERE f.customer_key IS NULL AND f.customer_id IS NOT NULL

# COMMAND ----------

# DBTITLE 1,G7: Revenue by region — MV vs direct Silver computation (should match within rounding)
# MAGIC %sql
# MAGIC SELECT src, region, sales, profit, orders FROM (
# MAGIC   SELECT 'mv_revenue' AS src, region, ROUND(SUM(total_sales),2) AS sales, ROUND(SUM(total_profit),2) AS profit, SUM(order_count) AS orders
# MAGIC   FROM globalmart.gold.mv_revenue_by_region GROUP BY region
# MAGIC   UNION ALL
# MAGIC   SELECT 'silver_direct', c.region, ROUND(SUM(t.sales),2), ROUND(SUM(t.profit),2), COUNT(DISTINCT t.order_id)
# MAGIC   FROM globalmart.silver.transactions t
# MAGIC   JOIN globalmart.silver.orders o ON t.order_id = o.order_id
# MAGIC   JOIN globalmart.mdm.customers c ON o.customer_id = c.customer_id
# MAGIC   GROUP BY c.region
# MAGIC ) ORDER BY region, src

# COMMAND ----------

# DBTITLE 1,G8: fact_sales revenue vs region — check for NULL region leakage
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COALESCE(region, '** NULL **') AS region,
# MAGIC   COUNT(*) AS rows,
# MAGIC   ROUND(SUM(sales),2) AS sales,
# MAGIC   ROUND(SUM(profit),2) AS profit
# MAGIC FROM globalmart.gold.fact_sales
# MAGIC GROUP BY region
# MAGIC ORDER BY region

# COMMAND ----------

# DBTITLE 1,G9: Revenue reconciliation — fact_sales total vs mv_revenue_by_region total
# MAGIC %sql
# MAGIC SELECT 'fact_sales' AS source, ROUND(SUM(sales),2) AS total_sales, ROUND(SUM(profit),2) AS total_profit
# MAGIC FROM globalmart.gold.fact_sales
# MAGIC UNION ALL
# MAGIC SELECT 'mv_revenue_by_region', ROUND(SUM(total_sales),2), ROUND(SUM(total_profit),2)
# MAGIC FROM globalmart.gold.mv_revenue_by_region

# COMMAND ----------

# DBTITLE 1,G10: Return rate by vendor — verify math (computed vs stored)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   vendor_id, vendor_name, total_orders_sold, total_returns, return_rate_pct,
# MAGIC   ROUND((total_returns * 100.0 / total_orders_sold), 2) AS computed_rate_pct,
# MAGIC   CASE WHEN ABS(return_rate_pct - ROUND((total_returns * 100.0 / total_orders_sold), 2)) < 0.01 THEN 'OK' ELSE '** MISMATCH **' END AS check_result,
# MAGIC   total_sales, total_refund_amount
# MAGIC FROM globalmart.gold.mv_return_rate_by_vendor
# MAGIC ORDER BY vendor_id

# COMMAND ----------

# DBTITLE 1,G11: Refund reconciliation — fact_returns total vs mv_return_rate total
# MAGIC %sql
# MAGIC SELECT 'fact_returns' AS source, COUNT(*) AS rows, ROUND(SUM(refund_amount),2) AS total_refunds
# MAGIC FROM globalmart.gold.fact_returns
# MAGIC UNION ALL
# MAGIC SELECT 'mv_return_rate_by_vendor', NULL, ROUND(SUM(total_refund_amount),2)
# MAGIC FROM globalmart.gold.mv_return_rate_by_vendor
# MAGIC UNION ALL
# MAGIC SELECT 'mv_customer_return_history', NULL, ROUND(SUM(total_refund_amount),2)
# MAGIC FROM globalmart.gold.mv_customer_return_history

# COMMAND ----------

# DBTITLE 1,G12: [BUG] return_to_sales_ratio — product matching produces extreme values
# MAGIC %sql
# MAGIC -- The array_sort product-matching algorithm in fact_returns matches refund_amount
# MAGIC -- to the closest sales value, but when an order has cheap items and an expensive
# MAGIC -- return, it matches to the wrong product. Ratios should be ~1.0, not 100x+.
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN return_to_sales_ratio IS NULL THEN 'NULL'
# MAGIC     WHEN return_to_sales_ratio <= 1.5 THEN '0-1.5x (normal)'
# MAGIC     WHEN return_to_sales_ratio <= 5 THEN '1.5-5x (suspicious)'
# MAGIC     WHEN return_to_sales_ratio <= 20 THEN '5-20x (very high)'
# MAGIC     ELSE '20x+ (impossible)'
# MAGIC   END AS ratio_bucket,
# MAGIC   COUNT(*) AS returns_count,
# MAGIC   ROUND(AVG(return_to_sales_ratio), 2) AS avg_ratio,
# MAGIC   ROUND(MIN(refund_amount), 2) AS min_refund,
# MAGIC   ROUND(MAX(refund_amount), 2) AS max_refund,
# MAGIC   ROUND(MIN(original_sales), 2) AS min_matched_sale,
# MAGIC   ROUND(MAX(original_sales), 2) AS max_matched_sale
# MAGIC FROM globalmart.gold.fact_returns
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# DBTITLE 1,G13: [BUG] Top 15 worst product-matching misses
# MAGIC %sql
# MAGIC SELECT return_to_sales_ratio, refund_amount, original_sales, product_id, order_id
# MAGIC FROM globalmart.gold.fact_returns
# MAGIC WHERE return_to_sales_ratio IS NOT NULL
# MAGIC ORDER BY return_to_sales_ratio DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,G14: Fraud risk scoring validation — check thresholds match code
# MAGIC %sql
# MAGIC -- HIGH = total_returns >= 5 AND avg_return_to_sales_ratio > 0.8
# MAGIC -- MEDIUM = total_returns >= 3 OR max_single_refund > 400
# MAGIC -- LOW = everything else
# MAGIC SELECT
# MAGIC   fraud_risk_score,
# MAGIC   COUNT(*) AS customers,
# MAGIC   MIN(total_returns) AS min_returns,
# MAGIC   MAX(total_returns) AS max_returns,
# MAGIC   ROUND(MIN(avg_return_to_sales_ratio), 2) AS min_ratio,
# MAGIC   ROUND(MAX(avg_return_to_sales_ratio), 2) AS max_ratio,
# MAGIC   ROUND(MIN(max_single_refund), 2) AS min_max_refund,
# MAGIC   ROUND(MAX(max_single_refund), 2) AS max_max_refund,
# MAGIC   ROUND(SUM(total_refund_amount), 2) AS total_refunds
# MAGIC FROM globalmart.gold.mv_customer_return_history
# MAGIC GROUP BY fraud_risk_score
# MAGIC ORDER BY fraud_risk_score

# COMMAND ----------

# DBTITLE 1,G15: [INFO] North region — 42 customers with 0 orders (missing from revenue MV)
# MAGIC %sql
# MAGIC SELECT c.region, COUNT(DISTINCT c.customer_id) AS total_customers,
# MAGIC   COUNT(DISTINCT o.order_id) AS total_orders,
# MAGIC   ROUND(COALESCE(SUM(t.sales), 0), 2) AS total_sales
# MAGIC FROM globalmart.mdm.customers c
# MAGIC LEFT JOIN globalmart.silver.orders o ON c.customer_id = o.customer_id
# MAGIC LEFT JOIN globalmart.silver.transactions t ON o.order_id = t.order_id
# MAGIC GROUP BY c.region
# MAGIC ORDER BY c.region

# COMMAND ----------

# DBTITLE 1,G16: dim_dates coverage — does the calendar span all order dates?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(d.full_date) AS calendar_start,
# MAGIC   MAX(d.full_date) AS calendar_end,
# MAGIC   COUNT(*) AS calendar_days,
# MAGIC   (SELECT MIN(TO_DATE(order_purchase_timestamp)) FROM globalmart.silver.orders WHERE order_purchase_timestamp IS NOT NULL) AS earliest_order,
# MAGIC   (SELECT MAX(TO_DATE(order_purchase_timestamp)) FROM globalmart.silver.orders WHERE order_purchase_timestamp IS NOT NULL) AS latest_order
# MAGIC FROM globalmart.gold.dim_dates d

# COMMAND ----------

# DBTITLE 1,G17: Slow-moving products — verify product count vs dim_products
# MAGIC %sql
# MAGIC SELECT
# MAGIC   'dim_products' AS source, COUNT(DISTINCT product_id) AS distinct_products FROM globalmart.gold.dim_products
# MAGIC UNION ALL
# MAGIC SELECT 'fact_sales', COUNT(DISTINCT product_id) FROM globalmart.gold.fact_sales
# MAGIC UNION ALL
# MAGIC SELECT 'mv_slow_moving_products', COUNT(DISTINCT product_id) FROM globalmart.gold.mv_slow_moving_products

# COMMAND ----------

# DBTITLE 1,=== AI DQ DISCOVERIES ===


# COMMAND ----------

# DBTITLE 1,D1: AI-discovered issues by entity and severity
# MAGIC %sql
# MAGIC SELECT entity, severity, issue_type, COUNT(*) AS findings
# MAGIC FROM globalmart.silver.ai_dq_discoveries
# MAGIC WHERE _discovery_status != 'error'
# MAGIC GROUP BY entity, severity, issue_type
# MAGIC ORDER BY entity, severity DESC, findings DESC

# COMMAND ----------

# DBTITLE 1,D2: All AI discoveries — full detail
# MAGIC %sql
# MAGIC SELECT entity, column_name, issue_type, severity, suggested_rule, description
# MAGIC FROM globalmart.silver.ai_dq_discoveries
# MAGIC ORDER BY entity, severity DESC, column_name