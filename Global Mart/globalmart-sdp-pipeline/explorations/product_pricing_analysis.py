# Databricks notebook source
# DBTITLE 1,=== PRODUCT PRICING ANALYSIS ===
# MAGIC %md
# MAGIC ## Product Pricing & Unit Economics Analysis
# MAGIC
# MAGIC **Key findings:**
# MAGIC 1. The `discount` field has two scales mixed: `transactions_2` uses whole numbers (20 = 20%), while `transactions_1` and `transactions_3` use decimals (0.2 = 20%). This affects 812 products.
# MAGIC 2. After normalizing the discount scale, **98.1% of products** have an identical derived list price across all transactions.
# MAGIC 3. The formula `sales = list_price × quantity × (1 - discount)` holds perfectly once discount is normalized.
# MAGIC 4. 32 products have genuinely different price points — likely different SKU variants sharing a product_id.

# COMMAND ----------

# DBTITLE 1,P1: Discount scale mismatch — which source file uses which scale?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN _source_file LIKE '%transactions_1%' THEN 'transactions_1'
# MAGIC     WHEN _source_file LIKE '%transactions_2%' THEN 'transactions_2'
# MAGIC     WHEN _source_file LIKE '%transactions_3%' THEN 'transactions_3'
# MAGIC     ELSE 'unknown'
# MAGIC   END AS source_file,
# MAGIC   COUNT(*) AS txns,
# MAGIC   ROUND(MIN(discount), 3) AS min_discount,
# MAGIC   ROUND(MAX(discount), 3) AS max_discount,
# MAGIC   SUM(CASE WHEN discount > 0 AND discount < 1 THEN 1 ELSE 0 END) AS decimal_scale,
# MAGIC   SUM(CASE WHEN discount >= 1 THEN 1 ELSE 0 END) AS whole_number_scale,
# MAGIC   SUM(CASE WHEN discount = 0 THEN 1 ELSE 0 END) AS zero_discount
# MAGIC FROM globalmart.silver.transactions
# MAGIC WHERE sales IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# DBTITLE 1,P2: How many products are affected by mixed discount scales?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN has_decimal AND has_whole THEN 'BOTH scales (broken unit economics)'
# MAGIC     WHEN has_decimal THEN 'Decimal only (correct)'
# MAGIC     WHEN has_whole THEN 'Whole number only (needs /100)'
# MAGIC     ELSE 'Zero discount only'
# MAGIC   END AS discount_pattern,
# MAGIC   COUNT(*) AS products
# MAGIC FROM (
# MAGIC   SELECT product_id,
# MAGIC     MAX(CASE WHEN discount > 0 AND discount < 1 THEN 1 ELSE 0 END) = 1 AS has_decimal,
# MAGIC     MAX(CASE WHEN discount >= 1 THEN 1 ELSE 0 END) = 1 AS has_whole
# MAGIC   FROM globalmart.silver.transactions WHERE sales IS NOT NULL
# MAGIC   GROUP BY product_id
# MAGIC )
# MAGIC GROUP BY 1
# MAGIC ORDER BY products DESC

# COMMAND ----------

# DBTITLE 1,P3: After normalizing discount — does list price become consistent?
# MAGIC %sql
# MAGIC -- Normalize: if discount >= 1, divide by 100
# MAGIC -- Then derive list_price = sales / (qty * (1 - normalized_discount))
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN price_range_pct <= 1 THEN 'A: 0-1% (identical list price)'
# MAGIC     WHEN price_range_pct <= 5 THEN 'B: 1-5% (near identical)'
# MAGIC     ELSE 'C: 5%+ (genuinely different — likely variant SKUs)'
# MAGIC   END AS after_normalization,
# MAGIC   COUNT(*) AS products
# MAGIC FROM (
# MAGIC   SELECT product_id,
# MAGIC     ROUND((MAX(lp) - MIN(lp)) / NULLIF(MAX(lp), 0) * 100, 1) AS price_range_pct
# MAGIC   FROM (
# MAGIC     SELECT product_id,
# MAGIC       sales / (NULLIF(quantity, 0) * (1 - CASE WHEN discount >= 1 THEN discount/100.0 ELSE discount END)) AS lp
# MAGIC     FROM globalmart.silver.transactions
# MAGIC     WHERE sales IS NOT NULL AND quantity > 0
# MAGIC       AND (CASE WHEN discount >= 1 THEN discount/100.0 ELSE discount END) < 1
# MAGIC   )
# MAGIC   GROUP BY product_id HAVING COUNT(*) > 1
# MAGIC )
# MAGIC GROUP BY 1 ORDER BY 1

# COMMAND ----------

# DBTITLE 1,P4: Case study — OFF-PA-10001970 (two genuine price points: $55.98 vs $12.28)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id, sales, quantity, discount,
# MAGIC   CASE WHEN discount >= 1 THEN discount/100.0 ELSE discount END AS norm_discount,
# MAGIC   ROUND(sales / NULLIF(quantity, 0), 4) AS raw_unit_price,
# MAGIC   ROUND(sales / (NULLIF(quantity, 0) * (1 - CASE WHEN discount >= 1 THEN discount/100.0 ELSE discount END)), 4) AS derived_list_price,
# MAGIC   profit,
# MAGIC   CASE WHEN _source_file LIKE '%transactions_1%' THEN 'txn_1'
# MAGIC        WHEN _source_file LIKE '%transactions_2%' THEN 'txn_2'
# MAGIC        WHEN _source_file LIKE '%transactions_3%' THEN 'txn_3' END AS source
# MAGIC FROM globalmart.silver.transactions
# MAGIC WHERE product_id = 'OFF-PA-10001970'
# MAGIC ORDER BY derived_list_price DESC, sales DESC

# COMMAND ----------

# DBTITLE 1,P5: Unit economics per product — list price, avg discount, margin, volume
# MAGIC %sql
# MAGIC -- Core unit economics table using normalized discount
# MAGIC SELECT
# MAGIC   t.product_id,
# MAGIC   p.product_name,
# MAGIC   p.brand,
# MAGIC   SPLIT(p.categories, ',')[0] AS top_category,
# MAGIC   COUNT(*) AS total_txns,
# MAGIC   SUM(t.quantity) AS total_units_sold,
# MAGIC   -- Pricing
# MAGIC   ROUND(PERCENTILE(lp, 0.5), 2) AS median_list_price,
# MAGIC   ROUND(MIN(lp), 2) AS min_list_price,
# MAGIC   ROUND(MAX(lp), 2) AS max_list_price,
# MAGIC   ROUND(AVG(t.sales / NULLIF(t.quantity, 0)), 2) AS avg_realized_unit_price,
# MAGIC   -- Discounting
# MAGIC   ROUND(AVG(nd) * 100, 1) AS avg_discount_pct,
# MAGIC   ROUND(MAX(nd) * 100, 1) AS max_discount_pct,
# MAGIC   -- Revenue
# MAGIC   ROUND(SUM(t.sales), 2) AS total_revenue,
# MAGIC   -- Profitability
# MAGIC   ROUND(SUM(t.profit), 2) AS total_profit,
# MAGIC   ROUND(AVG(t.profit / NULLIF(t.quantity, 0)), 2) AS avg_unit_profit,
# MAGIC   ROUND(SUM(t.profit) / NULLIF(SUM(t.sales), 0) * 100, 1) AS margin_pct,
# MAGIC   -- Price consistency flag
# MAGIC   CASE WHEN (MAX(lp) - MIN(lp)) / NULLIF(MAX(lp), 0) > 0.05 THEN 'MULTI-PRICE' ELSE 'STABLE' END AS price_flag
# MAGIC FROM globalmart.silver.transactions t
# MAGIC JOIN globalmart.mdm.products p ON t.product_id = p.product_id
# MAGIC CROSS JOIN LATERAL (
# MAGIC   SELECT
# MAGIC     CASE WHEN t.discount >= 1 THEN t.discount/100.0 ELSE t.discount END AS nd
# MAGIC ) d
# MAGIC CROSS JOIN LATERAL (
# MAGIC   SELECT
# MAGIC     CASE WHEN d.nd < 1 AND t.quantity > 0 THEN t.sales / (t.quantity * (1 - d.nd)) ELSE NULL END AS lp
# MAGIC ) lpc
# MAGIC WHERE t.sales IS NOT NULL AND t.quantity > 0
# MAGIC GROUP BY t.product_id, p.product_name, p.brand, SPLIT(p.categories, ',')[0]
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 50

# COMMAND ----------

# DBTITLE 1,P6: Unit economics by VENDOR — who gives better margins?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   o.vendor_id, v.vendor_name,
# MAGIC   COUNT(*) AS txns,
# MAGIC   ROUND(SUM(t.quantity), 0) AS total_units,
# MAGIC   ROUND(AVG(t.sales / NULLIF(t.quantity, 0)), 2) AS avg_unit_price,
# MAGIC   ROUND(AVG(CASE WHEN t.discount >= 1 THEN t.discount/100.0 ELSE t.discount END) * 100, 1) AS avg_discount_pct,
# MAGIC   ROUND(SUM(t.sales), 2) AS total_revenue,
# MAGIC   ROUND(SUM(t.profit), 2) AS total_profit,
# MAGIC   ROUND(SUM(t.profit) / NULLIF(SUM(t.sales), 0) * 100, 1) AS margin_pct,
# MAGIC   ROUND(AVG(t.profit / NULLIF(t.quantity, 0)), 2) AS avg_unit_profit
# MAGIC FROM globalmart.silver.transactions t
# MAGIC JOIN globalmart.silver.orders o ON t.order_id = o.order_id
# MAGIC JOIN globalmart.mdm.vendors v ON o.vendor_id = v.vendor_id
# MAGIC WHERE t.sales IS NOT NULL AND t.quantity > 0
# MAGIC GROUP BY o.vendor_id, v.vendor_name
# MAGIC ORDER BY margin_pct DESC

# COMMAND ----------

# DBTITLE 1,P7: Unit economics by REGION — pricing and margin geography
# MAGIC %sql
# MAGIC SELECT
# MAGIC   c.region,
# MAGIC   COUNT(*) AS txns,
# MAGIC   ROUND(SUM(t.quantity), 0) AS total_units,
# MAGIC   ROUND(AVG(t.sales / NULLIF(t.quantity, 0)), 2) AS avg_unit_price,
# MAGIC   ROUND(AVG(CASE WHEN t.discount >= 1 THEN t.discount/100.0 ELSE t.discount END) * 100, 1) AS avg_discount_pct,
# MAGIC   ROUND(SUM(t.sales), 2) AS total_revenue,
# MAGIC   ROUND(SUM(t.profit), 2) AS total_profit,
# MAGIC   ROUND(SUM(t.profit) / NULLIF(SUM(t.sales), 0) * 100, 1) AS margin_pct,
# MAGIC   ROUND(AVG(t.profit / NULLIF(t.quantity, 0)), 2) AS avg_unit_profit
# MAGIC FROM globalmart.silver.transactions t
# MAGIC JOIN globalmart.silver.orders o ON t.order_id = o.order_id
# MAGIC JOIN globalmart.mdm.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE t.sales IS NOT NULL AND t.quantity > 0
# MAGIC GROUP BY c.region
# MAGIC ORDER BY margin_pct DESC

# COMMAND ----------

# DBTITLE 1,P8: Unit economics by CATEGORY — which categories have best margins?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SPLIT(p.categories, ',')[0] AS top_category,
# MAGIC   COUNT(*) AS txns,
# MAGIC   COUNT(DISTINCT t.product_id) AS products,
# MAGIC   ROUND(SUM(t.quantity), 0) AS total_units,
# MAGIC   ROUND(AVG(t.sales / NULLIF(t.quantity, 0)), 2) AS avg_unit_price,
# MAGIC   ROUND(AVG(CASE WHEN t.discount >= 1 THEN t.discount/100.0 ELSE t.discount END) * 100, 1) AS avg_discount_pct,
# MAGIC   ROUND(SUM(t.sales), 2) AS total_revenue,
# MAGIC   ROUND(SUM(t.profit), 2) AS total_profit,
# MAGIC   ROUND(SUM(t.profit) / NULLIF(SUM(t.sales), 0) * 100, 1) AS margin_pct
# MAGIC FROM globalmart.silver.transactions t
# MAGIC JOIN globalmart.mdm.products p ON t.product_id = p.product_id
# MAGIC WHERE t.sales IS NOT NULL AND t.quantity > 0
# MAGIC GROUP BY SPLIT(p.categories, ',')[0]
# MAGIC HAVING COUNT(*) >= 10
# MAGIC ORDER BY margin_pct DESC

# COMMAND ----------

# DBTITLE 1,P9: Unit economics by VENDOR × REGION — cross-cutting view
# MAGIC %sql
# MAGIC SELECT
# MAGIC   v.vendor_name, c.region,
# MAGIC   COUNT(*) AS txns,
# MAGIC   ROUND(AVG(t.sales / NULLIF(t.quantity, 0)), 2) AS avg_unit_price,
# MAGIC   ROUND(AVG(CASE WHEN t.discount >= 1 THEN t.discount/100.0 ELSE t.discount END) * 100, 1) AS avg_discount_pct,
# MAGIC   ROUND(SUM(t.sales), 2) AS total_revenue,
# MAGIC   ROUND(SUM(t.profit) / NULLIF(SUM(t.sales), 0) * 100, 1) AS margin_pct
# MAGIC FROM globalmart.silver.transactions t
# MAGIC JOIN globalmart.silver.orders o ON t.order_id = o.order_id
# MAGIC JOIN globalmart.mdm.vendors v ON o.vendor_id = v.vendor_id
# MAGIC JOIN globalmart.mdm.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE t.sales IS NOT NULL AND t.quantity > 0
# MAGIC GROUP BY v.vendor_name, c.region
# MAGIC ORDER BY v.vendor_name, c.region

# COMMAND ----------

# DBTITLE 1,P10: Negative-margin products — which products lose money?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.product_id, p.product_name, p.brand,
# MAGIC   SPLIT(p.categories, ',')[0] AS top_category,
# MAGIC   COUNT(*) AS txns,
# MAGIC   ROUND(SUM(t.sales), 2) AS total_revenue,
# MAGIC   ROUND(SUM(t.profit), 2) AS total_profit,
# MAGIC   ROUND(SUM(t.profit) / NULLIF(SUM(t.sales), 0) * 100, 1) AS margin_pct,
# MAGIC   ROUND(AVG(CASE WHEN t.discount >= 1 THEN t.discount/100.0 ELSE t.discount END) * 100, 1) AS avg_discount_pct
# MAGIC FROM globalmart.silver.transactions t
# MAGIC JOIN globalmart.mdm.products p ON t.product_id = p.product_id
# MAGIC WHERE t.sales IS NOT NULL AND t.quantity > 0 AND t.profit IS NOT NULL
# MAGIC GROUP BY t.product_id, p.product_name, p.brand, SPLIT(p.categories, ',')[0]
# MAGIC HAVING SUM(t.profit) < 0
# MAGIC ORDER BY total_profit ASC
# MAGIC LIMIT 25

# COMMAND ----------

# DBTITLE 1,P11: The 32 multi-price products — what are they?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.product_id, p.product_name,
# MAGIC   COUNT(*) AS txns,
# MAGIC   ROUND(MIN(lp), 2) AS price_point_1,
# MAGIC   ROUND(MAX(lp), 2) AS price_point_2,
# MAGIC   ROUND(MAX(lp) / NULLIF(MIN(lp), 0), 1) AS price_ratio
# MAGIC FROM globalmart.silver.transactions t
# MAGIC JOIN globalmart.mdm.products p ON t.product_id = p.product_id
# MAGIC CROSS JOIN LATERAL (
# MAGIC   SELECT CASE WHEN t.discount >= 1 THEN t.discount/100.0 ELSE t.discount END AS nd
# MAGIC ) d
# MAGIC CROSS JOIN LATERAL (
# MAGIC   SELECT CASE WHEN d.nd < 1 AND t.quantity > 0 THEN t.sales / (t.quantity * (1 - d.nd)) ELSE NULL END AS lp
# MAGIC ) lpc
# MAGIC WHERE t.sales IS NOT NULL AND t.quantity > 0 AND lp IS NOT NULL
# MAGIC GROUP BY t.product_id, p.product_name
# MAGIC HAVING (MAX(lp) - MIN(lp)) / NULLIF(MAX(lp), 0) > 0.05
# MAGIC ORDER BY price_ratio DESC

# COMMAND ----------

# DBTITLE 1,P12: Discount impact on profit — high-discount txns vs low-discount
# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN nd = 0 THEN '0% (no discount)'
# MAGIC     WHEN nd <= 0.1 THEN '1-10%'
# MAGIC     WHEN nd <= 0.2 THEN '10-20%'
# MAGIC     WHEN nd <= 0.5 THEN '20-50%'
# MAGIC     ELSE '50%+'
# MAGIC   END AS discount_band,
# MAGIC   COUNT(*) AS txns,
# MAGIC   ROUND(SUM(sales), 2) AS total_revenue,
# MAGIC   ROUND(SUM(profit), 2) AS total_profit,
# MAGIC   ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 1) AS margin_pct,
# MAGIC   ROUND(AVG(profit / NULLIF(quantity, 0)), 2) AS avg_unit_profit
# MAGIC FROM (
# MAGIC   SELECT sales, profit, quantity,
# MAGIC     CASE WHEN discount >= 1 THEN discount/100.0 ELSE discount END AS nd
# MAGIC   FROM globalmart.silver.transactions
# MAGIC   WHERE sales IS NOT NULL AND quantity > 0 AND profit IS NOT NULL
# MAGIC )
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1