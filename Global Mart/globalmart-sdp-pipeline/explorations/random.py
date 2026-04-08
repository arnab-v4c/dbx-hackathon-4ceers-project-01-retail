# Databricks notebook source
# MAGIC %sql
# MAGIC select distinct o.order_id, product_id, vendor_id, sales, quantity, customer_id from globalmart.silver.transactions  
# MAGIC inner join
# MAGIC globalmart.silver.orders o on 
# MAGIC o.order_id = globalmart.silver.transactions.order_id
# MAGIC where 
# MAGIC o.order_id = 'CA-2015-142755'
# MAGIC and
# MAGIC product_id = 'OFF-PA-10001970'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalmart.silver.transactions
# MAGIC where 
# MAGIC -- o.order_id = 'CA-2016-147375'
# MAGIC product_id = 'OFF-PA-10001970'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalmart.silver.orders 
# MAGIC where order_id = 'CA-2015-142755'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalmart.silver.products where product_id  = 'OFF-PA-10001970'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalmart.silver.customers where region like '%N%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalmart.mdm.customers where region like '%N%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalmart.gold.dim_customers where region like '%N%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalmart.silver.orders o
# MAGIC inner join globalmart.silver.customers c
# MAGIC on 
# MAGIC o.customer_id = c.customer_id
# MAGIC     where region = 'north'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Silver.Returns' as Layer, COUNT(*) as Count FROM globalmart.silver.returns
# MAGIC UNION ALL
# MAGIC SELECT 'Gold.Fact_Returns' as Layer, COUNT(*) as Count FROM globalmart.gold.fact_returns
# MAGIC  UNION ALL
# MAGIC SELECT 'Gold.MV_Vendor_Returns' as Layer, SUM(total_returns) as Count FROM globalmart.gold.mv_return_rate_by_vendor
# MAGIC UNION ALL
# MAGIC SELECT 'Gold.MV_Customer_Returns' as Layer, SUM(total_returns) as Count FROM globalmart.gold.mv_customer_return_history;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Silver.Returns' as Layer, SUM(refund_amount) as Total_Refunds FROM globalmart.silver.returns
# MAGIC UNION ALL
# MAGIC  SELECT 'Gold.Fact_Returns' as Layer, SUM(refund_amount) as Total_Refunds FROM globalmart.gold.fact_returns
# MAGIC  UNION ALL
# MAGIC SELECT 'Gold.MV_Vendor_Returns' as Layer, SUM(total_refund_amount) as Total_Refunds FROM globalmart.gold.mv_return_rate_by_vendor
# MAGIC   UNION ALL
# MAGIC  SELECT 'Gold.MV_Customer_Returns' as Layer, SUM(total_refund_amount) as Total_Refunds FROM globalmart.gold.mv_customer_return_history;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC      COUNT(*) as total_returns,
# MAGIC       SUM(CASE WHEN original_sales IS NULL THEN 1 ELSE 0 END) as returns_without_transactions,
# MAGIC      SUM(CASE WHEN original_sales IS NULL THEN refund_amount ELSE 0 END) as lost_comparison_value
# MAGIC FROM globalmart.gold.fact_returns;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as orders_count
# MAGIC  FROM globalmart.silver.orders
# MAGIC  WHERE order_id IN (
# MAGIC    SELECT DISTINCT r.order_id 
# MAGIC     FROM globalmart.silver.returns r
# MAGIC     LEFT JOIN globalmart.silver.transactions t ON r.order_id = t.order_id
# MAGIC      WHERE t.order_id IS NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC    SELECT 
# MAGIC        'Gold.Fact_Returns' as Table, 
# MAGIC        COUNT(*) as total_rows,
# MAGIC        SUM(CASE WHEN return_date_key IS NULL THEN 1 ELSE 0 END) as null_date_keys
# MAGIC  FROM globalmart.gold.fact_returns;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from globalmart.gold.fact_returns

# COMMAND ----------

# MAGIC %sql
# MAGIC