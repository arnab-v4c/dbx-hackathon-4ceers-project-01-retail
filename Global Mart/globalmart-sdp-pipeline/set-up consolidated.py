# Databricks notebook source
# DBTITLE 1,his handle
# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC -- =============================================================================
# MAGIC -- globalmart — Consolidated Infrastructure & Framework Setup
# MAGIC -- Creates: Catalog (globalmart) → Schemas (raw, bronze, silver, gold, mdm, config)
# MAGIC -- Creates: Unity Catalog Volume for raw files
# MAGIC -- Creates & Seeds: Config tables (column_mapping, dq_rules, mdm_rules)
# MAGIC -- Run once to bootstrap the environment.
# MAGIC -- =============================================================================
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 1: Create the Catalog & Main Schemas
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS globalmart;
# MAGIC USE CATALOG globalmart;
# MAGIC
# MAGIC -- RAW: Staging area for Unity Catalog Volumes
# MAGIC CREATE SCHEMA IF NOT EXISTS globalmart.raw;
# MAGIC
# MAGIC -- BRONZE: Raw data ingested into Delta tables (no transformations)
# MAGIC CREATE SCHEMA IF NOT EXISTS globalmart.bronze;
# MAGIC
# MAGIC -- SILVER: Harmonized, cleaned, quality-enforced tables + quarantine
# MAGIC CREATE SCHEMA IF NOT EXISTS globalmart.silver;
# MAGIC
# MAGIC -- GOLD: Star schema for analytics
# MAGIC CREATE SCHEMA IF NOT EXISTS globalmart.gold;
# MAGIC
# MAGIC -- MDM: Golden records
# MAGIC CREATE SCHEMA IF NOT EXISTS globalmart.mdm;
# MAGIC
# MAGIC -- INGESTION CONTROL: Metadata tables driving dynamic pipelines
# MAGIC CREATE SCHEMA IF NOT EXISTS globalmart.config;
# MAGIC
# MAGIC -- Source data validation table
# MAGIC -- CREATE TABLE IF NOT EXISTS globalmart.raw.schema_validation_log;
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 2: Create the Unity Catalog Volume for raw file storage
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS globalmart.raw.source_files;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS globalmart.bronze.pipeline_metadata;
# MAGIC -- =============================================================================
# MAGIC -- STEP 3: Create Metadata Framework Tables
# MAGIC -- =============================================================================
# MAGIC
# MAGIC -- Table 1: column_mapping
# MAGIC CREATE TABLE IF NOT EXISTS globalmart.config.column_mapping (
# MAGIC   entity              STRING      NOT NULL,
# MAGIC   source_column       STRING      NOT NULL,
# MAGIC   canonical_column    STRING      NOT NULL,
# MAGIC   transformation      STRING
# MAGIC );
# MAGIC
# MAGIC -- Table 2: dq_rules
# MAGIC CREATE TABLE IF NOT EXISTS globalmart.config.dq_rules (
# MAGIC   rule_id             STRING      NOT NULL,
# MAGIC   entity              STRING      NOT NULL,
# MAGIC   rule_name           STRING      NOT NULL,
# MAGIC   rule_expression     STRING      NOT NULL,
# MAGIC   severity            STRING      NOT NULL,
# MAGIC   consequence         STRING      NOT NULL,
# MAGIC   issue_type          STRING      NOT NULL,
# MAGIC   failure_reason      STRING      NOT NULL
# MAGIC );
# MAGIC
# MAGIC -- Table 3: mdm_rules (formerly survivorship_rules)
# MAGIC CREATE TABLE IF NOT EXISTS globalmart.config.mdm_rules (
# MAGIC   entity                STRING      NOT NULL,
# MAGIC   canonical_column      STRING      NOT NULL,
# MAGIC   strategy              STRING      NOT NULL,
# MAGIC   source_priority_order STRING
# MAGIC );
# MAGIC
# MAGIC -- Table 4: fraud config rules 
# MAGIC CREATE TABLE IF NOT EXISTS globalmart.config.fraud_rule_config(
# MAGIC     rule_name        STRING  NOT NULL,
# MAGIC     threshold        DOUBLE  NOT NULL,
# MAGIC     weight           DOUBLE  NOT NULL,
# MAGIC     is_active        BOOLEAN NOT NULL,
# MAGIC     description      STRING,
# MAGIC     human_reasoning  STRING
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 4: Seed column_mapping
# MAGIC -- =============================================================================
# MAGIC
# MAGIC INSERT INTO globalmart.config.column_mapping VALUES
# MAGIC ('customers', 'customer_id',         'customer_id',    NULL),
# MAGIC ('customers', 'customerid',          'customer_id',    NULL),
# MAGIC ('customers', 'cust_id',             'customer_id',    NULL),
# MAGIC ('customers', 'customer_identifier', 'customer_id',    NULL),
# MAGIC ('customers', 'customer_email',      'customer_email', NULL),
# MAGIC ('customers', 'email_address',       'customer_email', NULL),
# MAGIC ('customers', 'customer_name',       'customer_name',  NULL),
# MAGIC ('customers', 'full_name',           'customer_name',  NULL),
# MAGIC ('customers', 'segment',             'segment',        "CASE WHEN {col} IN ('CONS', 'Cosumer') THEN 'Consumer' WHEN {col} = 'CORP' THEN 'Corporate' WHEN {col} = 'HO' THEN 'Home Office' ELSE {col} END"),
# MAGIC ('customers', 'customer_segment',    'segment',        "CASE WHEN {col} IN ('CONS', 'Cosumer') THEN 'Consumer' WHEN {col} = 'CORP' THEN 'Corporate' WHEN {col} = 'HO' THEN 'Home Office' ELSE {col} END"),
# MAGIC ('customers', 'country',             'country',        NULL),
# MAGIC ('customers', 'city',                'city',           NULL),
# MAGIC ('customers', 'state',               'state',          NULL),
# MAGIC ('customers', 'postal_code',         'postal_code',    NULL),
# MAGIC  ('customers','region',  'region',"CASE WHEN UPPER(TRIM({col})) = 'S' THEN 'South' WHEN UPPER(TRIM({col})) = 'C' THEN 'Central' WHEN UPPER(TRIM({col})) = 'W' THEN 'West' WHEN UPPER(TRIM({col})) = 'E' THEN 'East' WHEN UPPER(TRIM({col})) = 'N' THEN 'North' ELSE {col} END"
# MAGIC );
# MAGIC
# MAGIC INSERT INTO globalmart.config.column_mapping VALUES
# MAGIC ('orders', 'order_id',                      'order_id',                           NULL),
# MAGIC ('orders', 'customer_id',                   'customer_id',                        NULL),
# MAGIC ('orders', 'vendor_id',                     'vendor_id',                          NULL),
# MAGIC ('orders', 'ship_mode',                     'ship_mode',                          NULL),
# MAGIC ('orders', 'order_status',                  'order_status',                       NULL),
# MAGIC ('orders', 'order_purchase_date',           'order_purchase_timestamp',           "COALESCE(TRY_TO_TIMESTAMP({col}, 'MM/dd/yyyy HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd H:mm'))"),
# MAGIC ('orders', 'order_approved_at',             'order_approved_timestamp',           "COALESCE(TRY_TO_TIMESTAMP({col}, 'MM/dd/yyyy HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd H:mm'))"),
# MAGIC ('orders', 'order_delivered_carrier_date',  'order_delivered_carrier_timestamp',  "COALESCE(TRY_TO_TIMESTAMP({col}, 'MM/dd/yyyy HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd H:mm'))"),
# MAGIC ('orders', 'order_delivered_customer_date', 'order_delivered_customer_timestamp', "COALESCE(TRY_TO_TIMESTAMP({col}, 'MM/dd/yyyy HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd H:mm'))"),
# MAGIC ('orders', 'order_estimated_delivery_date', 'order_estimated_delivery_timestamp', "COALESCE(TRY_TO_TIMESTAMP({col}, 'MM/dd/yyyy HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd HH:mm'), TRY_TO_TIMESTAMP({col}, 'yyyy-MM-dd H:mm'))");
# MAGIC
# MAGIC INSERT INTO globalmart.config.column_mapping VALUES
# MAGIC ('transactions', 'order_id',             'order_id',             NULL),
# MAGIC ('transactions', 'product_id',           'product_id',           NULL),
# MAGIC ('transactions', 'sales',                'sales',                "CAST(REGEXP_REPLACE({col}, '^\\$', '') AS DECIMAL(10,3))"),
# MAGIC ('transactions', 'quantity',             'quantity',             "CAST({col} AS INT)"),
# MAGIC ('transactions', 'discount',             'discount',             "CASE WHEN CAST(REGEXP_REPLACE({col}, '[^0-9\\.]', '') AS DECIMAL(10,3)) >= 1 THEN CAST(REGEXP_REPLACE({col}, '[^0-9\\.]', '') AS DECIMAL(10,3)) / 100.0 ELSE CAST(REGEXP_REPLACE({col}, '[^0-9\\.]', '') AS DECIMAL(10,3)) END"),
# MAGIC ('transactions', 'profit',               'profit',               "CAST(REGEXP_REPLACE({col}, '^\\$', '') AS DECIMAL(10,3))"),
# MAGIC ('transactions', 'payment_type',         'payment_type',         "CASE WHEN {col} = '?' THEN NULL ELSE {col} END"),
# MAGIC ('transactions', 'payment_installments', 'payment_installments', "CAST({col} AS INT)");
# MAGIC
# MAGIC INSERT INTO globalmart.config.column_mapping VALUES
# MAGIC ('returns', 'order_id',       'order_id',      NULL),
# MAGIC ('returns', 'orderid',        'order_id',      NULL),
# MAGIC ('returns', 'refund_amount',  'refund_amount', "CASE WHEN {col} = '?' THEN NULL ELSE CAST(REGEXP_REPLACE({col}, '^\\$|[^0-9\\.]', '') AS DECIMAL(10,2)) END"),
# MAGIC ('returns', 'amount',         'refund_amount', "CASE WHEN {col} = '?' THEN NULL ELSE CAST(REGEXP_REPLACE({col}, '^\\$|[^0-9\\.]', '') AS DECIMAL(10,2)) END"),
# MAGIC ('returns', 'return_date',    'return_date',   "CASE WHEN {col} IS NULL OR {col} = 'NULL' THEN NULL ELSE COALESCE(TO_DATE({col}, 'yyyy-MM-dd'), TO_DATE({col}, 'MM-dd-yyyy')) END"),
# MAGIC ('returns', 'date_of_return', 'return_date',   "CASE WHEN {col} IS NULL OR {col} = 'NULL' THEN NULL ELSE COALESCE(TO_DATE({col}, 'yyyy-MM-dd'), TO_DATE({col}, 'MM-dd-yyyy')) END"),
# MAGIC ('returns', 'return_reason',  'return_reason', "CASE WHEN {col} = '?' THEN NULL ELSE {col} END"),
# MAGIC ('returns', 'reason',         'return_reason', "CASE WHEN {col} = '?' THEN NULL ELSE {col} END"),
# MAGIC ('returns', 'return_status',  'return_status', "CASE WHEN {col} = 'PENDG' THEN 'Pending' WHEN {col} = 'APPRVD' THEN 'Approved' WHEN {col} = 'RJCTD' THEN 'Rejected' ELSE {col} END"),
# MAGIC ('returns', 'status',         'return_status', "CASE WHEN {col} = 'PENDG' THEN 'Pending' WHEN {col} = 'APPRVD' THEN 'Approved' WHEN {col} = 'RJCTD' THEN 'Rejected' ELSE {col} END");
# MAGIC
# MAGIC INSERT INTO globalmart.config.column_mapping VALUES
# MAGIC ('products', 'product_id',         'product_id',         NULL),
# MAGIC ('products', 'product_name',       'product_name',       NULL),
# MAGIC ('products', 'brand',              'brand',              NULL),
# MAGIC ('products', 'categories',         'categories',         NULL),
# MAGIC ('products', 'colors',             'colors',             NULL),
# MAGIC ('products', 'manufacturer',       'manufacturer',       NULL),
# MAGIC ('products', 'dimension',          'dimension',          NULL),
# MAGIC ('products', 'sizes',              'sizes',              NULL),
# MAGIC ('products', 'upc',                'upc',                NULL),
# MAGIC ('products', 'weight',             'weight',             NULL),
# MAGIC ('products', 'product_photos_qty', 'product_photos_qty', NULL),
# MAGIC ('products', 'dateadded',          'date_added',         "TRY_TO_TIMESTAMP({col})"),
# MAGIC ('products', 'dateupdated',        'date_updated',       "TRY_TO_TIMESTAMP({col})");
# MAGIC
# MAGIC INSERT INTO globalmart.config.column_mapping VALUES
# MAGIC ('vendors', 'vendor_id',   'vendor_id',   NULL),
# MAGIC ('vendors', 'vendor_name', 'vendor_name', NULL);
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 5: Seed dq_rules
# MAGIC -- =============================================================================
# MAGIC
# MAGIC INSERT INTO globalmart.config.dq_rules VALUES
# MAGIC ('CU-001', 'customers', 'customer_id_not_null',   'customer_id IS NOT NULL',                                     'CRITICAL', 'drop', 'null_primary_key',       'customer_id is null after coalescing all 4 regional ID variants'),
# MAGIC ('CU-002', 'customers', 'valid_customer_email',   'customer_email IS NOT NULL',                                  'WARNING',  'warn', 'missing_optional_field', 'customer_email is null - Region 5 has no email column, Regions 3 and 6 have gaps'),
# MAGIC ('CU-003', 'customers', 'valid_customer_name',    'customer_name IS NOT NULL',                                   'WARNING',  'warn', 'missing_optional_field', 'customer_name is null after coalescing customer_name and full_name'),
# MAGIC ('CU-004', 'customers', 'valid_segment',          "segment IN ('Consumer', 'Corporate', 'Home Office')",         'WARNING',  'warn', 'invalid_category',       'segment value not in allowed set after abbreviation and typo mapping'),
# MAGIC ('CU-005','customers','detect_location_swap', "(state RLIKE '^[A-Z]{2}$') OR (LENGTH(state) > 2 AND state NOT LIKE '% %' AND state RLIKE '^[A-Z][a-z]+$')",  'WARNING','quarantine','structural_anomaly','State column failed validation (Expected 2-letter code). Likely City/State data swap detected (Region 4 anomaly)'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO globalmart.config.dq_rules VALUES
# MAGIC ('OR-001', 'orders', 'order_id_not_null',         'order_id IS NOT NULL',                                        'CRITICAL', 'drop', 'null_primary_key',       'order_id is null'),
# MAGIC ('OR-002', 'orders', 'valid_purchase_date',       'order_purchase_timestamp IS NOT NULL',                        'WARNING',  'warn', 'format_error',           'order_purchase_date could not be parsed from either MM/dd/yyyy or yyyy-MM-dd format'),
# MAGIC ('OR-003', 'orders', 'valid_ship_mode',           'ship_mode IS NOT NULL',                                       'WARNING',  'warn', 'missing_optional_field', 'ship_mode is null - anomalously high in orders_2 (11.4%)'),
# MAGIC ('OR-004', 'orders', 'valid_delivery_customer',   'order_delivered_customer_timestamp IS NOT NULL',              'WARNING',  'warn', 'missing_optional_field', 'order_delivered_customer_date is null - expected for undelivered orders'),
# MAGIC ('OR-005', 'orders', 'valid_delivery_carrier',    'order_delivered_carrier_timestamp IS NOT NULL',               'WARNING',  'warn', 'missing_optional_field', 'order_delivered_carrier_date is null - expected for in-transit orders');
# MAGIC
# MAGIC INSERT INTO globalmart.config.dq_rules VALUES
# MAGIC ('TR-001', 'transactions', 'order_id_not_null',   'order_id IS NOT NULL',                                        'CRITICAL', 'drop', 'null_foreign_key',       'order_id is null - transaction cannot be linked to any order'),
# MAGIC ('TR-002', 'transactions', 'product_id_not_null', 'product_id IS NOT NULL',                                      'CRITICAL', 'drop', 'null_foreign_key',       'product_id is null - transaction cannot be linked to any product'),
# MAGIC ('TR-003', 'transactions', 'valid_sales',         'sales > 0 AND sales IS NOT NULL',                             'WARNING',  'warn', 'format_error',           'sales value is negative, zero, or null after stripping $ prefix and casting to decimal'),
# MAGIC ('TR-004', 'transactions', 'valid_profit',        'profit IS NOT NULL',                                          'WARNING',  'warn', 'missing_optional_field', 'profit is null or not parsable after stripping $ prefix and casting to decimal'),
# MAGIC ('TR-005', 'transactions', 'valid_discount',      'discount IS NOT NULL AND discount >= 0 AND discount < 1',    'WARNING',  'warn', 'missing_optional_field', 'discount is out of normalized range [0, 1) after scale correction — value may still be in whole-number format (e.g. 20 instead of 0.20)'),
# MAGIC ('TR-006', 'transactions', 'valid_payment_type',  'payment_type IS NOT NULL',                                    'WARNING',  'warn', 'invalid_category',       'payment_type is null after replacing ? placeholders'),
# MAGIC ('TR-007', 'transactions', 'valid_quantity',      'quantity IS NOT NULL',                                        'WARNING',  'warn', 'missing_optional_field', 'quantity is null or not parsable as integer'),
# MAGIC ('TR-008', 'transactions', 'valid_profit_value',  'profit >= 0',                                                 'WARNING',  'warn', 'format_error',           'profit value is negative after casting to decimal');
# MAGIC
# MAGIC INSERT INTO globalmart.config.dq_rules VALUES
# MAGIC ('RE-001', 'returns', 'order_id_not_null',        'order_id IS NOT NULL',                                        'CRITICAL', 'drop', 'null_foreign_key',       'order_id is null after coalescing order_id and orderid'),
# MAGIC ('RE-002', 'returns', 'valid_return_status',      "return_status IN ('Pending', 'Approved', 'Rejected')",        'WARNING',  'warn', 'invalid_category',       'return_status not in allowed set after abbreviation mapping'),
# MAGIC ('RE-003', 'returns', 'valid_return_reason',      'return_reason IS NOT NULL',                                   'WARNING',  'warn', 'missing_optional_field', 'return_reason is null after replacing ? placeholders'),
# MAGIC ('RE-004', 'returns', 'valid_refund_amount',      'refund_amount > 0',                                           'WARNING',  'warn', 'format_error',           'refund_amount value is negative or zero - violates financial audit integrity');
# MAGIC
# MAGIC INSERT INTO globalmart.config.dq_rules VALUES
# MAGIC ('PR-001', 'products', 'product_id_not_null',     'product_id IS NOT NULL',                                      'CRITICAL', 'drop', 'null_primary_key',       'product_id is null'),
# MAGIC ('PR-002', 'products', 'valid_product_name',      'product_name IS NOT NULL',                                    'WARNING',  'warn', 'missing_optional_field', 'product_name is null - product is not human-identifiable in reports'),
# MAGIC ('PR-003', 'products', 'valid_brand',             'brand IS NOT NULL',                                           'WARNING',  'warn', 'missing_optional_field', 'brand is null - affects product performance reporting');
# MAGIC
# MAGIC INSERT INTO globalmart.config.dq_rules VALUES
# MAGIC ('VE-001', 'vendors', 'vendor_id_not_null',       'vendor_id IS NOT NULL',                                       'CRITICAL', 'drop', 'null_primary_key',       'vendor_id is null'),
# MAGIC ('VE-002', 'vendors', 'valid_vendor_name',        'vendor_name IS NOT NULL',                                     'WARNING',  'warn', 'missing_optional_field', 'vendor_name is null - vendor is not identifiable in reports');
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 6: Seed mdm_rules (formerly survivorship_rules)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC INSERT INTO globalmart.config.mdm_rules VALUES
# MAGIC ('customers', 'customer_email', 'most_complete',   NULL),
# MAGIC ('customers', 'customer_name',  'source_priority', '["customers_1","customers_2","customers_3","customers_4","customers_5","customers_6"]'),
# MAGIC ('customers', 'segment',        'source_priority', '["customers_1","customers_2","customers_3","customers_4","customers_5","customers_6"]'),
# MAGIC ('customers', 'city',           'most_recent',     NULL),
# MAGIC ('customers', 'state',          'most_recent',     NULL),
# MAGIC ('customers', 'postal_code',    'most_recent',     NULL),
# MAGIC ('customers', 'region',         'source_priority', '["customers_1","customers_2","customers_3","customers_4","customers_5","customers_6"]'),
# MAGIC ('customers', 'country',        'source_priority', '["customers_1","customers_2","customers_3","customers_4","customers_5","customers_6"]');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- STEP 7: Verify seed data
# MAGIC -- =============================================================================
# MAGIC SELECT entity, COUNT(*) AS mapping_count FROM globalmart.config.column_mapping GROUP BY entity ORDER BY entity;
# MAGIC SELECT entity, severity, COUNT(*) AS rule_count FROM globalmart.config.dq_rules GROUP BY entity, severity ORDER BY entity, severity;
# MAGIC SELECT * FROM globalmart.config.mdm_rules;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO globalmart.config.fraud_rule_config VALUES
# MAGIC -- Rule 1: High return volume
# MAGIC ('high_return_volume', 5, 20, true,
# MAGIC  'Total returns >= 5. Dataset: 32 customers qualify. Top customer: 8 returns.',
# MAGIC  'A customer with 5 or more returns is in the top 10% of the customer base by return count. While some returns are legitimate, high volume increases the chance that at least one is fraudulent and makes manual review worthwhile.'),
# MAGIC -- Rule 2: High total refund value
# MAGIC ('high_refund_value', 1000, 10, true,
# MAGIC  'Total refund_amount >= $1000. Dataset: 43 customers. Dataset mean = $584.',
# MAGIC  'A customer who has received over $1,000 in refunds is above the dataset average of $584. High refund totals increase GlobalMart financial exposure and warrant a closer look at whether the returned items were genuinely defective or unused.'),
# MAGIC -- Rule 3: High order return rate
# MAGIC ('high_order_return_rate', 0.3, 10, true,
# MAGIC  'total_returns / total_orders >= 30%. Always 0-100%. Max in dataset = 50%.',
# MAGIC  'Returning 3 in every 10 orders is significantly above normal shopping behaviour. Customers with a high return rate are more likely to be exploiting the returns policy — for example, buying items for temporary use and then returning them.'),
# MAGIC -- Rule 4: Refund inflation
# MAGIC ('refund_inflation', 500, 20, true,
# MAGIC  'SUM(refund_amount - original_sales) >= $500. Catches full-order refund on partial return.',
# MAGIC  'Refund inflation occurs when a customer receives back more money than the returned product was worth. This happens when a full order refund is issued for a single item return. A cumulative gap of $500 or more is a strong indicator that GlobalMart has overpaid this customer across multiple returns.'),
# MAGIC -- Rule 5: Return filed before delivery
# MAGIC ('return_before_delivery', 1, 10, true,
# MAGIC  'return_date < order_delivered_date. 221 returns affected, avg gap = 563 days.',
# MAGIC  'Filing a return before an order is delivered is physically impossible under normal circumstances. When this occurs it suggests either a timestamp manipulation attempt, a system exploit where return forms are submitted before delivery, or a legacy data format issue from one of the 6 regional systems.'),
# MAGIC -- Rule 6: Duplicate Order return reason
# MAGIC ('duplicate_order_reason', 1, 10, true,
# MAGIC  'return_reason = Duplicate Order. 31 returns, 28 customers in dataset.',
# MAGIC  'Claiming an order was a duplicate is a known tactic to obtain a refund on a deliberate purchase. Only 31 such returns exist across the entire dataset, making each individual case more suspicious. This reason should trigger a check of whether the claimed duplicate order actually exists.'),
# MAGIC -- Rule 7: Same Day shipping return
# MAGIC ('same_day_ship_return', 1, 5, true,
# MAGIC  'Return on Same Day shipping order. 16.9% rate vs 16.0% average. Weak standalone.',
# MAGIC  'Customers who return Same Day orders return at a slightly higher rate (16.9%) than average (16.0%). This is a weak signal on its own — a customer triggering only this rule will not reach the flag threshold. It is included as a tiebreaker when a customer is near the threshold on other rules.'),
# MAGIC -- Rule 8: No matching order
# MAGIC ('no_matching_order', 1, 15, true,
# MAGIC  'Return has no matching order_id in fact_orders. Unmatched return = potential phantom return.',
# MAGIC  'A return that has no matching order in GlobalMart order records cannot be verified as a legitimate purchase. This could mean the customer returned an item they never bought from GlobalMart, is exploiting a loose return policy, or that the order data is missing due to a regional system gap. Each unmatched return must be verified manually.'),
# MAGIC -- Flag threshold: not a rule — just the scoring cutoff
# MAGIC ('flag_threshold', 40, 0, true,
# MAGIC  'Customers scoring >= 40 are flagged for investigation. Requires triggering at least 2 rules.',
# MAGIC  'The flag threshold is the minimum total score needed to be added to the investigation queue. At 40 points, a customer must trigger at least 2 rules to be flagged, preventing single-rule false positives. The threshold was chosen to keep the flagged list manageable for a team reviewing 40-60 returns daily.');
# MAGIC

# COMMAND ----------

