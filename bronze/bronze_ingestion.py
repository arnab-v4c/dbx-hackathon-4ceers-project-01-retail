# Databricks notebook source
# MAGIC %md
# MAGIC # GlobalMart Bronze Layer — Raw Ingestion Pipeline
# MAGIC
# MAGIC **Pipeline type:** Spark Declarative Pipeline (SDP)
# MAGIC **Ingestion method:** Auto Loader (`cloudFiles`) — incremental; only new files are processed on each run
# MAGIC **Dataset type:** Streaming Tables — tracks checkpoint state so re-runs add zero duplicate records
# MAGIC
# MAGIC ## Design Decisions
# MAGIC
# MAGIC | Decision | Rationale |
# MAGIC |---|---|
# MAGIC | **Auto Loader** | Tracks which files have been loaded via checkpoints. New files only — no full reprocessing. |
# MAGIC | **All CSV columns as STRING** | Bronze layer preserves raw data exactly as received. No type casting. |
# MAGIC | **JSON with natural types** | JSON values carry their own types (string, int). Preserving them is "as received". |
# MAGIC | **schemaEvolutionMode = addNewColumns** | Handles schema mismatches across regions. New/different columns are added, not rejected. |
# MAGIC | **recursiveFileLookup = true** | Picks up files from both Region sub-folders and root directory in one pass. |
# MAGIC | **_source_file + _load_timestamp** | Every record is traceable to its source file and ingestion time. |
# MAGIC | **schemaLocation** | Stores inferred schema in a dedicated volume so Auto Loader can track schema evolution across runs. |
# MAGIC
# MAGIC ## Tables Created
# MAGIC
# MAGIC | Table | Sources | Format | Notes |
# MAGIC |---|---|---|---|
# MAGIC | `customers` | customers_1.csv through customers_6.csv | CSV | 6 files, 4 different ID column names, 1 file missing email |
# MAGIC | `orders` | orders_1.csv through orders_3.csv | CSV | 3 files, orders_3 missing estimated_delivery_date |
# MAGIC | `transactions` | transactions_1.csv through transactions_3.csv | CSV | 3 files, transactions_2 missing profit column |
# MAGIC | `returns` | returns_1.json, returns_2.json | JSON | 2 files, completely different column names |
# MAGIC | `products` | products.json | JSON | 1 file, high sparsity on optional fields |
# MAGIC | `vendors` | vendors.csv | CSV | 1 file, 7 rows reference data |

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

# Pipeline configuration — parameterized for portability across workspaces
RAW_PATH = spark.conf.get("raw_volume_path", "/Volumes/globalmart/raw/source_files")
SCHEMA_BASE = spark.conf.get("schema_location_base", "/Volumes/globalmart/bronze/pipeline_metadata/schemas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers — 6 CSV files from all regions
# MAGIC
# MAGIC Each region uses different column names for the same data:
# MAGIC - Region 1: `customer_id` | Region 2: `CustomerID` | Region 3: `cust_id`
# MAGIC - Region 4: `customer_id` (city/state swapped in column order)
# MAGIC - Region 5: `customer_id` (missing `customer_email` entirely — only 8 columns)
# MAGIC - Region 6: `customer_identifier`, `email_address`, `full_name`, `customer_segment`
# MAGIC
# MAGIC Auto Loader with `addNewColumns` creates a **superset schema** — every column from every
# MAGIC file appears in the table. Where a file lacks a column, the value is NULL. This preserves all
# MAGIC original data without any renaming or mapping (that happens in Silver).

# COMMAND ----------

@dp.table(
    name="customers",
    comment="Raw customer data from 6 regional systems. Schema varies across regions — superset of all columns preserved."
)
def customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/customers")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "customers_*.csv")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders — 3 CSV files from Regions 1, 3, 5
# MAGIC
# MAGIC All three share the same column names, but:
# MAGIC - `orders_3` is **missing `order_estimated_delivery_date`** (9 columns instead of 10)
# MAGIC - Date formats differ: `orders_1` uses `MM/DD/YYYY HH:MM`, others use `YYYY-MM-DD H:MM`
# MAGIC - Ship mode labels vary: "First Class" vs "1st Class"
# MAGIC
# MAGIC All loaded as-is; harmonization happens in Silver.

# COMMAND ----------

@dp.table(
    name="orders",
    comment="Raw order headers from 3 regional systems. orders_3 missing estimated_delivery_date column."
)
def orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/orders")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "orders_*.csv")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions — 3 CSV files from Regions 2, 4, and root
# MAGIC
# MAGIC Key inconsistencies preserved in bronze:
# MAGIC - Column casing varies: `Order_id` (txn 1 & 2) vs `Order_ID` (txn 3)
# MAGIC - `transactions_2` is **missing the `profit` column** (7 columns instead of 8)
# MAGIC - `transactions_1`: 20.5% of Sales values have `$` prefix (e.g., "$22.25")
# MAGIC - `transactions_2`: discount as percentage with `%` sign (e.g., "40%")
# MAGIC - `transactions_1`: `?` placeholder in payment_type (9.6% of rows)
# MAGIC
# MAGIC These are all loaded raw. Silver layer strips `$`, converts `%`, maps `?` to NULL.

# COMMAND ----------

@dp.table(
    name="transactions",
    comment="Raw transaction line items from 3 regional systems. transactions_2 missing profit column. Mixed formats in Sales and discount."
)
def transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/transactions")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "transactions_*.csv")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Returns — 2 JSON files (Region 6 + root)
# MAGIC
# MAGIC The two return files have **completely different column names**:
# MAGIC
# MAGIC | Canonical | returns_1.json | returns_2.json |
# MAGIC |---|---|---|
# MAGIC | order_id | `order_id` | `OrderId` |
# MAGIC | refund_amount | `refund_amount` | `amount` |
# MAGIC | return_date | `return_date` | `date_of_return` |
# MAGIC | return_reason | `return_reason` | `reason` |
# MAGIC | return_status | `return_status` | `status` |
# MAGIC
# MAGIC Auto Loader treats each differently-named column as a separate column in the superset
# MAGIC schema. The resulting bronze table has ~10 data columns (5 from each file), with NULLs
# MAGIC where one file's columns don't exist in the other.

# COMMAND ----------

@dp.table(
    name="returns",
    comment="Raw return/refund records from 2 regional systems. Completely different column names across files — superset preserved."
)
def returns():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/returns")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "returns_*.json")
        .option("multiLine", "true")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products — 1 JSON file (root)
# MAGIC
# MAGIC Single source of truth for the product catalog. High sparsity on optional fields:
# MAGIC - weight: 94% null | dimension: 88% null | sizes: 66% null
# MAGIC - manufacturer: 59% null | upc: 39% null
# MAGIC
# MAGIC `product_photos_qty` is the only non-string column (LONG) — naturally typed from JSON.

# COMMAND ----------

@dp.table(
    name="products",
    comment="Raw product catalog. Single JSON source. High sparsity on weight, dimension, sizes, manufacturer, upc."
)
def products():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/products")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "products.json")
        .option("multiLine", "true")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vendors — 1 CSV file (root)
# MAGIC
# MAGIC Reference table: 7 vendors (VEN01–VEN07). Only VEN01–VEN04 appear in orders.

# COMMAND ----------

@dp.table(
    name="vendors",
    comment="Vendor reference data. 7 vendors, only 4 referenced in orders."
)
def vendors():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/vendors")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "vendors.csv")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )
