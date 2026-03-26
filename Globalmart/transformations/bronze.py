# Databricks notebook source
# =============================================================================
# GlobalMart — Bronze Layer Ingestion Pipeline
# Target Schema: globalmart.bronze
# Pipeline Type: Spark Declarative Pipelines (SDP)
#
# Ingests raw CSV and JSON files from the Unity Catalog Volume into
# Delta tables — one table per entity. No transformations applied
# beyond column name sanitization for safe downstream access.
#
# Key design decisions:
#   - Auto Loader (cloudFiles) for incremental file ingestion
#   - schemaEvolutionMode: addNewColumns to handle schema drift
#   - inferColumnTypes: false (CSV) to preserve all data as strings
#   - recursiveFileLookup: true to unify files across regional folders
#   - _source_file and _load_timestamp on every record for lineage
#   - Column name sanitization via shared utility to handle spaces
#     and special characters at the point of ingestion
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col
from utilities.utils import sanitize_column_names

# -- Configuration -----------------------------------------------------------
# Paths are parameterized via pipeline settings for portability.
# Defaults point to the standard Volume and metadata locations.

RAW_PATH = spark.conf.get(
    "raw_volume_path",
    "/Volumes/globalmart/raw/source_files/GlobalMart_Retail_Data"
)
SCHEMA_BASE = spark.conf.get(
    "schema_location_base",
    "/Volumes/globalmart/bronze/pipeline_metadata/schemas"
)

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA bronze")

# =============================================================================
# CUSTOMERS — 6 regional CSV files → 1 Bronze table
# =============================================================================
# Source files: customers_1.csv through customers_6.csv
# Schema varies across regions:
#   - 4 different ID column names (customer_id, CustomerID, cust_id, customer_identifier)
#   - customers_5 missing customer_email entirely
#   - customers_6 uses full_name, email_address, customer_segment
#   - customers_4 has state/city column order swapped
# All columns land as-is. Harmonization is deferred to Silver.

@dp.table(
    name="customers"
)
def customers():
    return sanitize_column_names(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/customers")
        .option("readerCaseSensitive", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "customers_*.csv")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )


# =============================================================================
# ORDERS — 3 regional CSV files → 1 Bronze table
# =============================================================================
# Source files: orders_1.csv through orders_3.csv
# orders_3 is missing order_estimated_delivery_date (9 cols vs 10).
# Auto Loader adds it as a NULL column via addNewColumns.
# All date columns remain as strings at Bronze.

@dp.table(
    name="orders"
)
def orders():
    return sanitize_column_names(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/orders")
        .option("readerCaseSensitive", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "orders_*.csv")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )


# =============================================================================
# TRANSACTIONS — 3 regional CSV files → 1 Bronze table
# =============================================================================
# Source files: transactions_1.csv through transactions_3.csv
# Known issues preserved at Bronze (cleaned in Silver):


@dp.table(
    name="transactions"
)
def transactions():
    return sanitize_column_names(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/transactions")
        .option("readerCaseSensitive", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "transactions_*.csv")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )


# =============================================================================
# RETURNS — 2 regional JSON files → 1 Bronze table
# =============================================================================
# Source files: returns_1.json, returns_2.json
# Every column name differs between the two files:
#   order_id / OrderId, refund_amount / amount,
#   return_date / date_of_return, return_reason / reason,
#   return_status / status
# inferColumnTypes: true for JSON — required for correct parsing.
# All variant column names land as separate columns; merged in Silver.

@dp.table(
    name="returns"
)
def returns():
    return sanitize_column_names(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/returns")
        .option("readerCaseSensitive", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "returns_*.json")
        .option("multiLine", "true")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )


# =============================================================================
# PRODUCTS — 1 JSON file → 1 Bronze table
# =============================================================================
# Source file: products.json
# High sparsity: weight (94%), dimension (88%), sizes (66%),
# manufacturer (59%), upc (39%) are mostly null.
# product_photos_qty is the only non-string column in the source.
# inferColumnTypes: true for JSON — required for correct parsing.

@dp.table(
    name="products"
   
)
def products():
    return sanitize_column_names(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/products")
        .option("readerCaseSensitive", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "products.json")
        .option("multiLine", "true")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )

# =============================================================================
# VENDORS — 1 CSV file → 1 Bronze table
# =============================================================================
# Source file: vendors.csv
# Simple reference table: 7 rows, 2 columns (vendor_id, vendor_name).
# Only 4 of 7 vendors are referenced in orders (VEN01–VEN04).

@dp.table(
    name="vendors"
)
def vendors():
    return sanitize_column_names(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/vendors")
        .option("readerCaseSensitive", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "vendors.csv")
        .load(RAW_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_load_timestamp", current_timestamp())
    )