# Databricks notebook source
# MAGIC %md
# MAGIC # GlobalMart Platform Setup
# MAGIC
# MAGIC Creates the Unity Catalog structure for the GlobalMart data platform:
# MAGIC - **Catalog:** `globalmart`
# MAGIC - **Schemas:** `raw` (file storage), `bronze` (raw tables), `silver`, `gold`
# MAGIC - **Volume:** `raw.source_files` (stores the 16 source files from 6 regional systems)
# MAGIC - **Volume:** `bronze.pipeline_metadata` (Auto Loader schema inference metadata)
# MAGIC
# MAGIC Run this notebook **once** before deploying the bronze pipeline or uploading data.

# COMMAND ----------

catalog_name = "globalmart"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
print(f"Catalog '{catalog_name}' is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Schemas

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS raw COMMENT 'Raw file storage — source files from 6 regional systems, preserved as-is'")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingestion layer — streaming tables loaded via Auto Loader'")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Harmonized and quality-enforced layer — unified schemas, validated data'")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Business-ready dimensional model — star schema for analytics and Gen AI'")

print("Schemas created:")
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Volumes

# COMMAND ----------

# Volume for raw source files (CSV + JSON) — maintains original regional folder structure
spark.sql("CREATE VOLUME IF NOT EXISTS raw.source_files COMMENT 'Raw source files from 6 regional systems (CSV + JSON)'")

# Volume for Auto Loader schema inference metadata
spark.sql("CREATE VOLUME IF NOT EXISTS bronze.pipeline_metadata COMMENT 'Auto Loader schema inference metadata for bronze pipeline'")

print("Volumes created:")
display(spark.sql("SHOW VOLUMES IN raw"))
display(spark.sql("SHOW VOLUMES IN bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Setup

# COMMAND ----------

print(f"Raw files volume:    /Volumes/{catalog_name}/raw/source_files/")
print(f"Schema metadata:     /Volumes/{catalog_name}/bronze/pipeline_metadata/schemas/")
print()
print("Expected folder structure in raw.source_files:")
print("  Region 1/ -> customers_1.csv, orders_1.csv")
print("  Region 2/ -> customers_2.csv, transactions_1.csv")
print("  Region 3/ -> customers_3.csv, orders_2.csv")
print("  Region 4/ -> customers_4.csv, transactions_2.csv")
print("  Region 5/ -> customers_5.csv, orders_3.csv")
print("  Region 6/ -> customers_6.csv, returns_1.json")
print("  (root)    -> products.json, returns_2.json, transactions_3.csv, vendors.csv")
