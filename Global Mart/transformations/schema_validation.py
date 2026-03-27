# Databricks notebook source
# =============================================================================
# GlobalMart — Schema Contract Validation (SDP)
# Target Schema: globalmart.raw
#
# Reads raw files directly from the Unity Catalog Volume, validates individual
# file schemas against minimum contracts, and outputs a streaming table.
# Runs as a Pre-Bronze check to catch file-level anomalies before Auto Loader.
# =============================================================================

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import Row
import uuid

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA raw")

_RUN_ID = str(uuid.uuid4())

CONTRACTS = {
    "customers": {"key_variants": "customer_id,customerid,cust_id,customer_identifier", "required": "country,region", "min_cols": 7},
    "orders": {"key_variants": "order_id", "required": "order_status", "min_cols": 9},
    "transactions": {"key_variants": "order_id", "required": "quantity,discount", "min_cols": 6},
    "returns": {"key_variants": "order_id,orderid", "required": "", "min_cols": 4},
    "products": {"key_variants": "product_id", "required": "product_name", "min_cols": 5},
    "vendors": {"key_variants": "vendor_id", "required": "vendor_name", "min_cols": 2},
}

# COMMAND ----------

@dp.table(
    name="schema_validation_current",
    comment="Pre-Bronze schema contract validation. One row per source file per run."
)
def schema_validation_current():
    results = []

    # 1. Discover all raw files recursively from the Volume
    # This is a DLT-safe way to list files without using dbutils
    files_df = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load("/Volumes/globalmart/raw/source_files/")
    file_paths = [row.path for row in files_df.select("path").collect()]

    for path in file_paths:
        file_name = path.split("/")[-1]
        
        # 2. Determine which entity this file belongs to based on name/path
        entity = None
        for e in CONTRACTS.keys():
            if e in file_name.lower() or e in path.lower():
                entity = e
                break
                
        if not entity:
            continue
            
        c = CONTRACTS[entity]
        
        # 3. Read the exact schema of this specific file directly from the raw volume
        file_format = "json" if path.endswith(".json") else "csv"
        try:
            if file_format == "csv":
                file_df = spark.read.format("csv").option("header", "true").load(path)
            else:
                file_df = spark.read.format("json").load(path)
            
            data_cols = [col_name.lower() for col_name in file_df.columns]
        except Exception as e:
            data_cols = []
            
        key_variants = c["key_variants"].split(",")
        required = [r for r in c["required"].split(",") if r]

        min_ok = len(data_cols) >= c["min_cols"]
        key_ok = any(v in data_cols for v in key_variants)
        missing = [r for r in required if r not in data_cols]
        req_ok = len(missing) == 0

        overall = "PASS" if all([min_ok, key_ok, req_ok]) else "WARN"

        issues = []
        if not min_ok:
            issues.append(f"Columns {len(data_cols)} < minimum {c['min_cols']}")
        if not key_ok:
            issues.append(f"No key column from {key_variants}")
        if not req_ok:
            issues.append(f"Missing: {missing}")

        results.append(Row(
            file_name=file_name,
            entity=entity,
            file_path=path,
            status=overall,
            data_columns=len(data_cols),
            columns_found=", ".join(sorted(data_cols)),
            key_check="PASS" if key_ok else "WARN",
            required_check="PASS" if req_ok else "WARN",
            min_columns_check="PASS" if min_ok else "WARN",
            issues="; ".join(issues) if issues else "None",
            _run_id=_RUN_ID
        ))

    # Handle the edge case where no files exist yet to avoid PySpark schema inference errors
    if not results:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        schema = StructType([
            StructField("file_name", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("file_path", StringType(), True),
            StructField("status", StringType(), True),
            StructField("data_columns", IntegerType(), True),
            StructField("columns_found", StringType(), True),
            StructField("key_check", StringType(), True),
            StructField("required_check", StringType(), True),
            StructField("min_columns_check", StringType(), True),
            StructField("issues", StringType(), True),
            StructField("_run_id", StringType(), True)
        ])
        return spark.createDataFrame([], schema).withColumn("validated_at", current_timestamp())

    return (
        spark.createDataFrame(results)
        .withColumn("validated_at", current_timestamp())
    )