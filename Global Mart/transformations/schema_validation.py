# Databricks notebook source
# =============================================================================
# GlobalMart — Schema Contract Validation (SDP)
# Target Schema: globalmart.raw
#
# Reads Bronze streaming tables via dp.read(), validates schemas against
# minimum contracts, outputs a streaming table with results.
# Runs as part of the Silver pipeline (not Bronze — avoids circular refs).
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
    name="globalmart.gold.schema_validation_current",
    comment="Post-Bronze schema contract validation. One row per entity per run."
)
def schema_validation_current():
    results = []

    for entity, c in CONTRACTS.items():
        bronze = spark.read.table(f"globalmart.bronze.{entity}")
        cols_lower = [col_name.lower() for col_name in bronze.columns]
        skip = {"source_file", "load_timestamp", "_source_file", "_load_timestamp", "rescued_data"}
        data_cols = [x for x in cols_lower if x not in skip]

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
            entity=entity,
            status=overall,
            data_columns=len(data_cols),
            columns_found=", ".join(sorted(data_cols)),
            key_check="PASS" if key_ok else "WARN",
            required_check="PASS" if req_ok else "WARN",
            min_columns_check="PASS" if min_ok else "WARN",
            issues="; ".join(issues) if issues else "None",
            _run_id=_RUN_ID
        ))

    return (
        spark.createDataFrame(results)
        .withColumn("validated_at", current_timestamp())
    )