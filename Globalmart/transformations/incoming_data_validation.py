# Databricks notebook source
# =============================================================================
# GlobalMart — Schema Contract Validation (SDP)
# Pipeline Type: Spark Declarative Pipelines
# Target Schema: globalmart.raw
#
# Post-Bronze validation that reads Bronze tables (via dp.read) and checks
# them against minimum schema contracts. Logs results as an SDP table.
# Never blocks the pipeline — observe only.
#
# This runs AFTER Bronze tables are populated, validating what landed.
# Uses dp.read() to reference Bronze tables within the same pipeline.
# =============================================================================

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.sql import Row
import uuid

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA raw")

_RUN_ID = str(uuid.uuid4())

CONTRACTS = {
    "customers": {
        "key_variants": ["customer_id", "customerid", "cust_id", "customer_identifier"],
        "required": ["country", "region"],
        "min_cols": 7
    },
    "orders": {
        "key_variants": ["order_id"],
        "required": ["order_status"],
        "min_cols": 9
    },
    "transactions": {
        "key_variants": ["order_id"],
        "required": ["quantity", "discount"],
        "min_cols": 6
    },
    "returns": {
        "key_variants": ["order_id", "orderid"],
        "required": [],
        "min_cols": 4
    },
    "products": {
        "key_variants": ["product_id"],
        "required": ["product_name"],
        "min_cols": 5
    },
    "vendors": {
        "key_variants": ["vendor_id"],
        "required": ["vendor_name"],
        "min_cols": 2
    }
}

# COMMAND ----------

@dp.table(
    name="schema_validation_current",
    comment="Post-Bronze schema contract validation. Checks Bronze tables against minimum expectations. Never blocks — observe only."
)
def schema_validation_current():
    results = []
    run_id = _RUN_ID
    contracts = CONTRACTS

    # Read each Bronze table — separate pipeline, so use spark.read.table()
    bronze_tables = {
        "customers": spark.read.table("globalmart.bronze.customers"),
        "orders": spark.read.table("globalmart.bronze.orders"),
        "transactions": spark.read.table("globalmart.bronze.transactions"),
        "returns": spark.read.table("globalmart.bronze.returns"),
        "products": spark.read.table("globalmart.bronze.products"),
        "vendors": spark.read.table("globalmart.bronze.vendors"),
    }

    for entity, c in contracts.items():
        try:
            bronze = bronze_tables[entity]
            cols_lower = [col_name.lower() for col_name in bronze.columns]
            col_count = len(cols_lower)

            # Check min columns (excluding metadata cols)
            data_cols = [c for c in cols_lower if not c.startswith("_") and c not in ["source_file", "load_timestamp", "rescued_data"]]
            data_col_count = len(data_cols)
            min_ok = data_col_count >= c["min_cols"]

            # Check key column
            key_ok = any(v in cols_lower for v in c["key_variants"])

            # Check required columns
            missing = [r for r in c["required"] if r not in cols_lower]
            req_ok = len(missing) == 0

            overall = "PASS" if all([min_ok, key_ok, req_ok]) else "WARN"

            issues = []
            if not min_ok:
                issues.append(f"Data column count {data_col_count} below minimum {c['min_cols']}")
            if not key_ok:
                issues.append(f"Missing key column — need one of: {c['key_variants']}")
            if not req_ok:
                issues.append(f"Missing required columns: {missing}")

            results.append({
                "entity": entity,
                "status": overall,
                "total_columns": col_count,
                "data_columns": data_col_count,
                "columns_found": ", ".join(sorted(data_cols)),
                "key_check": "PASS" if key_ok else "WARN",
                "required_check": "PASS" if req_ok else "WARN",
                "min_columns_check": "PASS" if min_ok else "WARN",
                "issues": "; ".join(issues) if issues else "None"
            })

        except Exception as e:
            results.append({
                "entity": entity,
                "status": "ERROR",
                "total_columns": 0,
                "data_columns": 0,
                "columns_found": "",
                "key_check": "ERROR",
                "required_check": "ERROR",
                "min_columns_check": "ERROR",
                "issues": str(e)
            })

    return (
        spark.createDataFrame(results)
        .withColumn("validated_at", current_timestamp())
        .withColumn("_run_id", lit(run_id))
    )