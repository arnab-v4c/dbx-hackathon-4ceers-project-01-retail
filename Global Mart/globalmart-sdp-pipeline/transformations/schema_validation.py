# =============================================================================
# GlobalMart — Schema Contract Validation (SDP)
# Target Schema: globalmart.raw
#
# PURPOSE:
#   Pre-Bronze contract check. Evaluates raw files in Unity Catalog Volumes
#   BEFORE Auto Loader ingestion. Flags file-level anomalies (missing columns,
#   missing keys) as structured rows for review.
#
# SDP-SAFE:
#   Uses binaryFile for file discovery and Spark SQL for all validation.
#   No .collect() calls inside the @dp.table function.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    lower, size, array, array_contains, array_intersect,
    split, transform, trim, concat_ws, when, length
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utilities.utils import CONTRACTS
import uuid

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA raw")

_RUN_ID = str(uuid.uuid4())

# Volume path for raw source files
_RAW_VOLUME = "/Volumes/globalmart/raw/source_files/"


def _validate_single_file(spark, path: str, file_name: str) -> dict:
    """
    Reads a single file's schema and validates against contracts.
    Called from the driver but NOT inside a @dp.table — it's invoked
    by _build_validation_df which is a helper, not a decorated function.
    """
    # Match file to entity
    entity = None
    for e in CONTRACTS:
        if e in file_name.lower() or e in path.lower():
            entity = e
            break
    if not entity:
        return None

    c = CONTRACTS[entity]
    file_format = "json" if path.endswith(".json") else "csv"

    try:
        if file_format == "csv":
            file_df = spark.read.format("csv").option("header", "true").load(path)
        else:
            file_df = spark.read.format("json").load(path)
        data_cols = [cn.lower() for cn in file_df.columns]
    except Exception:
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
        issues.append(f"Missing required columns: {missing}")

    return {
        "file_name": file_name,
        "entity": entity,
        "file_path": path,
        "status": overall,
        "data_columns": len(data_cols),
        "columns_found": ", ".join(sorted(data_cols)),
        "key_check": "PASS" if key_ok else "WARN",
        "required_check": "PASS" if req_ok else "WARN",
        "min_columns_check": "PASS" if min_ok else "WARN",
        "issues": "; ".join(issues) if issues else "None",
        "_run_id": _RUN_ID,
    }


# Pre-compute validation results at module level (outside @dp.table).
# binaryFile discovery + per-file schema reads happen here, keeping
# the decorated function free of .collect() calls.

_EMPTY_SCHEMA = StructType([
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
    StructField("_run_id", StringType(), True),
])

# Discover files and validate at module level
_files_df = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(_RAW_VOLUME)
_file_paths = [row.path for row in _files_df.select("path").collect()]

_validation_results = []
for _p in _file_paths:
    _fn = _p.split("/")[-1]
    _result = _validate_single_file(spark, _p, _fn)
    if _result is not None:
        _validation_results.append(_result)


@dp.table(
    name="schema_validation_bronze",
    comment="Pre-Bronze schema contract validation. One row per source file per run."
)
def schema_validation_bronze():
    """
    Returns pre-computed validation results as a DataFrame.
    All .collect() and file I/O happened at module level — this function
    only wraps the results into a DataFrame, satisfying SDP constraints.
    """
    if not _validation_results:
        return spark.createDataFrame([], _EMPTY_SCHEMA).withColumn("validated_at", current_timestamp())

    return (
        spark.createDataFrame(_validation_results, _EMPTY_SCHEMA)
        .withColumn("validated_at", current_timestamp())
    )
