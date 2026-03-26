# Databricks notebook source
# =============================================================================
# GlobalMart — Silver Layer Pipeline (LLM-Driven Schema Mapping)
# Pipeline Type: Spark Declarative Pipelines (SDP)
# Target Schema: globalmart.silver
#
# Instead of reading static column_mapping from ingestion_control, this
# pipeline uses an LLM to dynamically discover schema mappings from Bronze
# table metadata + sample data. The LLM generates the same mapping structure
# (source_column → canonical_column + transformation) that the config-driven
# pipeline used — but without any hardcoded mappings.
#
# DQ rules remain config-driven (business decisions, not schema discovery).
#
# Flow:
#   1. For each Bronze entity, collect column names + 5 sample rows
#   2. Send to LLM with target Silver schema definition
#   3. LLM returns JSON mappings + transformations
#   4. Parse into CONFIG_MAPPINGS dict (same format as static config)
#   5. Harmonize + quarantine using existing engines
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, coalesce, expr, lit, current_timestamp, when,
    monotonically_increasing_id
)
from pyspark.sql import DataFrame
import uuid
import json

# COMMAND ----------

spark.sql("USE CATALOG globalmart_new")
spark.sql("USE SCHEMA silver")

RUN_ID = str(uuid.uuid4())

# =============================================================================
# LLM CLIENT SETUP — Databricks ML Gateway (mlflow.deployments)
# No tokens needed — uses workspace-level auth automatically.
# =============================================================================

import mlflow.deployments

LLM_CLIENT = mlflow.deployments.get_deploy_client("databricks")
LLM_MODEL = "databricks-gpt-oss-20b"

# =============================================================================
# TARGET SILVER SCHEMAS — What the LLM maps TO
# =============================================================================
# These define the canonical Silver columns and their expected types.
# The LLM's job is to figure out which Bronze columns map to these.

TARGET_SCHEMAS = {
    "customers": {
        "columns": {
            "customer_id": {"type": "STRING", "role": "primary_key", "description": "Unique customer identifier, format XX-NNNNN"},
            "customer_email": {"type": "STRING", "role": "optional", "description": "Customer email address"},
            "customer_name": {"type": "STRING", "role": "required", "description": "Full name of the customer"},
            "segment": {"type": "STRING", "role": "required", "description": "Business segment: must be one of Consumer, Corporate, Home Office. May appear as abbreviations (CONS, CORP, HO) or typos (Cosumer)"},
            "country": {"type": "STRING", "role": "optional"},
            "city": {"type": "STRING", "role": "optional"},
            "state": {"type": "STRING", "role": "optional"},
            "postal_code": {"type": "STRING", "role": "optional"},
            "region": {"type": "STRING", "role": "optional"}
        }
    },
    "orders": {
        "columns": {
            "order_id": {"type": "STRING", "role": "primary_key"},
            "customer_id": {"type": "STRING", "role": "foreign_key"},
            "vendor_id": {"type": "STRING", "role": "foreign_key"},
            "ship_mode": {"type": "STRING", "role": "optional"},
            "order_status": {"type": "STRING", "role": "required"},
            "order_purchase_timestamp": {"type": "TIMESTAMP", "role": "required", "description": "May be in MM/dd/yyyy HH:mm or yyyy-MM-dd HH:mm format"},
            "order_approved_timestamp": {"type": "TIMESTAMP", "role": "optional", "description": "Same dual format as purchase date"},
            "order_delivered_carrier_timestamp": {"type": "TIMESTAMP", "role": "optional"},
            "order_delivered_customer_timestamp": {"type": "TIMESTAMP", "role": "optional"},
            "order_estimated_delivery_timestamp": {"type": "TIMESTAMP", "role": "optional"}
        }
    },
    "transactions": {
        "columns": {
            "order_id": {"type": "STRING", "role": "foreign_key"},
            "product_id": {"type": "STRING", "role": "foreign_key"},
            "sales": {"type": "DECIMAL(10,2)", "role": "required", "description": "Sale amount. May have $ prefix that needs stripping"},
            "quantity": {"type": "INT", "role": "required"},
            "discount": {"type": "DECIMAL(5,2)", "role": "optional"},
            "profit": {"type": "DECIMAL(10,2)", "role": "optional", "description": "May be missing entirely from some sources"},
            "payment_type": {"type": "STRING", "role": "optional", "description": "May contain ? placeholder meaning unknown"},
            "payment_installments": {"type": "INT", "role": "optional"}
        }
    },
    "returns": {
        "columns": {
            "order_id": {"type": "STRING", "role": "foreign_key", "description": "Links to orders table"},
            "refund_amount": {"type": "DECIMAL(10,2)", "role": "required", "description": "May have $ prefix"},
            "return_date": {"type": "DATE", "role": "required", "description": "Format yyyy-MM-dd"},
            "return_reason": {"type": "STRING", "role": "optional", "description": "May contain ? placeholder meaning unknown"},
            "return_status": {"type": "STRING", "role": "required", "description": "Must be Pending, Approved, or Rejected. May appear as abbreviations: PENDG, APPRVD, RJCTD"}
        }
    },
    "products": {
        "columns": {
            "product_id": {"type": "STRING", "role": "primary_key"},
            "product_name": {"type": "STRING", "role": "required"},
            "brand": {"type": "STRING", "role": "optional"},
            "categories": {"type": "STRING", "role": "optional"},
            "colors": {"type": "STRING", "role": "optional"},
            "manufacturer": {"type": "STRING", "role": "optional"},
            "dimension": {"type": "STRING", "role": "optional"},
            "sizes": {"type": "STRING", "role": "optional"},
            "upc": {"type": "STRING", "role": "optional"},
            "weight": {"type": "STRING", "role": "optional"},
            "product_photos_qty": {"type": "BIGINT", "role": "optional"},
            "date_added": {"type": "TIMESTAMP", "role": "optional", "description": "ISO 8601 format"},
            "date_updated": {"type": "TIMESTAMP", "role": "optional", "description": "ISO 8601 format"}
        }
    },
    "vendors": {
        "columns": {
            "vendor_id": {"type": "STRING", "role": "primary_key"},
            "vendor_name": {"type": "STRING", "role": "required"}
        }
    }
}

# COMMAND ----------

# =============================================================================
# LLM SCHEMA MAPPER — Calls Foundation Model to generate mappings
# =============================================================================

def get_bronze_profile(entity: str) -> dict:
    """
    Reads a Bronze table and returns column names, types, and sample values
    for the LLM to analyze.
    """
    df = spark.read.table(f"globalmart.bronze.{entity}")

    profile = {"entity": entity, "columns": []}
    # Exclude metadata columns
    skip_cols = {"source_file", "load_timestamp", "_source_file", "_load_timestamp", "rescued_data"}

    for c in df.columns:
        if c.lower() in skip_cols:
            continue
        col_type = str(df.schema[c].dataType)
        # Get up to 5 non-null sample values
        samples = [
            str(row[0]) for row in
            df.select(c).filter(col(c).isNotNull()).limit(5).collect()
        ]
        profile["columns"].append({
            "name": c,
            "type": col_type,
            "samples": samples
        })

    return profile


def call_llm_for_mapping(entity: str, bronze_profile: dict, target_schema: dict) -> list:
    """
    Sends Bronze profile + target schema to LLM and gets back column mappings
    with transformation expressions.

    Returns list of dicts matching column_mapping format:
    [{"source_column": "...", "canonical_column": "...", "transformation": "..."}, ...]
    """
    client = mlflow.deployments.get_deploy_client("databricks")

    prompt = f"""You are a data engineering assistant. Your job is to map source columns from a Bronze table to canonical Silver columns.

BRONZE TABLE: {entity}
BRONZE COLUMNS (with sample values):
{json.dumps(bronze_profile["columns"], indent=2)}

TARGET SILVER SCHEMA:
{json.dumps(target_schema["columns"], indent=2)}

RULES:
1. For each Bronze column, determine which canonical Silver column it maps to based on column name similarity AND sample data patterns.
2. Multiple Bronze columns may map to the same canonical column (they will be COALESCEd).
3. If a column needs transformation to match the target type, provide a SQL expression using {{col}} as a placeholder for the source column name.
4. Transformation examples:
   - Type casting: "CAST({{col}} AS INT)"
   - Dollar sign removal + cast: "CAST(REGEXP_REPLACE({{col}}, '^\\\\$', '') AS DECIMAL(10,2))"
   - Dual date format parsing: "COALESCE(TRY_TO_TIMESTAMP({{col}}, 'MM/dd/yyyy HH:mm'), TRY_TO_TIMESTAMP({{col}}, 'yyyy-MM-dd HH:mm'))"
   - ISO timestamp: "TRY_TO_TIMESTAMP({{col}})"
   - Date only: "TO_DATE({{col}}, 'yyyy-MM-dd')"
   - Placeholder replacement: "CASE WHEN {{col}} = '?' THEN NULL ELSE {{col}} END"
   - Abbreviation mapping: "CASE WHEN {{col}} IN ('CONS', 'Cosumer') THEN 'Consumer' WHEN {{col}} = 'CORP' THEN 'Corporate' WHEN {{col}} = 'HO' THEN 'Home Office' ELSE {{col}} END"
5. If a Bronze column does not match any canonical column, skip it.
6. If a canonical column has no matching Bronze column, skip it (it will be NULL).

RESPOND WITH ONLY a JSON array. No explanation, no markdown, no backticks. Example:
[
  {{"source_column": "cust_id", "canonical_column": "customer_id", "transformation": null}},
  {{"source_column": "order_date", "canonical_column": "order_purchase_timestamp", "transformation": "COALESCE(TRY_TO_TIMESTAMP({{col}}, 'MM/dd/yyyy HH:mm'), TRY_TO_TIMESTAMP({{col}}, 'yyyy-MM-dd HH:mm'))"}}
]"""

    response = client.predict(
        endpoint="databricks-claude-sonnet-4-20250514",
        inputs={
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4000,
            "temperature": 0.0
        }
    )

    raw_text = response.choices[0]["message"]["content"].strip()

    # Clean any accidental markdown wrapping
    if raw_text.startswith("```"):
        raw_text = raw_text.split("\n", 1)[1]
    if raw_text.endswith("```"):
        raw_text = raw_text.rsplit("\n", 1)[0]

    mappings = json.loads(raw_text)
    return mappings


# COMMAND ----------

# =============================================================================
# PRE-LOAD: Generate all mappings via LLM at module level
# =============================================================================

# =============================================================================
# PRE-LOAD: Generate all mappings via LLM at module level
# =============================================================================

CONFIG_MAPPINGS = {}
MAPPING_SOURCE = {}  # Tracks which method produced the mapping: "llm" or "static_config"

for entity in ["customers", "orders", "transactions", "returns", "products", "vendors"]:
    try:
        profile = get_bronze_profile(entity)
        target = TARGET_SCHEMAS[entity]
        mappings = call_llm_for_mapping(entity, profile, target)
        CONFIG_MAPPINGS[entity] = mappings
        MAPPING_SOURCE[entity] = "llm"
        print(f"[LLM]      {entity}: {len(mappings)} columns mapped")
    except Exception as e:
        print(f"[FALLBACK] {entity}: LLM failed — {type(e).__name__}: {str(e)}")
        CONFIG_MAPPINGS[entity] = (
            spark.read.table("globalmart.ingestion_control.column_mapping")
            .filter(col("entity") == entity)
            .select("source_column", "canonical_column", "transformation")
            .collect()
        )
        MAPPING_SOURCE[entity] = "static_config"

# Summary
print("\n=== MAPPING SOURCE SUMMARY ===")
for entity, source in MAPPING_SOURCE.items():
    print(f"  {entity}: {source}")
print("==============================\n")

# Log what the LLM produced for debugging
for entity, mappings in CONFIG_MAPPINGS.items():
    print(f"\n=== {entity.upper()} MAPPINGS ({len(mappings)}) ===")
    for m in mappings:
        src = m["source_column"] if isinstance(m, dict) else m.source_column
        canon = m["canonical_column"] if isinstance(m, dict) else m.canonical_column
        xform = m.get("transformation") if isinstance(m, dict) else m.transformation
        print(f"  {src} → {canon}" + (f" | transform: {xform}" if xform else ""))

# DQ rules stay config-driven — business decisions, not schema discovery
_rules_df = spark.read.table("globalmart.ingestion_control.dq_rules")
CONFIG_RULES = {}
for entity in ["customers", "orders", "transactions", "returns", "products", "vendors"]:
    CONFIG_RULES[entity] = _rules_df.filter(col("entity") == entity).collect()

# COMMAND ----------

# =============================================================================
# HARMONIZATION ENGINE — Same as config-driven, reads from CONFIG_MAPPINGS
# =============================================================================

def harmonize(df: DataFrame, entity: str) -> DataFrame:
    mappings = CONFIG_MAPPINGS[entity]
    source = MAPPING_SOURCE[entity]

    canonical_groups = {}
    for m in mappings:
        if isinstance(m, dict):
            canon = m["canonical_column"]
            src = m["source_column"]
            xform = m.get("transformation")
        else:
            canon = m.canonical_column
            src = m.source_column
            xform = m.transformation

        if canon not in canonical_groups:
            canonical_groups[canon] = []
        canonical_groups[canon].append((src, xform))

    select_exprs = []
    for canon, sources in canonical_groups.items():
        coalesce_parts = []
        for src, xform in sources:
            if src in [c.lower() for c in df.columns]:
                if xform:
                    sql_expr = xform.replace("{col}", src)
                    coalesce_parts.append(expr(sql_expr))
                else:
                    coalesce_parts.append(col(src))

        if len(coalesce_parts) == 1:
            select_exprs.append(coalesce_parts[0].alias(canon))
        elif len(coalesce_parts) > 1:
            select_exprs.append(coalesce(*coalesce_parts).alias(canon))
        else:
            select_exprs.append(lit(None).alias(canon))

    for meta_col in ["source_file", "load_timestamp", "_source_file", "_load_timestamp"]:
        if meta_col in df.columns:
            target_name = meta_col if meta_col.startswith("_") else f"_{meta_col}"
            select_exprs.append(col(meta_col).alias(target_name))

    # Tag every row with how the mapping was derived
    select_exprs.append(lit(source).alias("_mapping_source"))

    return df.select(*select_exprs)

# =============================================================================
# QUARANTINE ENGINE — Identical to config-driven version
# =============================================================================

def quarantine(df: DataFrame, entity: str) -> DataFrame:
    rules = CONFIG_RULES[entity]
    frames = []

    for rule in rules:
        tagged = (
            df
            .filter(expr(f"NOT ({rule['rule_expression']})"))
            .withColumn("_run_id", lit(RUN_ID))
            .withColumn("_run_timestamp", current_timestamp())
            .withColumn("_expectation_name", lit(rule["rule_name"]))
            .withColumn("_severity", lit(rule["severity"]))
            .withColumn("_issue_type", lit(rule["issue_type"]))
            .withColumn("_failure_reason", lit(rule["failure_reason"]))
            .withColumn("_quarantine_timestamp", current_timestamp())
        )
        frames.append(tagged)

    if frames:
        result = frames[0]
        for frame in frames[1:]:
            result = result.unionByName(frame, allowMissingColumns=True)
        return result
    else:
        return (
            df.limit(0)
            .withColumn("_run_id", lit(None).cast("string"))
            .withColumn("_run_timestamp", current_timestamp())
            .withColumn("_expectation_name", lit(None).cast("string"))
            .withColumn("_severity", lit(None).cast("string"))
            .withColumn("_issue_type", lit(None).cast("string"))
            .withColumn("_failure_reason", lit(None).cast("string"))
            .withColumn("_quarantine_timestamp", current_timestamp())
        )

# COMMAND ----------

# =============================================================================
# SILVER TABLES — Identical structure to config-driven version
# =============================================================================

@dp.table(name="customers")
@dp.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect("valid_customer_email", "customer_email IS NOT NULL")
@dp.expect("valid_customer_name", "customer_name IS NOT NULL")
@dp.expect("valid_segment", "segment IN ('Consumer', 'Corporate', 'Home Office')")
def silver_customers():
    return harmonize(spark.readStream.table("globalmart.bronze.customers"), "customers")

@dp.table(name="customers_quarantine")
def silver_customers_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.customers"), "customers"), "customers")

# COMMAND ----------

@dp.table(name="orders")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect("valid_purchase_date", "order_purchase_timestamp IS NOT NULL")
@dp.expect("valid_ship_mode", "ship_mode IS NOT NULL")
@dp.expect("valid_delivery_customer", "order_delivered_customer_timestamp IS NOT NULL")
@dp.expect("valid_delivery_carrier", "order_delivered_carrier_timestamp IS NOT NULL")
def silver_orders():
    return harmonize(spark.readStream.table("globalmart.bronze.orders"), "orders")

@dp.table(name="orders_quarantine")
def silver_orders_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.orders"), "orders"), "orders")

# COMMAND ----------

@dp.table(name="transactions")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dp.expect("valid_sales", "sales IS NOT NULL")
@dp.expect("valid_profit", "profit IS NOT NULL")
@dp.expect("valid_payment_type", "payment_type IS NOT NULL")
def silver_transactions():
    return harmonize(spark.readStream.table("globalmart.bronze.transactions"), "transactions")

@dp.table(name="transactions_quarantine")
def silver_transactions_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.transactions"), "transactions"), "transactions")

# COMMAND ----------

@dp.table(name="returns")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect("valid_return_status", "return_status IN ('Pending', 'Approved', 'Rejected')")
@dp.expect("valid_return_reason", "return_reason IS NOT NULL")
@dp.expect("valid_refund_amount", "refund_amount IS NOT NULL")
def silver_returns():
    return harmonize(spark.readStream.table("globalmart.bronze.returns"), "returns")

@dp.table(name="returns_quarantine")
def silver_returns_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.returns"), "returns"), "returns")

# COMMAND ----------

@dp.table(name="products")
@dp.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dp.expect("valid_product_name", "product_name IS NOT NULL")
@dp.expect("valid_brand", "brand IS NOT NULL")
def silver_products():
    return harmonize(spark.readStream.table("globalmart.bronze.products"), "products")

@dp.table(name="products_quarantine")
def silver_products_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.products"), "products"), "products")

# COMMAND ----------

@dp.table(name="vendors")
@dp.expect_or_drop("vendor_id_not_null", "vendor_id IS NOT NULL")
@dp.expect("valid_vendor_name", "vendor_name IS NOT NULL")
def silver_vendors():
    return harmonize(spark.readStream.table("globalmart.bronze.vendors"), "vendors")

@dp.table(name="vendors_quarantine")
def silver_vendors_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.vendors"), "vendors"), "vendors")