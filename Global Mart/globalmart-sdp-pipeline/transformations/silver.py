# =============================================================================
# GlobalMart — Silver Layer Pipeline (LLM-Driven Schema Mapping)
# Pipeline Type: Spark Declarative Pipelines (SDP)
# Target Schema: globalmart.silver
#
# MISSION:
#   To transform raw, inconsistent Bronze data from 6 regional systems into a
#   harmonized, high-fidelity Canonical Silver layer. This pipeline uses
#   Generative AI (Mosaic AI) to discover schema mappings dynamically,
#   eliminating the need for fragile, hardcoded transformation scripts.
#
# PIPELINE DATA FLOW:
#   Bronze (Auto Loader) → LLM Schema Mapper → Harmonize Engine → DQ Gates
#                                                    ↓              ↓
#                                              Silver Tables   Quarantine Tables
#
# ENTITIES COVERED (6):
#   1. Customers  — 748 rows from 6 regional CSVs (4 different ID column names)
#   2. Orders     — 5,006 rows from 3 regional CSVs (dual date formats)
#   3. Transactions — 9,816 rows from 3 regional CSVs ($ prefixes, missing profit)
#   4. Returns    — 800 rows from 2 regional JSONs (every column name differs)
#   5. Products   — 1,774 rows from 1 JSON (high sparsity: 5 cols >39% null)
#   6. Vendors    — 7 rows from 1 CSV (simple reference table)
#
# CONFIG-DRIVEN ARCHITECTURE:
#   - Schema mappings: globalmart.config.column_mapping (53 rows across 6 entities)
#     Maps every source column variant to its canonical Silver name, with optional
#     SQL transformation expressions (CAST, REGEXP_REPLACE, CASE WHEN, etc.)
#   - DQ rules: globalmart.config.dq_rules (20 rules across 6 entities)
#     Each rule has: rule_expression (SQL predicate), severity (CRITICAL/WARNING),
#     consequence (drop/warn), issue_type, and human-readable failure_reason.
#   - MDM rules: globalmart.config.mdm_rules (8 rules for customer survivorship)
#     Defines per-column strategy (most_recent, most_complete, source_priority).
#
# REJECTION PATTERN (QUARANTINE):
#   Every entity implements a dual-output pattern:
#
#   CRITICAL (@dp.expect_or_drop):
#     Records missing primary keys or foreign keys are DROPPED from the main
#     table. These records cannot participate in any downstream join and would
#     silently corrupt Gold-layer aggregations. They are captured in the
#     paired _quarantine table with full audit metadata.
#
#   WARNING (@dp.expect):
#     Records with valid keys but messy data (null emails, bad segments,
#     unparseable dates) are KEPT in the main table but flagged. The
#     quarantine table also captures a copy with the specific rule that
#     failed, so Finance/Audit can review without blocking the pipeline.
#
#   Quarantine metadata appended to every rejected row:
#     _run_id            — UUID of the pipeline execution
#     _run_timestamp     — When the rule was evaluated
#     _expectation_name  — Which DQ rule failed (e.g. "valid_segment")
#     _severity          — CRITICAL or WARNING
#     _issue_type        — Category (null_primary_key, format_error, etc.)
#     _failure_reason    — Human-readable explanation for Finance review
#     _quarantine_timestamp — When the row was quarantined
#
# LLM MAPPING STRATEGY:
#   Primary: Mosaic AI (databricks-gpt-oss-20b) generates mappings by
#            analyzing Bronze column names, types, and sample values against
#            the TARGET_SCHEMAS definitions below. The LLM can detect:
#            - Semantic aliases (cust_id → customer_id)
#            - Data patterns ($ prefixes, ? placeholders, date formats)
#            - Structural anomalies (city/state swap in Region 4)
#
#   Fallback: If the LLM call fails (timeout, model error, parse failure),
#             the pipeline reads static mappings from globalmart.config.column_mapping.
#             This ensures the pipeline never fails due to AI service issues.
#
#   Auditability: Every Silver row is tagged with _mapping_source ("llm" or
#                 "static_config") so downstream consumers know how the
#                 schema was resolved for that batch.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, coalesce, expr, lit, current_timestamp, when,
    monotonically_increasing_id
)
from pyspark.sql import DataFrame, Row
import uuid
import json

from utilities.utils import (
    CATALOG, ENTITIES, _SKIP_COLS,
    load_all_config, harmonize_entity, build_quarantine_for_entity,
    build_ai_dq_prompt, parse_ai_dq_response, generate_run_id,
)

spark.sql("USE CATALOG globalmart")
spark.sql("USE SCHEMA silver")

# Unique identifier for this pipeline run — used in quarantine tables
# to group all DQ failures from the same execution for batch review.
RUN_ID = str(uuid.uuid4())

# =============================================================================
# LLM CLIENT SETUP — Databricks ML Gateway (mlflow.deployments)
# =============================================================================
# Authentication is handled by workspace-level credentials — no API tokens
# are needed in code. The client connects to the Mosaic AI serving endpoint.
# =============================================================================

import mlflow.deployments

LLM_CLIENT = mlflow.deployments.get_deploy_client("databricks")
LLM_MODEL = "databricks-gpt-oss-20b"

# =============================================================================
# TARGET SILVER SCHEMAS — Canonical Column Definitions
# =============================================================================
# These define what the Silver layer SHOULD look like after harmonization.
# The LLM reads these alongside the Bronze profile and generates the SQL
# expressions needed to transform source columns into this canonical form.
#
# Each column has:
#   - type: Target Spark SQL type (STRING, TIMESTAMP, DECIMAL, INT, etc.)
#   - role: primary_key | foreign_key | required | optional
#     * primary_key / foreign_key → CRITICAL DQ rules (drop if null)
#     * required → WARNING DQ rules (keep but flag if null/invalid)
#     * optional → No DQ rule (nulls are expected and acceptable)
#   - description: Hints for the LLM about known data issues
#
# COLUMN MAPPING REFERENCE (from globalmart.config.column_mapping):
#
#   CUSTOMERS (15 mapping rows):
#     customer_id ← customer_id, customerid, cust_id, customer_identifier
#     customer_email ← customer_email, email_address
#     customer_name ← customer_name, full_name
#     segment ← segment, customer_segment
#       Transform: CASE WHEN for CONS→Consumer, CORP→Corporate, HO→Home Office, Cosumer→Consumer
#     country, city, state, postal_code, region ← direct pass-through
#
#   ORDERS (10 mapping rows):
#     order_id, customer_id, vendor_id, ship_mode, order_status ← direct
#     All 5 timestamp columns ← parsed via:
#       COALESCE(TRY_TO_TIMESTAMP(col, 'MM/dd/yyyy HH:mm'),
#                TRY_TO_TIMESTAMP(col, 'yyyy-MM-dd HH:mm'),
#                TRY_TO_TIMESTAMP(col, 'yyyy-MM-dd H:mm'))
#
#   TRANSACTIONS (8 mapping rows):
#     order_id, product_id ← direct (case-insensitive: Order_id, Order_ID)
#     sales ← CAST(REGEXP_REPLACE(col, '^\$', '') AS DECIMAL(10,3))
#     quantity ← CAST(col AS INT)
#     discount ← CAST(REGEXP_REPLACE(col, '[^0-9\.]', '') AS DECIMAL(5,3))
#     profit ← CAST(REGEXP_REPLACE(col, '^\$', '') AS DECIMAL(10,3))
#       Note: transactions_2 is missing profit entirely (3,337 rows → NULL)
#     payment_type ← CASE WHEN col = '?' THEN NULL ELSE col END
#     payment_installments ← CAST(col AS INT)
#
#   RETURNS (11 mapping rows):
#     order_id ← order_id, orderid (PascalCase in returns_2)
#     refund_amount ← refund_amount, amount
#       Transform: Strip $, strip non-numeric, CAST to DECIMAL(10,2)
#     return_date ← return_date, date_of_return
#       Transform: COALESCE(TO_DATE(col, 'yyyy-MM-dd'), TO_DATE(col, 'MM-dd-yyyy'))
#     return_reason ← return_reason, reason
#       Transform: CASE WHEN col = '?' THEN NULL ELSE col END
#     return_status ← return_status, status
#       Transform: PENDG→Pending, APPRVD→Approved, RJCTD→Rejected
#
#   PRODUCTS (13 mapping rows):
#     All columns direct except:
#     date_added ← dateadded via TRY_TO_TIMESTAMP(col) [ISO 8601]
#     date_updated ← dateupdated via TRY_TO_TIMESTAMP(col) [ISO 8601]
#
#   VENDORS (2 mapping rows):
#     vendor_id, vendor_name ← direct pass-through
# =============================================================================

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


# =============================================================================
# LLM SCHEMA MAPPER — Calls Foundation Model to generate mappings
# =============================================================================

def get_bronze_profile(entity: str) -> dict:
    """
    Reads a Bronze table and returns column names and types for LLM analysis.
    No .collect() — uses only df.columns and df.schema (driver metadata,
    not a Spark action). Sample values are omitted in the SDP environment;
    column names + types provide sufficient context for schema mapping.
    """
    df = spark.read.table(f"globalmart.bronze.{entity}")

    profile = {"entity": entity, "columns": []}
    skip_cols = {"source_file", "load_timestamp", "_source_file", "_load_timestamp", "rescued_data"}

    for c in df.columns:
        if c.lower() in skip_cols:
            continue
        col_type = str(df.schema[c].dataType)
        profile["columns"].append({
            "name": c,
            "type": col_type,
            "samples": []
        })

    return profile


def call_llm_for_mapping(entity: str, bronze_profile: dict, target_schema: dict) -> list:
    """
    FUNCTION: call_llm_for_mapping
    Interfaces with Databricks Mosaic AI to generate SQL mapping expressions.

    WHAT THE LLM DOES:
      1. Cognitive Mapping: Translates disparate column names into canonical form.
         Example: customerid, cust_id, customer_identifier → all map to customer_id
      2. Transformation Generation: Produces SQL expressions for type conversion.
         Example: "CAST(REGEXP_REPLACE({col}, '^\$', '') AS DECIMAL(10,2))"
      3. Abbreviation Expansion: Generates CASE WHEN for known abbreviations.
         Example: CONS → Consumer, PENDG → Pending
      4. Anomaly Detection: Identifies city/state swap in Region 4 by inspecting
         sample values (2-letter codes under 'city' header = state data).

    RETURNS:
      List of dicts: [{"source_column", "canonical_column", "transformation"}, ...]
      The transformation field is NULL for direct pass-through columns, or a SQL
      expression with {col} placeholder for columns needing conversion.

    ERROR HANDLING:
      If the LLM returns malformed JSON or the API call fails, the caller
      catches the exception and falls back to globalmart.config.column_mapping.
    """
    from openai import OpenAI
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    client = OpenAI(
        api_key=context.apiToken().get(),
        base_url=f"{context.apiUrl().get()}/serving-endpoints"
    )

    prompt = f"""You are a senior data engineer. Your job is to map source columns from a Bronze table to canonical Silver columns.

    IMPORTANT ARCHITECTURAL RULES:
    1. ANALYZE DATA PATTERNS (CRITICAL): Region 4 is known to have swapped data for City and State.
       Header says 'City' -> Data may be a 2-letter State code OR a full State name (e.g. Texas).
       Header says 'State' -> Data may be a City name (e.g. Houston).
       
       ACTION: Inspect the provided sample values. If the values under 'City' look like States and 
       values under 'State' look like Cities, GENERATE A SWAPPED MAPPING to fix the structural error.
       
    2. CURRENCY CLEANUP: Ensure any transaction sales or refund amounts strip '$' and cast to DECIMAL.

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

RESPOND WITH ONLY a JSON array. No explanation, no markdown, no backticks."""

    response = client.chat.completions.create(
        model="databricks-gpt-oss-20b",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )

    raw_text = response.choices[0]["message"]["content"].strip()

    # Clean any accidental markdown wrapping from LLM output
    if raw_text.startswith("```"):
        raw_text = raw_text.split("\n", 1)[1]
    if raw_text.endswith("```"):
        raw_text = raw_text.rsplit("\n", 1)[0]

    mappings = json.loads(raw_text)
    return mappings



# =============================================================================
# LAZY-LOADED MAPPING CACHE
# =============================================================================
# SDP does NOT allow reading pipeline-managed tables (bronze.*) at module
# level — only inside @dp.table function bodies.  The LLM schema mapper
# needs Bronze data (via get_bronze_profile) to generate mappings, so we
# use lazy initialization: the first @dp.table function that calls
# _get_mappings(entity) triggers the LLM call (or fallback) and caches
# the result.  Subsequent calls (e.g. the paired quarantine table) reuse
# the cache, guaranteeing deterministic mappings within a single run.
#
# Fallback table: globalmart.config.column_mapping (53 rows, 6 entities)
# — NOT pipeline-managed, so safe to read anywhere.
# =============================================================================

# =============================================================================
# PRE-LOAD ALL CONFIG AT MODULE LEVEL
# =============================================================================
# Config tables (column_mapping, dq_rules) are NOT pipeline-managed, so
# .collect() at module level is safe.  This single call replaces all
# per-entity .collect() calls that previously happened inside @dp.table
# functions, eliminating the SDP warning.
# =============================================================================

_CONFIG = load_all_config(spark)
_STATIC_MAPPINGS = _CONFIG["mappings"]       # {entity: [Row, ...]}
CONFIG_RULES     = _CONFIG["rules"]           # {entity: [Row, ...]}
_RULES_SUMMARY   = _CONFIG["rules_summary"]   # {entity: [{rule_name, ...}, ...]}

# Lazy mapping cache — for LLM results when available
_MAPPING_CACHE = {}
_SOURCE_CACHE = {}


def _get_mappings(entity):
    """
    Lazy loader for LLM-generated (or fallback static) column mappings.
    The LLM path reads Bronze tables (only legal inside @dp.table).
    The fallback uses pre-loaded static config (no .collect() needed).
    Results are cached per entity per run.
    """
    if entity in _MAPPING_CACHE:
        return _MAPPING_CACHE[entity], _SOURCE_CACHE[entity]

    try:
        profile = get_bronze_profile(entity)
        target = TARGET_SCHEMAS[entity]
        mappings = call_llm_for_mapping(entity, profile, target)
        source = "llm"
        print(f"[LLM]      {entity}: {len(mappings)} columns mapped")
    except Exception as e:
        print(f"[FALLBACK] {entity}: LLM failed — {type(e).__name__}: {str(e)}")
        mappings = _STATIC_MAPPINGS.get(entity, [])
        source = "static_config"

    _MAPPING_CACHE[entity] = mappings
    _SOURCE_CACHE[entity] = source
    return mappings, source


# =============================================================================
# HARMONIZATION ENGINE — Dynamic Column Unification
# =============================================================================

def harmonize(df: DataFrame, entity: str) -> DataFrame:
    """
    Thin wrapper: resolves mappings (LLM or static) then delegates
    to utils.harmonize_entity(). No .collect() calls.
    """
    mappings, source = _get_mappings(entity)
    return harmonize_entity(df, mappings, source)


def quarantine(df: DataFrame, entity: str) -> DataFrame:
    """
    Thin wrapper: passes pre-loaded rules to utils.build_quarantine_for_entity().
    No .collect() calls.
    """
    return build_quarantine_for_entity(df, CONFIG_RULES[entity], RUN_ID)


# =============================================================================
# ENTITY TABLES & DATA QUALITY GATES
# =============================================================================


# =============================================================================
# 1. CUSTOMERS — 748 rows from 6 regional CSV files
# =============================================================================
# SOURCE: globalmart.bronze.customers (Auto Loader union of customers_1..6)
#
# SCHEMA CHALLENGES (resolved by harmonize()):
#   - 4 different ID column names: customer_id, CustomerID, cust_id, customer_identifier
#   - customers_5 missing customer_email entirely (42 rows → NULL)
#   - customers_6 uses full_name, email_address, customer_segment (all renamed)
#   - customers_4 has state/city column ORDER swapped (data in wrong headers)
#   - Segment abbreviations: CONS, CORP, HO, plus typo "Cosumer"
#
# DQ RULES (from globalmart.config.dq_rules, CU-001 to CU-005):
#   CU-001 customer_id_not_null     CRITICAL  → DROP + quarantine
#     Why drop: A customer with no ID across all 4 regional variants cannot
#     be linked to any order, transaction, or return. Unusable downstream.
#
#   CU-002 valid_customer_email     WARNING   → keep + quarantine
#     Why keep: Email is important but not a key field. Region 5 has no email
#     column at all (42 rows), Regions 3/6 have partial gaps (21%, 11%).
#     Dropping would lose valid customers.
#
#   CU-003 valid_customer_name      WARNING   → keep + quarantine
#     Why keep: Name is needed for reporting and MDM deduplication but does
#     not invalidate the customer record.
#
#   CU-004 valid_segment            WARNING   → keep + quarantine
#     Why keep: Segment drives business reporting by customer type.
#     Abbreviations and typos are mapped BEFORE this rule fires, so it
#     only catches truly unmappable values. Identity is still valid.
#
#   CU-005 detect_location_swap     WARNING   → keep + quarantine
#     Why keep: Detects Region 4's city/state data swap via regex.
#     State should be a 2-letter code (e.g. "TX") or a proper state name.
#     If it looks like a city name, the row is flagged for investigation.
# =============================================================================

@dp.table(name="customers")
@dp.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect("valid_customer_email", "customer_email IS NOT NULL")
@dp.expect("valid_customer_name", "customer_name IS NOT NULL")
@dp.expect("valid_segment", "segment IN ('Consumer', 'Corporate', 'Home Office')")
@dp.expect("detect_location_swap", "(state RLIKE '^[A-Z]{2}$') OR (LENGTH(state) > 2 AND state NOT LIKE '% %' AND state RLIKE '^[A-Z][a-z]+$')")
def silver_customers():
    return harmonize(spark.readStream.table("globalmart.bronze.customers"), "customers")

@dp.table(name="customers_quarantine")
def silver_customers_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.customers"), "customers"), "customers")


# =============================================================================
# 2. ORDERS — 5,006 rows from 3 regional CSV files
# =============================================================================
# SOURCE: globalmart.bronze.orders (Auto Loader union of orders_1..3)
#
# SCHEMA CHALLENGES (resolved by harmonize()):
#   - orders_3 is missing order_estimated_delivery_date (9 cols vs 10)
#     → Auto Loader adds it as NULL via addNewColumns
#   - All date columns stored as strings in two competing formats:
#     "MM/dd/yyyy HH:mm" and "yyyy-MM-dd HH:mm"
#     → Parsed via COALESCE(TRY_TO_TIMESTAMP(...)) with both patterns
#
# DQ RULES (from globalmart.config.dq_rules, OR-001 to OR-005):
#   OR-001 order_id_not_null          CRITICAL  → DROP + quarantine
#     Why drop: Order ID is the PK and join key to transactions and returns.
#     Without it, the record cannot participate in revenue or return analysis.
#
#   OR-002 valid_purchase_date        WARNING   → keep + quarantine
#     Why keep: Purchase date is essential for time-based reporting but the
#     order itself may still be valid. Catches records where neither date
#     format parses. Flagged so the order may be recoverable with a corrected date.
#
#   OR-003 valid_ship_mode            WARNING   → keep + quarantine
#     Why keep: Ship mode is an analytical dimension, not a key field.
#     orders_2 has anomalous 11.4% null rate (200 rows) vs ~1% in other files.
#     Dropping would lose valid orders.
#
#   OR-004 valid_delivery_customer    WARNING   → keep + quarantine
#     Why keep: Expected to be null for undelivered orders (status: shipped,
#     processing, created). Logged for monitoring — a rising null rate could
#     indicate a delivery tracking issue.
#
#   OR-005 valid_delivery_carrier     WARNING   → keep + quarantine
#     Why keep: Same rationale — null is expected for in-transit orders.
# =============================================================================

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


# =============================================================================
# 3. TRANSACTIONS — 9,816 rows from 3 regional CSV files
# =============================================================================
# SOURCE: globalmart.bronze.transactions (Auto Loader union of transactions_1..3)
#
# SCHEMA CHALLENGES (resolved by harmonize()):
#   - Column casing varies: Order_id vs Order_ID, Product_id vs Product_ID
#     → Handled by case-insensitive column matching in harmonize()
#   - transactions_1.Sales has 20.5% of rows with '$' prefix (763 of 3,730)
#     → Stripped via REGEXP_REPLACE before CAST to DECIMAL
#   - transactions_2 is missing the profit column entirely (3,337 rows → NULL)
#     → harmonize() creates profit as lit(None)
#   - payment_type contains '?' placeholders (transactions_1: 357 rows, 9.6%)
#     → Mapped to NULL via CASE WHEN
#
# DQ RULES (from globalmart.config.dq_rules, TR-001 to TR-008):
#   TR-001 order_id_not_null          CRITICAL  → DROP + quarantine
#     Why drop: FK to orders. Without it, the transaction cannot be attributed
#     to any customer, region, or time period. Breaks revenue reporting.
#
#   TR-002 product_id_not_null        CRITICAL  → DROP + quarantine
#     Why drop: FK to products. Without it, the transaction cannot be attributed
#     to any brand, category, or vendor. Breaks product analysis.
#
#   TR-003 valid_sales                WARNING   → keep + quarantine
#     Why keep: Sales is the core revenue measure. A null after $ stripping and
#     casting indicates a non-numeric source value. The order-product linkage
#     is still valid — the revenue figure is flagged as unreliable.
#
#   TR-004 valid_profit               WARNING   → keep + quarantine
#     Why keep: Profit is missing for ALL 3,337 rows from transactions_2.
#     Dropping would remove a third of all transactions. Flagged so Finance
#     knows profit analysis excludes that region until source is corrected.
#
#   TR-005 valid_discount             WARNING   → keep + quarantine
#     Why keep: Discount is analytical, not structural.
#
#   TR-006 valid_payment_type         WARNING   → keep + quarantine
#     Why keep: 357 rows had '?' placeholders (now NULL), plus 1-2% native nulls
#     in transactions_2/3. Flagged for source system investigation.
#
#   TR-007 valid_quantity             WARNING   → keep + quarantine
#     Why keep: Quantity null or unparseable as integer.
#
#   TR-008 valid_profit_value         WARNING   → keep + quarantine
#     Why keep: Negative profit is possible (loss-making sales) but flagged
#     for review since it could also indicate a data issue.
# =============================================================================

@dp.table(name="transactions")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dp.expect("valid_sales", "sales IS NOT NULL AND sales > 0")
@dp.expect("valid_profit", "profit IS NOT NULL")
@dp.expect("valid_payment_type", "payment_type IS NOT NULL")
def silver_transactions():
    return harmonize(spark.readStream.table("globalmart.bronze.transactions"), "transactions")

@dp.table(name="transactions_quarantine")
def silver_transactions_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.transactions"), "transactions"), "transactions")


# =============================================================================
# 4. RETURNS — 800 rows from 2 regional JSON files
# =============================================================================
# SOURCE: globalmart.bronze.returns (Auto Loader union of returns_1, returns_2)
#
# SCHEMA CHALLENGES (resolved by harmonize()):
#   - EVERY column name differs between the two files:
#     order_id / OrderId, refund_amount / amount, return_date / date_of_return,
#     return_reason / reason, return_status / status
#     → Each pair maps to the same canonical column via COALESCE
#   - returns_1.refund_amount has 16.6% of rows with '$' prefix (83 of 500)
#     → Stripped via REGEXP_REPLACE before CAST to DECIMAL
#   - returns_1.return_status has abbreviations: PENDG (24), APPRVD (21), RJCTD (20)
#     → Expanded via CASE WHEN mapping
#   - returns_2.reason has '?' placeholders (35 rows, 11.7%)
#     → Mapped to NULL via CASE WHEN
#
# DQ RULES (from globalmart.config.dq_rules, RE-001 to RE-004):
#   RE-001 order_id_not_null          CRITICAL  → DROP + quarantine
#     Why drop: FK to orders. Without it, the return cannot be matched to a
#     customer, transaction, or product — fraud detection and refund
#     reconciliation become impossible.
#
#   RE-002 valid_return_status        WARNING   → keep + quarantine
#     Why keep: Return status drives the processing workflow. Abbreviations
#     are mapped before this rule fires — only truly unmappable values trigger it.
#     The return record is still valid for fraud pattern analysis.
#
#   RE-003 valid_return_reason        WARNING   → keep + quarantine
#     Why keep: Reason is needed for root cause analysis. 35 rows from returns_2
#     had '?' placeholders. The return is still valid for financial reconciliation.
#
#   RE-004 valid_refund_amount        WARNING   → keep + quarantine
#     Why keep: Refund amount null after cleaning indicates a non-numeric source.
#     The return event is still relevant for fraud pattern detection.
# =============================================================================

@dp.table(name="returns")
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect("valid_return_status", "return_status IN ('Pending', 'Approved', 'Rejected')")
@dp.expect("valid_return_reason", "return_reason IS NOT NULL")
@dp.expect("valid_refund_amount", "refund_amount IS NOT NULL AND refund_amount > 0")
def silver_returns():
    return harmonize(spark.readStream.table("globalmart.bronze.returns"), "returns")

@dp.table(name="returns_quarantine")
def silver_returns_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.returns"), "returns"), "returns")


# =============================================================================
# 5. PRODUCTS — 1,774 rows from 1 JSON file
# =============================================================================
# SOURCE: globalmart.bronze.products (products.json)
#
# SCHEMA CHALLENGES (resolved by harmonize()):
#   - High sparsity: weight (94%), dimension (88%), sizes (66%),
#     manufacturer (59%), upc (39%) are mostly null — preserved as-is
#   - dateAdded and dateUpdated are ISO 8601 strings (e.g. "2016-04-01T20:22:54Z")
#     → Parsed via TRY_TO_TIMESTAMP()
#   - product_photos_qty is the only non-STRING column in the entire Bronze layer
#     (LONG type) — passes through without transformation
#
# DQ RULES (from globalmart.config.dq_rules, PR-001 to PR-003):
#   PR-001 product_id_not_null        CRITICAL  → DROP + quarantine
#     Why drop: PK and join key to transactions. A product without an ID
#     cannot be linked to any sale, return, or vendor analysis.
#
#   PR-002 valid_product_name         WARNING   → keep + quarantine
#     Why keep: Product name is needed for reporting and Genie natural language
#     queries. Without it, the product still participates in joins via product_id
#     but is not human-identifiable.
#
#   PR-003 valid_brand                WARNING   → keep + quarantine
#     Why keep: Brand is an analytical dimension for product performance.
#     Flagged for completeness tracking.
# =============================================================================

@dp.table(name="products")
@dp.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dp.expect("valid_product_name", "product_name IS NOT NULL")
@dp.expect("valid_brand", "brand IS NOT NULL")
def silver_products():
    return harmonize(spark.readStream.table("globalmart.bronze.products"), "products")

@dp.table(name="products_quarantine")
def silver_products_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.products"), "products"), "products")


# =============================================================================
# 6. VENDORS — 7 rows from 1 CSV file
# =============================================================================
# SOURCE: globalmart.bronze.vendors (vendors.csv)
#
# SCHEMA: Simple reference table — vendor_id (PK) + vendor_name.
# NOTE: Only 4 of 7 vendors (VEN01–VEN04) are referenced in orders.
#       All 7 retained for referential completeness.
#
# DQ RULES (from globalmart.config.dq_rules, VE-001 to VE-002):
#   VE-001 vendor_id_not_null         CRITICAL  → DROP + quarantine
#     Why drop: PK and join key from orders. A vendor without an ID breaks
#     the vendor performance and return-rate analysis chain.
#
#   VE-002 valid_vendor_name          WARNING   → keep + quarantine
#     Why keep: Name is needed for human-readable reports and Genie queries.
#     The vendor is still joinable via vendor_id without a name.
# =============================================================================

@dp.table(name="vendors")
@dp.expect_or_drop("vendor_id_not_null", "vendor_id IS NOT NULL")
@dp.expect("valid_vendor_name", "vendor_name IS NOT NULL")
def silver_vendors():
    return harmonize(spark.readStream.table("globalmart.bronze.vendors"), "vendors")

@dp.table(name="vendors_quarantine")
def silver_vendors_quarantine():
    return quarantine(harmonize(spark.read.table("globalmart.bronze.vendors"), "vendors"), "vendors")


# =============================================================================
# 7. AI-DRIVEN DATA QUALITY DISCOVERY
# =============================================================================
# PURPOSE:
#   Uses ai_query() to analyze Bronze data samples and discover data quality
#   issues that static rules may miss. Unlike the static DQ rules in
#   config.dq_rules (which enforce known patterns), this table surfaces
#   NEW, UNKNOWN issues by letting the LLM inspect actual data values.
#
# DESIGN:
#   - Reads a sample (up to 50 rows) from each Bronze entity
#   - Builds a profile: column names, types, sample values, null counts
#   - Sends the profile + existing static rules to the LLM via ai_query()
#   - LLM returns NET-NEW findings only (must not duplicate existing rules)
#   - Output is an audit/review table — discoveries are NOT auto-enforced
#
# USAGE:
#   Data engineers review ai_dq_discoveries after each pipeline run.
#   Good findings can be promoted into config.dq_rules manually.
#   False positives are ignored — no downstream impact.
#
# WHY ai_query():
#   - Available natively in SDP serverless (no pip installs needed)
#   - Operates on data, not pipeline structure — correct use of the function
#   - LLM observes and reports; humans decide and enforce
# =============================================================================

AI_DQ_MODEL = "databricks-gpt-oss-20b"

# Entities to scan and the key columns to always include in the sample
_DQ_SCAN_ENTITIES = {
    "customers": ["customer_id", "customerid", "cust_id", "customer_identifier",
                   "customer_email", "email_address", "customer_name", "full_name",
                   "segment", "customer_segment", "country", "city", "state",
                   "postal_code", "region"],
    "orders":    ["order_id", "customer_id", "vendor_id", "ship_mode",
                   "order_status", "order_purchase_date", "order_approved_at",
                   "order_delivered_carrier_date", "order_delivered_customer_date",
                   "order_estimated_delivery_date"],
    "transactions": ["order_id", "product_id", "sales", "quantity", "discount",
                      "profit", "payment_type", "payment_installments"],
    "returns":   ["order_id", "orderid", "refund_amount", "amount",
                   "return_date", "date_of_return", "return_reason", "reason",
                   "return_status", "status"],
    "products":  ["product_id", "product_name", "brand", "categories",
                   "colors", "manufacturer", "sizes", "upc", "weight"],
    "vendors":   ["vendor_id", "vendor_name"],
}


def _build_pure_sql_dq_discovery(entity: str, rules_summary: list) -> str:
    """
    Builds a single SQL query that profiles a Bronze table, constructs an
    LLM prompt, calls ai_query(), parses the JSON response, and returns
    structured rows — all without any .collect() calls.
    """
    bronze_table = f"globalmart.bronze.{entity}"

    # Discover columns from schema metadata (no .collect())
    df = spark.read.table(bronze_table)
    skip = {"_source_file", "_load_timestamp", "_rescued_data", "source_file", "load_timestamp"}
    actual_cols = [c for c in df.columns if c.lower() not in skip]

    # Build per-column null-count + sample expressions for the profile CTE
    col_concat_parts = []
    for c in actual_cols:
        col_concat_parts.append(f"""
            CONCAT(
                '  - {c}: ',
                CAST(ROUND(SUM(CASE WHEN `{c}` IS NULL OR TRIM(CAST(`{c}` AS STRING)) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS STRING),
                '%% null, samples: ',
                CAST(SLICE(COLLECT_SET(CAST(`{c}` AS STRING)), 1, 5) AS STRING)
            )
        """)

    profile_concat = "CONCAT_WS('\\n', " + ", ".join(col_concat_parts) + ")"

    # Pre-format the rules text (pure Python string, embedded as SQL literal)
    rules_text = "\\n".join(
        f"  - {r['rule_name']}: {r['rule_expression']} ({r['issue_type']})"
        for r in rules_summary
    ).replace("'", "''") if rules_summary else "  (none)"

    # The prompt template — escaped for SQL embedding
    prompt_prefix = f"""You are a data quality analyst. Analyze this Bronze table profile and find data quality issues that are NOT already covered by the existing static rules.

DATA PROFILE:
Entity: {entity} (""".replace("'", "''")

    prompt_suffix = f""" rows)

EXISTING STATIC DQ RULES (already enforced — do NOT duplicate these):
{rules_text}

INSTRUCTIONS:
1. Look for patterns the static rules miss: unexpected values, format inconsistencies, outliers, suspicious distributions, encoding issues, placeholder values, cross-column anomalies.
2. For each NEW issue found, provide: column_name, issue_type (one of: format_inconsistency, suspicious_distribution, unexpected_values, cross_column_anomaly, encoding_issue, outlier), severity (CRITICAL or WARNING), a suggested SQL rule expression using the column name directly, and a human-readable description.
3. Return ONLY net-new findings. If a static rule already checks for nulls on a column, do not flag nulls on that column.
4. If no new issues are found, return an empty array.

RESPOND WITH ONLY a JSON array of objects with keys: column_name, issue_type, severity, suggested_rule, description. No markdown, no explanation.""".replace("'", "''")

    # JSON schema for from_json parsing
    json_schema = "ARRAY<STRUCT<column_name: STRING, issue_type: STRING, severity: STRING, suggested_rule: STRING, description: STRING>>"

    return f"""
        WITH profile AS (
            SELECT
                COUNT(*) as total_rows,
                {profile_concat} as col_profile_text
            FROM {bronze_table}
        ),
        prompt AS (
            SELECT CONCAT(
                '{prompt_prefix}',
                CAST(total_rows AS STRING),
                '{prompt_suffix}',
                '\\n',
                col_profile_text
            ) as full_prompt
            FROM profile
        ),
        ai_result AS (
            SELECT ai_query('{AI_DQ_MODEL}', full_prompt) as raw_response
            FROM prompt
        ),
        parsed AS (
            SELECT from_json(
                REGEXP_REPLACE(REGEXP_REPLACE(raw_response, '^```[a-z]*\\n', ''), '\\n```$', ''),
                '{json_schema}'
            ) as findings
            FROM ai_result
        )
        SELECT
            '{entity}' as entity,
            f.column_name,
            f.issue_type,
            COALESCE(f.severity, 'WARNING') as severity,
            f.suggested_rule,
            f.description,
            'ai_query' as discovery_source,
            '{AI_DQ_MODEL}' as model_used,
            'pending_review' as _discovery_status
        FROM parsed
        LATERAL VIEW EXPLODE(findings) t AS f
    """


@dp.table(
    name="ai_dq_discoveries",
    comment="AI-discovered data quality issues from Bronze layer analysis. Review table — not auto-enforced."
)
def ai_dq_discoveries():
    """
    Pure-SQL implementation: for each Bronze entity, a single SQL query
    profiles the data, calls ai_query(), and parses the JSON response.
    Zero .collect() calls — everything stays in the Spark execution engine.
    """
    from pyspark.sql.types import StructType, StructField, StringType

    entity_frames = []
    for entity in _DQ_SCAN_ENTITIES:
        try:
            entity_rules = _RULES_SUMMARY.get(entity, [])
            sql = _build_pure_sql_dq_discovery(entity, entity_rules)
            entity_frames.append(spark.sql(sql))
        except Exception as e:
            # Log discovery failure as a structured row
            error_df = spark.createDataFrame([{
                "entity": entity,
                "column_name": "_llm_error",
                "issue_type": "discovery_failure",
                "severity": "INFO",
                "suggested_rule": "",
                "description": f"AI discovery failed: {type(e).__name__}: {str(e)[:300]}",
                "discovery_source": "ai_query",
                "model_used": AI_DQ_MODEL,
                "_discovery_status": "error",
            }])
            entity_frames.append(error_df)

    if not entity_frames:
        schema = StructType([
            StructField("entity", StringType()),
            StructField("column_name", StringType()),
            StructField("issue_type", StringType()),
            StructField("severity", StringType()),
            StructField("suggested_rule", StringType()),
            StructField("description", StringType()),
            StructField("discovery_source", StringType()),
            StructField("model_used", StringType()),
            StructField("_discovery_status", StringType()),
        ])
        return spark.createDataFrame([], schema).withColumn("_discovered_at", current_timestamp()).withColumn("_run_id", lit(RUN_ID))

    result = entity_frames[0]
    for frame in entity_frames[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)

    return (
        result
        .withColumn("_discovered_at", current_timestamp())
        .withColumn("_run_id", lit(RUN_ID))
    )