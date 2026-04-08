# =============================================================================
# GlobalMart — Pipeline Utilities
# Path: /Workspace/Users/kushagra.n.verma@v4c.ai/globalmart-sdp-test/utilities/utils.py
#
# Shared functions used across Bronze, Silver, and Schema Validation pipelines.
# Bronze uses: sanitize_column_names
# Silver uses: load_all_config, harmonize_entity, build_quarantine_for_entity,
#              build_ai_dq_prompt, parse_ai_dq_response
# Schema Validation uses: CONTRACTS
# =============================================================================

import re
import uuid
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce, expr, lit, current_timestamp


# =============================================================================
# CONSTANTS
# =============================================================================

CATALOG = "globalmart"

ENTITIES = ["customers", "orders", "transactions", "returns", "products", "vendors"]

# Schema contracts for pre-Bronze validation
CONTRACTS = {
    "customers":    {"key_variants": "customer_id,customerid,cust_id,customer_identifier", "required": "country,region", "min_cols": 7},
    "orders":       {"key_variants": "order_id", "required": "order_status", "min_cols": 9},
    "transactions": {"key_variants": "order_id", "required": "quantity,discount", "min_cols": 6},
    "returns":      {"key_variants": "order_id,orderid", "required": "", "min_cols": 4},
    "products":     {"key_variants": "product_id", "required": "product_name", "min_cols": 5},
    "vendors":      {"key_variants": "vendor_id", "required": "vendor_name", "min_cols": 2},
}

# Bronze metadata columns to skip in profiling
_SKIP_COLS = {"_source_file", "_load_timestamp", "_rescued_data", "source_file", "load_timestamp", "rescued_data"}


# =============================================================================
# BRONZE UTILITIES
# =============================================================================

def sanitize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardizes all column names: strips whitespace, replaces special
    characters with underscores, collapses consecutive underscores,
    and lowercases everything to prevent case-sensitivity conflicts.
    """
    sanitized = df
    for original in df.columns:
        clean = original.strip()
        clean = re.sub(r"[^\w]", "_", clean)
        clean = re.sub(r"_+", "_", clean)
        clean = clean.strip("_")
        clean = clean.lower()
        if clean != original:
            sanitized = sanitized.withColumnRenamed(original, clean)
    return sanitized


# =============================================================================
# CONFIG LOADERS — Called at module level (outside @dp.table)
# =============================================================================
# These read from config tables which are NOT pipeline-managed,
# so collecting at module level is safe and avoids the SDP warning
# about .collect() inside decorated functions.
# =============================================================================

def load_all_config(spark: SparkSession) -> dict:
    """
    Loads all column mappings, DQ rules, and existing rules summary
    from config tables in a single pass. Returns a dict with:
      - 'mappings': {entity: [Row(source_column, canonical_column, transformation), ...]}
      - 'rules': {entity: [Row(rule_id, entity, rule_name, ...), ...]}
      - 'rules_summary': {entity: [{'rule_name', 'rule_expression', 'issue_type'}, ...]}
    """
    # Column mappings
    mappings_df = spark.read.table(f"{CATALOG}.config.column_mapping")
    all_mappings = mappings_df.select("entity", "source_column", "canonical_column", "transformation").collect()

    mappings_by_entity = {}
    for row in all_mappings:
        mappings_by_entity.setdefault(row["entity"], []).append(row)

    # DQ rules
    rules_df = spark.read.table(f"{CATALOG}.config.dq_rules")
    all_rules = rules_df.collect()

    rules_by_entity = {}
    rules_summary_by_entity = {}
    for row in all_rules:
        e = row["entity"]
        rules_by_entity.setdefault(e, []).append(row)
        rules_summary_by_entity.setdefault(e, []).append({
            "rule_name": row["rule_name"],
            "rule_expression": row["rule_expression"],
            "issue_type": row["issue_type"]
        })

    return {
        "mappings": mappings_by_entity,
        "rules": rules_by_entity,
        "rules_summary": rules_summary_by_entity,
    }


# =============================================================================
# HARMONIZATION ENGINE
# =============================================================================

def harmonize_entity(df: DataFrame, mappings: list, source_label: str) -> DataFrame:
    """
    Applies column mappings to produce a canonical Silver DataFrame.
    Accepts pre-loaded mappings (no .collect() inside this function).

    Args:
        df: Bronze DataFrame (streaming or batch)
        mappings: List of Row/dict with source_column, canonical_column, transformation
        source_label: "llm" or "static_config" — tagged onto every row
    """
    # Group source columns by canonical target
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

    # Build SELECT expressions
    select_exprs = []
    df_cols_lower = [c.lower() for c in df.columns]

    for canon, sources in canonical_groups.items():
        coalesce_parts = []
        for src, xform in sources:
            if src in df_cols_lower:
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

    # Preserve lineage columns
    for meta_col in ["source_file", "load_timestamp", "_source_file", "_load_timestamp"]:
        if meta_col in df.columns:
            target_name = meta_col if meta_col.startswith("_") else f"_{meta_col}"
            select_exprs.append(col(meta_col).alias(target_name))

    select_exprs.append(lit(source_label).alias("_mapping_source"))
    return df.select(*select_exprs)


# =============================================================================
# QUARANTINE ENGINE
# =============================================================================

def build_quarantine_for_entity(df: DataFrame, rules: list, run_id: str) -> DataFrame:
    """
    Evaluates pre-loaded DQ rules against a harmonized DataFrame.
    Returns all failing records tagged with audit metadata.
    No .collect() calls — rules are passed in as a pre-loaded list.
    """
    frames = []

    for rule in rules:
        tagged = (
            df
            .filter(expr(f"NOT ({rule['rule_expression']})"))
            .withColumn("_run_id", lit(run_id))
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


# =============================================================================
# AI DQ DISCOVERY HELPERS
# =============================================================================

def build_ai_dq_prompt(entity: str, profile_text: str, rules_summary: list) -> str:
    """
    Builds the ai_query() prompt for DQ discovery on a given entity.
    All inputs are plain strings/lists — no Spark operations.
    """
    rules_text = "\n".join(
        f"  - {r['rule_name']}: {r['rule_expression']} ({r['issue_type']})"
        for r in rules_summary
    ) if rules_summary else "  (none)"

    return f"""You are a data quality analyst. Analyze this Bronze table profile and find data quality issues that are NOT already covered by the existing static rules.

DATA PROFILE:
{profile_text}

EXISTING STATIC DQ RULES (already enforced — do NOT duplicate these):
{rules_text}

INSTRUCTIONS:
1. Look for patterns the static rules miss: unexpected values, format inconsistencies, outliers, suspicious distributions, encoding issues, placeholder values, cross-column anomalies.
2. For each NEW issue found, provide: column_name, issue_type (one of: format_inconsistency, suspicious_distribution, unexpected_values, cross_column_anomaly, encoding_issue, outlier), severity (CRITICAL or WARNING), a suggested SQL rule expression using the column name directly, and a human-readable description.
3. Return ONLY net-new findings. If a static rule already checks for nulls on a column, do not flag nulls on that column.
4. If no new issues are found, return an empty array.

RESPOND WITH ONLY a JSON array of objects with keys: column_name, issue_type, severity, suggested_rule, description. No markdown, no explanation."""


def parse_ai_dq_response(raw_response: str) -> list:
    """
    Parses the LLM's JSON response into a list of finding dicts.
    Handles markdown wrapping and malformed JSON gracefully.
    """
    import json
    raw_text = raw_response.strip()
    if raw_text.startswith("```"):
        raw_text = raw_text.split("\n", 1)[1]
    if raw_text.endswith("```"):
        raw_text = raw_text.rsplit("\n", 1)[0]
    return json.loads(raw_text)


def generate_run_id() -> str:
    """Generates a unique run ID for pipeline execution tracking."""
    return str(uuid.uuid4())
