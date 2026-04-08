# =============================================================================
# GlobalMart — Pipeline Utilities
# Path: /Workspace/dbx_retail/Globalmart/utilities/utils.py
#
# Shared functions used across Bronze and Silver pipelines.
# Bronze uses: sanitize_column_names
# Silver uses: load_column_mapping, load_dq_rules, harmonize_columns, build_quarantine
# =============================================================================

import re
import uuid
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce, expr, lit, current_timestamp


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
# SILVER UTILITIES — Config Loaders
# =============================================================================

def load_column_mapping(spark: SparkSession, entity: str) -> list:
    """
    Loads active column mappings for a given entity from globalmart.ingestion_control.
    Returns a list of Rows: [{source_column, canonical_column, transformation}, ...]
    """
    return (
        spark.read.table("globalmart.ingestion_control.column_mapping")
        .filter(col("entity") == entity)
        .select("source_column", "canonical_column", "transformation")
        .collect()
    )


def load_dq_rules(spark: SparkSession, entity: str) -> list:
    """
    Loads active DQ rules for a given entity from globalmart.ingestion_control.
    Returns a list of Rows with all rule metadata.
    """
    return (
        spark.read.table("globalmart.ingestion_control.dq_rules")
        .filter(col("entity") == entity)
        .collect()
    )


# =============================================================================
# SILVER UTILITIES — Harmonization Engine
# =============================================================================

def harmonize_columns(df: DataFrame, spark: SparkSession, entity: str, mappings: list = None) -> DataFrame:
    """
    Reads column_mapping for the entity and produces canonical columns via
    COALESCE. For each canonical column, all source variants are coalesced.
    If a transformation is defined, it is applied before coalescing.
    """
    if mappings is None:
        mappings = load_column_mapping(spark, entity)

    # Group source columns by canonical name
    canonical_groups = {}
    for m in mappings:
        canon = m["canonical_column"]
        src = m["source_column"]
        xform = m["transformation"]
        if canon not in canonical_groups:
            canonical_groups[canon] = []
        canonical_groups[canon].append((src, xform))

    # Build COALESCE expressions for each canonical column
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

    # Preserve lineage columns from Bronze
    for meta_col in ["source_file", "load_timestamp", "_source_file", "_load_timestamp"]:
        if meta_col in df.columns:
            # Standardize to underscore-prefixed names for Silver
            target_name = meta_col if meta_col.startswith("_") else f"_{meta_col}"
            select_exprs.append(col(meta_col).alias(target_name))

    return df.select(*select_exprs)


# =============================================================================
# SILVER UTILITIES — Quarantine Engine
# =============================================================================

def build_quarantine(df: DataFrame, spark: SparkSession, entity: str, run_id: str, rules: list = None) -> DataFrame:
    """
    Evaluates all active DQ rules for the entity. Returns a DataFrame of all
    failing records (CRITICAL + WARNING) tagged with quarantine metadata.
    """
    if rules is None:
        rules = load_dq_rules(spark, entity)

    total_count = df.count()
    quarantine_frames = []

    for rule in rules:
        try:
            # Select records that FAIL the rule (inverse of the expression)
            failing = df.filter(expr(f"NOT ({rule['rule_expression']})"))
            fail_count = failing.count()

            if fail_count > 0:
                tagged = (
                    failing
                    .withColumn("_run_id", lit(run_id))
                    .withColumn("_run_timestamp", current_timestamp())
                    .withColumn("_expectation_name", lit(rule["rule_name"]))
                    .withColumn("_severity", lit(rule["severity"]))
                    .withColumn("_issue_type", lit(rule["issue_type"]))
                    .withColumn("_failure_reason", lit(rule["failure_reason"]))
                    .withColumn("_total_records_processed", lit(total_count))
                    .withColumn("_quarantine_timestamp", current_timestamp())
                )
                quarantine_frames.append(tagged)
        except Exception as e:
            print(f"WARNING: Rule '{rule['rule_name']}' failed to evaluate: {str(e)}")
            continue

    if quarantine_frames:
        result = quarantine_frames[0]
        for frame in quarantine_frames[1:]:
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
            .withColumn("_total_records_processed", lit(None).cast("int"))
            .withColumn("_quarantine_timestamp", current_timestamp())
        )

def generate_run_id() -> str:
    """Generates a unique run ID for pipeline execution tracking."""
    return str(uuid.uuid4())