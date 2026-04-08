# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Business DQ Reporter
# MAGIC
# MAGIC **Purpose:** Read the 5 Gold quarantine tables (already populated by the Gold pipeline), generate AI explanations for each rule, and write results to `globalmart.gold.gold_dq_audit_report`.
# MAGIC
# MAGIC **Reads:** 5 existing quarantine tables — does NOT apply rules or create tables
# MAGIC
# MAGIC **Writes:** `globalmart.gold.gold_dq_audit_report`, `globalmart.config.pipeline_run_log`
# MAGIC
# MAGIC | Rule | Quarantine Table | Business Failure |
# MAGIC |---|---|---|
# MAGIC | no_orphan_customers | fact_sales_quarantine | Revenue Audit |
# MAGIC | refund_not_exceeding_5x_sales | fact_returns_quarantine | Returns Fraud |
# MAGIC | region_not_null | mv_revenue_by_region_quarantine | Revenue Audit |
# MAGIC | return_rate_in_range | mv_return_rate_by_vendor_quarantine | Vendor Quality |
# MAGIC | valid_days_since_sale | mv_slow_moving_products_quarantine | Inventory Blindspot |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %pip install openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Imports and configuration

# COMMAND ----------

import time
import re
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from openai import OpenAI

client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    base_url="https://7474644504640858.ai-gateway.cloud.databricks.com/mlflow/v1/"
)
MODEL_NAME    = "databricks-gpt-oss-20b"
NOTEBOOK_NAME = "Gold_Layer_Business_DQ_Reporter"
CATALOG       = "globalmart.gold"

TABLE_GOLD_DQ = f"{CATALOG}.gold_dq_audit_report"
TABLE_RUN_LOG = f"globalmart.config.pipeline_run_log"

# Existing quarantine tables — created and populated by the Gold pipeline
Q_FACT_SALES     = f"{CATALOG}.fact_sales_quarantine"
Q_FACT_RETURNS   = f"{CATALOG}.fact_returns_quarantine"
Q_MV_REVENUE     = f"{CATALOG}.mv_revenue_by_region_quarantine"
Q_MV_RETURN_RATE = f"{CATALOG}.mv_return_rate_by_vendor_quarantine"
Q_MV_SLOW        = f"{CATALOG}.mv_slow_moving_products_quarantine"

print(f"Setup complete | Model: {MODEL_NAME} | Started: {datetime.now():%Y-%m-%d %H:%M:%S}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — LLM helper functions

# COMMAND ----------

def extract_text(response) -> str:
    content = response.choices[0].message.content
    if isinstance(content, list):
        return " ".join(
            block["text"] for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
    return content.strip()


def call_llm(prompt: str, system_msg: str, retries: int = 3, wait: int = 2) -> str:
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.3
            )
            return extract_text(response)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(wait * (attempt + 1))
            else:
                return f"LLM_UNAVAILABLE: {e}"


def validate_llm_output(text: str, min_words: int = 30) -> dict:
    required_sections = [
        "WHAT IS THE PROBLEM:",
        "WHY THIS RECORD TYPE CANNOT BE ACCEPTED:",
        "WHAT IS AT RISK:"
    ]
    refusal_phrases = ["i cannot", "as an ai", "i don't have", "i'm unable"]
    issues = []
    if len(text.split()) < min_words:
        issues.append("too_short")
    if text.startswith("LLM_UNAVAILABLE"):
        issues.append("llm_call_failed")
    if any(p in text.lower() for p in refusal_phrases):
        issues.append("refusal_detected")
    if not all(s in text for s in required_sections):
        issues.append("missing_section")
    if not text.strip():
        text = "[REVIEW REQUIRED - LLM returned empty response]"
    text = re.sub(r'\*{1,2}([^*]+)\*{1,2}', r'\1', text)
    text = re.sub(r'#{1,3}\s*', '', text)
    text = text.strip()
    return {
        "text":             text,
        "llm_check":        "PASS" if not issues else "REVIEW",
        "llm_check_detail": ", ".join(issues) if issues else "none"
    }


SYSTEM_MSG_GOLD_DQ = (
    "You are a senior data quality analyst writing audit documentation for a finance director. "
    "Every explanation must answer exactly three questions in order, using these exact labels: "
    "WHAT IS THE PROBLEM, WHY THIS RECORD TYPE CANNOT BE ACCEPTED, WHAT IS AT RISK. "
    "Write in plain English. Never use database or programming terminology. "
    "Do not use any markdown formatting. Plain text only."
)

print("LLM helpers ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Read quarantine tables
# MAGIC
# MAGIC Reads each quarantine table and counts how many records are currently flagged. These tables are created and populated by the Gold pipeline — this notebook only reads them.

# COMMAND ----------

def count_quarantine(table_name: str) -> int:
    """Count rows in a quarantine table. Returns 0 if table does not exist yet."""
    try:
        return spark.table(table_name).count()
    except Exception as e:
        print(f"  Warning: could not read {table_name} — {e}")
        return 0


def sample_evidence(table_name: str, n: int = 3) -> list:
    """Pull up to n sample _business_impact values for the prompt."""
    try:
        col = "_business_impact" if "_business_impact" in spark.table(table_name).columns else None
        if col:
            rows = spark.table(table_name).select(col).distinct().limit(n).collect()
            return [r[0] for r in rows if r[0]]
    except Exception:
        pass
    return []


r1_count = count_quarantine(Q_FACT_SALES)
r2_count = count_quarantine(Q_FACT_RETURNS)
r3_count = count_quarantine(Q_MV_REVENUE)
r4_count = count_quarantine(Q_MV_RETURN_RATE)
r5_count = count_quarantine(Q_MV_SLOW)

print("Quarantine table counts:")
print(f"  fact_sales_quarantine                  : {r1_count:,} records")
print(f"  fact_returns_quarantine                : {r2_count:,} records")
print(f"  mv_revenue_by_region_quarantine        : {r3_count:,} records")
print(f"  mv_return_rate_by_vendor_quarantine    : {r4_count:,} records")
print(f"  mv_slow_moving_products_quarantine     : {r5_count:,} records")
print(f"  Total failing records                  : {r1_count+r2_count+r3_count+r4_count+r5_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Generate AI explanations
# MAGIC
# MAGIC One LLM call per rule. Each explanation answers the three required audit sections.
# MAGIC
# MAGIC A 2-second pause between calls prevents gateway rate limiting. Any REVIEW record is retried once with a 5-second wait before writing to Delta.

# COMMAND ----------

# Rule definitions — metadata used to build the prompt for each rule
# _business_impact is read from the quarantine table itself where available
rules = [
    {
        "rule_name":     "no_orphan_customers",
        "table":         "fact_sales",
        "quarantine":    Q_FACT_SALES,
        "field":         "customer_key",
        "count":         r1_count,
        "business_fail": "Revenue Audit",
        "trigger":       "customer_key IS NULL — transaction has no customer reference",
        "impact":        (
            "This transaction cannot be attributed to any customer, segment, or region. "
            "Unattributed revenue was the root cause of the 9 percent revenue overstatement. "
            "The finance team cannot explain where this money came from during the audit."
        ),
    },
    {
        "rule_name":     "refund_not_exceeding_5x_sales",
        "table":         "fact_returns",
        "quarantine":    Q_FACT_RETURNS,
        "field":         "refund_amount vs original_sales",
        "count":         r2_count,
        "business_fail": "Returns Fraud",
        "trigger":       "refund_amount > original_sales * 5",
        "impact":        (
            "A refund more than 5 times the product sale price is a strong fraud indicator. "
            "This is contributing to the 2.3 million dollar annual return fraud loss. "
            "The returns team must investigate before processing this refund."
        ),
    },
    {
        "rule_name":     "region_not_null",
        "table":         "mv_revenue_by_region",
        "quarantine":    Q_MV_REVENUE,
        "field":         "region",
        "count":         r3_count,
        "business_fail": "Revenue Audit",
        "trigger":       "region IS NULL — revenue row has no geographic assignment",
        "impact":        (
            "Revenue without a region cannot be included in any regional report. "
            "Auditors will flag this as an unresolved reconciliation gap. "
            "The finance team cannot reconcile total revenue with the sum of regional figures."
        ),
    },
    {
        "rule_name":     "return_rate_in_range",
        "table":         "mv_return_rate_by_vendor",
        "quarantine":    Q_MV_RETURN_RATE,
        "field":         "return_rate_pct",
        "count":         r4_count,
        "business_fail": "Vendor Quality",
        "trigger":       "return_rate_pct < 0 OR return_rate_pct > 100",
        "impact":        (
            "A return rate below 0 percent or above 100 percent is mathematically impossible. "
            "This indicates a calculation error in the Gold layer. "
            "The merchandising team will make wrong vendor contract decisions based on this figure."
        ),
    },
    {
        "rule_name":     "valid_days_since_sale",
        "table":         "mv_slow_moving_products",
        "quarantine":    Q_MV_SLOW,
        "field":         "days_since_last_sale",
        "count":         r5_count,
        "business_fail": "Inventory Blindspot",
        "trigger":       "days_since_last_sale IS NULL OR days_since_last_sale < 0",
        "impact":        (
            "Without a valid days-since-last-sale figure this product is invisible to the "
            "merchandising team discount and redistribution process. "
            "This contributes to the 12-18 percent revenue loss from inventory blindspots."
        ),
    },
]

audit_records  = []
llm_call_count = 0
review_count   = 0

for rule in rules:
    prompt = (
        "You are writing a Gold layer data quality audit report for GlobalMart finance director.\n\n"
        f"RULE: {rule['rule_name']}\n"
        f"TABLE: {rule['table']}\n"
        f"FIELD: {rule['field']}\n"
        f"FAILING RECORDS: {rule['count']:,}\n"
        f"BUSINESS FAILURE CATEGORY: {rule['business_fail']}\n"
        f"TRIGGER CONDITION: {rule['trigger']}\n"
        f"BUSINESS IMPACT: {rule['impact']}\n\n"
        "Write the explanation in EXACTLY this format:\n\n"
        f"WHAT IS THE PROBLEM: Exactly {rule['count']:,} records from {rule['table']} "
        f"were set aside because the {rule['field']} field failed the {rule['rule_name']} check. "
        "Describe what the bad value looked like using the trigger condition above.\n\n"
        "WHY THIS RECORD TYPE CANNOT BE ACCEPTED: Explain specifically why records failing "
        f"this check on {rule['field']} cannot be used in any report or calculation. "
        "Name what breaks downstream.\n\n"
        "WHAT IS AT RISK: Use the business impact provided. "
        "Name the specific report or decision affected. State who must fix it.\n\n"
        "Rules:\n"
        "- Do NOT use: pipeline, DLT, Spark, Delta, ETL, quarantine\n"
        "- Instead say: set aside for review, removed from reporting\n"
        "- Start directly with WHAT IS THE PROBLEM\n"
        "- Plain text only, no markdown"
    )

    raw    = call_llm(prompt, SYSTEM_MSG_GOLD_DQ)
    time.sleep(2)
    result = validate_llm_output(raw)
    llm_call_count += 1

    if result["llm_check"] == "REVIEW":
        review_count += 1

    status = "CLEAN" if rule["count"] == 0 else f"FAIL ({rule['count']:,} records)"
    print(f"[{result['llm_check']}] {rule['rule_name']:<38} {status}")

    audit_records.append({
        "rule_name":         rule["rule_name"],
        "table_checked":     rule["table"],
        "field_checked":     rule["field"],
        "failing_count":     int(rule["count"]),
        "business_failure":  rule["business_fail"],
        "trigger_condition": rule["trigger"],
        "business_impact":   rule["impact"],
        "ai_explanation":    result["text"],
        "llm_check":         result["llm_check"],
        "llm_check_detail":  result["llm_check_detail"],
        "generated_at":      datetime.now().isoformat()
    })


# Retry any REVIEW records with a longer wait
review_idx = [i for i, r in enumerate(audit_records) if r["llm_check"] == "REVIEW"]
if review_idx:
    print(f"\nRetrying {len(review_idx)} REVIEW record(s)...")
    for idx in review_idx:
        rec  = audit_records[idx]
        rule = next(r for r in rules if r["rule_name"] == rec["rule_name"])
        time.sleep(5)
        retry_prompt = (
            f"Rule: {rule['rule_name']} | Table: {rule['table']} | Field: {rule['field']}\n"
            f"Failing records: {rule['count']:,} | Business failure: {rule['business_fail']}\n"
            f"Trigger: {rule['trigger']}\nImpact: {rule['impact']}\n\n"
            "Write three sections with exact labels:\n"
            "WHAT IS THE PROBLEM: (one or two sentences)\n"
            "WHY THIS RECORD TYPE CANNOT BE ACCEPTED: (one or two sentences)\n"
            "WHAT IS AT RISK: (one or two sentences)\n"
            "Plain English only. No markdown."
        )
        retry_raw    = call_llm(retry_prompt, SYSTEM_MSG_GOLD_DQ)
        retry_result = validate_llm_output(retry_raw)
        llm_call_count += 1
        if retry_result["llm_check"] == "PASS":
            audit_records[idx]["ai_explanation"]   = retry_result["text"]
            audit_records[idx]["llm_check"]        = "PASS"
            audit_records[idx]["llm_check_detail"] = "none"
            review_count -= 1
            print(f"  Retry PASSED: {rec['rule_name']}")
        else:
            print(f"  Retry still REVIEW: {rec['rule_name']}")
else:
    print("\nNo REVIEW records — all explanations passed first attempt.")

total_failing = sum(r["failing_count"] for r in audit_records)
print(f"\nDone. {len(audit_records)} explanations | LLM calls: {llm_call_count} | Review: {review_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Write Gold DQ audit report

# COMMAND ----------

schema = StructType([
    StructField("rule_name",         StringType(),  True),
    StructField("table_checked",     StringType(),  True),
    StructField("field_checked",     StringType(),  True),
    StructField("failing_count",     IntegerType(), True),
    StructField("business_failure",  StringType(),  True),
    StructField("trigger_condition", StringType(),  True),
    StructField("business_impact",   StringType(),  True),
    StructField("ai_explanation",    StringType(),  True),
    StructField("llm_check",         StringType(),  True),
    StructField("llm_check_detail",  StringType(),  True),
    StructField("generated_at",      StringType(),  True),
])

df_gold_dq = spark.createDataFrame(audit_records, schema=schema)

(
    df_gold_dq.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_GOLD_DQ)
)

print(f"Written to {TABLE_GOLD_DQ}")
display(
    spark.table(TABLE_GOLD_DQ)
    .orderBy(F.col("failing_count").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Log the run

# COMMAND ----------

run_log = [{
    "notebook":          NOTEBOOK_NAME,
    "run_timestamp":     datetime.now().isoformat(),
    "records_processed": 5,
    "llm_calls_made":    llm_call_count,
    "outputs_flagged":   review_count,
    "status":            "SUCCESS" if review_count == 0 else "PARTIAL_REVIEW",
    "notes":             f"gold_dq_rules=5 | total_failing={total_failing}"
}]

(
    spark.createDataFrame(run_log)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_RUN_LOG)
)

print(f"Run log written. Status: {run_log[0]['status']}")
print(f"Rules checked  : 5")
print(f"Total failing  : {total_failing:,}")
print(f"LLM calls      : {llm_call_count}")
print(f"Review flags   : {review_count}")