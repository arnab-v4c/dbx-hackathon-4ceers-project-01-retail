# Databricks notebook source
# MAGIC %md
# MAGIC # UC1 — AI Data Quality Reporter
# MAGIC
# MAGIC **Purpose:** Read rejected records from the Silver quarantine tables, group them by issue type, generate a plain-English audit explanation for each group using an LLM, and write the full audit report to the Gold layer.
# MAGIC
# MAGIC **Output table:** `globalmart.gold.dq_audit_report`
# MAGIC
# MAGIC **Each AI explanation answers three specific questions required by the audit:**
# MAGIC 1. What the problem is and what value or pattern triggered the rejection
# MAGIC 2. Why this record type cannot be accepted into the reporting database
# MAGIC 3. What business report, audit figure, or operational decision is at risk
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. Install dependencies (run once, then restart Python)
# MAGIC 2. Setup — imports, config, LLM connection
# MAGIC 3. Load quarantine tables
# MAGIC 4. Watermark — skip records already processed on a previous run
# MAGIC 5. Aggregate — group rejections by issue type
# MAGIC 6. Classify severity per issue group
# MAGIC 7. Build prompt and system message
# MAGIC 8. Generate AI explanations (with automatic retry for failed calls)
# MAGIC 9. Write results to Gold layer
# MAGIC 10. Log the run
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Install dependencies
# MAGIC
# MAGIC Run this cell once, then restart Python before continuing.

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

# ── LLM connection ────────────────────────────────────────────────────────
# Uses the workspace token automatically — no API key needed
client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    base_url="https://7474644504640858.ai-gateway.cloud.databricks.com/mlflow/v1/"
)
MODEL_NAME    = "databricks-gpt-oss-20b"
NOTEBOOK_NAME = "UC1_AI_Data_Quality_Reporter"

# ── Quarantine tables (Silver layer rejects land here) ────────────────────
TABLE_CUSTOMERS    = "globalmart.silver.customers_quarantine"
TABLE_ORDERS       = "globalmart.silver.orders_quarantine"
TABLE_TRANSACTIONS = "globalmart.silver.transactions_quarantine"
TABLE_PRODUCTS     = "globalmart.silver.products_quarantine"
TABLE_RETURNS      = "globalmart.silver.returns_quarantine"
TABLE_VENDORS      = "globalmart.silver.vendors_quarantine"

# ── Output tables (Gold layer) ────────────────────────────────────────────
TABLE_AUDIT   = "globalmart.gold.dq_audit_report"
TABLE_RUN_LOG = "globalmart.config.pipeline_run_log"

print(f"Setup complete | Model: {MODEL_NAME} | Started: {datetime.now():%Y-%m-%d %H:%M:%S}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — LLM helper functions

# COMMAND ----------

def extract_text(response) -> str:
    """
    Pull the answer text out of the model response.

    databricks-gpt-oss-20b returns a list of content blocks:
      [{"type": "reasoning", ...}, {"type": "text", "text": "the answer"}]
    We only want the "text" block. If the response is already a plain
    string (older model versions), return it directly.
    """
    content = response.choices[0].message.content
    if isinstance(content, list):
        return " ".join(
            block["text"]
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
    return content.strip()


def call_llm(prompt: str, system_msg: str, retries: int = 3, wait: int = 2) -> str:
    """
    Call the LLM with automatic retry on failure.

    Returns the answer text on success, or a string starting with
    'LLM_UNAVAILABLE:' if all retries fail — so the pipeline keeps
    running and failures are recorded rather than crashing.
    """
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.3   # Low temperature = consistent, factual output
            )
            return extract_text(response)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(wait * (attempt + 1))  # Back off before retrying
            else:
                return f"LLM_UNAVAILABLE: {e}"


def validate_llm_output(text: str, min_words: int = 30) -> dict:
    """
    Check the LLM output quality before writing to the audit table.

    Returns a dict with:
      text             -- the explanation text (cleaned)
      llm_check        -- PASS or REVIEW
      issues           -- comma-separated list of problems found, or 'none'

    REVIEW means a human should check the explanation before sharing
    it with the external auditor.

    Validation checks:
      too_short                 -- fewer than 30 words (likely truncated or empty)
      llm_call_failed           -- API returned an error
      refusal_detected          -- model refused to answer
      technical_jargon_detected -- explanation contains pipeline/layer terminology
      missing_section           -- one of the three required sections is absent
    """
    refusal_phrases   = ["i cannot", "as an ai", "i don't have", "i'm unable", "i am unable"]
    jargon_phrases    = ["silver layer", "bronze layer", "harmonization process",
                         "the analytics layer", "the pipeline", "dlt", "delta table"]
    required_sections = ["WHAT IS THE PROBLEM:", "WHY THIS RECORD TYPE CANNOT BE ACCEPTED:", "WHAT IS AT RISK:"]

    issues = []

    if len(text.split()) < min_words:
        issues.append("too_short")
    if text.startswith("LLM_UNAVAILABLE"):
        issues.append("llm_call_failed")
    if any(phrase in text.lower() for phrase in refusal_phrases):
        issues.append("refusal_detected")
    if any(phrase in text.lower() for phrase in jargon_phrases):
        issues.append("technical_jargon_detected")
    if not all(section in text for section in required_sections):
        issues.append("missing_section")

    # Strip markdown formatting the model occasionally adds despite instructions
    text = re.sub(r'\*{1,2}([^*]+)\*{1,2}', r'\1', text)
    text = re.sub(r'#{1,3}\s*', '', text)
    text = text.strip()

    # If text is empty after stripping, write a placeholder so the field is
    # never blank — an empty ai_explanation is harder to spot than a labelled one
    if not text.strip():
        text = f"[REVIEW REQUIRED — LLM returned empty response. Issues: {', '.join(issues) if issues else 'unknown'}]"

    return {
        "text":      text,
        "llm_check": "PASS" if not issues else "REVIEW",
        "issues":    ", ".join(issues) if issues else "none"
    }


print("LLM helpers ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Load quarantine tables
# MAGIC
# MAGIC Each table holds records that the Silver pipeline rejected. All six tables share the same metadata columns:
# MAGIC
# MAGIC | Column | What it contains |
# MAGIC |---|---|
# MAGIC | `_source_file` | Exact path of the CSV/JSON the record came from |
# MAGIC | `_expectation_name` | Name of the quality rule that rejected the record |
# MAGIC | `_issue_type` | Category: `missing_optional_field`, `format_error`, `invalid_category` |
# MAGIC | `_failure_reason` | The actual failure message, including the bad value |
# MAGIC | `_quarantine_timestamp` | When the record was rejected |

# COMMAND ----------

df_q_customers    = spark.table(TABLE_CUSTOMERS)
df_q_orders       = spark.table(TABLE_ORDERS)
df_q_transactions = spark.table(TABLE_TRANSACTIONS)
df_q_products     = spark.table(TABLE_PRODUCTS)
df_q_returns      = spark.table(TABLE_RETURNS)
df_q_vendors      = spark.table(TABLE_VENDORS)

print("Quarantine table row counts:")
for name, df in [
    ("customers",    df_q_customers),
    ("orders",       df_q_orders),
    ("transactions", df_q_transactions),
    ("products",     df_q_products),
    ("returns",      df_q_returns),
    ("vendors",      df_q_vendors),
]:
    print(f"  {name:<15} : {df.count():,} rejected records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Incremental watermark
# MAGIC
# MAGIC On the first run, all quarantine records are processed. On subsequent runs, only records quarantined *after* the last run are processed. This prevents duplicate explanations being generated for the same issue group.

# COMMAND ----------

try:
    last_run_ts = spark.sql(
        f"SELECT MAX(generated_at) AS last_ts FROM {TABLE_AUDIT}"
    ).collect()[0]["last_ts"]
    print(f"Last run: {last_run_ts} — processing new records only.")
except Exception:
    last_run_ts = None
    print("No previous run found — processing all quarantine records.")


def apply_watermark(df):
    """Keep only records quarantined after the last run. Skip filter if first run."""
    if last_run_ts is not None and "_quarantine_timestamp" in df.columns:
        return df.filter(F.col("_quarantine_timestamp") > F.lit(last_run_ts))
    return df


df_q_customers    = apply_watermark(df_q_customers)
df_q_orders       = apply_watermark(df_q_orders)
df_q_transactions = apply_watermark(df_q_transactions)
df_q_products     = apply_watermark(df_q_products)
df_q_returns      = apply_watermark(df_q_returns)
df_q_vendors      = apply_watermark(df_q_vendors)

print("\nPost-watermark counts (records to process this run):")
for name, df in [
    ("customers",    df_q_customers),
    ("orders",       df_q_orders),
    ("transactions", df_q_transactions),
    ("products",     df_q_products),
    ("returns",      df_q_returns),
    ("vendors",      df_q_vendors),
]:
    print(f"  {name:<15} : {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Aggregate rejections into issue groups
# MAGIC
# MAGIC Instead of generating one explanation per rejected row (slow and repetitive), we group by rule name and issue type first. One LLM call per group covers all records that failed for the same reason.
# MAGIC
# MAGIC For each group we collect:
# MAGIC - **Sample failure messages** from `_failure_reason` — actual bad values the LLM can cite
# MAGIC - **Source files** from `_source_file` — which regional CSV/JSON files contributed the failures
# MAGIC - **Time range** — when the earliest and latest failures occurred

# COMMAND ----------

def aggregate_quarantine(df, entity_name: str, table_name: str):
    """
    Group rejections by rule name and issue type.
    Collect evidence so the LLM prompt has concrete data to reference.
    """
    return (
        df.groupBy("_issue_type", "_expectation_name")
          .agg(
              F.count("*").alias("rejected_count"),
              F.slice(F.collect_set("_failure_reason"), 1, 3).alias("sample_reasons"),
              F.slice(F.collect_set("_source_file"), 1, 5).alias("source_files"),
              F.min("_quarantine_timestamp").alias("earliest_failure"),
              F.max("_quarantine_timestamp").alias("latest_failure"),
          )
          .withColumn("entity",       F.lit(entity_name))
          .withColumn("source_table", F.lit(table_name))
          .withColumnRenamed("_expectation_name", "field_affected")
          .withColumnRenamed("_issue_type",        "issue_type")
    )


dq_df = (
    aggregate_quarantine(df_q_customers,    "customers",    TABLE_CUSTOMERS)
    .unionByName(aggregate_quarantine(df_q_orders,       "orders",       TABLE_ORDERS),       allowMissingColumns=True)
    .unionByName(aggregate_quarantine(df_q_transactions, "transactions", TABLE_TRANSACTIONS), allowMissingColumns=True)
    .unionByName(aggregate_quarantine(df_q_products,     "products",     TABLE_PRODUCTS),     allowMissingColumns=True)
    .unionByName(aggregate_quarantine(df_q_returns,      "returns",      TABLE_RETURNS),      allowMissingColumns=True)
    .unionByName(aggregate_quarantine(df_q_vendors,      "vendors",      TABLE_VENDORS),      allowMissingColumns=True)
    .filter(F.col("rejected_count") > 0)
)

dq_summary = dq_df.collect()

print(f"Issue groups to explain this run: {len(dq_summary)}")
display(
    dq_df
    .select("entity", "field_affected", "issue_type", "rejected_count", "source_table")
    .orderBy("entity", F.col("rejected_count").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Classify severity
# MAGIC
# MAGIC Each issue group is classified so the finance team knows which rows to address first.
# MAGIC
# MAGIC | Severity | Criteria |
# MAGIC |---|---|
# MAGIC | CRITICAL | Missing primary key, null ID, invalid category on a financial field, or > 100 records |
# MAGIC | HIGH | Invalid reference, negative amount, format error, or > 30 records |
# MAGIC | MEDIUM | Missing optional fields, invalid ranges, everything else |

# COMMAND ----------

def classify_severity(entity: str, issue_type: str, count: int) -> str:
    critical_issues = {
        "missing_value", "null_primary_key",
        "missing_customer_id", "missing_order_id",
        "invalid_category"
    }
    high_issues = {
        "invalid_reference", "negative_amount",
        "orphaned_record", "duplicate_primary_key",
        "format_error"
    }
    if issue_type.lower() in critical_issues or count > 100:
        return "CRITICAL"
    elif issue_type.lower() in high_issues or count > 30:
        return "HIGH"
    else:
        return "MEDIUM"


print("Severity classifier ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Build the LLM prompt
# MAGIC
# MAGIC The prompt produces a **three-section structured explanation** that directly maps to the three questions required by the audit rubric:
# MAGIC
# MAGIC | Section label | Rubric requirement |
# MAGIC |---|---|
# MAGIC | **WHAT IS THE PROBLEM** | What the problem is and what value or pattern triggered the rejection |
# MAGIC | **WHY THIS RECORD TYPE CANNOT BE ACCEPTED** | Why this record type cannot be accepted into the reporting database |
# MAGIC | **WHAT IS AT RISK** | What business report, audit figure, or operational decision is at risk |
# MAGIC
# MAGIC Key design decisions:
# MAGIC - A `BUSINESS_IMPACT` dictionary maps every `(entity, issue_type)` combination to the exact report or figure it affects — the LLM is told to use this context directly, not invent its own
# MAGIC - Sample failure messages are injected so the LLM can name the actual bad value or pattern
# MAGIC - Source file paths are shortened to `filename.csv (Region N)` for readability
# MAGIC - Technical terms (Silver layer, pipeline, ETL) are explicitly banned
# MAGIC - `validate_llm_output` checks all three section labels are present — flags REVIEW if any missing

# COMMAND ----------

def _format_sample_reasons(sample_reasons: list) -> str:
    """Format sample failure messages for the prompt."""
    if not sample_reasons:
        return "  - No sample messages available"
    return "\n".join(f"  - {r}" for r in sample_reasons[:3])


def _format_source_files(source_files: list) -> str:
    """
    Shorten full Volume paths to readable filenames with region labels.
    /Volumes/globalmart/raw/.../Region 3/customers_3.csv
    becomes: customers_3.csv (Region 3)
    """
    if not source_files:
        return "  - Source file information not available"
    lines = []
    for path in list(source_files)[:4]:
        filename = path.split("/")[-1] if "/" in path else path
        region   = ""
        for part in path.split("/"):
            if "Region" in part or "region" in part:
                region = f" ({part})"
                break
        lines.append(f"  - {filename}{region}")
    return "\n".join(lines)


BUSINESS_IMPACT = {
    ("customers", "null_primary_key"):
        "customer_id is the link between every sale, return, and order. Without it, revenue and "
        "return figures cannot be attributed to any customer — the customer revenue report and "
        "fraud investigation both have unattributable rows.",
    ("customers", "missing_optional_field"):
        "Missing customer email breaks the customer contact list used for audit confirmation letters. "
        "Missing customer name means the fraud investigation queue has anonymous entries investigators "
        "cannot act on.",
    ("customers", "invalid_category"):
        "Unrecognised customer segment values (Consumer, Corporate, Home Office) mean those records "
        "are excluded from the segment revenue breakdown — the finance team uses this split for "
        "quarterly profit reporting.",
    ("orders", "null_primary_key"):
        "order_id is the primary key that links transactions and returns to orders. Orders with no ID "
        "cannot be matched to any revenue figure — the order fulfilment rate and on-time delivery "
        "KPIs both drop incorrectly.",
    ("orders", "format_error"):
        "Purchase timestamps that could not be parsed mean those orders have no date. The monthly "
        "revenue trend report and the year-to-date profit figure both depend on order date — undated "
        "orders fall out of every time-series chart.",
    ("orders", "missing_optional_field"):
        "Missing ship_mode and delivery timestamps mean those orders cannot be included in the "
        "delivery performance report. The operations team uses on-time delivery rate for carrier "
        "contract reviews.",
    ("transactions", "null_foreign_key"):
        "Transactions with no order_id or product_id cannot be linked to any order or product. "
        "These records are excluded from the total revenue and total profit figures — every sales "
        "KPI dashboard is understated by exactly this many transactions.",
    ("transactions", "format_error"):
        "Sales amounts that could not be parsed (likely due to currency symbol formatting) are "
        "treated as zero revenue. The total revenue figure and the profit margin calculation are "
        "both understated for the affected regions.",
    ("transactions", "missing_optional_field"):
        "Missing profit values (entirely absent in Region 4) mean profit margin cannot be calculated "
        "for those transactions. The profit margin report for Region 4 is blank — this is a known "
        "gap that must be disclosed in the audit.",
    ("transactions", "invalid_category"):
        "Unrecognised payment types mean those transactions cannot be included in the payment method "
        "breakdown. The finance team uses this split to reconcile card and installment payments "
        "against bank statements.",
    ("returns", "null_foreign_key"):
        "Returns with no order_id cannot be matched to any original sale. These unmatched returns "
        "inflate the return count without reducing any revenue figure — the return rate KPI and "
        "refund exposure total are both wrong.",
    ("returns", "invalid_category"):
        "Return status values that do not match the allowed set (Approved, Pending, Rejected) are "
        "excluded from the returns processing queue. The returns team cannot see or action these "
        "records — they are invisible in the investigation dashboard.",
    ("returns", "missing_optional_field"):
        "Missing return reason means those returns cannot be categorised. The return reason breakdown "
        "— used to identify product defect patterns and vendor accountability — is incomplete for "
        "the affected regions.",
    ("returns", "format_error"):
        "Refund amounts that could not be parsed mean those returns show a zero refund. The total "
        "refund exposure figure and the vendor return rate calculation are both understated.",
    ("products", "null_primary_key"):
        "Products with no product_id cannot be linked to any transaction. Sales of these products "
        "are orphaned — the product revenue report and the slow-moving inventory analysis both have "
        "gaps for unidentified products.",
    ("products", "missing_optional_field"):
        "Missing product name or brand means those products appear as blank entries in every product "
        "performance report. The merchandising team cannot identify which products are slow-moving "
        "or generating returns.",
    ("vendors", "null_primary_key"):
        "Vendors with no vendor_id cannot be linked to any transaction or return. The vendor return "
        "rate report and the vendor performance scorecard both have unattributable rows — the "
        "procurement team cannot hold unnamed vendors accountable.",
    ("vendors", "missing_optional_field"):
        "Missing vendor name means those vendors appear as blank entries in the vendor return rate "
        "report. The procurement team uses this report for contract review — unnamed vendors cannot "
        "be actioned.",
}


def get_business_impact(entity: str, issue_type: str) -> str:
    """Return the specific business impact statement for this entity and issue type."""
    key = (entity.lower(), issue_type.lower())
    if key in BUSINESS_IMPACT:
        return BUSINESS_IMPACT[key]
    entity_reports = {
        "customers":    "the customer revenue report and fraud investigation queue",
        "orders":       "the order fulfilment rate and monthly revenue trend report",
        "transactions": "the total revenue figure and profit margin calculation",
        "returns":      "the return rate KPI and refund exposure total",
        "products":     "the product revenue report and slow-moving inventory analysis",
        "vendors":      "the vendor return rate report and procurement scorecard",
    }
    report = entity_reports.get(entity.lower(), "the financial reports")
    return (
        f"These records are excluded from {report}. The data owner for {entity} must source the "
        "correct values from the originating regional system and reload before the audit deadline."
    )


def build_dq_prompt(
    entity: str, field: str, issue_type: str, count: int, severity: str,
    source_table: str, sample_reasons: list, source_files: list,
    earliest_failure: str, latest_failure: str,
) -> str:
    """
    Build the prompt for one issue group.
    Three labelled sections map directly to the three audit rubric requirements:
      WHAT IS THE PROBLEM                      — value/pattern that triggered rejection
      WHY THIS RECORD TYPE CANNOT BE ACCEPTED  — why it breaks downstream processing
      WHAT IS AT RISK                          — specific report, figure, or decision at risk
    """
    time_note = ""
    if earliest_failure and latest_failure and earliest_failure != latest_failure:
        time_note = f"Failures span {earliest_failure[:10]} to {latest_failure[:10]}."
    elif earliest_failure:
        time_note = f"First recorded on {earliest_failure[:10]}."

    reasons_block   = _format_sample_reasons(sample_reasons)
    files_block     = _format_source_files(source_files)
    business_impact = get_business_impact(entity, issue_type)

    return (
        "You are writing a data quality audit report for GlobalMart's external finance auditors.\n"
        "GlobalMart unified data from 6 disconnected regional systems. "
        "Records that failed validation were set aside before reaching any reports or financial figures.\n\n"
        "Write the explanation in EXACTLY this format — three labelled sections, "
        "each one or two sentences. Use the exact labels shown below:\n\n"
        f"WHAT IS THE PROBLEM: Exactly {count:,} {entity} records were removed from the reporting "
        f"database because the {field} field contained an invalid or missing value. "
        "Name the specific value or pattern that triggered the removal using the sample messages below. "
        "Do not say 'was flagged' or 'failed validation' — describe what the data actually looked like.\n\n"
        "WHY THIS RECORD TYPE CANNOT BE ACCEPTED: Explain specifically why records with this problem "
        f"in the {field} field cannot be used in any report or calculation. "
        "Name what breaks downstream — for example: records cannot be linked to an order, "
        "a customer cannot be identified, a financial total cannot be computed.\n\n"
        "WHAT IS AT RISK: Use the business context provided below — do not invent your own. "
        "Name the specific report or figure that is affected. "
        "State who must fix it and what they need to do before the audit deadline.\n\n"
        f"Business context for this issue:\n  {business_impact}\n\n"
        f"Evidence:\n"
        f"  Entity   : {entity}\n"
        f"  Field    : {field}\n"
        f"  Table    : {source_table}\n"
        f"  Severity : {severity}\n"
        f"  Count    : {count:,} records removed\n"
        f"  {time_note}\n\n"
        f"  Sample failure messages (what the bad data looked like):\n{reasons_block}\n\n"
        f"  Source files affected:\n{files_block}\n\n"
        "Rules:\n"
        "- Do NOT use: Silver layer, Bronze layer, pipeline, DLT, Spark, Delta, ETL, quarantine, harmonization\n"
        "- Instead say: 'removed from the reporting database', 'set aside before processing'\n"
        f"- Always name the exact field ({field}) and exact count ({count:,}) in WHAT IS THE PROBLEM\n"
        "- Always name at least one file and region in WHERE IT CAME FROM\n"
        "- WHAT IS AT RISK must use the business context provided above — name the specific report\n"
        "- One or two sentences per section — the reader is scanning, not reading\n"
        "- Start directly with WHAT IS THE PROBLEM — no preamble, no heading, no introduction"
    )


SYSTEM_MSG_UC1 = (
    "You are a senior data quality analyst writing audit documentation for a finance director. "
    "Every explanation must answer exactly three questions in order, using these exact labels: "
    "WHAT IS THE PROBLEM, WHY THIS RECORD TYPE CANNOT BE ACCEPTED, WHAT IS AT RISK. "
    "Write in plain English — name specific fields, counts, files, and reports. "
    "Never use database or programming terminology. "
    "Do not use any markdown formatting — no asterisks, no bold, no italic, no bullet points. "
    "Plain text only."
)

print("Prompt builder ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Generate AI explanations
# MAGIC
# MAGIC One LLM call per issue group. The loop has two phases:
# MAGIC
# MAGIC **Phase 1 — Main generation loop:**
# MAGIC Calls the LLM for every issue group. A 2-second pause between calls prevents gateway rate limiting which was causing empty responses in some runs.
# MAGIC
# MAGIC **Phase 2 — Automatic retry for REVIEW records:**
# MAGIC After the main loop, any record flagged REVIEW (empty response, truncated response, or missing sections) is retried once with a longer 5-second wait. If the retry passes, the PASS result replaces the failed one before writing to Delta. This means the final table contains clean explanations with no manual intervention needed.

# COMMAND ----------

audit_records  = []
llm_call_count = 0
review_count   = 0

# ── Phase 1: Main generation loop ────────────────────────────────────────
# 2-second pause between calls prevents gateway rate limiting,
# which was the cause of empty/truncated responses in previous runs.

for row in dq_summary:
    entity         = row["entity"]
    field          = row["field_affected"]
    issue          = row["issue_type"]
    count          = row["rejected_count"]
    source_table   = row["source_table"]
    sample_reasons = list(row["sample_reasons"]) if row["sample_reasons"] else []
    source_files   = list(row["source_files"])   if row["source_files"]   else []
    earliest       = str(row["earliest_failure"]) if row["earliest_failure"] else ""
    latest         = str(row["latest_failure"])   if row["latest_failure"]   else ""
    severity       = classify_severity(entity, issue, count)

    prompt     = build_dq_prompt(entity, field, issue, count, severity,
                                  source_table, sample_reasons, source_files, earliest, latest)
    raw_output = call_llm(prompt, SYSTEM_MSG_UC1)

    # Pause between calls to avoid hitting gateway rate limits
    time.sleep(2)

    validated      = validate_llm_output(raw_output)
    llm_call_count += 1

    if validated["llm_check"] == "REVIEW":
        review_count += 1

    audit_records.append({
        "entity":           entity,
        "field_affected":   field,
        "issue_type":       issue,
        "rejected_count":   int(count),
        "severity":         severity,
        "source_table":     source_table,
        "source_files":     " | ".join(source_files[:5]) if source_files else "not available",
        "earliest_failure": earliest,
        "latest_failure":   latest,
        "sample_evidence":  " | ".join(sample_reasons[:3]) if sample_reasons else "none",
        # Store raw data needed for retry — sample_reasons and source_files as lists
        "_sample_reasons":  sample_reasons,
        "_source_files":    source_files,
        "ai_explanation":   validated["text"],
        "llm_check":        validated["llm_check"],
        "llm_check_detail": validated["issues"],
        "generated_at":     datetime.now().isoformat()
    })

    print("=" * 70)
    print(f"[{severity}] {entity} | {field} | {issue} | {count:,} records")
    print(f"Source : {source_table}")
    print(f"Files  : {', '.join(source_files[:2]) if source_files else 'unknown'}")
    if validated["llm_check"] == "REVIEW":
        print(f"*** REVIEW FLAG: {validated['issues']} ***")
    print(f"\n{validated['text']}")
    print("=" * 70 + "\n")

print(f"Phase 1 done. {len(audit_records)} explanations generated.")
print(f"LLM calls: {llm_call_count}  |  Flagged for review: {review_count}")


# ── Phase 2: Automatic retry for REVIEW records ───────────────────────────
# Any record that came back empty, truncated, or missing a required section
# is retried once here with a longer wait before the call.
# If the retry passes, the record is updated in-place before writing to Delta.
# If the retry still fails, the record stays as REVIEW — no data is lost.

review_indices = [i for i, r in enumerate(audit_records) if r["llm_check"] == "REVIEW"]

if review_indices:
    print(f"\nRetrying {len(review_indices)} REVIEW record(s)...")

    for idx in review_indices:
        rec = audit_records[idx]
        print(f"  Retrying: [{rec['severity']}] {rec['entity']} | {rec['field_affected']} | {rec['issue_type']}")

        # Wait longer before retry to let the gateway recover
        time.sleep(5)

        # Rebuild prompt using the stored raw data
        retry_prompt = build_dq_prompt(
            rec["entity"],
            rec["field_affected"],
            rec["issue_type"],
            rec["rejected_count"],
            rec["severity"],
            rec["source_table"],
            rec["_sample_reasons"],   # original list stored during Phase 1
            rec["_source_files"],     # original list stored during Phase 1
            rec["earliest_failure"],
            rec["latest_failure"],
        )

        retry_raw      = call_llm(retry_prompt, SYSTEM_MSG_UC1)
        retry_validated = validate_llm_output(retry_raw)
        llm_call_count += 1

        if retry_validated["llm_check"] == "PASS":
            # Update the record with the successful retry result
            audit_records[idx]["ai_explanation"]   = retry_validated["text"]
            audit_records[idx]["llm_check"]        = "PASS"
            audit_records[idx]["llm_check_detail"] = "none"
            audit_records[idx]["generated_at"]     = datetime.now().isoformat()
            review_count -= 1
            print(f"    Retry PASSED.")
        else:
            print(f"    Retry still REVIEW: {retry_validated['issues']} — will write as-is.")

else:
    print("\nNo REVIEW records — all explanations passed on first attempt.")


# ── Remove internal helper fields before writing to Delta ─────────────────
# _sample_reasons and _source_files were stored to support the retry logic.
# They must be removed before creating the Spark DataFrame.
for rec in audit_records:
    rec.pop("_sample_reasons", None)
    rec.pop("_source_files",   None)

print(f"\nFinal counts:")
print(f"  Total explanations : {len(audit_records)}")
print(f"  LLM calls made     : {llm_call_count}")
print(f"  Still REVIEW       : {review_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Write audit report to Gold layer
# MAGIC
# MAGIC Appends to `globalmart.gold.dq_audit_report`. `mergeSchema: true` handles new columns being added to an existing table automatically.

# COMMAND ----------

schema = StructType([
    StructField("entity",           StringType(),  True),
    StructField("field_affected",   StringType(),  True),
    StructField("issue_type",       StringType(),  True),
    StructField("rejected_count",   IntegerType(), True),
    StructField("severity",         StringType(),  True),
    StructField("source_table",     StringType(),  True),
    StructField("source_files",     StringType(),  True),
    StructField("earliest_failure", StringType(),  True),
    StructField("latest_failure",   StringType(),  True),
    StructField("sample_evidence",  StringType(),  True),
    StructField("ai_explanation",   StringType(),  True),
    StructField("llm_check",        StringType(),  True),
    StructField("llm_check_detail", StringType(),  True),
    StructField("generated_at",     StringType(),  True),
])

df_audit = spark.createDataFrame(audit_records, schema=schema)

(
    df_audit.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_AUDIT)
)

print(f"Written to {TABLE_AUDIT}")
print(f"Rows written this run: {df_audit.count()}")

display(
    spark.table(TABLE_AUDIT)
    .orderBy(
        F.when(F.col("severity") == "CRITICAL", 1)
         .when(F.col("severity") == "HIGH",     2)
         .otherwise(3),
        F.col("rejected_count").desc()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Log the run
# MAGIC
# MAGIC Records pipeline metadata for the Pipeline Health dashboard.

# COMMAND ----------

run_log = [{
    "notebook":          NOTEBOOK_NAME,
    "run_timestamp":     datetime.now().isoformat(),
    "records_processed": len(dq_summary),
    "llm_calls_made":    llm_call_count,
    "outputs_flagged":   review_count,
    "status":            "SUCCESS" if review_count == 0 else "PARTIAL_REVIEW",
    "notes":             f"watermark_applied={last_run_ts is not None}"
}]

(
    spark.createDataFrame(run_log)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_RUN_LOG)
)

print(f"Run log written to {TABLE_RUN_LOG}")
print(f"Status         : {run_log[0]['status']}")
print(f"Issue groups   : {len(dq_summary)}")
print(f"LLM calls      : {llm_call_count}")
print(f"Flagged REVIEW : {review_count}")