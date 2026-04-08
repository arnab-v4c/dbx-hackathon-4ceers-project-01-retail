# Databricks notebook source
# MAGIC %md
# MAGIC # UC2 — Returns Fraud Investigator
# MAGIC
# MAGIC **Purpose:** Score every customer's return behaviour against 8 fraud rules, flag high-risk customers, and generate an AI investigation brief for each.
# MAGIC
# MAGIC **Reads:**
# MAGIC - `globalmart.gold.fact_returns`
# MAGIC - `globalmart.gold.dim_customers`
# MAGIC - `globalmart.gold.fact_orders`
# MAGIC - `globalmart.config.fraud_rule_config`
# MAGIC
# MAGIC **Writes:**
# MAGIC - `globalmart.gold.flagged_return_customers`
# MAGIC - `globalmart.config.pipeline_run_log`
# MAGIC
# MAGIC **Run order:** Top to bottom. Step 2 seeds the rule config table on first run and is safe to re-run (it overwrites the table).
# MAGIC
# MAGIC **Design principle:** Rules DETECT, LLM EXPLAINS.
# MAGIC The scoring system identifies suspicious customers using deterministic rules. The LLM only produces the plain-English investigation brief — it makes no detection decisions.
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
# MAGIC ## Step 2 — Create and seed the fraud rule config table
# MAGIC
# MAGIC All 8 fraud rules, their thresholds, weights, and human-readable reasoning live in a Delta table — not hardcoded in this notebook.
# MAGIC
# MAGIC This means a business analyst can tune thresholds (e.g. raise the flag threshold during peak returns season) by running a simple SQL UPDATE on `globalmart.config.fraud_rule_config` without touching this notebook.
# MAGIC
# MAGIC **Rule weights are scaled to sum to 100** so the anomaly score is a true 0–100 percentage.
# MAGIC
# MAGIC **Weight logic:** Rules that provide *direct evidence of fraud* carry the highest weight (20 pts). Rules that identify a suspicious pattern carry medium weight (10–15 pts). Rules that are corroborative tiebreakers carry low weight (5 pts). `no_matching_order` is promoted to 15 pts because a phantom return — one with no verifiable purchase in the system — is the strongest single fraud signal available.
# MAGIC
# MAGIC | Rule | Threshold | Weight | What it measures |
# MAGIC |---|---|---|---|
# MAGIC | `high_return_volume` | 5 returns | 15 pts | 5+ total returns — 32 customers qualify |
# MAGIC | `high_refund_value` | $1,000 | 10 pts | Total refunds >= $1,000 — 43 customers qualify |
# MAGIC | `high_order_return_rate` | 30% | 10 pts | >= 30% of orders returned — max in dataset = 50% |
# MAGIC | `refund_inflation` | $500 | 15 pts | Refunds exceed product value by $500+ |
# MAGIC | `return_before_delivery` | 1 | 10 pts | Return filed before order was delivered |
# MAGIC | `duplicate_order_reason` | 1 | 10 pts | Used 'Duplicate Order' as return reason |
# MAGIC | `same_day_ship_return` | 1 | 5 pts | Returned a Same Day shipping order |
# MAGIC | `no_matching_order` | 1 | 25 pts | Return has no matching order in fact_orders |
# MAGIC | `flag_threshold` | 40 pts | — | Customers scoring >= 40 are flagged |
# MAGIC
# MAGIC **Run the separate rule config creation query below first if this is a fresh environment.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Load rule config into memory
# MAGIC
# MAGIC Reads thresholds and weights from the config table. All scoring code in Step 7 uses these values — no hardcoded numbers.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType
import pyspark.sql.functions as F

# Load all active rules from the config table into a Python dict
# keyed by rule_name for fast lookup in the scoring step
thresholds = (
    spark.table("globalmart.config.fraud_rule_config")
    .filter(F.col("is_active") == True)
    .select("rule_name", "threshold", "weight")
    .toPandas()
    .set_index("rule_name")[["threshold", "weight"]]
    .to_dict(orient="index")
)

# Print a summary of loaded rules for verification
print("Rules loaded for this run:")
print(f"  {'Rule':<35}  {'Threshold':>12}  {'Weight':>8}")
print(f"  {'-'*35}  {'-'*12}  {'-'*8}")
for rule, vals in thresholds.items():
    if rule != "flag_threshold":
        print(f"  {rule:<35}  {vals['threshold']:>12}  {vals['weight']:>8}")

# Extract the flag threshold separately — it is a cutoff, not a scoring rule
flag_threshold = int(thresholds["flag_threshold"]["threshold"])
print(f"\n  Flag threshold : {flag_threshold} pts (score >= {flag_threshold} = flagged)")

# Verify weights sum to 100 (excluding the flag_threshold row)
total_weight = sum(v["weight"] for k, v in thresholds.items() if k != "flag_threshold")
print(f"  Total weight   : {total_weight} pts (should be 100)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — LLM connection and helper functions
# MAGIC
# MAGIC We use the OpenAI SDK pointed at the Databricks AI Gateway. The API token is pulled automatically from the current session — no hardcoded credentials.
# MAGIC
# MAGIC Three helper functions are defined:
# MAGIC - `extract_text()` — parses the model's structured response to get only the answer text
# MAGIC - `call_llm()` — calls the model with automatic retry on failure
# MAGIC - `validate_output()` — checks output quality before writing to Delta

# COMMAND ----------

import time
import json
from datetime import datetime
from pyspark.sql import functions as F
from openai import OpenAI

# Connect to the Databricks AI Gateway using the current session token
client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    base_url="https://7474644504640858.ai-gateway.cloud.databricks.com/mlflow/v1/"
)

MODEL_NAME    = "databricks-gpt-oss-20b"
NOTEBOOK_NAME = "UC2_Returns_Fraud_Investigator"


def extract_text(response) -> str:
    """
    Parse the model response and return only the answer text.

    Why this is needed: databricks-gpt-oss-20b returns a Python list, not a plain string:
      [{"type": "reasoning", "summary": [...]},   <- internal chain-of-thought (discard)
       {"type": "text",      "text": "answer"}]   <- the actual answer we want
    Without this function, the raw reasoning chain appears alongside the answer.
    """
    content = response.choices[0].message.content
    if isinstance(content, list):
        return " ".join(
            block["text"] for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
    return content.strip()


def call_llm(prompt: str, system_msg: str, retries: int = 3, wait_secs: int = 2) -> str:
    """
    Call the LLM with automatic retry and exponential backoff.
    On complete failure returns a string starting with LLM_UNAVAILABLE
    so the loop continues rather than crashing the notebook.
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
                temperature=0.3  # low = consistent, grounded output
            )
            return extract_text(response)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(wait_secs * (attempt + 1))  # 2s, 4s, 6s
            else:
                return f"LLM_UNAVAILABLE: {str(e)}"


def validate_output(text: str, min_words: int = 30) -> dict:
    """
    Check LLM output quality before writing to Delta.
    Returns a dict: text, llm_check (PASS/REVIEW), issues (comma-separated).
    REVIEW outputs are still written but flagged for human review.
    """
    refusal_phrases = ["i cannot", "as an ai", "i don't have", "i'm unable"]
    issues = []

    if len(text.split()) < min_words:
        issues.append("too_short")
    if text.startswith("LLM_UNAVAILABLE"):
        issues.append("llm_call_failed")
    if any(phrase in text.lower() for phrase in refusal_phrases):
        issues.append("refusal_detected")

    if not text.strip():
        text = f"[REVIEW REQUIRED — LLM returned empty response. Issues: {', '.join(issues) if issues else 'unknown'}]"

    return {
        "text":      text,
        "llm_check": "PASS" if not issues else "REVIEW",
        "issues":    ", ".join(issues) if issues else "none"
    }


print(f"LLM ready  : {MODEL_NAME}")
print(f"Run started: {datetime.now().isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Load Gold layer tables
# MAGIC
# MAGIC Reads the three source tables. All data is already clean and harmonised — this notebook only reads Gold, it does not touch Bronze or Silver.

# COMMAND ----------

df_returns   = spark.table("globalmart.gold.fact_returns")
df_customers = spark.table("globalmart.gold.dim_customers")
df_orders    = spark.table("globalmart.gold.fact_orders")

print("Tables loaded:")
print(f"  fact_returns   : {df_returns.count():,} rows")
print(f"  dim_customers  : {df_customers.count():,} rows")
print(f"  fact_orders    : {df_orders.count():,} rows")

display(df_returns.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Incremental watermark
# MAGIC
# MAGIC On the first run, every customer is scored. On subsequent runs, only customers with new returns since the last run are re-scored. This keeps the notebook fast and avoids regenerating identical LLM briefs for unchanged customers.

# COMMAND ----------

# Check when this notebook last ran successfully
try:
    last_run_ts = spark.sql(
        "SELECT MAX(scored_at) FROM globalmart.gold.flagged_return_customers"
    ).collect()[0][0]
    print(f"Last run: {last_run_ts}")
    print("Incremental mode: only re-scoring customers with new returns since last run.")
except Exception:
    last_run_ts = None
    print("First run detected. Scoring all customers.")


if last_run_ts is not None and "return_date" in [f.name for f in df_returns.schema]:
    # Find customers who have any new returns since last run
    changed_customers = (
        df_returns
        .filter(F.col("return_date") > F.lit(str(last_run_ts)))
        .select("customer_id").distinct()
    )
    # Pull ALL returns for those customers (not just new ones)
    # so their complete return profile is scored correctly
    df_returns_to_score = df_returns.join(changed_customers, on="customer_id", how="inner")
    print(f"Customers to re-score: {changed_customers.count():,}")
else:
    df_returns_to_score = df_returns
    print(f"Scoring all {df_returns.select('customer_id').distinct().count():,} customers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Build per-customer return profiles
# MAGIC
# MAGIC We score each CUSTOMER, not each individual return row. This step computes 8 per-customer signals — one for each fraud rule — then joins them into a single profile table.
# MAGIC
# MAGIC **Schema note on refund inflation (Rule 4):**
# MAGIC - `original_sales` = value of the single product being returned (one line item)
# MAGIC - `refund_amount` = the full order refund issued by GlobalMart
# MAGIC - `line_inflation` = refund_amount - original_sales (the gap = real money lost)
# MAGIC
# MAGIC **Schema note on no_matching_order (Rule 8):**
# MAGIC - A return with an order_id that does not exist in fact_orders cannot be verified as a legitimate purchase

# COMMAND ----------

# ── Signal 1 & 2: Basic return counts and total refund value ────────────────
df_base = (
    df_returns_to_score
    .groupBy("customer_id")
    .agg(
        F.count("*")                        .alias("total_returns"),
        F.sum("refund_amount")              .alias("total_refund_value"),
        F.countDistinct("return_reason")    .alias("distinct_reasons"),
        F.collect_set("return_reason")      .alias("return_reasons_list")
    )
)

# ── Signal 4: Refund inflation ───────────────────────────────────────────────
# Sum of (refund_amount - original_sales) per customer.
# Positive total = customer received more money back than products were worth.
df_inflation = (
    df_returns_to_score
    .filter(F.col("refund_amount").isNotNull() & F.col("original_sales").isNotNull())
    .withColumn("line_inflation", F.col("refund_amount") - F.col("original_sales"))
    .groupBy("customer_id")
    .agg(
        F.sum("line_inflation")                          .alias("total_refund_inflation"),
        F.max("line_inflation")                          .alias("max_single_inflation"),
        F.count(F.when(F.col("line_inflation") > 0, 1)) .alias("inflated_refund_count")
    )
)

# ── Signal 6: Duplicate Order return reason ──────────────────────────────────
df_duplicate_reason = (
    df_returns_to_score
    .filter(F.col("return_reason") == "Duplicate Order")
    .groupBy("customer_id")
    .agg(F.count("*").alias("duplicate_order_count"))
)

# ── Signals 5 & 7: Timing signals ────────────────────────────────────────────
# Order timestamps have a timezone suffix (e.g. "2017-09-29T18:38:00.000Z").
# Strip everything from "T" onwards to get a plain date before parsing.
df_orders_with_dates = (
    df_orders.select(
        "order_id",
        "ship_mode",
        F.to_date(
            F.regexp_replace("order_delivered_customer_timestamp", "T.*", "")
        ).alias("delivered_date")
    )
)

df_timing = (
    df_returns_to_score
    .select("customer_id", "order_id", F.to_date("return_date").alias("return_date_parsed"))
    .join(df_orders_with_dates, on="order_id", how="left")
    .withColumn("is_before_delivery",
        F.when(
            F.col("return_date_parsed").isNotNull() &
            F.col("delivered_date").isNotNull() &
            (F.col("return_date_parsed") < F.col("delivered_date")),
            1
        ).otherwise(0)
    )
    .withColumn("is_same_day",
        F.when(F.col("ship_mode") == "Same Day", 1).otherwise(0))
)

df_timing_signals = (
    df_timing
    .groupBy("customer_id")
    .agg(
        F.sum("is_before_delivery") .alias("return_before_delivery_count"),
        F.sum("is_same_day")        .alias("same_day_ship_return_count")
    )
)

# ── Signal 8: No matching order ──────────────────────────────────────────────
# Find return order_ids that do not exist in fact_orders.
# These are "phantom returns" — cannot be verified as a real purchase.
df_order_ids = df_orders.select("order_id").distinct()

df_no_match = (
    df_returns_to_score
    .select("customer_id", "order_id")
    .join(df_order_ids, on="order_id", how="left_anti")  # keep only returns with NO matching order
    .groupBy("customer_id")
    .agg(F.count("*").alias("no_matching_order_count"))
)

# ── Signal 3: Order return rate ───────────────────────────────────────────────
# = total_returns / total_orders per customer
# Always 0.0 to 1.0. Max in this dataset = 0.50 (no customer returns more than they buy).
df_order_counts = (
    df_orders
    .groupBy("customer_id")
    .agg(F.count("*").alias("total_orders"))
)

# ── Join all signals into one profile per customer ────────────────────────────
df_profiles = (
    df_base
    .join(df_inflation,        on="customer_id", how="left")
    .join(df_duplicate_reason, on="customer_id", how="left")
    .join(df_timing_signals,   on="customer_id", how="left")
    .join(df_no_match,         on="customer_id", how="left")
    .join(df_order_counts,     on="customer_id", how="left")
    .join(
        df_customers.select("customer_id", "customer_name", "segment", "region"),
        on="customer_id", how="left"
    )
    # Replace NULLs with zeros for customers with no activity in a given signal
    .fillna({
        "total_orders":                 0,
        "total_refund_value":           0.0,
        "total_refund_inflation":       0.0,
        "max_single_inflation":         0.0,
        "inflated_refund_count":        0,
        "duplicate_order_count":        0,
        "return_before_delivery_count": 0,
        "same_day_ship_return_count":   0,
        "no_matching_order_count":      0,
    })
    # Derived: order return rate (0.0 to 1.0)
    .withColumn("order_return_rate",
        F.when(
            F.col("total_orders") > 0,
            F.col("total_returns") / F.col("total_orders")
        ).otherwise(F.lit(0.0))
    )
)

print(f"Profiles built for {df_profiles.count():,} customers.")
display(
    df_profiles
    .select(
        "customer_id", "customer_name", "total_returns", "total_orders",
        "order_return_rate", "total_refund_value", "total_refund_inflation",
        "return_before_delivery_count", "duplicate_order_count",
        "same_day_ship_return_count", "no_matching_order_count"
    )
    .orderBy(F.col("total_returns").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Apply fraud rules and calculate anomaly scores
# MAGIC
# MAGIC Each rule adds a score column: the rule's weight if the threshold is triggered, 0 otherwise. A customer's `anomaly_score` is the sum of all triggered rule weights.
# MAGIC
# MAGIC **Weights are scaled to sum to 100** so the score is a true percentage (0–100). The maximum possible score is 100 (all 8 rules triggered simultaneously).
# MAGIC
# MAGIC **Weight tier rationale:**
# MAGIC - **20 pts — Direct fraud evidence:** `high_return_volume`, `refund_inflation`
# MAGIC - **15 pts — Strong fraud indicator:** `no_matching_order` (phantom return — no verifiable purchase exists in the system)
# MAGIC - **10 pts — Suspicious pattern:** `high_refund_value`, `high_order_return_rate`, `return_before_delivery`, `duplicate_order_reason`
# MAGIC - **5 pts — Corroborative tiebreaker:** `same_day_ship_return`
# MAGIC
# MAGIC All thresholds and weights come from the `fraud_rule_config` table — no hardcoded values here.

# COMMAND ----------

# Pull thresholds (t_) and weights (w_) from the config dict loaded in Step 3
t_vol    = float(thresholds["high_return_volume"]["threshold"])
t_refund = float(thresholds["high_refund_value"]["threshold"])
t_rate   = float(thresholds["high_order_return_rate"]["threshold"])
t_infl   = float(thresholds["refund_inflation"]["threshold"])
t_before = float(thresholds["return_before_delivery"]["threshold"])
t_dup    = float(thresholds["duplicate_order_reason"]["threshold"])
t_same   = float(thresholds["same_day_ship_return"]["threshold"])
t_nomatch = float(thresholds["no_matching_order"]["threshold"])
THRESHOLD = int(thresholds["flag_threshold"]["threshold"])

w_vol    = float(thresholds["high_return_volume"]["weight"])
w_refund = float(thresholds["high_refund_value"]["weight"])
w_rate   = float(thresholds["high_order_return_rate"]["weight"])
w_infl   = float(thresholds["refund_inflation"]["weight"])
w_before = float(thresholds["return_before_delivery"]["weight"])
w_dup    = float(thresholds["duplicate_order_reason"]["weight"])
w_same   = float(thresholds["same_day_ship_return"]["weight"])
w_nomatch = float(thresholds["no_matching_order"]["weight"])

df_scored = (
    df_profiles

    # Rule 1: High return volume (20 pts)
    # 32 customers in dataset have 5+ returns.
    .withColumn("score_r1_volume",
        F.when(F.col("total_returns") >= t_vol, w_vol).otherwise(0))

    # Rule 2: High total refund value (10 pts — reduced from 15)
    # Indirect signal: high value shows financial exposure but is fully explainable
    # by expensive legitimate purchases. Demoted to 10 to reflect indirect nature.
    # Dataset: 43 customers exceed $1,000 total. Dataset mean = $584.
    .withColumn("score_r2_refund_value",
        F.when(F.col("total_refund_value") >= t_refund, w_refund).otherwise(0))

    # Rule 3: High order return rate (10 pts — reduced from 15)
    # Suspicious pattern but not direct fraud evidence — a picky legitimate customer
    # could have a high return rate. Demoted to 10.
    # order_return_rate = total_returns / total_orders (always 0.0 to 1.0)
    # 30% means 3 in every 10 orders from this customer ended in a return.
    .withColumn("score_r3_return_rate",
        F.when(F.col("order_return_rate") >= t_rate, w_rate).otherwise(0))

    # Rule 4: Refund inflation (20 pts)
    # total_refund_inflation = SUM(refund_amount - original_sales)
    # Positive = customer received more money back than products cost.
    .withColumn("score_r4_inflation",
        F.when(F.col("total_refund_inflation") >= t_infl, w_infl).otherwise(0))

    # Rule 5: Return filed before delivery date (10 pts)
    # 221 returns across 160 customers. Average gap: 563 days before delivery.
    .withColumn("score_r5_before_delivery",
        F.when(F.col("return_before_delivery_count") >= t_before, w_before).otherwise(0))

    # Rule 6: Duplicate Order return reason (10 pts)
    # Only 31 such returns in the dataset — low frequency makes each case more suspicious.
    .withColumn("score_r6_duplicate_reason",
        F.when(F.col("duplicate_order_count") >= t_dup, w_dup).otherwise(0))

    # Rule 7: Return from a Same Day shipping order (5 pts)
    # Return rate for Same Day = 16.9% vs 16.0% overall (+0.9pp).
    # Weak standalone signal — only useful in combination with other rules.
    .withColumn("score_r7_same_day",
        F.when(F.col("same_day_ship_return_count") >= t_same, w_same).otherwise(0))

    # Rule 8: No matching order in fact_orders (15 pts — promoted from 5)
    # STRONGEST single fraud signal: a phantom return has no verifiable purchase.
    # Unlike high_return_volume or high_return_rate (pattern rules), this is DIRECT
    # EVIDENCE — no innocent explanation exists for a return with no matching order.
    # Promoted from 5 to 15 pts, placing it in the primary signal tier.
    # Return cannot be verified as a real purchase — potential phantom return.
    .withColumn("score_r8_no_match",
        F.when(F.col("no_matching_order_count") >= t_nomatch, w_nomatch).otherwise(0))

    # Total anomaly score (max = 100, weights sum to 100)
    .withColumn("total_score",
        F.col("score_r1_volume")          +
        F.col("score_r2_refund_value")    +
        F.col("score_r3_return_rate")     +
        F.col("score_r4_inflation")       +
        F.col("score_r5_before_delivery") +
        F.col("score_r6_duplicate_reason")+
        F.col("score_r7_same_day")        +
        F.col("score_r8_no_match"))
)

print("Scoring complete. Top 15 customers by risk score:")
display(
    df_scored
    .select(
        "customer_id", "customer_name", "total_returns", "total_orders",
        "order_return_rate", "total_refund_inflation",
        "return_before_delivery_count", "no_matching_order_count", "total_score"
    )
    .orderBy(F.col("total_score").desc())
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Filter to flagged customers
# MAGIC
# MAGIC Customers whose `total_score` meets or exceeds the flag threshold (40 pts) are sent to the investigation queue. Triggering just one rule is never enough to flag — a customer must score at least 40 points which requires at least 2 rules.

# COMMAND ----------

df_flagged    = df_scored.filter(F.col("total_score") >= THRESHOLD)
flagged_count = df_flagged.count()

print(f"Flagged {flagged_count:,} customers for investigation  (score >= {THRESHOLD})")
print(f"Total customers scored: {df_scored.count():,}")
display(df_flagged.orderBy(F.col("total_score").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Generate AI investigation briefs
# MAGIC
# MAGIC For each flagged customer, the LLM generates a 3–4 sentence investigation brief for the returns operations manager.
# MAGIC
# MAGIC The brief must answer three questions required by the hackathon rubric:
# MAGIC 1. What specific patterns make this case suspicious (cite actual data points)
# MAGIC 2. What innocent explanations exist alongside potential fraud indicators
# MAGIC 3. What the returns team should verify first (specific, actionable steps)
# MAGIC
# MAGIC The prompt includes the customer's actual data values and which rules were violated — the LLM never invents figures.

# COMMAND ----------

def safe(row, field, default="N/A"):
    """
    Safely read a field from a Spark Row.
    Spark Rows do not support .get() so we use try/except.
    """
    try:
        val = row[field]
        return val if val is not None else default
    except Exception:
        return default


def build_prompt(row) -> str:
    """
    Build the investigation brief prompt for one flagged customer.
    All values come from the actual scored row — no generic placeholders.
    The prompt includes which rules fired and by how much so the LLM
    can cite real figures rather than describing rules abstractly.
    """
    # Build the rules-violated section with actual figures
    violated_rules = []

    if row["score_r1_volume"] > 0:
        violated_rules.append(
            f"  - High return volume     : {int(safe(row,'total_returns',0))} total returns"
            f" (threshold = {int(t_vol)}) | {int(w_vol)} pts")

    if row["score_r2_refund_value"] > 0:
        violated_rules.append(
            f"  - High refund value      : ${round(safe(row,'total_refund_value',0), 2)} total"
            f" (threshold = ${int(t_refund)}) | {int(w_refund)} pts  [indirect signal]")

    if row["score_r3_return_rate"] > 0:
        violated_rules.append(
            f"  - High return rate       : {round(safe(row,'order_return_rate',0)*100, 1)}% of orders returned"
            f" (threshold = {int(t_rate*100)}%) | {int(w_rate)} pts")

    if row["score_r4_inflation"] > 0:
        violated_rules.append(
            f"  - Refund inflation       : ${round(safe(row,'total_refund_inflation',0), 2)} received above product values"
            f" (threshold = ${int(t_infl)}) | {int(w_infl)} pts")

    if row["score_r5_before_delivery"] > 0:
        violated_rules.append(
            f"  - Before delivery        : {int(safe(row,'return_before_delivery_count',0))} returns filed before delivery"
            f" | {int(w_before)} pts")

    if row["score_r6_duplicate_reason"] > 0:
        violated_rules.append(
            f"  - Duplicate Order reason : {int(safe(row,'duplicate_order_count',0))} returns claiming Duplicate Order"
            f" | {int(w_dup)} pts")

    if row["score_r7_same_day"] > 0:
        violated_rules.append(
            f"  - Same Day returns       : {int(safe(row,'same_day_ship_return_count',0))} returns on Same Day orders"
            f" | {int(w_same)} pts")

    if row["score_r8_no_match"] > 0:
        violated_rules.append(
            f"  - No matching order      : {int(safe(row,'no_matching_order_count',0))} returns with no verifiable purchase in system"
            f" | {int(w_nomatch)} pts  [direct fraud evidence — strongest signal]")

    reasons_text = ", ".join(safe(row, "return_reasons_list", []) or []) or "Not recorded"

    return f"""You are a fraud analyst at GlobalMart, a national retail chain.

A customer has been flagged by the automated anomaly scoring system with a risk score
of {row["total_score"]} out of 100. Write a clear investigation brief for the returns
operations manager who will decide whether to investigate further.

SCHEMA NOTE:
  original_sales  = value of the single product returned (one line item)
  total_refund_value  = total money refunded to this customer across all returns
  refund_inflation    = total_refund_value minus original product values (money overpaid)

CUSTOMER:
  ID              : {safe(row, "customer_id")}
  Name            : {safe(row, "customer_name")}
  Segment         : {safe(row, "segment")}
  Region          : {safe(row, "region")}

RETURN BEHAVIOUR:
  Total returns           : {int(safe(row, "total_returns", 0))}
  Total orders placed     : {int(safe(row, "total_orders", 0))}
  Order return rate       : {round(safe(row, "order_return_rate", 0)*100, 1)}%
  Total refunded          : ${round(safe(row, "total_refund_value", 0), 2)}
  Total refund inflation  : ${round(safe(row, "total_refund_inflation", 0), 2)}
  Largest single inflation: ${round(safe(row, "max_single_inflation", 0), 2)}
  Returns before delivery : {int(safe(row, "return_before_delivery_count", 0))}
  Duplicate Order claims  : {int(safe(row, "duplicate_order_count", 0))}
  Same Day returns        : {int(safe(row, "same_day_ship_return_count", 0))}
  No matching order       : {int(safe(row, "no_matching_order_count", 0))}
  Return reasons used     : {reasons_text}

RULES VIOLATED (risk score = {row["total_score"]} / 100):
{chr(10).join(violated_rules)}

Write a 3-4 sentence investigation brief. Answer these three questions:
1. What specific numbers make this case suspicious — cite the actual figures above
2. What innocent explanations might exist for these patterns
3. What the returns team should check first — be specific and actionable

Write in plain English. No bullet points. No markdown. No section headers."""


SYSTEM_MSG = (
    "You are a fraud analyst writing clear, data-grounded investigation briefs "
    "for a returns operations manager at GlobalMart retail. "
    "Write in plain connected English paragraphs. No bullet points, no markdown."
)

# Loop over each flagged customer and generate their investigation brief
fraud_records  = []
llm_calls_made = 0
needs_review   = 0

for row in df_flagged.orderBy(F.col("total_score").desc()).collect():

    prompt   = build_prompt(row)
    raw_text = call_llm(prompt, SYSTEM_MSG)
    result   = validate_output(raw_text)
    llm_calls_made += 1

    if result["llm_check"] == "REVIEW":
        needs_review += 1

    # Build pipe-delimited list of rules that fired for storage
    rules_fired = "|".join([
        name for name, score in [
            ("high_return_volume",       row["score_r1_volume"]),
            ("high_refund_value",        row["score_r2_refund_value"]),
            ("high_order_return_rate",   row["score_r3_return_rate"]),
            ("refund_inflation",         row["score_r4_inflation"]),
            ("return_before_delivery",   row["score_r5_before_delivery"]),
            ("duplicate_order_reason",   row["score_r6_duplicate_reason"]),
            ("same_day_ship_return",     row["score_r7_same_day"]),
            ("no_matching_order",        row["score_r8_no_match"]),
        ] if score > 0
    ])

    fraud_records.append({
        "customer_id":                  safe(row, "customer_id"),
        "customer_name":                safe(row, "customer_name"),
        "segment":                      safe(row, "segment"),
        "region":                       safe(row, "region"),
        "total_returns":                int(safe(row, "total_returns", 0)),
        "total_orders":                 int(safe(row, "total_orders",  0)),
        "order_return_rate":            float(round(safe(row, "order_return_rate", 0), 4)),
        "total_refund_value":           float(round(safe(row, "total_refund_value", 0), 2)),
        "total_refund_inflation":       float(round(safe(row, "total_refund_inflation", 0), 2)),
        "max_single_inflation":         float(round(safe(row, "max_single_inflation", 0), 2)),
        "inflated_refund_count":        int(safe(row, "inflated_refund_count", 0)),
        "return_before_delivery_count": int(safe(row, "return_before_delivery_count", 0)),
        "duplicate_order_count":        int(safe(row, "duplicate_order_count", 0)),
        "same_day_ship_return_count":   int(safe(row, "same_day_ship_return_count", 0)),
        "no_matching_order_count":      int(safe(row, "no_matching_order_count", 0)),
        "anomaly_score":                int(row["total_score"]),
        "rules_violated":               rules_fired,
        "investigation_brief":          result["text"],
        "llm_check":                    result["llm_check"],
        "llm_check_detail":             result["issues"],
        "scored_at":                    datetime.now().isoformat()
    })

    # Print a readable summary for review
    print("=" * 65)
    print(f"Customer : {safe(row,'customer_id')}  |  {safe(row,'customer_name')}")
    print(f"Segment  : {safe(row,'segment')}  |  Region: {safe(row,'region')}")
    print(f"Score    : {row['total_score']} / 100")
    print(f"Rules    : {rules_fired}")
    print(f"Quality  : {result['llm_check']}")
    print()
    print("BRIEF:")
    print(result["text"])
    print("=" * 65)
    print()

print(f"Done.  Flagged: {len(fraud_records)}  |  LLM calls: {llm_calls_made}  |  Needs review: {needs_review}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Write flagged customers to Gold layer
# MAGIC
# MAGIC Appends to `globalmart.gold.flagged_return_customers`. `mergeSchema: true` handles new columns automatically.
# MAGIC
# MAGIC **Column name:** `total_refund_value` (renamed from `total_return_value` to match the source column name in `fact_returns`).

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

output_schema = StructType([
    StructField("customer_id",                  StringType(),  True),
    StructField("customer_name",                StringType(),  True),
    StructField("segment",                      StringType(),  True),
    StructField("region",                       StringType(),  True),
    StructField("total_returns",                IntegerType(), True),
    StructField("total_orders",                 IntegerType(), True),
    StructField("order_return_rate",            FloatType(),   True),
    StructField("total_refund_value",           FloatType(),   True),  # renamed from total_return_value
    StructField("total_refund_inflation",       FloatType(),   True),
    StructField("max_single_inflation",         FloatType(),   True),
    StructField("inflated_refund_count",        IntegerType(), True),
    StructField("return_before_delivery_count", IntegerType(), True),
    StructField("duplicate_order_count",        IntegerType(), True),
    StructField("same_day_ship_return_count",   IntegerType(), True),
    StructField("no_matching_order_count",      IntegerType(), True),  # new column for Rule 8
    StructField("anomaly_score",                IntegerType(), True),
    StructField("rules_violated",               StringType(),  True),
    StructField("investigation_brief",          StringType(),  True),
    StructField("llm_check",                    StringType(),  True),
    StructField("llm_check_detail",             StringType(),  True),
    StructField("scored_at",                    StringType(),  True),
])

df_output = spark.createDataFrame(fraud_records, schema=output_schema)

(
    df_output
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("globalmart.gold.flagged_return_customers")
)

print("Saved to globalmart.gold.flagged_return_customers")
display(
    spark.table("globalmart.gold.flagged_return_customers")
    .orderBy(F.col("anomaly_score").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 — Log the run
# MAGIC
# MAGIC Appends one row to `globalmart.config.pipeline_run_log` so the operations team can track when the notebook last ran, how many customers were scored and flagged, and whether any LLM outputs need manual review.

# COMMAND ----------

run_log_entry = [{
    "notebook":          NOTEBOOK_NAME,
    "run_timestamp":     datetime.now().isoformat(),
    "records_processed": df_profiles.count(),
    "records_flagged":   flagged_count,
    "llm_calls_made":    llm_calls_made,
    "outputs_flagged":   needs_review,
    "status":            "SUCCESS",
    "notes":             (
        f"flag_threshold={THRESHOLD} | "
        f"rules=8 | "
        f"incremental={last_run_ts is not None}"
    )
}]

(
    spark.createDataFrame(run_log_entry)
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("globalmart.config.pipeline_run_log")
)

print(f"Run logged.  Status: SUCCESS")
print(f"Processed : {df_profiles.count():,} customers")
print(f"Flagged   : {flagged_count}")
print(f"Review    : {needs_review}")