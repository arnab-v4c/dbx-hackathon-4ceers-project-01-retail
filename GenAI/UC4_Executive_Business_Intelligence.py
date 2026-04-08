# Databricks notebook source
# MAGIC %md
# MAGIC # UC4 — Executive Business Intelligence
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC Reads aggregated KPIs from the Gold Materialized Views and uses an LLM to generate plain-English executive synthesis — not data restating, but root-cause explanation.
# MAGIC
# MAGIC | Domain | Source table | Business problem |
# MAGIC |---|---|---|
# MAGIC | Revenue performance | mv_revenue_by_region | Which regions are growing and why |
# MAGIC | Vendor return rates | mv_return_rate_by_vendor | Which vendors need action |
# MAGIC | Slow-moving inventory | mv_slow_moving_products | What is sitting unsold and why |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Output tables
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `globalmart.gold.ai_business_insights` | 3 executive summaries, one per domain |
# MAGIC | `globalmart.config.pipeline_run_log` | One row per run with metadata |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Design rules
# MAGIC
# MAGIC - **Never pass raw rows to the LLM.** Always aggregate first.
# MAGIC - **Region name normalisation** — W->West, S->South, E->East, N->North applied before any KPI is passed to the LLM so summaries never show abbreviations.
# MAGIC - **Executive summary standard** — every summary must state whether a number is *alarming*, *expected*, or *an opportunity*. Numbers alone are not synthesis.
# MAGIC - **CATALOG constant** — change once to move the catalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import time
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from openai import OpenAI

client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    base_url="https://7474644504640858.ai-gateway.cloud.databricks.com/mlflow/v1/"
)

MODEL_NAME    = "databricks-gpt-oss-20b"
NOTEBOOK_NAME = "UC4_Executive_Business_Intelligence"
CATALOG       = "globalmart.gold"

# Region name normalisation — applied to all KPI data before passing to the LLM
# so summaries never contain abbreviations like W or S
REGION_MAP = {
    "W": "West", "S": "South", "E": "East", "N": "North",
    "West": "West", "South": "South", "East": "East",
    "North": "North", "Central": "Central",
}

def normalise_region(r):
    if r is None:
        return "Unknown"
    return REGION_MAP.get(str(r).strip(), str(r).strip())

# Spark column expression for region normalisation — used in withColumn()
region_map_expr = (
    F.when(F.col("region") == "W", "West")
     .when(F.col("region") == "S", "South")
     .when(F.col("region") == "E", "East")
     .when(F.col("region") == "N", "North")
     .otherwise(F.col("region"))
)

print(f"Model     : {MODEL_NAME}")
print(f"Catalog   : {CATALOG}")
print(f"Run started: {datetime.now().isoformat()}")


# COMMAND ----------

def extract_text(response) -> str:
    """
    Pull only the answer text from the model response.
    databricks-gpt-oss-20b returns a JSON array:
      [{"type": "reasoning", ...},        <- internal chain-of-thought, discard
       {"type": "text", "text": "..."}]   <- the actual answer, keep this
    """
    content = response.choices[0].message.content
    if isinstance(content, list):
        parts = [
            block["text"]
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ]
        return " ".join(parts).strip()
    return content.strip()


def call_llm(prompt: str, system_msg: str, retries: int = 3, wait: int = 2) -> str:
    """Call the LLM with automatic retry. Returns LLM_UNAVAILABLE on total failure."""
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.4
            )
            return extract_text(response)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(wait * (attempt + 1))
            else:
                return f"LLM_UNAVAILABLE: {str(e)}"


def validate_output(text: str, min_words: int = 40) -> dict:
    """Quality-check LLM output. Returns PASS or REVIEW with issue list."""
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
        "llm_check_detail":    ", ".join(issues) if issues else "none"
    }


# System message — forces the model to bridge the gap between numbers and decisions.
# A dashboard shows that West has a 22% return rate.
# This system message forces the model to answer: is that alarming, expected,
# or an opportunity — and what should leadership do about it?
SYSTEM_MSG_EXEC = (
    "You are a senior business analyst writing executive synthesis for a national retail chain. "
    "Your job is to bridge the gap between numbers and decisions. "
    "A dashboard already shows executives the figures — they do not need a restatement. "
    "For every key metric, you must explicitly state whether it is ALARMING, EXPECTED, "
    "or AN OPPORTUNITY — and justify that assessment in plain English. "
    "Identify the root cause behind each metric, name the specific regions or vendors, "
    "and end with a clear recommended action the leadership team should take in the next 30 days. "
    "Write in plain, authoritative business English. "
    "4-6 sentences of connected prose. No bullet points, no markdown, no headers, no company name."
)

print("LLM helpers defined.")


# COMMAND ----------

# Reads Gold only — does NOT touch Bronze or Silver.

df_revenue   = spark.table(f"{CATALOG}.mv_revenue_by_region")
df_vendor_rr = spark.table(f"{CATALOG}.mv_return_rate_by_vendor")
df_slow      = spark.table(f"{CATALOG}.mv_slow_moving_products")
df_vendors   = spark.table(f"{CATALOG}.dim_vendors")

print("Gold tables loaded:")
print(f"  mv_revenue_by_region      : {df_revenue.count():,} rows")
print(f"  mv_return_rate_by_vendor  : {df_vendor_rr.count():,} rows")
print(f"  mv_slow_moving_products   : {df_slow.count():,} rows")
print(f"  dim_vendors               : {df_vendors.count():,} rows")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part A — Executive Business Intelligence
# MAGIC
# MAGIC Each cell below follows the same pattern:
# MAGIC 1. **KPI Aggregation** — compute summary statistics, normalise region names, serialise to JSON
# MAGIC 2. **LLM Call** — pass the JSON with a prompt asking for root-cause synthesis
# MAGIC
# MAGIC **Executive summary standard:** every summary must explicitly state whether a number is *alarming*, *expected*, or *an opportunity*.

# COMMAND ----------

# DBTITLE 1,Cell 7
# Revenue performance KPI aggregation
# mv_revenue_by_region columns:
#   year, quarter, month, month_name, region,
#   total_sales, total_profit, line_item_count,
#   order_count, avg_line_item_value, avg_order_value


df_revenue_norm = df_revenue.withColumn("region", region_map_expr)

# Step 1: identify the latest (year, month) period in the data,
#         excluding any rows where year or month is NULL
df_valid = df_revenue_norm.filter(
    F.col("year").isNotNull() & F.col("month").isNotNull() & F.col("region").isNotNull()
)

latest_row = (
    df_valid
    .orderBy(F.col("year").desc(), F.col("month").desc())
    .select("year", "month")
    .first()
)
latest_year  = latest_row["year"]
latest_month = latest_row["month"]

# Step 2: identify the immediately prior (year, month) period
# Handles year boundary: e.g. month=1 of 2018 → prior = month=12 of 2017
if latest_month > 1:
    prior_year  = latest_year
    prior_month = latest_month - 1
else:
    prior_year  = latest_year - 1
    prior_month = 12

print(f"Latest period : {int(latest_year)}-{int(latest_month):02d}")
print(f"Prior period  : {int(prior_year)}-{int(prior_month):02d}")

# Step 3: aggregate current period
revenue_current = (
    df_valid
    .filter((F.col("year") == latest_year) & (F.col("month") == latest_month))
    .groupBy("region")
    .agg(
        F.round(F.sum("total_sales"),  2).alias("total_sales"),
        F.round(F.sum("total_profit"), 2).alias("total_profit"),
        F.sum("order_count")            .alias("order_count")
    )
    .orderBy(F.col("total_sales").desc())
    .collect()
)

# Step 4: aggregate prior period (single month, correct year)
prior_by_region = {
    r["region"]: r["prior_sales"]
    for r in df_valid
    .filter((F.col("year") == prior_year) & (F.col("month") == prior_month))
    .groupBy("region")
    .agg(F.sum("total_sales").alias("prior_sales"))
    .collect()
}

revenue_kpis = []
for r in revenue_current:
    region      = r["region"]   # already normalised
    current_val = round(float(r["total_sales"]  or 0), 2)
    profit_val  = round(float(r["total_profit"] or 0), 2)
    prior_val   = round(float(prior_by_region.get(region, current_val) or current_val), 2)
    mom_delta   = round(((current_val - prior_val) / prior_val * 100) if prior_val > 0 else 0, 1)
    margin_pct  = round((profit_val / current_val * 100) if current_val > 0 else 0, 1)
    revenue_kpis.append({
        "region":        region,
        "total_sales":   current_val,
        "total_profit":  profit_val,
        "profit_margin": f"{margin_pct}%",
        "order_count":   int(r["order_count"] or 0),
        "mom_delta_pct": f"{mom_delta:+.1f}%"
    })

total_revenue  = round(sum(r["total_sales"] for r in revenue_kpis), 2)
top_region     = revenue_kpis[0]["region"]  if revenue_kpis else "N/A"
bottom_region  = revenue_kpis[-1]["region"] if revenue_kpis else "N/A"
growing_regs   = [r["region"] for r in revenue_kpis if float(r["mom_delta_pct"].replace("%","").replace("+","")) > 5]
declining_regs = [r["region"] for r in revenue_kpis if float(r["mom_delta_pct"].replace("%","")) < -5]

revenue_kpi_json = json.dumps({
    "period":              f"{int(latest_year)}-{int(latest_month):02d}",
    "prior_period":        f"{int(prior_year)}-{int(prior_month):02d}",
    "total_revenue":       f"${total_revenue:,.2f}",
    "top_region":          top_region,
    "bottom_region":       bottom_region,
    "growing_regions":     growing_regs,
    "declining_regions":   declining_regs,
    "by_region":           revenue_kpis
})

print(f"Revenue KPIs — period: {int(latest_year)}-{int(latest_month):02d} vs {int(prior_year)}-{int(prior_month):02d}")
print(f"Total revenue : ${total_revenue:,.2f}")
print(f"  {'Region':<12}  {'Sales':>12}  {'Profit':>10}  Margin    MoM")
for r in revenue_kpis:
    print(f"  {r['region']:<12}  ${r['total_sales']:>11,.2f}  ${r['total_profit']:>9,.2f}  {r['profit_margin']:>6}  {r['mom_delta_pct']}")

# COMMAND ----------

# Revenue executive summary
# The prompt forces four specific answers executives actually need:
#   1. Is the overall picture alarming, expected, or an opportunity?
#   2. What is DRIVING the gap between regions?
#   3. What is the ROOT CAUSE for declining regions?
#   4. What should leadership do in the next 30 days?

revenue_prompt = f"""Regional revenue performance for {int(latest_year)}-{int(latest_month):02d} vs prior month {int(prior_year)}-{int(prior_month):02d} is shown below.
All region names are full names (West, South, East, North, Central).

KPI DATA:
{revenue_kpi_json}

Executives already see these numbers on a dashboard. What they need is the story behind them.

Write a 4-6 sentence executive synthesis that directly answers:
1. State clearly whether the overall revenue picture is ALARMING, EXPECTED, or AN OPPORTUNITY
   — and justify that assessment in one sentence using the MoM delta figures.
2. Name the specific regions driving the gap between top and bottom performers.
   Is this a demand problem, a coverage problem, or a momentum problem?
3. For regions showing month-over-month decline, state the single most likely root cause.
4. State exactly what leadership should prioritise in the next 30 days.

Do not restate the numbers — explain what they mean.
Plain business English. No bullet points. No markdown. No company name."""

revenue_summary = validate_output(call_llm(revenue_prompt, SYSTEM_MSG_EXEC))

print("REVENUE PERFORMANCE SUMMARY:")
print("=" * 70)
print(revenue_summary["text"])
print(f"\nllm_check: {revenue_summary['llm_check']}")


# COMMAND ----------

# DBTITLE 1,Cell 9
# Vendor return rate KPI aggregation
# mv_return_rate_by_vendor columns:
#   vendor_id, vendor_name, total_orders_sold, total_sales,
#   total_returns, total_refund_amount, avg_refund_amount, return_rate_pct
# Industry threshold: 20% — above this requires active vendor management.

vendor_agg = (
    df_vendors.select("vendor_id", "vendor_name")
    .join(
        df_vendor_rr.drop("vendor_name"),   # drop MV vendor_name to avoid duplicate
        on="vendor_id",
        how="left"                          # keep ALL vendors, including zero-return ones
    )
    .select(
        "vendor_id",
        "vendor_name",
        "total_returns",
        "total_orders_sold",
        "total_refund_amount"
    )
    .groupBy("vendor_id", "vendor_name")
    .agg(
        F.sum("total_returns")                .alias("total_returns"),
        F.sum("total_orders_sold")            .alias("total_orders"),
        F.round(F.sum("total_refund_amount"), 2).alias("total_refund_amount")
    )
    .orderBy(F.col("total_returns").desc())
    .collect()
)

# Weighted fleet average: sum(all returns) / sum(all orders) * 100
total_returns_all = sum(int(r["total_returns"] or 0) for r in vendor_agg)
total_orders_all  = sum(int(r["total_orders"]  or 0) for r in vendor_agg)
avg_return_overall = round(
    (total_returns_all / total_orders_all * 100) if total_orders_all > 0 else 0, 1
)

# Per-vendor weighted return rate: total_returns / total_orders * 100
def vendor_rate(r):
    orders  = int(r["total_orders"]  or 0)
    returns = int(r["total_returns"] or 0)
    return round((returns / orders * 100) if orders > 0 else 0.0, 1)

high_risk_vendors = [r for r in vendor_agg if vendor_rate(r) > 20.0]

vendor_kpi_json = json.dumps({
    "note":                            "return_rate values are percentages e.g. 18.7 means 18.7%",
    "total_vendors_analyzed":          len(vendor_agg),
    "vendors_above_20pct_threshold":   len(high_risk_vendors),
    "fleet_avg_return_rate_weighted":  f"{avg_return_overall}%",
    "industry_benchmark":              "20% — above this requires active vendor management",
    "vendors_ranked": [
        {
            "vendor":              r["vendor_name"] or r["vendor_id"],
            "return_rate":         f"{vendor_rate(r):.1f}%",
            "total_returns":       int(r["total_returns"] or 0),
            "total_orders":        int(r["total_orders"]  or 0),
            "total_refund_amount": f"${float(r['total_refund_amount'] or 0):,.2f}",
            "status": (
                "HIGH RISK"      if vendor_rate(r) > 20.0
                else "no_return_data" if int(r["total_orders"] or 0) == 0
                else "within threshold"
            )
        }
        for r in vendor_agg
    ]
})

print(f"Vendor return rate KPIs — {len(vendor_agg)} vendors analyzed (all vendors from dim_vendors)")
print(f"Vendors above 20% threshold   : {len(high_risk_vendors)}")
print(f"Fleet avg return rate (weighted): {avg_return_overall}%")
print()
print(f"  {'Vendor':<30}  Return Rate  Orders   Status")
for r in vendor_agg:
    name   = str(r["vendor_name"] or r["vendor_id"])
    rate   = vendor_rate(r)
    orders = int(r["total_orders"] or 0)
    status = "HIGH RISK" if rate > 20.0 else ("no_return_data" if orders == 0 else "within threshold")
    print(f"  {name:<30}  {rate:>6.1f}%   {orders:>6}   {status}")

# COMMAND ----------

# Vendor return rate executive summary
# The prompt forces diagnosis and specific action per vendor.
# All rates are labelled "%" in the KPI JSON so the model cannot misread them.

vendor_prompt = f"""Vendor return rate data across all active suppliers is shown below.
All return_rate values are already expressed as percentages (18.7 means 18.7%, NOT 1870%).
The industry benchmark for retail is 20% — above this requires active vendor management.

KPI DATA:
{vendor_kpi_json}

Executives see these return rates on a dashboard. What they need is what those rates mean
and what to do about them.

Write a 4-6 sentence executive synthesis that directly answers:
1. State clearly whether the vendor return picture is ALARMING, WITHIN NORMS, or IMPROVING
   — justify using the 20% industry threshold.
2. For each vendor above 20%, name the vendor, state its exact rate, and diagnose the most
   likely cause: product quality failure, fulfilment defect, or category mismatch.
3. State the financial consequence of leaving high-return vendors unchanged — reference
   the total refund amounts.
4. State the recommended action for each high-risk vendor: renegotiate, probation, or
   alternative sourcing.

Do not restate the numbers — explain what they mean and what to do.
Plain business English. No bullet points. No markdown. No company name."""

vendor_summary = validate_output(call_llm(vendor_prompt, SYSTEM_MSG_EXEC))

print("VENDOR RETURN RATE SUMMARY:")
print("=" * 70)
print(vendor_summary["text"])
print(f"\nllm_check: {vendor_summary['llm_check']}")


# COMMAND ----------

# DBTITLE 1,Cell 11
# Slow-moving inventory KPI aggregation
# mv_slow_moving_products columns:
#   product_id, product_name, brand, region,
#   total_quantity_sold, total_sales, order_count,
#   first_sale_date, last_sale_date,
#   days_since_last_sale, selling_period_days, avg_daily_quantity

df_slow_norm = df_slow.withColumn("region", region_map_expr)

# Derive the dataset's own as-of date (max last_sale_date in the MV)
dataset_asof_date = (
    df_slow_norm
    .agg(F.max("last_sale_date").alias("max_last_sale"))
    .collect()[0]["max_last_sale"]
)
print(f"Dataset as-of date (max last_sale_date): {dataset_asof_date}")

# Re-derive days_since_last_sale relative to dataset_asof_date
# Filter out NULL regions to avoid formatting errors
df_slow_fixed = (
    df_slow_norm
    .filter(F.col("region").isNotNull())
    .withColumn(
        "days_since_last_sale_fixed",
        F.datediff(F.lit(dataset_asof_date), F.col("last_sale_date"))
    )
)

slow_agg = (
    df_slow_fixed
    .groupBy("region")
    .agg(
        F.count("*").alias("slow_product_count"),
        F.round(F.sum(
            F.when(F.col("total_sales").isNotNull(), F.col("total_sales")).otherwise(0)
        ), 2).alias("estimated_exposure"),
        F.round(F.avg("days_since_last_sale_fixed"), 0).alias("avg_days_stagnant")
    )
    .orderBy(F.col("slow_product_count").desc())
    .collect()
)

total_slow     = sum(int(r["slow_product_count"] or 0) for r in slow_agg)
total_exposure = round(sum(float(r["estimated_exposure"] or 0) for r in slow_agg), 2)
worst_region   = slow_agg[0]["region"] if slow_agg else "N/A"

slow_kpi_json = json.dumps({
    "reference_date":               str(dataset_asof_date),
    "note":                         "avg_days_stagnant measured within dataset period, not from today",
    "total_slow_moving_products":   total_slow,
    "total_inventory_exposure_usd": f"${total_exposure:,.2f}",
    "worst_performing_region":      worst_region,
    "by_region": [
        {
            "region":             r["region"],
            "slow_product_count": int(r["slow_product_count"] or 0),
            "exposure_usd":       f"${float(r['estimated_exposure'] or 0):,.2f}",
            "avg_days_stagnant":  int(r["avg_days_stagnant"] or 0)
        }
        for r in slow_agg
    ]
})

print("Slow-moving inventory KPIs")
print(f"Total slow products : {total_slow}")
print(f"Total exposure      : ${total_exposure:,.2f}")
print(f"Worst region        : {worst_region}")
print()
print(f"  {'Region':<12}  Products  Avg days stagnant  Exposure ($)")
for r in slow_agg:
    print(f"  {r['region']:<12}  {r['slow_product_count']:>8}  "
          f"{int(r['avg_days_stagnant'] or 0):>17}  "
          f"${float(r['estimated_exposure'] or 0):>12,.2f}")

# COMMAND ----------

# Slow-moving inventory executive summary
# The prompt forces the model to explain WHY inventory sits unsold
# and what the FULL cost of inaction is — not just cite the exposure number.

slow_prompt = f"""Slow-moving inventory data by region is shown below.
All region names are full names (West, South, East, North, Central).

KPI DATA:
{slow_kpi_json}

Executives see these counts and exposure figures on a dashboard.
The dashboard does not show why the inventory is sitting unsold
or what it actually costs to leave it there.

Write a 4-6 sentence executive synthesis that directly answers:
1. State clearly whether this situation is ALARMING, A KNOWN SEASONAL PATTERN,
   or AN OPPORTUNITY — and justify that assessment in one sentence.
2. Name the worst-performing region by name, state its average days stagnant,
   and give the single most likely cause: pricing misalignment, vendor oversupply,
   merchandising failure, or regional demand mismatch.
3. State the FULL cost of inaction — not just the dollar exposure but also
   holding cost, markdown risk, and opportunity cost of tied-up shelf space.
4. Name the most appropriate resolution: clearance pricing, inter-region transfer,
   vendor buyback, or discontinuation — and which region to start with.

Do not restate the numbers — explain what they mean and why the situation exists.
Plain business English. No bullet points. No markdown. No company name."""

slow_summary = validate_output(call_llm(slow_prompt, SYSTEM_MSG_EXEC))

print("SLOW-MOVING INVENTORY SUMMARY:")
print("=" * 70)
print(slow_summary["text"])
print(f"\nllm_check: {slow_summary['llm_check']}")


# COMMAND ----------

# Write all three executive summaries to ai_business_insights
# Write mode = APPEND: each run adds rows so summaries can be compared across periods.
# kpi_json is stored for auditability.

insights = [
    {
        "insight_type":     "revenue_performance",
        "ai_summary":       revenue_summary["text"],
        "kpi_json":         revenue_kpi_json,
        "llm_check":        revenue_summary["llm_check"],
        "llm_check_detail": revenue_summary["llm_check_detail"],
        "generated_at":     datetime.now().isoformat()
    },
    {
        "insight_type":     "vendor_return_rate",
        "ai_summary":       vendor_summary["text"],
        "kpi_json":         vendor_kpi_json,
        "llm_check":        vendor_summary["llm_check"],
        "llm_check_detail": vendor_summary["llm_check_detail"],
        "generated_at":     datetime.now().isoformat()
    },
    {
        "insight_type":     "slow_moving_inventory",
        "ai_summary":       slow_summary["text"],
        "kpi_json":         slow_kpi_json,
        "llm_check":        slow_summary["llm_check"],
        "llm_check_detail": slow_summary["llm_check_detail"],
        "generated_at":     datetime.now().isoformat()
    }
]

schema = StructType([
    StructField("insight_type",     StringType(), True),
    StructField("ai_summary",       StringType(), True),
    StructField("kpi_json",         StringType(), True),
    StructField("llm_check",        StringType(), True),
    StructField("llm_check_detail", StringType(), True),
    StructField("generated_at",     StringType(), True)
])

df_insights = spark.createDataFrame(insights, schema=schema)

(
    df_insights.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.ai_business_insights")
)

spark.sql(f"OPTIMIZE {CATALOG}.ai_business_insights ZORDER BY (insight_type, generated_at)")

print(f"Written to {CATALOG}.ai_business_insights")
print("OPTIMIZE + ZORDER complete")
display(spark.table(f"{CATALOG}.ai_business_insights").orderBy(F.col("generated_at").desc()).limit(3))


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part B — ai_query() demonstrations
# MAGIC
# MAGIC `ai_query()` calls a Foundation Model directly inside SQL — no Python loop needed.
# MAGIC
# MAGIC **Response extraction:**
# MAGIC `databricks-gpt-oss-20b` returns a JSON array. Two `get_json_object()` calls extract the answer:
# MAGIC ```sql
# MAGIC get_json_object(ai_query(...), '$[1]')    -- gets the text block
# MAGIC get_json_object(above, '$.text')          -- gets the answer string
# MAGIC ```
# MAGIC
# MAGIC **Demo 1:** Vendor return rate risk assessment per vendor
# MAGIC
# MAGIC **Demo 2:** Regional revenue commentary — alarming, expected, or opportunity

# COMMAND ----------

# Demo 1: ai_query() on vendor return rates
# return_rate_pct is already a percentage — labelled explicitly in the CONCAT prompt.
# The model is told 20% is the industry threshold so it can give a calibrated assessment.

print("=" * 70)
print("DEMO 1 — ai_query() on vendor return rates")
print("Reads: globalmart.gold.mv_return_rate_by_vendor")
print("=" * 70)

spark.sql(f"""
-- Demo 1 — vendor return rates (FIXED)
SELECT
    vendor_id,
    vendor_name,
    ROUND(return_rate_pct, 1)                                   AS return_rate_pct,
    total_returns,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT(
            'Vendor ', vendor_name,
            ' has a return rate of ',
            CAST(ROUND(return_rate_pct, 1) AS STRING), '%. ',
            'The industry benchmark for retail is 20%. ',
            'In one sentence, state whether this rate is ACCEPTABLE (under 15%), ',
            'CONCERNING (15-20%), or CRITICAL (above 20%), ',
            'name the most likely cause (product quality, fulfilment error, or category mismatch), ',
            'and state one specific action the procurement team should take. ',
            'Plain English only. No markdown.'
        )
    )                                                           AS risk_assessment
FROM globalmart.gold.mv_return_rate_by_vendor
ORDER BY return_rate_pct DESC
""").display()


# Demo 2: ai_query() on revenue by region
# Region abbreviations are normalised inside SQL CASE WHEN before being
# passed into the ai_query() CONCAT — model always sees full region names.

print()
print("=" * 70)
print("DEMO 2 — ai_query() on revenue by region (latest month)")
print("Reads: globalmart.gold.mv_revenue_by_region")
print("=" * 70)

spark.sql(f"""
-- Demo 2 — revenue by region (FIXED)
SELECT
    CASE region
        WHEN 'W' THEN 'West'
        WHEN 'S' THEN 'South'
        WHEN 'E' THEN 'East'
        WHEN 'N' THEN 'North'
        ELSE region
    END                                                         AS region,
    ROUND(total_sales, 0)                                       AS total_sales,
    ROUND(total_profit, 0)                                      AS total_profit,
    order_count,
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT(
            'The ',
            CASE region
                WHEN 'W' THEN 'West'
                WHEN 'S' THEN 'South'
                WHEN 'E' THEN 'East'
                WHEN 'N' THEN 'North'
                ELSE region
            END,
            ' region generated $', CAST(ROUND(total_sales, 0) AS STRING),
            ' in revenue and $', CAST(ROUND(total_profit, 0) AS STRING),
            ' in profit across ', CAST(order_count AS STRING), ' orders this period. ',
            'In one sentence, state whether this is ALARMING, EXPECTED, or AN OPPORTUNITY, ',
            'give the single most likely reason, and name one action leadership should take. ',
            'Plain English only. No markdown.'
        )
    )                                                           AS revenue_commentary
FROM globalmart.gold.mv_revenue_by_region
WHERE month = (SELECT MAX(month) FROM globalmart.gold.mv_revenue_by_region)
ORDER BY total_sales DESC
""").display()

# COMMAND ----------

# Step 1: Inspect the raw ai_query() response structure
# This tells us exactly what JSON path to use for extraction
print("Inspecting raw ai_query() response structure...")

spark.sql(f"""
SELECT
    vendor_name,
    ai_query(
        '{MODEL_NAME}',
        CONCAT('Vendor ', vendor_name, ' has a return rate of ',
               CAST(ROUND(return_rate_pct, 1) AS STRING), '%. ',
               'In one sentence, is this acceptable or concerning? Plain English only.')
    ) AS raw_response
FROM {CATALOG}.mv_return_rate_by_vendor
LIMIT 1
""").display()

# COMMAND ----------

# Log the run

review_count = sum(
    1 for s in [revenue_summary, vendor_summary, slow_summary]
    if s["llm_check"] == "REVIEW"
)

run_log = [{
    "notebook":          NOTEBOOK_NAME,
    "run_timestamp":     datetime.now().isoformat(),
    "records_processed": 3,
    "llm_calls_made":    3,
    "outputs_flagged":   review_count,
    "status":            "SUCCESS" if review_count == 0 else "PARTIAL_REVIEW",
    "notes":             f"domains=revenue,vendor_return_rate,slow_inventory | period={latest_month}"
}]

(
    spark.createDataFrame(run_log)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("globalmart.config.pipeline_run_log")
)

print(f"Run log written. Status: {run_log[0]['status']}")
print(f"Executive summaries : 3 domains")
print(f"Review flags        : {review_count}")