# GlobalMart Data Intelligence Platform

> A production-grade Medallion Architecture on Databricks — unifying six disconnected regional retail systems into a single, AI-powered analytics platform.

---

## Table of Contents

- [The Business Problem](#the-business-problem)
- [Solution Overview](#solution-overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Bronze Layer](#bronze-layer--raw-ingestion)
- [Silver Layer](#silver-layer--harmonization-and-quality)
- [Gold Layer](#gold-layer--dimensional-model-and-analytics)
- [Gen AI Intelligence Layer](#gen-ai-intelligence-layer)
- [Governance and Observability](#governance-and-observability)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Platform Constraints and Design Decisions](#platform-constraints-and-design-decisions)

---

## The Business Problem

GlobalMart is a national retail chain operating across five US regions — East, West, Central, South, and North. Over the past decade, the company grew by acquiring six smaller regional retail operators. Each acquisition came with its own data systems, formats, and business rules. Nothing was ever unified.

This quarter, three critical failures surfaced simultaneously.

### 1. Revenue Audit Failure

Two regions reported overlapping customer orders under different IDs, overstating quarterly revenue by approximately 9%. External auditors require a unified, reconcilable customer registry within 60 days or GlobalMart faces qualification risk on its annual filing.

Without a unified platform, a data analyst manually downloads six CSV files every quarter to reconcile revenue. Systems spell names differently, format dates inconsistently, and matching records takes three days — with a 30% error rate.

### 2. Returns Fraud Exposure

Without a unified view of customer return history, the returns team processes each return in isolation. Return fraud has grown to an estimated $2.3M annual loss. A customer can submit a $480 return and the agent sees no prior history and approves it — unaware that the same customer made seven returns across two other regions in four months. Each system holds a fragment of the story; no single system sees the pattern.

### 3. Inventory Blindspot

The merchandising team has no visibility into which products are selling, which regions are underperforming, or which vendors supply the most returned items. Regional managers estimate 12–18% of potential revenue is lost from slow-moving inventory never identified in time to discount or redistribute. A product can sit unsold in the West region for 11 weeks while the Central region sold out of the same product in week two and turned away customers.

---

## Solution Overview

This project is a Proof of Concept built on the Databricks Intelligent Data Platform that proves:

- Data from six disconnected regional systems can be unified and trusted, automatically, without manual patching
- Data quality issues can be caught, quarantined, and explained — not silently ignored
- Business users can ask questions and get answers without writing SQL or waiting for engineering
- AI can explain data health and surface insights in language that leadership understands

The platform implements a **Medallion Architecture** (Bronze → Silver → Gold) with a **Gen AI Intelligence Layer** sitting above the Gold layer, serving four distinct business teams.

| Component | Technology |
|-----------|-----------|
| Platform | Databricks Free Edition |
| Pipeline Engine | Spark Declarative Pipelines (Auto Loader + EXPECT clauses) |
| Storage | Delta Lake + Unity Catalog Volumes |
| Governance | Unity Catalog (3-level namespace: `catalog.schema.table`) |
| AI Model | `databricks-gpt-oss-20b` via Databricks Foundation Models (free, no billing) |
| Vector Search | FAISS + `sentence-transformers/all-MiniLM-L6-v2` (local, no external API) |
| Serving | Databricks SQL Warehouse (serverless) + AI/BI Genie NLQ |

---

## Architecture

### High-Level Data Flow

```
6 Regional Source Systems (East · West · Central · South · North · Future)
  CSV · JSON · Parquet
          |
          v
+------------------------------------------------------------------+
|  INGESTION CONTROL  (metadata-driven, zero hardcoded paths)      |
|  source_registry  column_mapping  dq_rules  data_contracts       |
|  survivorship_rules  fraud_rule_config                           |
+-------------------------------+----------------------------------+
                                |
                                |  Auto Loader + Spark Declarative Pipeline
                                v
+-------------------------------+
|  BRONZE  (raw_zone)           |
|  Append-only Delta tables     |
|  6 entities, 1 table each     |
|  _source_file + _load_ts      |
+---------------+---------------+
                |
                |  AI-driven schema harmonization
                v
+---------------+---------------+
|  MDM  (master data mgmt)      |
|  Golden records per entity    |
|  master_customer_id assigned  |
+---------------+---------------+
                |
                |  Spark Declarative Pipelines (nightly)
                v
+-----------------------------------------------+
|  SILVER  (harmonized · validated · type-cast) |
|  6 clean entity tables                        |
|  6 quarantine tables  (_failure_reason)        |
|  EXPECT clauses enforce rules in-pipeline     |
+---------------------+-------------------------+
                      |
                      |  Star schema build
                      v
+------------------------------------------------------------------+
|  GOLD  (analytics catalog)                                       |
|  dim_customers  dim_products  dim_vendors  dim_dates             |
|  fact_sales  fact_orders  fact_returns                           |
|  mv_revenue_by_region  mv_return_rate_by_vendor                  |
|  mv_slow_moving_products  mv_customer_return_history             |
|  dq_audit_report  flagged_return_customers                       |
|  rag_query_history  ai_business_insights                         |
+-------------------+--------------------------------------------+
                    |
        +-----------+-----------+
        |           |           |
        v           v           v
  SQL Warehouse   AI/BI      Gen AI
  Dashboards      Genie NLQ  Intelligence (4 use cases)
```

### Architecture Layers

| Layer | Purpose | Key Behavior |
|-------|---------|--------------|
| **Ingestion Control** | Metadata-driven config tables that drive the entire pipeline. No hardcoded paths anywhere. | Adding a new region requires inserting one row into `source_registry`. Zero code change. |
| **Bronze** | Raw landing zone. Data preserved exactly as received. | Auto Loader detects new files incrementally. Schema drift captured in `_rescued_data`. Every record tagged with `_source_file` and `_load_timestamp`. |
| **MDM** | Master Data Management. Resolves the same customer appearing under different IDs across regions. | `survivorship_rules` config drives field-level deduplication. Emits one `master_customer_id` — the spine of the entire analytics platform. |
| **Silver** | Cleaned, harmonized, type-cast, validated data. | AI-driven schema mapping via `databricks-gpt-oss-20b`. EXPECT clauses enforce quality rules. Failed records routed to `_quarantine` tables, not silently dropped. |
| **Gold** | Business consumption layer — star schema + materialized views + AI output tables. | SCD Type 1 dimensions. Array-sort matching for return-to-sales reconciliation. Fraud risk scoring built in. Pre-aggregated MVs auto-refresh on pipeline completion. |
| **Gen AI** | Four AI use cases that turn Gold layer data into business action. | LLM used only for narrative and explanation. Rules used for detection. Outputs written back to Gold as queryable Delta tables. |
| **Serving** | Resource-isolated from the pipeline. SQL Warehouse + AI/BI Genie NLQ. | Auto-refresh triggers on pipeline completion. 30+ concurrent users supported. |

### Component Map

```
GOVERNANCE LAYER
  Unity Catalog
    globalmart.bronze.*
    globalmart.silver.*
    globalmart.gold.*
    globalmart.mdm.*
    globalmart.ingestion_control.*
  Unity Catalog Volumes      /Volumes/globalmart/raw/source_files/
  Column-level lineage       source file -> Bronze -> Silver -> Gold  (automatic)
  Access control             Row-level permissions on Gold tables per business team
  Delta Lake                 ACID transactions · time travel · MERGE for incremental

PIPELINE LAYER  (Spark Declarative Pipelines)
  Auto Loader                cloudFiles reads CSV/JSON from Volumes
  Schema evolution           addNewColumns mode — inferColumnTypes disabled
  Column mapping             ingestion_control.column_mapping — config-driven rename at ingest
  EXPECT clauses             NOT NULL · valid categories · positive values · FK presence
  Quarantine tables          silver.*_quarantine — 6 tables, failed records + _failure_reason
  Master Data Mgmt           mdm.customers · mdm.products · mdm.vendors — golden records
  pipeline_run_log           observability: notebook · status · records processed · run timestamp

GEN AI INTELLIGENCE LAYER  (reads from Gold only)
  UC1 — DQ Reporter          Input: silver.*_quarantine     Output: gold.dq_audit_report
  UC2 — Fraud Investigator   Input: fact_returns + dim_customers  Output: gold.flagged_return_customers
  UC3 — Product Assistant    Input: dim_products + dim_vendors + MVs  Output: gold.rag_query_history
  UC4 — Executive Insights   Input: all Gold MVs            Output: gold.ai_business_insights

SERVING LAYER  (resource-isolated from pipeline cluster)
  Databricks SQL Warehouse   Serverless · auto-scale · 30+ concurrent users
  AI/BI Genie                NLQ space over Gold tables · schema context instructions embedded
  MV auto-refresh            Triggered automatically on pipeline completion
```

---

## Data Sources

GlobalMart's six regional systems handed over 16 raw files across six entity types:

| Entity | Files | Format | Core Challenge |
|--------|-------|--------|----------------|
| Customers | 6 | CSV | Six different ID column names (`customer_id`, `CustomerID`, `cust_id`, `customer_identifier`). One file missing `customer_email`. One file with `state` and `city` positionally swapped. |
| Orders | 3 | CSV | Order IDs appearing in multiple regional files, driving the 9% revenue overstatement. Missing `order_estimated_delivery_date` in one file. |
| Transactions | 3 | CSV | `profit` column absent in one file. `discount` stored as string in one file and float in another. `Sales` column mixed types across files. |
| Returns | 2 | JSON | Fragmented return history — the same customer's activity split across two systems with no shared key. |
| Products | 1 | JSON | Single product catalogue, un-normalized, missing performance metrics at source. |
| Vendors | 1 | CSV | Reference table. Clean at source but requires return rate enrichment at Gold layer. |

Total records across all source files: approximately 25,000+ rows.

**Data Dictionary:** `architecture/GlobalMartDataDictionary.xlsx` — full field-level definitions for all entities and all regions.

---

## Bronze Layer — Raw Ingestion

The Bronze layer is the raw landing zone. Data is preserved exactly as received — no transformations, no type casting, no filtering.

### Pipeline Design

The ingestion engine is metadata-driven. It reads `ingestion_control.source_registry` at runtime and applies `ingestion_control.column_mapping` dynamically. There are no hardcoded file paths in any pipeline notebook.

```python
# Auto Loader with schema evolution — core Bronze ingestion pattern
spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  .option("cloudFiles.inferColumnTypes", "false")   # land all columns as string
  .option("rescuedDataColumn", "_rescued_data")     # captures unknown columns as JSON
  .load(source_path)
```

### Schema Evolution Handling

When a new column arrives in a source file — for example, `Birth Date` added by Region 3 after the pipeline has been running for months:

- `addNewColumns` mode adds the column to the Bronze table automatically, no pipeline failure
- Spaces and special characters in column names are sanitized at ingestion time: `Birth Date` becomes `birth_date`
- Unknown columns are captured in `_rescued_data` as a JSON blob for downstream review
- A schema drift warning is logged in `schema_validation_bronze`

### Lineage

Every record in every Bronze table carries two metadata columns:

- `_source_file` — exact path of the source file (e.g., `/Volumes/globalmart/raw/source_files/region3/customers_3.csv`)
- `_load_timestamp` — UTC timestamp of when the record was loaded

This provides full traceability from any Gold layer revenue figure back to the exact source file — a hard requirement for the external audit.

### Pre-Bronze Validation

Before Auto Loader ingests a file, a pre-bronze audit runs against a `CONTRACTS` dictionary per entity:

- Acceptable primary key column name variants
- Mandatory columns list
- Minimum row count threshold
- Minimum column count threshold

Files that produce a WARN (not FAIL) are logged to `schema_validation_bronze` with granular issue descriptions. The pipeline continues — Bronze always ingests what it receives.

### Bronze Tables Created

```
globalmart.bronze.customers       (merged from customers_1 through customers_6)
globalmart.bronze.orders          (merged from orders_1 through orders_3)
globalmart.bronze.transactions    (merged from transactions_1 through transactions_3)
globalmart.bronze.returns         (merged from returns_1 and returns_2)
globalmart.bronze.products
globalmart.bronze.vendors
```

---

## Silver Layer — Harmonization and Quality

The Silver layer transforms raw Bronze data into clean, consistent, analytics-ready data. It is the first layer where data can be trusted for business use.

### AI-Driven Schema Harmonization

The Silver pipeline uses `databricks-gpt-oss-20b` to handle the most complex part of the harmonization problem: mapping six different regional schemas to a single canonical schema, without hand-coding every column name variant.

```
Step 1 — TARGET_SCHEMAS dictionary defines canonical column names and data types
         (customer_id: STRING, order_purchase_date: TIMESTAMP, sales: DECIMAL(10,2), ...)

Step 2 — Source profiling: extract column names + 5 sample values from Bronze tables
         Detects: $ symbols, ? placeholders, mixed-type indicators, swapped columns

Step 3 — AI mapping: profiles passed to databricks-gpt-oss-20b
         Returns SQL expressions for casting and renaming each source column

Step 4 — Harmonization: AI mappings executed via Spark expr() + COALESCE()
         Prioritizes values across regional column variants into one canonical output

Step 5 — Fallback: if AI mapping confidence is below threshold, rule-based fallback applied
         Every record tagged with _mapping_source (LLM or FALLBACK) for auditability
```

### Quality Rules and Quarantine

Quality rules are declared as EXPECT clauses inside the pipeline definition — not applied in post-processing. This is a hard architectural requirement.

| Entity | Example Rules | Action on Failure |
|--------|--------------|-------------------|
| customers | `customer_id IS NOT NULL` · `segment IN ('Consumer','Corporate','Home Office')` | `expect_or_drop` |
| orders | `order_id IS NOT NULL` · `customer_id IS NOT NULL` | `expect_or_drop` |
| transactions | `sales > 0` · `quantity > 0` | `expect_or_drop` |
| returns | `return_id IS NOT NULL` · `refund_amount >= 0` | `expect_or_drop` |
| products | `product_id IS NOT NULL` · `category IS NOT NULL` | `expect_or_drop` |
| vendors | `vendor_id IS NOT NULL` · `vendor_name IS NOT NULL` | `expect_or_drop` |

Records that fail any rule are routed to the corresponding `_quarantine` table — never silently dropped. Each quarantine record includes a `_failure_reason` column describing which rule was violated, making failed records immediately actionable by the finance team.

### Structural Issues Found and Fixed

| Issue | Source | Resolution |
|-------|--------|-----------|
| `CustomerID` instead of `customer_id` | customers_2.csv | Normalized via column_mapping config |
| `cust_id` instead of `customer_id` | customers_3.csv | Normalized via column_mapping config |
| `customer_identifier`, `email_address`, `full_name` | customers_6.csv | All three normalized to canonical names |
| Missing `customer_email` column | customers_5.csv | NULL column injected, records preserved |
| `state` and `city` positionally swapped | customers_4.csv | Corrected via AI mapping (positional swap detection) |
| `Sales` typed as STRING | transactions_1.csv | Cast to DECIMAL(10,2) |
| `discount` typed as STRING | transactions_2.csv | Cast to FLOAT |
| Missing `profit` column | transactions_3.csv | NULL column injected |
| Inconsistent date formats across order files | orders_1,2,3 | Standardized to TIMESTAMP |

### Silver Tables Created

```
globalmart.silver.customers          globalmart.silver.customers_quarantine
globalmart.silver.orders             globalmart.silver.orders_quarantine
globalmart.silver.transactions       globalmart.silver.transactions_quarantine
globalmart.silver.returns            globalmart.silver.returns_quarantine
globalmart.silver.products           globalmart.silver.products_quarantine
globalmart.silver.vendors            globalmart.silver.vendors_quarantine
```

---

## Gold Layer — Dimensional Model and Analytics

The Gold layer is the single trusted source for all business questions at GlobalMart. It is structured as a star schema — designed so that every one of the three core business failures can be answered from any angle, in any combination.

### Dimensional Model

**Design principle:** one row must represent exactly one business event.

```
                        +-------------+
                        |  dim_dates  |
                        |  date_key   |
                        |  full_date  |
                        |  year       |
                        |  quarter    |
                        |  month      |
                        +------+------+
                               |
+--------------+    +----------+----------+    +--------------+
| dim_customers|    |    fact_sales        |    | dim_products |
|  customer_key+----+  PK sales_key        +----+  product_key |
|  customer_id |    |  FK customer_key     |    |  product_id  |
|  region      |    |  FK product_key      |    |  brand       |
|  segment     |    |  FK vendor_key       |    |  category    |
+--------------+    |  FK date_key         |    +--------------+
                    |  sales (MEASURE)     |
                    |  profit (MEASURE)    |    +--------------+
                    |  quantity (MEASURE)  |    |  dim_vendors |
                    |  discount (MEASURE)  +----+  vendor_key  |
                    |  payment_type        |    |  vendor_id   |
                    +---------------------+    |  vendor_name |
                                               +--------------+

+--------------+    +---------------------+
| dim_customers|    |    fact_returns      |
|  customer_key+----+  FK customer_key     |
+--------------+    |  FK product_key      |
                    |  FK date_key         |
                    |  refund_amount       |
                    |  original_sales      |
                    |  return_to_sales_ratio|
                    |  return_reason       |
                    +---------------------+

+--------------+    +---------------------+
| dim_customers|    |    fact_orders       |
|  customer_key+----+  PK order_key        |
+--------------+    |  FK customer_key     |
                    |  FK vendor_key       |
                    |  FK date_key         |
                    |  order_status        |
                    |  ship_mode           |
                    |  delivery timestamps |
                    +---------------------+
```

**fact_sales grain:** one row = one transaction line item (one product sold on one order)  
**fact_returns grain:** one row = one return event  
**fact_orders grain:** one row = one order header

### Advanced Return-to-Sales Matching

Return records arrive at the order level. Sales records exist at the line-item level. Matching them requires an array-sort approach: a lambda function sorts a struct array by smallest price delta to find the correct line-item match for each return event. This resolves the revenue reconciliation problem at the data layer before any reporting or AI runs.

### Materialized Views

Pre-aggregated Materialized Views are built on top of the fact and dimension tables. They auto-refresh when the upstream pipeline completes — no manual trigger, no engineer involvement required for dashboards to show current numbers.

| Materialized View | Grain | Business Problem Addressed |
|-------------------|-------|---------------------------|
| `mv_revenue_by_region` | year, quarter, month, region | Revenue audit — resolves the 9% overstatement by consolidating all regions into one reconciled number |
| `mv_return_rate_by_vendor` | vendor_id, vendor_name | Inventory blindspot — identifies which vendors drive returns |
| `mv_slow_moving_products` | product_id, brand, region | Inventory blindspot — flags products by `days_since_last_sale` so the merchandising team can act before the discount window closes |
| `mv_customer_return_history` | customer_id, segment, region | Returns fraud — consolidates each customer's return activity across all regions |

Three Metric Views expose these aggregations to Databricks AI/BI Genie for natural language querying:

```
metricview_revenue         (dims: Region, Segment, Year, Quarter, Month, Payment Type)
metricview_inventory       (dims: Region, Brand, Category, Slow Moving Flag)
metricview_vendor_returns  (dims: Vendor, Return Reason, Return Status, Region)
```

### Gold Tables Created

```
Dimensions
  globalmart.gold.dim_customers
  globalmart.gold.dim_products
  globalmart.gold.dim_vendors
  globalmart.gold.dim_dates

Fact Tables
  globalmart.gold.fact_sales
  globalmart.gold.fact_orders
  globalmart.gold.fact_returns

Materialized Views
  globalmart.gold.mv_revenue_by_region
  globalmart.gold.mv_return_rate_by_vendor
  globalmart.gold.mv_slow_moving_products
  globalmart.gold.mv_customer_return_history

AI Output Tables
  globalmart.gold.dq_audit_report
  globalmart.gold.flagged_return_customers
  globalmart.gold.rag_query_history
  globalmart.gold.ai_business_insights
```

---

## Gen AI Intelligence Layer

The Gen AI layer closes the gap between trusted data and business action. All four use cases use the same model and connection pattern, set up once.

**Model:** `databricks-gpt-oss-20b`
Free on Databricks Free Edition. 128K context window. Available under: Serving > Foundation Models.

```python
from openai import OpenAI

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook.getContext().apiToken().get()
_ws_url = spark.conf.get("spark.databricks.workspaceUrl").replace("https://", "").strip("/")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{_ws_url}/serving-endpoints"
)
MODEL_NAME = "databricks-gpt-oss-20b"
```

**Response parsing:** the model returns a structured JSON array. The text block must be extracted:

```python
def _extract_text(response):
    for block in response.choices[0].message.content:
        if isinstance(block, dict) and block.get("type") == "text":
            return block["text"]
    return ""
```

---

### UC1: AI Data Quality Reporter

**Business owner:** Finance team (external audit preparation)  
**Reads from:** `globalmart.silver.*_quarantine`  
**Writes to:** `globalmart.gold.dq_audit_report`

The finance team is preparing for an external audit. Hundreds of records were rejected during Silver harmonization. They need to understand what was rejected, why it matters, and what business decision is at risk — in plain English, not as a Python error or a row count.

**Pipeline:**

```
1. Read quarantine tables with watermark on generated_at (incremental — no duplicate explanations)
2. Group by rule_name + issue_type — collect sample failures and source file paths
3. Classify severity: CRITICAL / HIGH / MEDIUM — based on record volume and field importance
4. AI generation: LLM maps error groups to BUSINESS_IMPACT dictionary
   Prompt includes: entity name, field affected, rule violated, count, sample values
   Required output sections: what the problem is, why it cannot be accepted, what audit figure is at risk
5. Quality control: scan output for technical jargon, validate 3 sections present, auto-retry on failure
6. Append to globalmart.gold.dq_audit_report
```

**Output table schema:**
```
entity            STRING    (customers, orders, transactions, ...)
field_affected    STRING    (customer_id, sales, segment, ...)
issue_type        STRING    (null_pk, invalid_category, negative_value, ...)
rejected_count    INTEGER
ai_explanation    STRING    (3-4 sentence plain-English audit narrative)
severity          STRING    (CRITICAL / HIGH / MEDIUM)
generated_at      TIMESTAMP
```

---

### UC2: Returns Fraud Investigator

**Business owner:** Returns operations team (40–60 cases/day)  
**Reads from:** `globalmart.gold.fact_returns` + `globalmart.gold.dim_customers`  
**Writes to:** `globalmart.gold.flagged_return_customers`

**Design principle: Rules DETECT — LLM EXPLAINS.**

The LLM is not a classifier. Rule-based thresholds are reliable and auditable for structured return data. The LLM generates the investigation brief after a customer has already been flagged by the scoring system.

**Anomaly scoring system (8 rules, weighted 0–100):**

| Rule | Signal | Weight |
|------|--------|--------|
| `rule_no_matching_order` | Return with no corresponding order in fact_orders | Highest — direct fraud evidence |
| `rule_high_return_frequency` | Return count significantly above segment average | High |
| `rule_cross_region_returns` | Returns submitted across multiple regional systems | High |
| `rule_refund_exceeds_sale` | Refund amount exceeds original sale price | High |
| `rule_timing_anomaly` | Return submitted within hours of delivery | Medium |
| `rule_high_return_value` | Aggregate return value above risk threshold | Medium |
| `rule_return_rate` | Return rate above 60% of orders placed | Medium |
| `rule_repeated_category` | Repeated returns of same product category | Low |

Customers scoring at or above 40 are flagged for investigation. Threshold chosen to keep the returns team's daily queue manageable.

**AI investigation brief for each flagged customer answers:**
- What specific patterns make this case suspicious (citing actual data points)
- What innocent explanations exist alongside the fraud indicators
- What the returns team should verify first (specific, actionable steps)

**Output table schema:**
```
customer_id             STRING
customer_name           STRING
total_returns           INTEGER
total_return_value      DECIMAL(10,2)
anomaly_score           INTEGER        (0-100)
rules_violated          STRING         (comma-separated list)
ai_investigation_brief  STRING         (3-4 sentence structured brief)
flagged_at              TIMESTAMP
```

---

### UC3: Product Intelligence Assistant

**Business owner:** Merchandising team  
**Reads from:** `globalmart.gold.dim_products` + `globalmart.gold.dim_vendors` + Materialized Views  
**Writes to:** `globalmart.gold.rag_query_history`

The merchandising team asks the same questions every week. Every question currently requires a data analyst to write a query. UC3 eliminates that dependency with two complementary modes.

**Part A — RAG Product Q&A (Code)**

`dim_products` is a structured table. For RAG to work, it must be converted into natural language documents that carry semantic meaning.

```
Step 1 — Enrichment: load dim_products + dim_vendors, join with MVs
          Adds: slow_moving flag, return_rate_pct, days_since_last_sale, top_selling_region

Step 2 — Document synthesis: convert each enriched row to a natural language paragraph
          Example: "Product X, brand Y, category Z, supplied by Vendor A.
                    Currently slow-moving in the West region (32 days since last sale).
                    Vendor A has a 34% return rate. Top-selling region: East."

Step 3 — Embedding: sentence-transformers/all-MiniLM-L6-v2 (local, no external API)
          Vectors stored in FAISS index on Databricks Volume

Step 4 — Query serving: user query -> embed -> semantic search -> Top-5 documents
          Top-5 docs passed to databricks-gpt-oss-20b
          Instruction: answer ONLY from retrieved documents. If not in index, say so explicitly.

Step 5 — Validation: if answer is too brief or ungrounded -> standard refusal message
Step 6 — Log to globalmart.gold.rag_query_history: question, answer, retrieved_docs, distances, timestamp
```

Demonstrated test queries:
- "Which products are slow-moving in the West region?"
- "Which vendor has the highest return rate?"
- "What are our top-selling categories in the East?"
- "Which products supplied by Vendor B are underperforming?"
- "Show me fast-moving products with low return rates"

**Part B — Genie NLQ (Databricks UI)**

A Genie Space configured over the Gold layer for SQL-style analytical questions.

Tables in Genie Space:
```
fact_orders  fact_returns  dim_products  dim_customers  dim_vendors
mv_revenue_by_region  mv_return_rate_by_vendor  mv_slow_moving_products
```

Context instructions embedded in the Genie Space describe the schema, key metrics, and how to interpret slow-moving and return rate columns — enabling reliable NLQ without requiring business users to understand the data model.

---

### UC4: Executive Business Intelligence

**Business owner:** Executive leadership  
**Reads from:** All Gold Materialized Views  
**Writes to:** `globalmart.gold.ai_business_insights`

A dashboard shows that the West region has a 22% return rate and $1.4M in slow-moving inventory. It does not tell you whether that is a vendor quality problem, a pricing issue, or a merchandising failure. UC4 generates that narrative automatically.

**Three synthesis domains (run in parallel):**

1. **Revenue Performance:** Month-over-month deltas and margins by region. LLM labels each trend: `ALARMING` / `EXPECTED` / `OPPORTUNITY`. Includes a 30-day actionable recommendation.

2. **Vendor Risk Profiling:** Benchmarks each vendor against the 40% return rate threshold. Diagnoses whether the issue is a quality failure or a fulfilment defect. Produces a risk tier per vendor.

3. **Inventory Assessment:** Identifies the region with the highest financial exposure from slow-moving stock. Explains the cost of inaction: markdown risk, tied-up capital, missed redistribution window.

**`ai_query()` integration:**

UC4 also demonstrates Databricks SQL-native `ai_query()` — calling Foundation Models directly inside a SQL query:

```sql
SELECT
  region,
  total_sales,
  get_json_object(
    get_json_object(
      ai_query('databricks-gpt-oss-20b',
        CONCAT('Assess this regional revenue: ', to_json(struct(region, total_sales, mom_change)))
      ), '$[1]'
    ), '$.text'
  ) AS ai_assessment
FROM globalmart.gold.mv_revenue_by_region
```

Raw table rows are never passed to the LLM. All data is aggregated first and summary statistics are passed as JSON. A prompt with 5 KPIs produces a better result than a prompt with 5,000 raw rows — and it will not hit context limits.

**Output table schema:**
```
insight_type    STRING    (revenue_performance, vendor_return_rate, slow_moving_inventory)
ai_summary      STRING    (4-6 sentence executive narrative)
kpi_data_json   STRING    (the aggregated KPIs passed to the LLM — for auditability)
generated_at    TIMESTAMP
```

---

## Governance and Observability

**Unity Catalog** governs the entire platform as a single governance plane:

- 3-level namespace: `globalmart.{layer}.{table}`
- Automatic column-level lineage: every column traceable from source file through Bronze → Silver → Gold
- Row-level permissions per business team on Gold tables
- Finance team can read Gold layer but cannot access Bronze or Silver raw data
- Asset discovery: any team member can find what tables exist and when they were last loaded

**Observability tables:**

```
globalmart.ingestion_control.pipeline_run_log
  notebook  status (SUCCESS/FAIL)  run_timestamp  records_processed
  records_quarantined  error_message  duration_seconds

globalmart.bronze.schema_validation_bronze
  entity  file_path  issue_type  issue_detail  validation_timestamp
```

**New region onboarding (zero code change):**
1. Upload source files to `/Volumes/globalmart/raw/source_files/region7/`
2. Insert one row into `ingestion_control.source_registry`
3. Add column mappings to `ingestion_control.column_mapping` if needed
4. No pipeline notebook changes required

---

## Repository Structure

```
globalmart-data-intelligence/
|
+-- README.md
|
+-- architecture/
|   +-- Hackathon_drawio.xml              # Full architecture diagram (12 pages)
|   +-- GlobalMartDataDictionary.xlsx     # Field-level data dictionary
|   +-- dimensional_model.png            # Star schema ERD
|
+-- data/
|   +-- customers/   customers_1.csv ... customers_6.csv
|   +-- orders/      orders_1.csv ... orders_3.csv
|   +-- transactions/ transactions_1.csv ... transactions_3.csv
|   +-- returns/     returns_1.json  returns_2.json
|   +-- products/    products.json
|   +-- vendors/     vendors.csv
|
+-- notebooks/
|   +-- 00_setup/
|   |   +-- 00_create_catalog_and_schemas.py
|   |   +-- 01_upload_source_files_to_volumes.py
|   |   +-- 02_seed_ingestion_control_tables.py
|   |
|   +-- 01_bronze/
|   |   +-- bronze_pipeline.py
|   |   +-- pre_bronze_validation.py
|   |
|   +-- 02_silver/
|   |   +-- silver_pipeline.py
|   |   +-- silver_data_quality_rules.md
|   |
|   +-- 03_gold/
|   |   +-- gold_dimensions.py
|   |   +-- gold_facts.py
|   |   +-- gold_materialized_views.py
|   |
|   +-- 04_genai/
|       +-- uc1_dq_reporter.py
|       +-- uc2_fraud_investigator.py
|       +-- uc3_product_assistant.py
|       +-- uc4_executive_insights.py
|
+-- docs/
    +-- architecture_decisions.md
    +-- data_quality_strategy.md
    +-- gen_ai_design_notes.md
```

---

## Getting Started

### Prerequisites

- Databricks Free Edition workspace
- Git integration configured in your Databricks workspace
- No external API keys required — all AI capabilities use Databricks Foundation Models (free tier)

### Step-by-Step Setup

**Step 1 — Clone the repository**

In Databricks: Repos → Add Repo → paste the GitHub URL.

**Step 2 — Run setup notebooks in order**

```
notebooks/00_setup/00_create_catalog_and_schemas.py
notebooks/00_setup/01_upload_source_files_to_volumes.py
notebooks/00_setup/02_seed_ingestion_control_tables.py
```

These create the catalog structure and upload all 16 source files to `/Volumes/globalmart/raw/source_files/` with the regional folder structure intact.

**Step 3 — Bronze layer**

```
notebooks/01_bronze/pre_bronze_validation.py    # validate contracts first
notebooks/01_bronze/bronze_pipeline.py          # ingest all 6 entities
```

Verify: `globalmart.bronze.*` — 6 tables, `_source_file` and `_load_timestamp` populated on every row.

**Step 4 — Silver layer**

```
notebooks/02_silver/silver_pipeline.py
```

Verify: `globalmart.silver.*` — 6 clean tables + 6 quarantine tables. Quarantine tables populated with `_failure_reason` values.

**Step 5 — Gold layer**

```
notebooks/03_gold/gold_dimensions.py
notebooks/03_gold/gold_facts.py
notebooks/03_gold/gold_materialized_views.py
```

Verify: star schema populated, `mv_revenue_by_region` shows consolidated revenue across all six regions.

**Step 6 — Gen AI layer**

```
notebooks/04_genai/uc1_dq_reporter.py
notebooks/04_genai/uc2_fraud_investigator.py
notebooks/04_genai/uc3_product_assistant.py
notebooks/04_genai/uc4_executive_insights.py
```

Each notebook runs end-to-end without errors when cells are executed in sequence.

Verify: `dq_audit_report`, `flagged_return_customers`, `rag_query_history`, and `ai_business_insights` all contain rows with AI-generated content.

**Step 7 — Genie Space (Part B of UC3)**

Databricks: AI/BI → Genie → New Space. Add Gold tables per the UC3 section above. Paste the context instructions from `notebooks/04_genai/uc3_product_assistant.py`. Test with natural language queries.

---

## Platform Constraints and Design Decisions

**Why Spark Declarative Pipelines over plain notebooks?**

Quality rules declared as EXPECT clauses inside the pipeline definition are enforced automatically on every run and cannot be accidentally bypassed. Rules in post-processing notebooks can be skipped. Rules in the pipeline definition cannot. This was a hard architectural requirement.

**Why config-driven ingestion instead of hardcoded paths?**

With hardcoded paths, adding a new region requires modifying and retesting pipeline code. With `source_registry`, adding a region is a data operation — one INSERT row, zero code changes. The same applies to column mappings and quality rules.

**Why rules-based detection for fraud, not LLM detection?**

The LLM is not a reliable classifier for structured numerical data. A rule that says "return rate > 60%" is deterministic, auditable, and consistent across every run. An LLM asked to decide if a return history is suspicious will produce different results across runs and cannot be audited by the finance team. Rules detect. The LLM explains.

**Why FAISS for vector search instead of Databricks Vector Search?**

Databricks Vector Search requires a Pro or Enterprise tier workspace. FAISS runs locally on the notebook cluster at no cost and is sufficient for a product catalogue of this size. The index is stored on a Databricks Volume so it persists across sessions. For production at scale, Databricks Vector Search is the appropriate upgrade path.

**Why aggregate before passing data to the LLM?**

Passing 5,000 raw rows to a 128K context window model produces worse results than passing 5 pre-aggregated KPIs. Aggregation forces the model to reason about the business question rather than pattern-match individual rows. It also prevents hitting context limits and produces more consistent, reproducible outputs.

**Why write AI outputs back to Delta tables in Gold?**

AI outputs stored in Delta tables are versioned, queryable via SQL, auditable by time travel, and accessible to any downstream consumer. Outputs stored only in notebook cells die when the notebook session ends. Making AI outputs first-class Gold layer citizens is what makes the intelligence layer durable.

---

*This README covers the full end-to-end architecture of the GlobalMart Data Intelligence Platform. Every component described corresponds to working, runnable notebook code in this repository. The platform was built to prove that six disconnected regional systems can be unified, quality-enforced, and made AI-accessible — automatically, without manual intervention.*
