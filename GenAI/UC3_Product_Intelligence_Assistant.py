# Databricks notebook source
# MAGIC %md
# MAGIC # UC3 — Product Intelligence Assistant (RAG)
# MAGIC
# MAGIC **Purpose:** Eliminate the analyst bottleneck for routine merchandising questions.
# MAGIC Every row in `dim_products` and `dim_vendors` is converted to a natural language
# MAGIC document, embedded locally, and stored in a FAISS vector index. When a user asks
# MAGIC a question, the most relevant documents are retrieved and passed to the LLM — which
# MAGIC must answer only from what was retrieved.
# MAGIC
# MAGIC **Output table:** `globalmart.gold.rag_query_history`
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. Install dependencies (run once, then restart Python)
# MAGIC 2. Setup — imports, config, LLM client
# MAGIC 3. Load Gold tables and enrich products with MV metrics
# MAGIC 4. Convert each product row into a natural language document
# MAGIC 5. Build or load the FAISS vector index
# MAGIC 6. Define the RAG query pipeline
# MAGIC 7. Run 7 test questions and review answers
# MAGIC 8. Write query history to Gold layer
# MAGIC 9. Log the run
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Install dependencies
# MAGIC
# MAGIC Run this cell once, then restart Python before continuing.

# COMMAND ----------

# MAGIC %pip install sentence-transformers faiss-cpu openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Imports and configuration

# COMMAND ----------

import json
import time
import numpy as np
import faiss
from datetime import datetime
from sentence_transformers import SentenceTransformer
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from openai import OpenAI

# ── LLM connection ────────────────────────────────────────────────────────
client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    base_url="https://7474644504640858.ai-gateway.cloud.databricks.com/mlflow/v1/"
)
MODEL_NAME    = "databricks-gpt-oss-20b"
EMBED_MODEL   = "all-MiniLM-L6-v2"          # local embeddings — no API cost
NOTEBOOK_NAME = "UC3_Product_Intelligence_Assistant"

# ── Source tables ─────────────────────────────────────────────────────────
TABLE_PRODUCTS       = "globalmart.gold.dim_products"
TABLE_VENDORS        = "globalmart.gold.dim_vendors"
TABLE_SLOW_MOVING    = "globalmart.gold.mv_slow_moving_products"
TABLE_RETURN_RATES   = "globalmart.gold.mv_return_rate_by_vendor"
TABLE_REVENUE_REGION = "globalmart.gold.mv_revenue_by_region"  # NEW: regional revenue context

# ── Output tables ─────────────────────────────────────────────────────────
TABLE_QUERY_HISTORY = "globalmart.gold.rag_query_history"
TABLE_RUN_LOG       = "globalmart.config.pipeline_run_log"

# ── FAISS index persistence paths (Databricks Volume) ────────────────────
INDEX_PATH   = "/Volumes/globalmart/gold/rag_artifacts/product_index.faiss"
DOCS_PATH    = "/Volumes/globalmart/gold/rag_artifacts/product_docs.json"
VERSION_PATH = "/Volumes/globalmart/gold/rag_artifacts/index_version.json"

spark.sql("CREATE VOLUME IF NOT EXISTS globalmart.gold.rag_artifacts")

print(f"Setup complete | Model: {MODEL_NAME} | Embed: {EMBED_MODEL} | Started: {datetime.now():%Y-%m-%d %H:%M:%S}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — LLM helper functions

# COMMAND ----------

def extract_text(response) -> str:
    """
    Pull the answer text from the model response.
    databricks-gpt-oss-20b returns a list of typed content blocks.
    We only want the "text" block.
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
    """Call the LLM with automatic retry on failure."""
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.1
            )
            return extract_text(response)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(wait * (attempt + 1))
            else:
                return f"LLM_UNAVAILABLE: {e}"


def validate_llm_output(text: str, min_words: int = 10) -> dict:
    """
    Check the LLM output before writing to the query history table.
    Note: 'I could not find that information' is a VALID RAG response
    when the answer is genuinely not in the retrieved documents.
    """
    refusal_phrases = ["i cannot", "as an ai", "i don't have", "i'm unable", "i am unable"]
    issues = []
    if len(text.split()) < min_words:
        issues.append("too_short")
    if text.startswith("LLM_UNAVAILABLE"):
        issues.append("llm_call_failed")
    if any(p in text.lower() for p in refusal_phrases):
        issues.append("possible_refusal")
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
# MAGIC ## Step 4 — Load Gold tables and enrich products
# MAGIC
# MAGIC RAG requires natural language documents, not structured rows. Before embedding,
# MAGIC each product is enriched with vendor and performance data from the Materialized Views
# MAGIC so a single document can answer questions about inventory, vendor quality, and regional revenue.
# MAGIC
# MAGIC **Tables used:**
# MAGIC - `dim_products` — product_id, product_name, brand, categories, manufacturer
# MAGIC - `dim_vendors` — vendor_id, vendor_name
# MAGIC - `mv_slow_moving_products` — days_since_last_sale, slow regions per product
# MAGIC - `mv_return_rate_by_vendor` — vendor return rate and total returns
# MAGIC - `mv_revenue_by_region` *(NEW)* — total_sales, total_profit, order_count per region per period
# MAGIC
# MAGIC The `mv_revenue_by_region` table has one row per region per month (88 rows total). We aggregate it to one row per region (sum of sales, sum of profit, total orders) so it can be joined into each product document as a regional revenue context line.

# COMMAND ----------

df_products = spark.table(TABLE_PRODUCTS)
df_vendors  = spark.table(TABLE_VENDORS)

try:
    df_slow_moving  = spark.table(TABLE_SLOW_MOVING)
    df_return_rates = spark.table(TABLE_RETURN_RATES)
    print("Materialized views loaded.")
except Exception as e:
    print(f"MV load warning: {e} — proceeding with base tables only.")
    df_slow_moving  = spark.createDataFrame([], "product_id string, region string, days_since_last_sale int")
    df_return_rates = spark.createDataFrame([], "vendor_id string, return_rate_pct double")

# ── Load mv_revenue_by_region and aggregate to one row per region ─────────
# The table has 88 rows (one per region per month). We collapse to one row
# per region so we can embed a total revenue figure per region in each document.
try:
    df_revenue_region = (
        spark.table(TABLE_REVENUE_REGION)
        .groupBy("region")
        .agg(
            F.round(F.sum("total_sales"),  2).alias("region_total_sales"),
            F.round(F.sum("total_profit"), 2).alias("region_total_profit"),
            F.sum("order_count")            .alias("region_order_count"),
        )
    )
    # Normalise region abbreviations to match the slow_moving regions
    region_map_expr = (
        F.when(F.col("region") == "W", "West")
         .when(F.col("region") == "S", "South")
         .when(F.col("region") == "E", "East")
         .when(F.col("region") == "N", "North")
         .otherwise(F.col("region"))
    )
    df_revenue_region = df_revenue_region.withColumn("region", region_map_expr)
    print(f"Revenue by region loaded: {df_revenue_region.count()} regions")
    display(df_revenue_region)
except Exception as e:
    print(f"Revenue region load warning: {e} — proceeding without regional revenue context.")
    df_revenue_region = spark.createDataFrame(
        [], "region string, region_total_sales double, region_total_profit double, region_order_count long"
    )

# Convert to a Python dict keyed by region for fast lookup in document builder
revenue_by_region = {
    row["region"]: {
        "sales":  row["region_total_sales"],
        "profit": row["region_total_profit"],
        "orders": row["region_order_count"],
    }
    for row in df_revenue_region.collect()
}
print(f"Revenue context available for regions: {list(revenue_by_region.keys())}")


# ── dim_products has no vendor_id — derive it from fact_sales ────────────
df_product_vendor = (
    spark.table("globalmart.gold.fact_sales")
    .select("product_id", "vendor_id")
    .groupBy("product_id", "vendor_id")
    .count()
    .withColumn("rank", F.row_number().over(
        __import__('pyspark.sql.window', fromlist=['Window'])
        .Window.partitionBy("product_id").orderBy(F.col("count").desc())
    ))
    .filter(F.col("rank") == 1)
    .select("product_id", "vendor_id")
)

# ── Aggregate slow_moving to one row per product ──────────────────────────
df_slow_agg = (
    df_slow_moving
    .groupBy("product_id")
    .agg(
        F.max("days_since_last_sale").alias("max_days_since_last_sale"),
        F.collect_set("region")      .alias("slow_regions"),
        F.sum("total_quantity_sold") .alias("total_quantity_sold"),
    )
)

# ── Join all sources into one enriched product table ─────────────────────
df_enriched = (
    df_products
    .join(df_product_vendor, on="product_id", how="left")
    .join(df_vendors.select("vendor_id", "vendor_name"), on="vendor_id", how="left")
    .join(df_slow_agg, on="product_id", how="left")
    .join(
        df_return_rates.select("vendor_id", "return_rate_pct", "total_returns", "total_refund_amount"),
        on="vendor_id", how="left"
    )
    .fillna({
        "vendor_name":             "Unknown Vendor",
        "max_days_since_last_sale": 0,
        "total_quantity_sold":      0,
        "return_rate_pct":          0.0,
        "total_returns":            0,
    })
    .toPandas()
)

print(f"Enriched product records: {len(df_enriched):,}")
print(f"Columns: {list(df_enriched.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Convert product rows to natural language documents
# MAGIC
# MAGIC FAISS indexes dense vector embeddings — it cannot embed a structured row directly.
# MAGIC Each row must become a sentence that a language model can read and embed.
# MAGIC
# MAGIC **Why every attribute matters:**
# MAGIC - Omit `brand` → questions like 'show me Nike products' retrieve wrong results
# MAGIC - Omit `slow_regions` → 'slow-moving in the West' matches nothing
# MAGIC - Omit `return_rate_pct` → 'highest return rate vendor' retrieves randomly
# MAGIC - Omit `vendor_name` → vendor questions are unanswerable from the document
# MAGIC - Omit `revenue context` → regional revenue questions return no useful answer
# MAGIC
# MAGIC The `mv_revenue_by_region` data is added as a sentence at the end of each document for every region the product is available in, so questions about regional revenue performance can retrieve relevant documents.

# COMMAND ----------

def safe(val, default="N/A") -> str:
    """Return val as a string, or default if null/empty/nan."""
    return str(val) if val is not None and str(val) not in ("nan", "None", "", "0") else default


def row_to_document(row) -> str:
    """
    Convert one enriched product row into a natural language document.

    The document covers every dimension a merchandising analyst might ask about:
    product identity, category, vendor, inventory velocity, vendor return performance,
    region availability, and regional revenue context from mv_revenue_by_region.
    """
    # ── Slow-moving context ───────────────────────────────────────────────
    days = row.get("max_days_since_last_sale", 0)
    try:
        days = int(float(days))
    except Exception:
        days = 0

    slow_regions = row.get("slow_regions", [])
    if slow_regions is not None:
        try:
            region_map   = {"W": "West", "S": "South", "E": "East", "N": "North"}
            slow_regions = [
                region_map.get(str(r).strip(), str(r).strip())
                for r in list(slow_regions)
                if r and str(r) not in ("nan", "None", "")
            ]
        except Exception:
            slow_regions = []

    if slow_regions:
        region_str = ", ".join(sorted(slow_regions))
        slow_note  = f"This product is slow-moving in the {region_str} region(s) with {days} days since last sale."
    elif days > 90:
        slow_note  = f"This product has not sold in {days} days and is considered slow-moving."
    else:
        slow_note  = "This product is selling normally."

    # ── Vendor return rate context ─────────────────────────────────────────
    rate = row.get("return_rate_pct", 0)
    try:
        rate_str = f"{round(float(rate), 1)}%"
    except Exception:
        rate_str = "N/A"
    total_returns = safe(row.get("total_returns"))

    # ── Region availability line ───────────────────────────────────────────
    availability = (
        f"This product is available in the following regions: {', '.join(sorted(slow_regions))}."
        if slow_regions else "Region availability data not available."
    )

    # ── Regional revenue context from mv_revenue_by_region ────────────────
    # For each region this product is available in, append the total revenue
    # and profit figures for that region so revenue-based questions can retrieve
    # relevant documents.
    revenue_lines = []
    for region in sorted(slow_regions):
        rev = revenue_by_region.get(region)
        if rev:
            revenue_lines.append(
                f"The {region} region generated total sales of ${rev['sales']:,.2f} "
                f"and total profit of ${rev['profit']:,.2f} across {rev['orders']:,} orders."
            )
    revenue_context = (" ".join(revenue_lines)
                       if revenue_lines
                       else "Regional revenue data not available for this product.")

    return (
        f"Product ID {safe(row.get('product_id'))} — {safe(row.get('product_name'))}. "
        f"Brand: {safe(row.get('brand'))}. "
        f"Category: {safe(row.get('categories'))}. "
        f"Manufacturer: {safe(row.get('manufacturer'))}. "
        f"Supplied by {safe(row.get('vendor_name'))} (vendor ID: {safe(row.get('vendor_id'))}). "
        f"{slow_note} "
        f"Vendor {safe(row.get('vendor_name'))} has a return rate of {rate_str} "
        f"across {total_returns} total returns. "
        f"{availability} "
        f"{revenue_context}"
    )


documents = []
for _, row in df_enriched.iterrows():
    documents.append(row_to_document(row))

print(f"Created {len(documents):,} product documents.")
print(f"\nSample document:\n{documents[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Build or load the FAISS vector index
# MAGIC
# MAGIC FAISS stores dense vector embeddings and retrieves the nearest neighbours for a query vector in milliseconds. The index is persisted to a Databricks Volume so it survives cluster restarts. On subsequent runs it is loaded from disk rather than rebuilt, unless the product count has changed.
# MAGIC
# MAGIC **Important:** Delete the index files and rerun this step whenever the document content changes (e.g. after adding the revenue context above).

# COMMAND ----------

embed_model = SentenceTransformer(EMBED_MODEL)
print(f"Embedding model loaded: {EMBED_MODEL}")


def build_and_save_index(docs: list) -> faiss.Index:
    """Embed all documents, build FAISS index, persist to Volume."""
    print(f"Building FAISS index for {len(docs):,} documents...")
    embeddings = embed_model.encode(docs, show_progress_bar=True)
    dimension  = embeddings.shape[1]
    index      = faiss.IndexFlatL2(dimension)
    index.add(np.array(embeddings, dtype=np.float32))

    faiss.write_index(index, INDEX_PATH)
    with open(DOCS_PATH, "w") as f:
        json.dump(docs, f)
    with open(VERSION_PATH, "w") as f:
        json.dump({"doc_count": len(docs), "built_at": datetime.now().isoformat()}, f)

    print(f"Index saved — {index.ntotal:,} vectors | dimension: {dimension}")
    return index


def load_index() -> tuple:
    """Load persisted FAISS index and documents from Volume."""
    index = faiss.read_index(INDEX_PATH)
    with open(DOCS_PATH, "r") as f:
        docs = json.load(f)
    print(f"Index loaded from Volume — {index.ntotal:,} vectors.")
    return index, docs


rebuild_needed = True
try:
    with open(VERSION_PATH, "r") as f:
        version = json.load(f)
    if version.get("doc_count") == len(documents):
        rebuild_needed = False
        print(f"Index is current ({len(documents):,} docs). Loading from Volume.")
    else:
        print(f"Product count changed ({version.get('doc_count')} → {len(documents)}). Rebuilding.")
except Exception:
    print("No existing index found. Building fresh.")

if rebuild_needed:
    index = build_and_save_index(documents)
else:
    index, documents = load_index()

print(f"\nIndex ready — {index.ntotal:,} vectors indexed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Define the RAG query pipeline
# MAGIC
# MAGIC Each query goes through four steps:
# MAGIC 1. **Embed** the question using the same model used to build the index
# MAGIC 2. **Retrieve** the top-k most similar documents from FAISS
# MAGIC 3. **Prompt** the LLM with the retrieved documents as context
# MAGIC 4. **Return** the answer plus the retrieved documents for transparency
# MAGIC
# MAGIC The LLM is instructed to answer **only from the retrieved documents**. If the answer is not in the index, it must say so explicitly.

# COMMAND ----------

SYSTEM_MSG_RAG = (
    "You are a product intelligence assistant for GlobalMart, a national retail chain. "
    "Answer questions using ONLY the product documents provided. "
    "If the answer is not in the documents, say exactly: "
    "'I could not find that information in the current product catalog.' "
    "Never invent product names, vendor names, categories, or return rates. "
    "Always mention specific product IDs, vendor names, and figures from the documents. "
    "Write in plain prose only — no markdown, no bullet points, no bold text, no tables."
)


def rag_query(question: str, top_k: int = 5) -> dict:
    """
    Run a full RAG query:
      1. Embed the question
      2. Retrieve top_k most relevant product documents from FAISS
      3. Build a grounded prompt with the retrieved context
      4. Call the LLM — answer must come only from retrieved docs
      5. Return the answer and retrieved documents for logging
    """
    q_vector = np.array(embed_model.encode([question]), dtype=np.float32)
    distances, indices = index.search(q_vector, top_k)
    retrieved_docs = [documents[i] for i in indices[0]]

    context = "\n".join(f"- {doc}" for doc in retrieved_docs)
    prompt  = (
        "Answer the question below using ONLY the product documents provided.\n"
        "If the answer is not in the documents, say so explicitly.\n\n"
        f"PRODUCT DOCUMENTS:\n{context}\n\n"
        f"QUESTION: {question}\n\n"
        "Answer in plain English. Name specific product IDs, vendor names, "
        "categories, regions, revenue figures, and return rates from the documents."
    )

    answer = call_llm(prompt, SYSTEM_MSG_RAG)

    return {
        "question":       question,
        "answer":         answer,
        "retrieved_docs": retrieved_docs,
        "distances":      [round(float(d), 4) for d in distances[0]],
    }


print("RAG query function ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Run 7 test questions
# MAGIC
# MAGIC Seven questions covering the required hackathon query types plus two additional questions designed to test the boundaries of the RAG system:
# MAGIC
# MAGIC **Questions 1–5** — core coverage: slow-moving lookup, vendor return rate, region availability, region slow-moving, cross-dimension semantic
# MAGIC
# MAGIC **Question 6** — revenue context from `mv_revenue_by_region`: tests whether regional revenue figures embedded in documents are retrieved correctly
# MAGIC
# MAGIC **Question 7** — out-of-scope question: tests that the system correctly says 'I could not find that information' rather than hallucinating an answer when the question cannot be answered from the product catalog

# COMMAND ----------

test_questions = [
    # 1. Slow-moving product lookup (required)
    "Which products have been slow-moving for the longest time?",

    # 2. Vendor return rate comparison (required)
    "Which vendor has the highest return rate?",

    # 3. Region-specific product availability (required)
    "Which products are available in the West region?",

    # 4. Region-specific slow-moving lookup (required)
    "Which products are slow-moving in the East region?",

    # 5. Cross-dimension semantic query (required)
    "Which vendor supplies slow-moving products and also has a high return rate?",

    # 6. Regional revenue question — tests mv_revenue_by_region context in docs
    # Expected: should cite region total sales and profit figures from the documents
    "Which region has the highest total sales and what products are available there?",

    # 7. Out-of-scope question — tests graceful fallback
    # Expected: should say 'I could not find that information' since customer
    # data is not in the product catalog index
    "Which customers have purchased the most products this year?",
]

query_log      = []
llm_call_count = 0
review_count   = 0

for question in test_questions:
    result    = rag_query(question, top_k=5)
    validated = validate_llm_output(result["answer"])
    llm_call_count += 1

    if validated["llm_check"] == "REVIEW":
        review_count += 1

    print("=" * 70)
    print(f"QUESTION : {question}")
    print("-" * 70)
    print("RETRIEVED DOCUMENTS:")
    for i, (doc, dist) in enumerate(zip(result["retrieved_docs"], result["distances"]), 1):
        print(f"  {i}. [distance = {dist}]")
        print(f"     {doc[:200]}...")
    print("-" * 70)
    if validated["llm_check"] == "REVIEW":
        print(f"*** LLM CHECK FLAG: {validated['issues']} ***")
    print(f"ANSWER:\n{validated['text']}")
    print("=" * 70 + "\n")

    query_log.append({
        "question":         question,
        "answer":           validated["text"],
        "retrieved_docs":   result["retrieved_docs"],
        "distances":        str(result["distances"]),
        "llm_check":        validated["llm_check"],
        "llm_check_detail": validated["issues"],
        "queried_at":       datetime.now().isoformat()
    })

print(f"Done. {len(query_log)} questions answered.")
print(f"LLM calls: {llm_call_count}  |  Flagged for review: {review_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Write query history to Gold layer
# MAGIC
# MAGIC `retrieved_docs` is stored as an array of strings — one entry per retrieved document. This is cleaner than splitting into separate columns and works regardless of `top_k`.

# COMMAND ----------

schema = StructType([
    StructField("question",         StringType(),            True),
    StructField("answer",           StringType(),            True),
    StructField("retrieved_docs",   ArrayType(StringType()), True),
    StructField("distances",        StringType(),            True),
    StructField("llm_check",        StringType(),            True),
    StructField("llm_check_detail", StringType(),            True),
    StructField("queried_at",       StringType(),            True),
])

df_query_log = spark.createDataFrame(query_log, schema=schema)

(
    df_query_log.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_QUERY_HISTORY)
)

print(f"Written to {TABLE_QUERY_HISTORY}")
print(f"Rows written this run: {df_query_log.count()}")
display(spark.table(TABLE_QUERY_HISTORY))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Log the run

# COMMAND ----------

run_log = [{
    "notebook":          NOTEBOOK_NAME,
    "run_timestamp":     datetime.now().isoformat(),
    "records_processed": len(documents),
    "llm_calls_made":    llm_call_count,
    "outputs_flagged":   review_count,
    "status":            "SUCCESS",
    "notes":             f"index_rebuilt={rebuild_needed} | docs_indexed={index.ntotal}"
}]

(
    spark.createDataFrame(run_log)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_RUN_LOG)
)

print(f"Run log written to {TABLE_RUN_LOG}")
print(f"Status         : SUCCESS")
print(f"Docs indexed   : {index.ntotal:,}")
print(f"Questions run  : {len(query_log)}")
print(f"LLM calls      : {llm_call_count}")
print(f"Flagged REVIEW : {review_count}")