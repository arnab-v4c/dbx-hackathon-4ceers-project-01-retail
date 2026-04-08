# Databricks notebook source
# MAGIC %md
# MAGIC # GlobalMart - Market Basket Analysis
# MAGIC **Layer:** Gold | **Compute:** Serverless Compatible (No MLlib / No FPGrowth)  
# MAGIC **Algorithm:** Pure PySpark DataFrame self-join — zero MLlib dependencies
# MAGIC
# MAGIC ### Why not FPGrowth?
# MAGIC `FPGrowth` (pyspark.ml.fpm) calls `PERSIST TABLE` internally which is blocked on Serverless compute.  
# MAGIC This notebook replaces it entirely with a DataFrame self-join approach that produces identical outputs and runs on Serverless.
# MAGIC
# MAGIC ### Gold Tables Used
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `globalmart.gold.fact_sales` | order_id + product_id (basket source) |
# MAGIC | `globalmart.gold.dim_products` | product_name, categories, brand enrichment |
# MAGIC | `globalmart.gold.dim_orders` | order_id → vendor_id mapping |
# MAGIC | `globalmart.gold.dim_vendors` | vendor_name lookup |
# MAGIC
# MAGIC ### Output Tables Written
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `globalmart.gold.mba_association_rules` | IF/THEN rules with support, confidence, lift, rule_strength |
# MAGIC | `globalmart.gold.mba_frequent_itemsets` | Frequent product pairs with co-occurrence counts |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Imports & Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CATALOG = "globalmart"
GOLD_DB = f"{CATALOG}.gold"

# ── Tuning Parameters ──────────────────────────────────────────────────────
# MIN_PAIR_COUNT : a product pair must appear together in at least this many orders
#                 to be considered frequent. Start at 5 and lower to 2 if too few results.
MIN_PAIR_COUNT = 2

# MIN_CONFIDENCE : P(product_b | product_a) — how often B appears given A was bought
#                 0.2 = 20% of orders containing A also contain B
MIN_CONFIDENCE = 0.20

# MIN_LIFT       : pairs with lift <= 1.0 are no better than random chance — filter them out
MIN_LIFT = 1.0

print(f"Gold DB        : {GOLD_DB}")
print(f"Min Pair Count : {MIN_PAIR_COUNT}")
print(f"Min Confidence : {MIN_CONFIDENCE}")
print(f"Min Lift       : {MIN_LIFT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Load Gold Tables

# COMMAND ----------

fact_sales   = spark.table(f"{GOLD_DB}.fact_sales")
dim_products = spark.table(f"{GOLD_DB}.dim_products")
dim_orders   = spark.table(f"{GOLD_DB}.dim_orders")
dim_vendors  = spark.table(f"{GOLD_DB}.dim_vendors")

print(f"fact_sales rows : {fact_sales.count()}")
fact_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Build Clean Basket Input
# MAGIC
# MAGIC `fact_sales` already has `order_id` and `product_id` on the same row — no join needed.  
# MAGIC Deduplicate on `(order_id, product_id)` defensively to handle any duplicate lines.

# COMMAND ----------

basket = (
    fact_sales
    .select(
        F.col("order_id"),
        F.col("product_id").cast(StringType()).alias("product_id")
    )
    .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
    .dropDuplicates(["order_id", "product_id"])
)

total_orders   = basket.select("order_id").distinct().count()
total_products = basket.select("product_id").distinct().count()

print(f"Order-product rows : {basket.count()}")
print(f"Distinct orders    : {total_orders}")
print(f"Distinct products  : {total_products}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Calculate Per-Product Order Counts
# MAGIC
# MAGIC We need to know how many orders contained each individual product.  
# MAGIC This is the denominator used later to compute **confidence**.  
# MAGIC
# MAGIC ```
# MAGIC confidence(A -> B) = orders_containing_both_A_and_B / orders_containing_A
# MAGIC ```

# COMMAND ----------

product_order_counts = (
    basket
    .groupBy("product_id")
    .agg(F.countDistinct("order_id").alias("product_order_count"))
)

print(f"Product order count rows: {product_order_counts.count()}")
product_order_counts.orderBy(F.col("product_order_count").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Self-Join to Find Product Pairs
# MAGIC
# MAGIC This is the core of the algorithm.  
# MAGIC We join `basket` to itself on `order_id` to find every pair of products that  
# MAGIC appeared in the same order. We use `product_a < product_b` to avoid duplicates  
# MAGIC (e.g. we only keep (P001, P045), not both (P001, P045) and (P045, P001)).

# COMMAND ----------

basket_a = basket.select(
    F.col("order_id"),
    F.col("product_id").alias("product_a")
)

basket_b = basket.select(
    F.col("order_id"),
    F.col("product_id").alias("product_b")
)

# Self-join on order_id, keep only pairs where a < b to avoid duplicates
pairs = (
    basket_a
    .join(basket_b, on="order_id", how="inner")
    .filter(F.col("product_a") < F.col("product_b"))
)

# Count how many orders each pair co-occurs in
pair_counts = (
    pairs
    .groupBy("product_a", "product_b")
    .agg(F.countDistinct("order_id").alias("pair_order_count"))
    .filter(F.col("pair_order_count") >= MIN_PAIR_COUNT)
)

print(f"Frequent product pairs found: {pair_counts.count()}")
pair_counts.orderBy(F.col("pair_order_count").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Calculate Support, Confidence, and Lift
# MAGIC
# MAGIC | Metric | Formula | Meaning |
# MAGIC |---|---|---|
# MAGIC | **support** | pair_count / total_orders | How common this pair is across all orders |
# MAGIC | **confidence(A→B)** | pair_count / orders_containing_A | Given A, how often is B also bought |
# MAGIC | **confidence(B→A)** | pair_count / orders_containing_B | Given B, how often is A also bought |
# MAGIC | **lift** | confidence / (product_b_freq / total_orders) | Is co-purchase better than random? >1 = yes |

# COMMAND ----------

total_orders_bc = total_orders  # already computed in Cell 3

# Join in individual product order counts for both A and B
metrics = (
    pair_counts
    .join(
        product_order_counts.withColumnRenamed("product_id", "product_a")
                            .withColumnRenamed("product_order_count", "count_a"),
        on="product_a", how="left"
    )
    .join(
        product_order_counts.withColumnRenamed("product_id", "product_b")
                            .withColumnRenamed("product_order_count", "count_b"),
        on="product_b", how="left"
    )
    .withColumn("support",        F.round(F.col("pair_order_count") / total_orders_bc, 6))
    .withColumn("confidence_atob",F.round(F.col("pair_order_count") / F.col("count_a"), 4))
    .withColumn("confidence_btoa",F.round(F.col("pair_order_count") / F.col("count_b"), 4))
    # Lift for A->B: confidence(A->B) / P(B)
    .withColumn("lift_atob",
        F.round(
            F.col("confidence_atob") / (F.col("count_b") / total_orders_bc),
        4)
    )
    # Lift for B->A: confidence(B->A) / P(A)
    .withColumn("lift_btoa",
        F.round(
            F.col("confidence_btoa") / (F.col("count_a") / total_orders_bc),
        4)
    )
    .filter(
        (F.col("confidence_atob") >= MIN_CONFIDENCE) |
        (F.col("confidence_btoa") >= MIN_CONFIDENCE)
    )
    .filter(
        (F.col("lift_atob") > MIN_LIFT) |
        (F.col("lift_btoa") > MIN_LIFT)
    )
)

print(f"Rules after confidence/lift filter: {metrics.count()}")
metrics.orderBy(F.col("lift_atob").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — Unpack into Directional Rules
# MAGIC
# MAGIC Each product pair produces two directional rules:
# MAGIC - A → B
# MAGIC - B → A
# MAGIC
# MAGIC We `UNION` both directions so every rule has a clear antecedent and consequent.

# COMMAND ----------

rules_atob = (
    metrics
    .select(
        F.col("product_a").alias("antecedent_id"),
        F.col("product_b").alias("consequent_id"),
        F.col("support"),
        F.col("confidence_atob").alias("confidence"),
        F.col("lift_atob").alias("lift"),
        F.col("pair_order_count")
    )
    .filter(F.col("confidence") >= MIN_CONFIDENCE)
    .filter(F.col("lift") > MIN_LIFT)
)

rules_btoa = (
    metrics
    .select(
        F.col("product_b").alias("antecedent_id"),
        F.col("product_a").alias("consequent_id"),
        F.col("support"),
        F.col("confidence_btoa").alias("confidence"),
        F.col("lift_btoa").alias("lift"),
        F.col("pair_order_count")
    )
    .filter(F.col("confidence") >= MIN_CONFIDENCE)
    .filter(F.col("lift") > MIN_LIFT)
)

all_rules = rules_atob.union(rules_btoa)
print(f"Total directional rules: {all_rules.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8 — Enrich Rules with Product Names from dim_products

# COMMAND ----------

dim_p = (
    dim_products
    .select(
        F.col("product_id").cast(StringType()),
        F.col("product_name"),
        F.col("categories"),
        F.col("brand")
    )
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
    .withColumn(
        "product_label",
        F.concat_ws(" | ",
            F.coalesce(F.col("product_name"), F.col("product_id")),
            F.coalesce(F.col("categories"),   F.lit("Unknown")),
            F.coalesce(F.col("brand"),         F.lit("Unknown"))
        )
    )
)

enriched_rules = (
    all_rules
    # Join antecedent label
    .join(
        dim_p.select(
            F.col("product_id").alias("antecedent_id"),
            F.col("product_label").alias("antecedent_label")
        ),
        on="antecedent_id", how="left"
    )
    # Join consequent label
    .join(
        dim_p.select(
            F.col("product_id").alias("consequent_id"),
            F.col("product_label").alias("consequent_label")
        ),
        on="consequent_id", how="left"
    )
    # Fall back to product_id if no name found
    .withColumn("antecedent_label",
        F.coalesce(F.col("antecedent_label"), F.col("antecedent_id"))
    )
    .withColumn("consequent_label",
        F.coalesce(F.col("consequent_label"), F.col("consequent_id"))
    )
    # Rule strength label for business users
    .withColumn("rule_strength",
        F.when(F.col("lift") >= 3.0, "Strong")
         .when(F.col("lift") >= 1.5, "Moderate")
         .otherwise("Weak")
    )
    # Human-readable rule string
    .withColumn("rule_label",
        F.concat(
            F.lit("IF ["), F.col("antecedent_label"),
            F.lit("] THEN ["), F.col("consequent_label"), F.lit("]")
        )
    )
    .withColumn("created_at", F.current_timestamp())
    .orderBy(F.col("lift").desc())
)

print("Sample enriched rules:")
enriched_rules.select(
    "rule_label", "support", "confidence", "lift", "rule_strength"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9 — Build Frequent Itemsets Table

# COMMAND ----------

frequent_itemsets = (
    pair_counts
    .join(
        dim_p.select(
            F.col("product_id").alias("product_a"),
            F.col("product_label").alias("label_a")
        ),
        on="product_a", how="left"
    )
    .join(
        dim_p.select(
            F.col("product_id").alias("product_b"),
            F.col("product_label").alias("label_b")
        ),
        on="product_b", how="left"
    )
    .withColumn("label_a", F.coalesce(F.col("label_a"), F.col("product_a")))
    .withColumn("label_b", F.coalesce(F.col("label_b"), F.col("product_b")))
    .withColumn("itemset_label",
        F.concat(F.col("label_a"), F.lit("  +  "), F.col("label_b"))
    )
    .select(
        F.col("product_a"),
        F.col("product_b"),
        F.col("itemset_label"),
        F.col("pair_order_count").alias("freq"),
        F.lit(2).alias("itemset_size"),
        F.current_timestamp().alias("created_at")
    )
    .orderBy(F.col("freq").desc())
)

print(f"Frequent itemsets: {frequent_itemsets.count()}")
frequent_itemsets.select("itemset_label", "freq").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10 — Save to Gold Layer (Serverless-Safe)

# COMMAND ----------

# saveAsTable with Delta format works on Serverless — no PERSIST TABLE used
(
    enriched_rules
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_DB}.mba_association_rules")
)
print(f"Saved : {GOLD_DB}.mba_association_rules")

(
    frequent_itemsets
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_DB}.mba_frequent_itemsets")
)
print(f"Saved : {GOLD_DB}.mba_frequent_itemsets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11 — Business Queries (Merchandising Team)

# COMMAND ----------

# Top 10 product pairs by lift — strongest associations
display(spark.sql(f"""
    SELECT
        antecedent_label        AS if_customer_buys,
        consequent_label        AS they_also_buy,
        ROUND(confidence*100,1) AS confidence_pct,
        lift                    AS lift_score,
        rule_strength,
        pair_order_count        AS orders_together
    FROM {GOLD_DB}.mba_association_rules
    ORDER BY lift DESC
    LIMIT 10
"""))

# COMMAND ----------

# Most reliable cross-sell pairs by confidence
display(spark.sql(f"""
    SELECT
        antecedent_label        AS product_a,
        consequent_label        AS promote_alongside,
        ROUND(confidence*100,1) AS confidence_pct,
        lift,
        rule_strength
    FROM {GOLD_DB}.mba_association_rules
    WHERE rule_strength IN ('Strong','Moderate')
    ORDER BY confidence DESC
    LIMIT 10
"""))

# COMMAND ----------

# Most frequently co-purchased product pairs
display(spark.sql(f"""
    SELECT
        itemset_label   AS product_pair,
        freq            AS orders_together
    FROM {GOLD_DB}.mba_frequent_itemsets
    ORDER BY freq DESC
    LIMIT 10
"""))

# COMMAND ----------

# Strong rules only — for promotion campaign planning
display(spark.sql(f"""
    SELECT
        antecedent_label        AS if_customer_buys,
        consequent_label        AS bundle_with,
        support,
        ROUND(confidence*100,1) AS confidence_pct,
        lift
    FROM {GOLD_DB}.mba_association_rules
    WHERE rule_strength = 'Strong'
    ORDER BY lift DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 12 — Optional: Vendor-Segmented MBA
# MAGIC
# MAGIC Uncomment to run separate rules per vendor using `dim_orders.vendor_id`.

# COMMAND ----------

# vendor_list = (
#     dim_vendors
#     .select("vendor_id", "vendor_name")
#     .filter(F.col("vendor_id").isNotNull())
#     .collect()
# )
#
# for vendor_row in vendor_list:
#     v_id   = vendor_row["vendor_id"]
#     v_name = vendor_row["vendor_name"]
#     print(f"\nVendor: {v_name}")
#
#     vendor_order_ids = (
#         dim_orders
#         .filter(F.col("vendor_id") == v_id)
#         .select("order_id")
#     )
#
#     v_basket = basket.join(vendor_order_ids, "order_id", "inner")
#     v_total  = v_basket.select("order_id").distinct().count()
#
#     if v_total < 50:
#         print(f"  Skipping — only {v_total} orders")
#         continue
#
#     v_pc = (
#         v_basket.groupBy("product_id")
#         .agg(F.countDistinct("order_id").alias("product_order_count"))
#     )
#     v_pairs = (
#         v_basket.select(F.col("order_id"), F.col("product_id").alias("product_a"))
#         .join(v_basket.select(F.col("order_id"), F.col("product_id").alias("product_b")), "order_id")
#         .filter(F.col("product_a") < F.col("product_b"))
#         .groupBy("product_a","product_b")
#         .agg(F.countDistinct("order_id").alias("pair_order_count"))
#         .filter(F.col("pair_order_count") >= MIN_PAIR_COUNT)
#         .join(v_pc.withColumnRenamed("product_id","product_a").withColumnRenamed("product_order_count","count_a"), "product_a","left")
#         .join(v_pc.withColumnRenamed("product_id","product_b").withColumnRenamed("product_order_count","count_b"), "product_b","left")
#         .withColumn("confidence", F.round(F.col("pair_order_count")/F.col("count_a"),4))
#         .withColumn("lift",       F.round((F.col("pair_order_count")/F.col("count_a"))/(F.col("count_b")/v_total),4))
#         .withColumn("vendor_id",  F.lit(v_id))
#         .withColumn("vendor_name",F.lit(v_name))
#         .withColumn("created_at", F.current_timestamp())
#         .filter(F.col("confidence") >= MIN_CONFIDENCE)
#         .filter(F.col("lift") > MIN_LIFT)
#     )
#     (
#         v_pairs.write.format("delta").mode("append")
#         .option("mergeSchema","true")
#         .saveAsTable(f"{GOLD_DB}.mba_rules_by_vendor")
#     )
#     print(f"  Saved {v_pairs.count()} rules")

print("Uncomment above to run vendor-segmented MBA.")