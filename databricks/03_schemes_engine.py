"""
Adhikar-Aina | Schemes Engine

Purpose:
- Ingest raw schemes and normalize eligibility expressions for downstream matching.

Creates:
- workspace.default.aa_schemes_bronze
- workspace.default.aa_schemes
"""

# ── CELL 1 ────────────────────────────────────────────────────────────────────
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark.sql("USE CATALOG workspace")
spark.sql("USE default")

# ── BRONZE ────────────────────────────────────────────────────────────────────
df_bronze_raw = spark.table("workspace.default.updated_data")

print("Kaggle columns:", df_bronze_raw.columns)
print(f"Raw scheme count: {df_bronze_raw.count()}")

(df_bronze_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.aa_schemes_bronze"))

print("✅ aa_schemes_bronze written")
# ── CELL 2 ────────────────────────────────────────────────────────────────────
# Eligibility mapping (rule engine v1)

eligibility_map = {
    "housing":      "housing_status IN ('kutcha', 'semi_pucca') AND has_bpl_card = true",
    "agriculture":  "land_acres > 0 AND income_bracket IN ('EWS', 'LIG', 'MIG')",
    "women":        "has_girl_child = true",
    "tribal":       "is_tribal = true",
    "employment":   "employment_days < 100",
    "nutrition":    "has_girl_child = true",
    "sc":           "caste_category = 'SC'",
    "st":           "caste_category = 'ST'",
    "solar":        "has_electricity = false",
    "health":       "has_bpl_card = true",
    "education":    "has_girl_child = true",
}

default_sql = "income_bracket IN ('EWS', 'LIG')"

def map_eligibility(text):
    if not text:
        return default_sql
    text = str(text).lower()
    for key, sql in eligibility_map.items():
        if key in text:
            return sql
    return default_sql

map_elig_udf = F.udf(map_eligibility, StringType())
# ── CELL 3 ────────────────────────────────────────────────────────────────────
df_bronze = spark.table("workspace.default.aa_schemes_bronze")

df_silver = (
    df_bronze

    # ID
    .withColumn("scheme_id",
        F.concat(F.lit("SCH"),
        F.lpad(F.monotonically_increasing_id().cast("string"), 6, "0"))
    )

    # Clean columns
    .withColumn("scheme_name", F.col("scheme_name"))
    .withColumn("benefit_type", F.col("schemeCategory"))   # ✅ FIX
    .withColumn("benefit_amount", F.col("benefits"))
    .withColumn("required_docs", F.col("documents"))

    # Short code
    .withColumn("short_code",
        F.regexp_replace(
            F.upper(F.substring(F.col("scheme_name"), 1, 15)),
            " ", "-"
        )
    )

    # 🔥 SMART ELIGIBILITY (multi-column context)
    .withColumn("eligibility_context",
        F.concat_ws(" ",
            F.col("schemeCategory"),
            F.col("tags"),
            F.col("details"),
            F.col("eligibility")
        )
    )

    .withColumn("eligibility_sql",
        map_elig_udf(F.col("eligibility_context"))
    )

    # Meta
    .withColumn("is_active", F.lit(True))
    .withColumn("created_at", F.current_timestamp())

    # Final select
    .select(
        "scheme_id",
        "scheme_name",
        "short_code",
        "benefit_type",
        "benefit_amount",
        "eligibility_sql",
        "required_docs",
        "is_active",
        "created_at"
    )

    .dropDuplicates(["scheme_name"])
    .filter(F.col("scheme_name").isNotNull())
)

print(f"Schemes after silver transform: {df_silver.count()}")
# ── CELL 4 ────────────────────────────────────────────────────────────────────
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.aa_schemes"))

print("✅ aa_schemes written")
# ── CELL 5 ────────────────────────────────────────────────────────────────────
spark.sql("""
    SELECT benefit_type, COUNT(*) as scheme_count
    FROM workspace.default.aa_schemes
    GROUP BY benefit_type
    ORDER BY scheme_count DESC
""").show(20, truncate=False)

spark.sql("""
    SELECT scheme_id, scheme_name, eligibility_sql
    FROM workspace.default.aa_schemes
    LIMIT 10
""").show(truncate=False)

print(f"\n✅ Total schemes: {spark.table('workspace.default.aa_schemes').count()}")
