"""
Adhikar-Aina | Silver Layer - Citizens Processing

Purpose:
- Remove sensitive columns, derive analytical attributes, and publish LLM-safe view.

Creates:
- workspace.default.aa_citizens_silver
- workspace.default.aa_citizens_for_llm (view)
"""

# Adhikar-Aina | Notebook 02 — Silver Layer + PII Masking
# Catalog: workspace.default

# ── CELL 1 ────────────────────────────────────────────────────────────────────
spark.sql("USE CATALOG workspace")
spark.sql("USE default")
print("✅ Ready")

# ── CELL 2 ────────────────────────────────────────────────────────────────────
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

df_bronze = spark.table("workspace.default.aa_citizens_bronze")

df_silver = (
    df_bronze
    .drop("aadhaar_hash")  # never passes to LLM
    .withColumn("income_bracket",
        when(col("annual_income") < 50000,  lit("EWS"))
       .when(col("annual_income") < 100000, lit("LIG"))
       .when(col("annual_income") < 200000, lit("MIG"))
       .otherwise(lit("HIG"))
    )
    .withColumn("land_category",
        when(col("land_acres") < 1.0, lit("marginal"))
       .when(col("land_acres") < 2.5, lit("small"))
       .when(col("land_acres") < 5.0, lit("medium"))
       .otherwise(lit("large"))
    )
    .withColumn("girl_child_age_years",
        when(
            col("has_girl_child") & col("girl_child_dob").isNotNull(),
            floor(datediff(current_date(), col("girl_child_dob")) / 365)
        ).otherwise(lit(None).cast(IntegerType()))
    )
    .drop("annual_income", "land_acres", "girl_child_dob")
    .withColumn("silver_processed_at", current_timestamp())
)

print(f"✅ Silver transform done. Rows: {df_silver.count()}")

# ── CELL 3 ────────────────────────────────────────────────────────────────────
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("district")
    .saveAsTable("workspace.default.aa_citizens_silver"))

print("✅ Table written: workspace.default.aa_citizens_silver")

# ── CELL 4 ────────────────────────────────────────────────────────────────────
# Enable Change Data Feed — lets the eligibility engine react to
# new life events (birth registered → auto-trigger SSY check)
spark.sql("""
    ALTER TABLE workspace.default.aa_citizens_silver
    SET TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")
print("✅ Change Data Feed enabled")

# ── CELL 5 ────────────────────────────────────────────────────────────────────
# LLM-safe view — zero PII, this is the ONLY thing BharatGen ever queries
spark.sql("""
    CREATE OR REPLACE VIEW workspace.default.aa_citizens_for_llm AS
    SELECT
        citizen_id,
        district,
        taluka,
        village,
        ward_no,
        caste_category,
        is_tribal,
        has_girl_child,
        girl_child_age_years,
        has_bpl_card,
        housing_status,
        has_electricity,
        has_water_source,
        employment_days,
        income_bracket,
        land_category,
        data_source,
        updated_at
    FROM workspace.default.aa_citizens_silver
""")
print("✅ View created: workspace.default.aa_citizens_for_llm")

# ── CELL 6 ────────────────────────────────────────────────────────────────────
print("\n── Silver distribution ──")
spark.sql("""
    SELECT income_bracket, COUNT(*) as citizens,
           SUM(CAST(is_tribal AS INT)) as tribal,
           SUM(CAST(has_bpl_card AS INT)) as bpl
    FROM workspace.default.aa_citizens_silver
    GROUP BY income_bracket ORDER BY income_bracket
""").show()

print("\n── LLM view sample (zero PII) ──")
spark.sql("SELECT * FROM workspace.default.aa_citizens_for_llm LIMIT 3").show(truncate=False)

print("✅ Notebook 02 done — move to 03")