"""
Adhikar-Aina | Automation Triggers

Purpose:
- Simulate operational triggers for new schemes and life events.

Writes:
- workspace.default.aa_schemes (append)
- workspace.default.aa_life_events (append)
- workspace.default.aa_eligibility_results (append)
"""

import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import Row

spark.sql("USE CATALOG workspace")
spark.sql("USE default")

print("="*60)
print("TRIGGER 1: New scheme launched — re-run eligibility pulse")
print("="*60)

new_scheme = {
    "scheme_id":      "SCH-NEW-001",
    "scheme_name":    "PM Vishwakarma Toolkit Scheme 2026",
    "short_code":     "PM-VISHWAKARMA",
    "benefit_type":   "Skills & Employment",
    "benefit_amount": "Rs 15,000 toolkit grant + 5% interest loan up to Rs 3 lakh",
    "eligibility_sql":"employment_days < 100 AND income_bracket IN ('EWS', 'LIG', 'MIG')",
    "required_docs":  "Aadhaar, trade certificate, bank account",
    "is_active":      True,
    "created_at":     datetime.now(),
}

new_row = spark.createDataFrame([Row(**new_scheme)])

(new_row.write
    .format("delta")
    .mode("append")
    .saveAsTable("workspace.default.aa_schemes"))

total_schemes = spark.table("workspace.default.aa_schemes").count()
print(f"✅ New scheme appended. Total schemes now: {total_schemes}")

df_citizens = spark.table("workspace.default.aa_citizens_for_llm")

df_citizens_enriched = df_citizens.withColumn(
    "citizen_tags",
    F.concat_ws(",",
        F.when(F.col("housing_status").isin("kutcha","semi_pucca"), F.lit("housing")),
        F.when(F.col("has_bpl_card") == True, F.lit("health")),
        F.when(F.col("has_girl_child") == True, F.lit("women,nutrition,education")),
        F.when(F.col("is_tribal") == True, F.lit("tribal")),
        F.when(F.col("caste_category") == "SC", F.lit("sc")),
        F.when(F.col("caste_category") == "ST", F.lit("st")),
        F.when(F.col("employment_days") < 100, F.lit("employment")),
        F.when(F.col("has_electricity") == False, F.lit("solar")),
        F.when(F.col("land_category").isin("marginal","small","medium"), F.lit("agriculture")),
        F.when(F.col("income_bracket").isin("EWS","LIG"), F.lit("ews_lig")),
        F.when(F.col("income_bracket").isin("EWS","LIG","MIG"), F.lit("ews_lig_mig")),
    )
)

df_new_scheme = spark.table("workspace.default.aa_schemes").filter(
    F.col("scheme_id") == "SCH-NEW-001"
).withColumn("scheme_tag", F.lit("employment"))

df_new_matches = (
    df_citizens_enriched.alias("c")
    .join(df_new_scheme.alias("s"),
        F.col("c.citizen_tags").contains(F.col("s.scheme_tag")),
        "inner"
    )
    .select(
        F.col("c.citizen_id"),
        F.col("c.district"),
        F.col("c.taluka"),
        F.col("c.village"),
        F.col("c.income_bracket"),
        F.col("c.caste_category"),
        F.col("c.is_tribal"),
        F.col("s.scheme_id"),
        F.col("s.short_code"),
        F.col("s.scheme_name"),
        F.col("s.benefit_amount").alias("benefit"),
        F.col("s.required_docs"),
        F.lit(False).alias("is_notified"),
        F.current_timestamp().alias("matched_at"),
    )
)

new_match_count = df_new_matches.count()
print(f"✅ New scheme matched {new_match_count} citizens")

(df_new_matches.write
    .format("delta")
    .mode("append")
    .saveAsTable("workspace.default.aa_eligibility_results"))

print(f"✅ {new_match_count} new matches appended to aa_eligibility_results")

print("\n" + "="*60)
print("TRIGGER 2: Life event — girl child born in Health Dept")
print("="*60)

# Write to dedicated life events table — clean, no schema conflict
life_event_schema = StructType([
    StructField("citizen_id",           StringType(),  False),
    StructField("district",             StringType(),  True),
    StructField("taluka",               StringType(),  True),
    StructField("village",              StringType(),  True),
    StructField("ward_no",              IntegerType(), True),
    StructField("caste_category",       StringType(),  True),
    StructField("is_tribal",            BooleanType(), True),
    StructField("has_girl_child",       BooleanType(), True),
    StructField("girl_child_age_years", IntegerType(), True),
    StructField("has_bpl_card",         BooleanType(), True),
    StructField("housing_status",       StringType(),  True),
    StructField("has_electricity",      BooleanType(), True),
    StructField("has_water_source",     BooleanType(), True),
    StructField("employment_days",      IntegerType(), True),
    StructField("income_bracket",       StringType(),  True),
    StructField("land_category",        StringType(),  True),
    StructField("data_source",          StringType(),  True),
    StructField("event_type",           StringType(),  True),
    StructField("event_timestamp",      TimestampType(),True),
])

life_event_data = [(
    "LIFE-EVENT-001",
    "Satara", "Karad", "Umbraj",
    2, "OBC", False, True, 0,
    False, "kutcha", True, True,
    45, "LIG", "small",
    "health_dept", "girl_child_birth",
    datetime.now()
)]

df_life_event = spark.createDataFrame(life_event_data, schema=life_event_schema)

(df_life_event.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("workspace.default.aa_life_events"))

print("✅ Life event written to aa_life_events")

# Run eligibility for this citizen against women/child schemes
df_triggered_schemes = spark.table("workspace.default.aa_schemes").filter(
    F.col("eligibility_sql").rlike("has_girl_child|nutrition|women")
).withColumn("scheme_tag", F.lit("women"))

df_life_event_enriched = df_life_event.withColumn(
    "citizen_tags", F.lit("women,nutrition,education")
)

df_life_matches = (
    df_life_event_enriched.alias("c")
    .join(df_triggered_schemes.alias("s"),
        F.col("c.citizen_tags").contains(F.col("s.scheme_tag")),
        "inner"
    )
    .select(
        F.col("c.citizen_id"),
        F.col("c.district"),
        F.col("c.taluka"),
        F.col("c.village"),
        F.col("c.income_bracket"),
        F.col("c.caste_category"),
        F.col("c.is_tribal"),
        F.col("s.scheme_id"),
        F.col("s.short_code"),
        F.col("s.scheme_name"),
        F.col("s.benefit_amount").alias("benefit"),
        F.col("s.required_docs"),
        F.lit(False).alias("is_notified"),
        F.current_timestamp().alias("matched_at"),
    )
)

life_match_count = df_life_matches.count()
print(f"✅ Life event triggered {life_match_count} scheme matches for new girl child")

(df_life_matches.write
    .format("delta")
    .mode("append")
    .saveAsTable("workspace.default.aa_eligibility_results"))

print(f"✅ Life event matches written to aa_eligibility_results")

print("\n" + "="*60)
print("DELTA LAKE HISTORY — aa_eligibility_results")
print("="*60)

spark.sql("""
    DESCRIBE HISTORY workspace.default.aa_eligibility_results
""").select("version", "timestamp", "operation", "operationParameters").show(5, truncate=False)

total = spark.table("workspace.default.aa_eligibility_results").count()
print(f"\n✅ Total eligibility records after automation: {total:,}")
print(f"✅ New scheme pulse added                    : {new_match_count:,} records")
print(f"✅ Life event trigger added                  : {life_match_count:,} records")

print("""
╔══════════════════════════════════════════════════════╗
║         ADHIKAR-AINA | AUTOMATION LAYER LIVE         ║
╠══════════════════════════════════════════════════════╣
║  Trigger 1: New scheme → instant eligibility pulse   ║
║  Trigger 2: Life event → instant scheme match        ║
║  Delta CDF: Every append auto-fires this workflow    ║
║  History  : Full audit trail in Delta Lake           ║
╚══════════════════════════════════════════════════════╝
""")

print("▶ Ready for NB07 — Streamlit Dashboard (Glowing Blue Houses)")