"""
Adhikar-Aina | 06 Automation Trigger

Purpose:
- Simulate proactive pulse when new schemes are added.
- Re-run matching for newly added schemes and report newly eligible citizens.

Input Delta tables:
- silver_citizens
- schemes_clean
- eligibility_results

Output Delta tables:
- eligibility_results (updated)
- schemes_snapshot (state table for change detection)
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_TABLE = "silver_citizens"
SCHEMES_TABLE = "schemes_clean"
RESULTS_TABLE = "eligibility_results"
SNAPSHOT_TABLE = "schemes_snapshot"


def get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return SparkSession.builder.appName("adhikar-aina-06-trigger").getOrCreate()


def _match_with_subset(citizens_df: DataFrame, scheme_subset_df: DataFrame) -> DataFrame:
    citizens = citizens_df.select(
        "citizen_id",
        F.lower(F.trim(F.coalesce(F.col("occupation"), F.lit("")))).alias("occupation_norm"),
        F.coalesce(F.col("income").cast("double"), F.lit(0.0)).alias("income_norm"),
        F.coalesce(F.col("land_acres").cast("double"), F.lit(0.0)).alias("land_norm"),
        F.upper(F.trim(F.coalesce(F.col("category"), F.lit("GEN")))).alias("category_norm"),
    )

    schemes = scheme_subset_df.select(
        "scheme_name",
        "benefit",
        F.coalesce(F.col("min_income").cast("double"), F.lit(0.0)).alias("min_income_norm"),
        F.coalesce(F.col("max_income").cast("double"), F.lit(100000000.0)).alias("max_income_norm"),
        F.coalesce(F.col("max_land").cast("double"), F.lit(999999.0)).alias("max_land_norm"),
        F.lower(F.trim(F.coalesce(F.col("occupation"), F.lit("any")))).alias("occupation_req"),
        F.upper(F.trim(F.coalesce(F.col("category"), F.lit("ANY")))).alias("category_req"),
    )

    matched = citizens.crossJoin(F.broadcast(schemes)).withColumn(
        "eligibility_status",
        (
            (F.col("income_norm") >= F.col("min_income_norm"))
            & (F.col("income_norm") <= F.col("max_income_norm"))
            & (F.col("land_norm") <= F.col("max_land_norm"))
            & (
                (F.col("occupation_req") == F.lit("any"))
                | (F.col("occupation_norm") == F.col("occupation_req"))
            )
            & (
                (F.col("category_req") == F.lit("ANY"))
                | (F.col("category_norm") == F.col("category_req"))
            )
        ),
    )

    return matched.select("citizen_id", "scheme_name", "benefit", "eligibility_status")


def ensure_snapshot_exists(spark_session: SparkSession) -> None:
    if not spark_session.catalog.tableExists(SNAPSHOT_TABLE):
        spark_session.table(SCHEMES_TABLE).select("scheme_id").dropDuplicates().write.format("delta").mode("overwrite").saveAsTable(SNAPSHOT_TABLE)


def append_new_scheme(spark_session: SparkSession) -> str:
    new_scheme_id = f"SCH-NEW-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    rows = [
        (
            new_scheme_id,
            "Proactive Rural Farmer Booster",
            0.0,
            380000.0,
            "farmer",
            2.5,
            "OBC",
            "Top-up support for small and marginal farmers",
        )
    ]
    schema = "scheme_id string, scheme_name string, min_income double, max_income double, occupation string, max_land double, category string, benefit string"
    spark_session.createDataFrame(rows, schema=schema).write.format("delta").mode("append").saveAsTable(SCHEMES_TABLE)
    return new_scheme_id


def detect_new_schemes(spark_session: SparkSession) -> DataFrame:
    current_schemes = spark_session.table(SCHEMES_TABLE).select("scheme_id", "scheme_name", "min_income", "max_income", "occupation", "max_land", "category", "benefit")
    snapshot = spark_session.table(SNAPSHOT_TABLE).select("scheme_id").dropDuplicates()
    return current_schemes.join(snapshot, on="scheme_id", how="left_anti")


def update_snapshot(spark_session: SparkSession) -> None:
    spark_session.table(SCHEMES_TABLE).select("scheme_id").dropDuplicates().write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SNAPSHOT_TABLE)


def run_test_4(spark_session: SparkSession) -> None:
    ensure_snapshot_exists(spark_session)
    inserted_scheme_id = append_new_scheme(spark_session)

    new_schemes_df = detect_new_schemes(spark_session)
    new_scheme_count = new_schemes_df.count()
    assert new_scheme_count >= 1, "Expected at least one new scheme after append"

    citizens_df = spark_session.table(SILVER_TABLE)
    new_results_df = _match_with_subset(citizens_df, new_schemes_df)

    new_results_df.write.format("delta").mode("append").saveAsTable(RESULTS_TABLE)

    newly_eligible = new_results_df.filter(F.col("eligibility_status") == F.lit(True)).count()
    assert newly_eligible >= 1, "Expected newly eligible citizens after adding new scheme"

    update_snapshot(spark_session)

    print(f"[TEST-4] New scheme trigger passed. inserted_scheme_id={inserted_scheme_id}")
    print(f"[TEST-4] new_scheme_count={new_scheme_count}, newly_eligible={newly_eligible}")


def main() -> None:
    spark_session = get_spark()
    run_test_4(spark_session)


if __name__ == "__main__":
    main()
