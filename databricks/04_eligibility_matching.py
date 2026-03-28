"""
Adhikar-Aina | 04 Eligibility Matching

Purpose:
- Deterministically match citizens to schemes using Spark DataFrame logic.

Input Delta tables:
- silver_citizens
- schemes_clean

Output Delta table:
- eligibility_results

Output schema:
- citizen_id
- scheme_name
- benefit
- eligibility_status
"""

from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_TABLE = "silver_citizens"
SCHEMES_TABLE = "schemes_clean"
RESULTS_TABLE = "eligibility_results"


def get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return SparkSession.builder.appName("adhikar-aina-04-matching").getOrCreate()


def match_citizens_to_schemes(citizens_df: DataFrame, schemes_df: DataFrame) -> DataFrame:
    citizens = (
        citizens_df.select(
            "citizen_id",
            F.lower(F.trim(F.coalesce(F.col("occupation"), F.lit("")))).alias("occupation_norm"),
            F.coalesce(F.col("income").cast("double"), F.lit(0.0)).alias("income_norm"),
            F.coalesce(F.col("land_acres").cast("double"), F.lit(0.0)).alias("land_norm"),
            F.upper(F.trim(F.coalesce(F.col("category"), F.lit("GEN")))).alias("category_norm"),
        )
    )

    schemes = (
        schemes_df.select(
            "scheme_name",
            "benefit",
            F.coalesce(F.col("min_income").cast("double"), F.lit(0.0)).alias("min_income_norm"),
            F.coalesce(F.col("max_income").cast("double"), F.lit(100000000.0)).alias("max_income_norm"),
            F.coalesce(F.col("max_land").cast("double"), F.lit(999999.0)).alias("max_land_norm"),
            F.lower(F.trim(F.coalesce(F.col("occupation"), F.lit("any")))).alias("occupation_req"),
            F.upper(F.trim(F.coalesce(F.col("category"), F.lit("ANY")))).alias("category_req"),
        )
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


def write_results(df: DataFrame) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(RESULTS_TABLE)
    )


def evaluate_single_profile(profile: Dict[str, Any], schemes_df: DataFrame) -> DataFrame:
    spark_session = get_spark()
    one_row = [
        (
            "TEST-CITIZEN",
            profile.get("occupation", ""),
            float(profile.get("income", 0.0)),
            float(profile.get("land_acres", 0.0)),
            profile.get("category", "GEN"),
        )
    ]
    schema = "citizen_id string, occupation string, income double, land_acres double, category string"
    test_citizen_df = spark_session.createDataFrame(one_row, schema=schema)
    return match_citizens_to_schemes(test_citizen_df, schemes_df)


def run_tests(citizens_df: DataFrame, schemes_df: DataFrame, results_df: DataFrame) -> None:
    # Test 2: primary matching case
    test_profile = {
        "income": 300000,
        "occupation": "farmer",
        "land_acres": 1.5,
        "category": "OBC",
    }
    test2_df = evaluate_single_profile(test_profile, schemes_df).filter(F.col("eligibility_status") == F.lit(True))
    test2_count = test2_df.count()
    assert test2_count >= 1, f"Expected at least 1 eligible scheme, found {test2_count}"
    print(f"[TEST-2] Matching logic passed with eligible schemes={test2_count}")

    # Test 3a: no matching schemes
    no_match_profile = {
        "income": 99999999,
        "occupation": "astronaut",
        "land_acres": 1000,
        "category": "GEN",
    }
    test3a_count = evaluate_single_profile(no_match_profile, schemes_df).filter(F.col("eligibility_status") == F.lit(True)).count()
    assert test3a_count == 0, f"Expected 0 for no-match case, found {test3a_count}"

    # Test 3b: missing values
    missing_profile = {
        "income": None,
        "occupation": None,
        "land_acres": None,
        "category": None,
    }
    test3b_df = evaluate_single_profile(missing_profile, schemes_df)
    assert test3b_df.count() > 0, "Missing value test should still evaluate without failure"

    # Test 3c: extreme income low
    low_income_profile = {
        "income": 0,
        "occupation": "laborer",
        "land_acres": 0,
        "category": "SC",
    }
    test3c_df = evaluate_single_profile(low_income_profile, schemes_df)
    assert test3c_df.count() > 0, "Extreme low income test should still evaluate"

    assert results_df.count() > 0, "eligibility_results should not be empty"

    print("[TEST-3] Edge cases passed (no-match, missing values, extreme income)")


def main() -> None:
    spark_session = get_spark()
    citizens_df = spark_session.table(SILVER_TABLE)
    schemes_df = spark_session.table(SCHEMES_TABLE)

    results_df = match_citizens_to_schemes(citizens_df, schemes_df)
    write_results(results_df)

    print(f"Eligibility results written to Delta table: {RESULTS_TABLE}")
    results_df.groupBy("eligibility_status").count().show()

    run_tests(citizens_df, schemes_df, results_df)


if __name__ == "__main__":
    main()
