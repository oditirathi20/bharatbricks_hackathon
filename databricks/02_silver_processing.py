"""
Adhikar-Aina | 02 Silver Processing

Purpose:
- Clean and standardize bronze citizen records.
- Normalize text values and handle nulls.
- Add derived analytical columns.

Input Delta table:
- bronze_citizens

Output Delta table:
- silver_citizens
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE_TABLE = "bronze_citizens"
SILVER_TABLE = "silver_citizens"


def get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return SparkSession.builder.appName("adhikar-aina-02-silver").getOrCreate()


def clean_citizens(df: DataFrame) -> DataFrame:
    cleaned = (
        df.withColumn("name", F.initcap(F.trim(F.coalesce(F.col("name"), F.lit("Unknown Citizen")))))
        .withColumn("district", F.lower(F.trim(F.coalesce(F.col("district"), F.lit("unknown")))))
        .withColumn("occupation", F.lower(F.trim(F.coalesce(F.col("occupation"), F.lit("unemployed")))))
        .withColumn("income", F.coalesce(F.col("income").cast("double"), F.lit(0.0)))
        .withColumn("income", F.when(F.col("income") < 0, F.lit(0.0)).otherwise(F.col("income")))
        .withColumn("land_acres", F.coalesce(F.col("land_acres").cast("double"), F.lit(0.0)))
        .withColumn("land_acres", F.when(F.col("land_acres") < 0, F.lit(0.0)).otherwise(F.col("land_acres")))
        .withColumn("category", F.upper(F.trim(F.coalesce(F.col("category"), F.lit("GEN")))))
        .withColumn("category", F.when(F.col("category").isin("SC", "ST", "OBC", "GEN"), F.col("category")).otherwise(F.lit("GEN")))
        .withColumn("has_daughter", F.coalesce(F.col("has_daughter").cast("boolean"), F.lit(False)))
        .withColumn(
            "income_band",
            F.when(F.col("income") <= 120000, F.lit("low"))
            .when(F.col("income") <= 350000, F.lit("middle"))
            .otherwise(F.lit("high")),
        )
        .withColumn("is_small_farmer", (F.col("occupation") == F.lit("farmer")) & (F.col("land_acres") <= F.lit(2.0)))
        .withColumn("silver_processed_at", F.current_timestamp())
    )
    return cleaned


def write_silver(df: DataFrame) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    spark_session = get_spark()
    bronze_df = spark_session.table(BRONZE_TABLE)
    silver_df = clean_citizens(bronze_df)
    write_silver(silver_df)

    print(f"Silver citizens written to Delta table: {SILVER_TABLE}")
    silver_df.groupBy("occupation").count().orderBy(F.desc("count")).show(10, truncate=False)


if __name__ == "__main__":
    main()
