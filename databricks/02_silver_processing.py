"""
Adhikar-Aina | 02 Silver Citizens

Reads bronze_citizens, performs feature engineering + tagging,
and writes Delta table:
- silver_citizens
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_TABLE = "bronze_citizens"
SILVER_TABLE = "silver_citizens"



def get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return SparkSession.builder.appName("adhikar-aina-02-silver").getOrCreate()



def build_silver_dataframe(bronze_df):
    base = (
        bronze_df
        .withColumn("district", F.initcap(F.trim(F.coalesce(F.col("district"), F.lit("Pune")))))
        .withColumn("state", F.initcap(F.trim(F.coalesce(F.col("state"), F.lit("Maharashtra")))))
        .withColumn("annual_income", F.coalesce(F.col("annual_income").cast("double"), F.lit(0.0)))
        .withColumn("occupation", F.lower(F.trim(F.coalesce(F.col("occupation"), F.lit("unemployed")))))
        .withColumn("land_acres", F.coalesce(F.col("land_acres").cast("double"), F.lit(0.0)))
        .withColumn("caste_category", F.upper(F.trim(F.coalesce(F.col("caste_category"), F.lit("GEN")))))
        .withColumn("is_tribal", F.coalesce(F.col("is_tribal").cast("boolean"), F.lit(False)))
        .withColumn("has_girl_child", F.coalesce(F.col("has_girl_child").cast("boolean"), F.lit(False)))
        .withColumn("has_bpl_card", F.coalesce(F.col("has_bpl_card").cast("boolean"), F.lit(False)))
        .withColumn("housing_status", F.lower(F.trim(F.coalesce(F.col("housing_status"), F.lit("semi_pucca")))))
        .withColumn("employment_days", F.coalesce(F.col("employment_days").cast("int"), F.lit(0)))
        .withColumn("age", F.coalesce(F.col("age").cast("int"), F.lit(0)))
    )

    engineered = (
        base
        .withColumn(
            "income_bracket",
            F.when(F.col("annual_income") < 100000, F.lit("EWS"))
            .when(F.col("annual_income") < 300000, F.lit("LIG"))
            .when(F.col("annual_income") < 1000000, F.lit("MIG"))
            .otherwise(F.lit("HIG")),
        )
        .withColumn(
            "land_category",
            F.when(F.col("land_acres") < 1, F.lit("marginal"))
            .when(F.col("land_acres") < 2, F.lit("small"))
            .when(F.col("land_acres") < 5, F.lit("medium"))
            .otherwise(F.lit("large")),
        )
        .withColumn(
            "occupation_category",
            F.when(F.col("occupation") == "farmer", F.lit("farmer"))
            .when(F.col("occupation") == "student", F.lit("student"))
            .when(F.col("occupation") == "worker", F.lit("worker"))
            .when(F.col("occupation") == "entrepreneur", F.lit("entrepreneur"))
            .otherwise(F.lit("unemployed")),
        )
        .withColumn("is_minor", F.col("age") < 18)
        .withColumn("is_senior", F.col("age") > 60)
    )

    tags_array = F.array(
        F.col("occupation_category"),
        F.when(F.col("income_bracket").isin("EWS", "LIG"), F.lit("ews_lig")).otherwise(F.lit("")),
        F.when(F.col("occupation_category") == "farmer", F.lit("agriculture")).otherwise(F.lit("")),
        F.when(F.col("occupation_category") == "student", F.lit("education")).otherwise(F.lit("")),
        F.when(F.col("occupation_category") == "unemployed", F.lit("welfare")).otherwise(F.lit("")),
        F.when(F.col("has_bpl_card"), F.lit("bpl")).otherwise(F.lit("")),
        F.when(F.col("has_girl_child"), F.lit("girl_child")).otherwise(F.lit("")),
        F.when(F.col("is_tribal"), F.lit("tribal")).otherwise(F.lit("")),
        F.when(F.col("housing_status") == "kutcha", F.lit("housing_support")).otherwise(F.lit("")),
    )

    tagged = (
        engineered
        .withColumn("tags_array", tags_array)
        .withColumn("clean_tags", F.expr("filter(tags_array, x -> x != '')"))
        .withColumn("citizen_tags", F.concat_ws(",", F.col("clean_tags")))
        .drop("tags_array", "clean_tags")
        .select(
            "citizen_id",
            "district",
            "state",
            "income_bracket",
            "occupation_category",
            "land_category",
            "caste_category",
            "is_tribal",
            "has_girl_child",
            "has_bpl_card",
            "housing_status",
            "employment_days",
            "citizen_tags",
        )
    )

    return tagged



def main() -> None:
    spark_session = get_spark()
    bronze_df = spark_session.table(BRONZE_TABLE)
    silver_df = build_silver_dataframe(bronze_df)

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )

    spark_session.sql("SELECT COUNT(*) FROM bronze_citizens").show()
    spark_session.sql("SELECT COUNT(*) FROM silver_citizens").show()

    spark_session.sql("SELECT occupation_category, COUNT(*) FROM silver_citizens GROUP BY occupation_category").show()

    spark_session.sql("SELECT citizen_tags FROM silver_citizens LIMIT 10").show(truncate=False)


if __name__ == "__main__":
    main()
