"""
Adhikar-Aina | 05 Adhikar Certificates

Purpose:
- Generate structured certificate objects for eligible citizen-scheme matches.

Input Delta tables:
- eligibility_results
- silver_citizens

Output Delta table:
- adhikar_certificates
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RESULTS_TABLE = "eligibility_results"
SILVER_TABLE = "silver_citizens"
CERT_TABLE = "adhikar_certificates"


def get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return SparkSession.builder.appName("adhikar-aina-05-certificate").getOrCreate()


def build_certificates(results_df: DataFrame, citizens_df: DataFrame) -> DataFrame:
    eligible = results_df.filter(F.col("eligibility_status") == F.lit(True))

    certificates = (
        eligible.join(citizens_df.select("citizen_id", "name"), on="citizen_id", how="left")
        .withColumn("status", F.lit("Eligible"))
        .withColumn("timestamp", F.current_timestamp())
        .withColumn("citizen_name", F.coalesce(F.col("name"), F.lit("Unknown Citizen")))
        .withColumn(
            "certificate_json",
            F.to_json(
                F.struct(
                    F.col("citizen_name"),
                    F.col("scheme_name"),
                    F.col("benefit"),
                    F.col("status"),
                    F.col("timestamp"),
                )
            ),
        )
        .select(
            "citizen_id",
            "citizen_name",
            "scheme_name",
            "benefit",
            "status",
            "timestamp",
            "certificate_json",
        )
    )

    return certificates


def write_certificates(df: DataFrame) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(CERT_TABLE)
    )


def main() -> None:
    spark_session = get_spark()
    results_df = spark_session.table(RESULTS_TABLE)
    citizens_df = spark_session.table(SILVER_TABLE)

    certificates_df = build_certificates(results_df, citizens_df)
    write_certificates(certificates_df)

    print(f"Certificates written to Delta table: {CERT_TABLE}")
    certificates_df.select("citizen_name", "scheme_name", "status", "timestamp").show(10, truncate=False)


if __name__ == "__main__":
    main()
