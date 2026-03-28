"""
Adhikar-Aina | 01 Bronze Citizens

Purpose:
- Create or load citizen data into bronze layer.
- Generate deterministic synthetic citizens (50-100 rows) when no source exists.

Output Delta table:
- bronze_citizens
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_TABLE = "bronze_citizens"


def get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return SparkSession.builder.appName("adhikar-aina-01-bronze").getOrCreate()


def citizen_schema() -> StructType:
    return StructType(
        [
            StructField("citizen_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("district", StringType(), True),
            StructField("occupation", StringType(), True),
            StructField("income", DoubleType(), True),
            StructField("land_acres", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("has_daughter", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
        ]
    )


def generate_synthetic_citizens(row_count: int = 80, seed: int = 42) -> List[dict]:
    random.seed(seed)

    first_names = [
        "Asha",
        "Rohan",
        "Sunita",
        "Kiran",
        "Mahesh",
        "Savita",
        "Arjun",
        "Neha",
        "Vijay",
        "Pooja",
    ]
    last_names = ["Patil", "Sharma", "Yadav", "Jadhav", "Kumar", "Singh", "More", "Rathod"]
    districts = ["satara", "kolhapur", "nagpur", "nashik", "pune", "solapur"]
    occupations = ["farmer", "laborer", "artisan", "teacher", "shopkeeper", "student", "unemployed"]
    categories = ["SC", "ST", "OBC", "GEN"]

    records = []
    for idx in range(row_count):
        occupation = random.choices(
            occupations,
            weights=[35, 18, 10, 8, 12, 6, 11],
            k=1,
        )[0]

        if occupation == "farmer":
            income = round(random.uniform(90000, 450000), 2)
            land_acres = round(random.uniform(0.3, 6.0), 2)
        elif occupation in {"laborer", "unemployed"}:
            income = round(random.uniform(30000, 220000), 2)
            land_acres = round(random.uniform(0.0, 1.2), 2)
        else:
            income = round(random.uniform(80000, 700000), 2)
            land_acres = round(random.uniform(0.0, 2.5), 2)

        name = f"{random.choice(first_names)} {random.choice(last_names)}"

        records.append(
            {
                "citizen_id": f"CIT-{idx + 1:05d}-{uuid.uuid4().hex[:6].upper()}",
                "name": name,
                "district": random.choice(districts),
                "occupation": occupation,
                "income": income,
                "land_acres": land_acres,
                "category": random.choice(categories),
                "has_daughter": random.random() < 0.42,
                "created_at": datetime.utcnow(),
            }
        )

    return records


def build_bronze_dataframe(spark_session: SparkSession, row_count: int = 80) -> DataFrame:
    synthetic_rows = generate_synthetic_citizens(row_count=row_count)
    return spark_session.createDataFrame(synthetic_rows, schema=citizen_schema())


def write_bronze(df: DataFrame) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )


def run_generation_tests(df: DataFrame) -> None:
    total_rows = df.count()
    assert total_rows >= 50, f"Expected >= 50 rows, found {total_rows}"

    occupation_counts = {
        row["occupation"]: row["count"]
        for row in df.groupBy("occupation").count().collect()
    }
    assert occupation_counts.get("farmer", 0) > 0, "Expected farmer records"

    income_range = df.select(F.min("income").alias("min_income"), F.max("income").alias("max_income")).first()
    assert income_range["min_income"] < 100000, "Expected low income citizens"
    assert income_range["max_income"] > 400000, "Expected high income citizens"

    land_range = df.select(F.min("land_acres").alias("min_land"), F.max("land_acres").alias("max_land")).first()
    assert land_range["max_land"] >= 3.0, "Expected citizens with larger land holdings"

    print("[TEST-1] Synthetic generation passed")
    print(f"[TEST-1] rows={total_rows}, occupations={occupation_counts}")


def main() -> None:
    spark_session = get_spark()
    bronze_df = build_bronze_dataframe(spark_session, row_count=80)
    write_bronze(bronze_df)
    run_generation_tests(bronze_df)
    print(f"Bronze citizens written to Delta table: {BRONZE_TABLE}")


if __name__ == "__main__":
    main()
