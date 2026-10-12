"""Validate normalized Bronze reference keys before Silver transformation."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


def check_reference(df: DataFrame, key: str) -> None:
    if df.filter(F.col(key).isNull() | (F.col(key) == "")).limit(1).count():
        raise ValueError(f"Reference key {key} contains missing values")
    if df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(
            f"Reference key {key} is not unique; join would duplicate bookings"
        )


def test(spark: SparkSession, output_dir: Path) -> None:
    """Validate Bronze reference keys using the normalization applied in Silver."""
    countries = spark.read.parquet(str(output_dir / "countries")).select(
        F.upper(F.trim("country_code")).alias("country"),
    )
    hotels = spark.read.parquet(str(output_dir / "hotels")).select(
        F.trim("hotel_name").alias("hotel"),
    )
    check_reference(countries, "country")
    check_reference(hotels, "hotel")
