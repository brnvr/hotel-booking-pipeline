"""Run the hotel-booking stages serially using one Spark session."""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

import transform.bronze
import transform.silver
import transform.gold
import validation.bronze
import validation.silver


def run(
    input_dir: Path,
    output_dir: Path,
    master: str = "local[2]",
) -> None:
    bronze_dir = output_dir / "bronze"
    silver_dir = output_dir / "silver"
    gold_dir = output_dir / "gold"

    spark = (
        SparkSession.Builder()
        .appName("befly-hotel-pipeline")
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        transform.bronze.run(spark, input_dir, bronze_dir)
        validation.bronze.test(spark, bronze_dir)
        transform.silver.run(spark, bronze_dir, silver_dir)
        validation.silver.test(spark, bronze_dir, silver_dir)
        transform.gold.run(spark, silver_dir, gold_dir)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
    )
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    run(args.input_dir.resolve(), args.output_dir.resolve(), args.master)


if __name__ == "__main__":
    main()
