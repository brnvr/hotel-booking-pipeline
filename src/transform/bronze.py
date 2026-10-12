"""Read the input CSVs and write the Bronze tables."""

from pathlib import Path

from pyspark.sql import SparkSession


def run(spark: SparkSession, input_dir: Path, output_dir: Path) -> None:
    def load(
        file_name: str,
        table_name: str,
        partition_by: list[str] = [],
    ) -> None:
        frame = (
            spark.read
            .options(
                header=True,
                sep=",",
                nullValue="NULL",
                mode="FAILFAST",
            )
            .option("inferSchema", False)
            .csv(str(input_dir / file_name))
        )
        (
            frame.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(
                str(output_dir / table_name),
                partitionBy=partition_by,
            )
        )

    load("hotel_bookings.csv", "bookings", partition_by=["arrival_date_year"])
    load("country_metadata.csv", "countries")
    load("hotel_metadata.csv", "hotels")
