"""Build aggregated business views for Gold."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


def build_revenue_by_hotel_month(silver: DataFrame) -> DataFrame:
    effective = F.col("is_canceled") == 0
    return (
        silver.groupBy("hotel", "arrival_date_year", "arrival_date_month_num")
        .agg(
            F.count("*").alias("total_bookings"),
            F.sum(F.when(effective, 1).otherwise(0)).alias("effective_bookings"),
            F.sum("is_canceled").alias("cancelled_bookings"),
            F.sum("revenue").alias("total_revenue"),
            F.avg(F.when(effective, F.col("adr"))).alias("avg_adr"),
            (
                F.sum(F.when(effective, F.col("total_nights")).otherwise(0))
                .alias("total_nights_sold")
            ),
        )
        .withColumn(
            "cancellation_rate",
            F.col("cancelled_bookings") / F.col("total_bookings"),
        )
    )


def build_cancellation_by_segment(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("market_segment", "customer_type", "distribution_channel")
        .agg(
            F.count("*").alias("total_bookings"),
            F.sum("is_canceled").alias("cancelled_bookings"),
            F.avg("lead_time").alias("avg_lead_time"),
            F.avg("total_of_special_requests").alias("avg_total_special_requests"),
        )
        .withColumn(
            "cancellation_rate",
            F.col("cancelled_bookings") / F.col("total_bookings"),
        )
    )


def build_top_countries_by_revenue(silver: DataFrame) -> DataFrame:
    effective = F.col("is_canceled") == 0
    return (
        silver.filter(effective)
        .groupBy("country", "country_name", "continent")
        .agg(
            F.count("*").alias("effective_bookings"),
            F.sum("revenue").alias("total_revenue"),
            F.avg("revenue").alias("avg_ticket"),
            F.avg("lead_time").alias("avg_lead_time"),
        )
        .orderBy(F.desc("total_revenue"), F.asc("country"))
        .limit(20)
    )


def run(spark: SparkSession, input_dir: Path, output_dir: Path) -> None:
    bookings = spark.read.parquet(str(input_dir / "bookings_enriched")).cache()
    try:
        def save(name, build) -> None:
            table = build(bookings)
            (
                table.write
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(str(output_dir / name))
            )

        save("revenue_by_hotel_month", build_revenue_by_hotel_month)
        save("cancellation_by_segment", build_cancellation_by_segment)
        save("top_countries_by_revenue", build_top_countries_by_revenue)
    finally:
        bookings.unpersist()
