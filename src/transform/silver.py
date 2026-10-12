"""Clean, enrich, and write Silver bookings."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from constants import MONTHS


def clean_bookings(bookings: DataFrame) -> DataFrame:
    trimmed = bookings.select(
        *[F.trim(F.col(name)).alias(name) for name in bookings.columns]
    )
    df = trimmed.select(
        F.col("hotel"),
        F.col("is_canceled").cast("int").alias("is_canceled"),
        F.col("lead_time").cast("int").alias("lead_time"),
        F.col("arrival_date_year").cast("int").alias("arrival_date_year"),
        F.col("arrival_date_month"),
        F.col("arrival_date_week_number").cast("int").alias("arrival_date_week_number"),
        (
            F.col("arrival_date_day_of_month").cast("int")
            .alias("arrival_date_day_of_month")
        ),
        F.col("stays_in_weekend_nights").cast("int").alias("stays_in_weekend_nights"),
        F.col("stays_in_week_nights").cast("int").alias("stays_in_week_nights"),
        F.col("adults").cast("int").alias("adults"),
        (
            F.coalesce(F.col("children").cast("double").cast("int"), F.lit(0))
            .alias("children")
        ),
        F.col("babies").cast("int").alias("babies"),
        F.col("meal"),
        F.coalesce(F.upper(F.col("country")), F.lit("UNK")).alias("country"),
        F.col("market_segment"),
        F.col("distribution_channel"),
        F.col("is_repeated_guest").cast("int").alias("is_repeated_guest"),
        F.col("previous_cancellations").cast("int").alias("previous_cancellations"),
        (
            F.col("previous_bookings_not_canceled").cast("int")
            .alias("previous_bookings_not_canceled")
        ),
        F.col("reserved_room_type"),
        F.col("assigned_room_type"),
        F.col("booking_changes").cast("int").alias("booking_changes"),
        F.col("deposit_type"),
        F.col("agent").cast("int").alias("agent"),
        F.col("company").cast("int").alias("company"),
        F.col("days_in_waiting_list").cast("int").alias("days_in_waiting_list"),
        F.col("customer_type"),
        F.col("adr").cast("double").alias("adr"),
        (
            F.col("required_car_parking_spaces").cast("int")
            .alias("required_car_parking_spaces")
        ),
        (
            F.col("total_of_special_requests").cast("int")
            .alias("total_of_special_requests")
        ),
        F.col("reservation_status"),
        (
            F.to_date("reservation_status_date", "yyyy-MM-dd")
            .alias("reservation_status_date")
        ),
    )
    month_map = F.create_map(
        *[
            v
            for i, month in enumerate(MONTHS, 1)
            for v in (F.lit(month.lower()), F.lit(i))
        ]
    )
    df = df.withColumn(
        "arrival_date_month_num",
        month_map[F.lower(F.col("arrival_date_month"))],
    )
    df = df.withColumn(
        "arrival_date",
        F.to_date(
            F.concat_ws(
                "-",
                F.col("arrival_date_year"),
                F.lpad(F.col("arrival_date_month_num").cast("string"), 2, "0"),
                F.lpad(F.col("arrival_date_day_of_month").cast("string"), 2, "0"),
            ),
            "yyyy-MM-dd",
        ),
    )
    return (
        df.withColumn(
            "total_nights",
            F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights"),
        )
        .withColumn(
            "total_guests",
            F.col("adults") + F.col("children") + F.col("babies"),
        )
        .withColumn(
            "is_family",
            ((F.col("children") > 0) | (F.col("babies") > 0)).cast("int"),
        )
        .withColumn("is_long_stay", (F.col("total_nights") > 7).cast("int"))
        .withColumn(
            "revenue",
            F.when(
                F.col("is_canceled") == 0,
                F.col("adr") * F.col("total_nights"),
            ).otherwise(0.0),
        )
        .withColumn(
            "booking_status",
            F.when(F.col("reservation_status") == "Check-Out", "CheckedOut")
            .when(F.col("reservation_status") == "No-Show", "NoShow")
            .otherwise(F.col("reservation_status")),
        )
    )


def clean_countries(countries: DataFrame) -> DataFrame:
    return countries.select(
        F.upper(F.trim("country_code")).alias("country"),
        F.trim("country_name").alias("country_name"),
        F.trim("continent").alias("continent"),
    )


def clean_hotels(hotels: DataFrame) -> DataFrame:
    return hotels.select(
        F.trim("hotel_name").alias("hotel"),
        F.trim("city").alias("hotel_city"),
        F.upper(F.trim("country_code")).alias("hotel_country_code"),
        F.col("star_rating").cast("int").alias("hotel_star_rating"),
        F.col("opened_year").cast("int").alias("hotel_opened_year"),
    )


def enrich_bookings(
    cleaned: DataFrame,
    countries: DataFrame,
    hotels: DataFrame,
) -> DataFrame:
    """Filter bookings and join the normalized reference tables."""
    valid = cleaned.filter((F.col("total_guests") != 0) & (F.col("adr") >= 0))
    return (
        valid.join(F.broadcast(countries), "country", "left")
        .join(F.broadcast(hotels), "hotel", "left")
        .fillna({"country_name": "Unknown", "continent": "Unknown"})
    )


def build_bookings_enriched(
    bookings: DataFrame,
    countries: DataFrame,
    hotels: DataFrame,
) -> DataFrame:
    """Transform bookings and enrich them without executing validation checks."""
    return enrich_bookings(clean_bookings(bookings), countries, hotels)


def run(spark: SparkSession, input_dir: Path, output_dir: Path) -> None:
    enriched = build_bookings_enriched(
        spark.read.parquet(str(input_dir / "bookings")),
        clean_countries(spark.read.parquet(str(input_dir / "countries"))),
        clean_hotels(spark.read.parquet(str(input_dir / "hotels"))),
    )
    (
        enriched.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("arrival_date_year", "arrival_date_month_num")
        .parquet(str(output_dir / "bookings_enriched"))
    )
    # A new write invalidates any report from a previous validation run.
    (output_dir / "quality_report.json").unlink(missing_ok=True)
