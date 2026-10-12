"""Validate Silver fields and row reconciliation against cleaned Bronze data."""

import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from transform.silver import clean_bookings

logger = logging.getLogger(__name__)


def check_required_fields(bookings: DataFrame, *, output: bool = False) -> None:
    invalid = (
        F.col("arrival_date").isNull()
        | F.col("reservation_status_date").isNull()
        | ~F.col("is_canceled").isin(0, 1)
        | F.col("is_canceled").isNull()
        | F.col("total_guests").isNull()
        | F.col("adr").isNull()
        | F.isnan("adr")
        | F.col("total_nights").isNull()
        | (F.col("total_guests") < 0)
        | (F.col("total_nights") < 0)
    )
    if output:
        invalid = invalid | (F.col("total_guests") == 0) | (F.col("adr") < 0)
    if bookings.filter(invalid).limit(1).count():
        raise ValueError("Invalid required fields in bookings")


def validate_bookings(cleaned: DataFrame, enriched: DataFrame) -> dict[str, int]:
    """Check cleaned input and persisted output, then reconcile quality metrics."""
    # Inspect all cleaned input rows, including rows removed during transformation.
    check_required_fields(cleaned)
    check_required_fields(enriched, output=True)
    metrics = {
        "input_rows": cleaned.count(),
        "removed_no_guests": cleaned.filter(F.col("total_guests") == 0).count(),
        "removed_negative_adr": (
            cleaned.filter(F.col("total_guests") != 0)
            .filter(F.col("adr") < 0)
            .count()
        ),
        "output_rows": enriched.count(),
    }
    expected = (
        metrics["input_rows"]
        - metrics["removed_no_guests"]
        - metrics["removed_negative_adr"]
    )
    if metrics["output_rows"] != expected:
        raise ValueError("Silver row count does not reconcile with cleaning filters")
    metrics["unmatched_country_rows"] = (
        enriched.filter(F.col("country_name") == "Unknown").count()
    )
    metrics["unmatched_hotel_rows"] = (
        enriched.filter(F.col("hotel_city").isNull()).count()
    )
    return metrics


def test(spark: SparkSession, input_dir: Path, output_dir: Path) -> dict[str, int]:
    """Validate Silver against Bronze and write the report only on success."""
    report = output_dir / "quality_report.json"
    report.unlink(missing_ok=True)
    cleaned = clean_bookings(
        spark.read.parquet(str(input_dir / "bookings"))
    ).cache()
    enriched = None
    try:
        enriched = spark.read.parquet(
            str(output_dir / "bookings_enriched")
        ).cache()
        metrics = validate_bookings(cleaned, enriched)
        report.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        logger.info("Silver quality metrics: %s", json.dumps(metrics, sort_keys=True))
        return metrics
    finally:
        cleaned.unpersist()
        if enriched is not None:
            enriched.unpersist()
