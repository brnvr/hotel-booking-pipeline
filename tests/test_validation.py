"""Test data-quality rules separately from the node transformations."""

import json
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from pyspark.sql import functions as F

from transform import silver as silver_transform
from validation import bronze, silver
from tests.fixtures import SparkTestCase, booking


class ValidationTests(SparkTestCase):
    def frames(self, rows):
        countries, hotels = self.references()
        cleaned = silver_transform.clean_bookings(self.frame("bookings", rows))
        enriched = silver_transform.enrich_bookings(
            cleaned,
            silver_transform.clean_countries(countries),
            silver_transform.clean_hotels(hotels),
        )
        return cleaned, enriched

    def test_bronze_rejects_normalized_duplicate_and_missing_reference_keys(self):
        cases = [
            ("countries", "country_code", "prt ", "not unique"),
            ("hotels", "hotel_name", " City Hotel ", "not unique"),
            ("countries", "country_code", None, "missing values"),
            ("countries", "country_code", "  ", "missing values"),
            ("hotels", "hotel_name", None, "missing values"),
            ("hotels", "hotel_name", "  ", "missing values"),
        ]
        for name, key, value, message in cases:
            with self.subTest(name=name, value=value):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    countries, hotels = self.references()
                    frames = {"countries": countries, "hotels": hotels}
                    changed = frames[name].withColumn(key, F.lit(value))
                    frames[name] = frames[name].union(changed)
                    for table, frame in frames.items():
                        frame.write.parquet(str(root / table))
                    with self.assertRaisesRegex(ValueError, message):
                        bronze.test(self.spark, root)

    def test_quality_metrics_and_duplicate_bookings(self):
        cleaned, enriched = self.frames([
            booking(), booking(), booking(adults="0", adr="-1"),
            booking(adr="-1"), booking(country=None),
        ])
        metrics = silver.validate_bookings(cleaned, enriched)
        self.assertEqual(metrics, {
            "input_rows": 5,
            "removed_no_guests": 1,
            "removed_negative_adr": 1,
            "output_rows": 3,
            "unmatched_country_rows": 1,
            "unmatched_hotel_rows": 0,
        })

    def test_silver_rejects_invalid_input_even_when_filtered_out(self):
        cases = [
            {"adr": None}, {"arrival_date_month": "Unknown"},
            {"is_canceled": "2"}, {"adr": "NaN"},
            {"adults": "0", "arrival_date_month": "Unknown"},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides):
                cleaned, enriched = self.frames([booking(**overrides)])
                with self.assertRaisesRegex(ValueError, "required fields"):
                    silver.validate_bookings(cleaned, enriched)

    def test_silver_rejects_invalid_output_and_row_count_mismatch(self):
        cleaned, enriched = self.frames([booking()])
        with self.assertRaisesRegex(ValueError, "required fields"):
            silver.validate_bookings(cleaned, enriched.withColumn("adr", F.lit(-1)))
        with self.assertRaisesRegex(ValueError, "does not reconcile"):
            silver.validate_bookings(cleaned, enriched.union(enriched))

    def test_failed_validation_removes_stale_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bronze_dir = root / "bronze"
            silver_dir = root / "silver"
            self.frame("bookings", [booking(adr=None)]).write.parquet(
                str(bronze_dir / "bookings")
            )
            _, valid_output = self.frames([booking()])
            valid_output.write.parquet(str(silver_dir / "bookings_enriched"))
            report = silver_dir / "quality_report.json"
            report.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "required fields"):
                silver.test(self.spark, bronze_dir, silver_dir)
            self.assertFalse(report.exists())


class ValidationLifecycleTests(unittest.TestCase):
    def test_report_is_written_only_on_success_and_caches_are_released(self):
        for fails in (False, True):
            with self.subTest(fails=fails), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp)
                report = output / "quality_report.json"
                report.write_text("stale report", encoding="utf-8")
                spark = Mock()
                cleaned = Mock()
                enriched = spark.read.parquet.return_value.cache.return_value
                metrics = {"input_rows": 1, "output_rows": 1}
                with (
                    patch.object(silver, "clean_bookings") as clean,
                    patch.object(silver, "validate_bookings") as validate,
                ):
                    clean.return_value.cache.return_value = cleaned
                    if fails:
                        validate.side_effect = ValueError("bad data")
                        with self.assertRaisesRegex(ValueError, "bad data"):
                            silver.test(spark, Path("bronze"), output)
                        self.assertFalse(report.exists())
                    else:
                        validate.return_value = metrics
                        self.assertEqual(
                            silver.test(spark, Path("bronze"), output), metrics,
                        )
                        self.assertEqual(json.loads(report.read_text()), metrics)
                    validate.assert_called_once_with(cleaned, enriched)
                    cleaned.unpersist.assert_called_once_with()
                    enriched.unpersist.assert_called_once_with()

    def test_output_read_failure_releases_cleaned_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            spark = Mock()
            spark.read.parquet.side_effect = [Mock(), ValueError("bad output")]
            cleaned = Mock()
            with patch.object(silver, "clean_bookings") as clean:
                clean.return_value.cache.return_value = cleaned
                with self.assertRaisesRegex(ValueError, "bad output"):
                    silver.test(spark, Path("bronze"), Path(tmp))
            cleaned.unpersist.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
