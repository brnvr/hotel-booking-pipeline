"""Test transformations and persisted tables with small Spark fixtures."""

import csv
import json
import tempfile
from pathlib import Path
import unittest

from pyspark.sql import types as T

from transform.gold import (
    build_cancellation_by_segment,
    build_revenue_by_hotel_month,
    build_top_countries_by_revenue,
    run as run_gold,
)
from transform.bronze import run as run_bronze
from validation.bronze import test as test_bronze
from validation.silver import test as test_silver
from transform.silver import (
    build_bookings_enriched,
    clean_countries,
    clean_hotels,
    run as run_silver,
)
from tests.fixtures import INPUT_FILES, INPUT_COLUMNS, SparkTestCase, booking


class TransformTests(SparkTestCase):
    def test_cleaning_and_gold_metrics(self):
        countries, hotels = self.references()
        rows = [booking(), booking(is_canceled="1", adr="300", reservation_status="Canceled"),
                booking(country=None, children="1.0", adr="50", stays_in_week_nights="8"),
                booking(adults="0"), booking(adr="-1")]
        silver = build_bookings_enriched(
            self.frame("bookings", rows),
            clean_countries(countries),
            clean_hotels(hotels))
        family = silver.filter("country = 'UNK'").first()
        self.assertEqual(str(family.arrival_date), "2016-02-29")
        self.assertEqual((family.children, family.is_family, family.is_long_stay), (1, 1, 1))
        self.assertEqual(family.country_name, "Unknown")
        self.assertEqual(family.hotel_star_rating, 4)
        monthly = build_revenue_by_hotel_month(silver).first()
        self.assertEqual((monthly.total_bookings, monthly.effective_bookings,
                          monthly.cancelled_bookings), (3, 2, 1))
        self.assertEqual(monthly.total_revenue, 600)
        self.assertEqual(monthly.total_nights_sold, 10)
        self.assertEqual(monthly.avg_adr, 75)
        self.assertAlmostEqual(monthly.cancellation_rate, 1 / 3)
        self.assertEqual(build_top_countries_by_revenue(silver).first().country, "UNK")
        self.assertEqual(build_cancellation_by_segment(silver).first().avg_lead_time, 10)

    def test_keeps_duplicate_bookings(self):
        countries, hotels = self.references()
        result = build_bookings_enriched(
            self.frame("bookings", [booking(), booking()]),
            clean_countries(countries),
            clean_hotels(hotels),
        )
        self.assertEqual(result.count(), 2)

    def test_end_to_end_csv_parquet_and_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            countries, hotels = self.references()
            input_rows = {"bookings": [booking()],
                           "countries": [r.asDict() for r in countries.collect()],
                           "hotels": [r.asDict() for r in hotels.collect()]}
            for name, rows in input_rows.items():
                with (root / INPUT_FILES[name]).open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=INPUT_COLUMNS[name])
                    writer.writeheader()
                    writer.writerows([{c: "NULL" if v is None else v for c, v in r.items()} for r in rows])
            output = root / "output"
            run_bronze(self.spark, root, output / "bronze")
            test_bronze(self.spark, output / "bronze")
            run_silver(self.spark, output / "bronze", output / "silver")
            self.assertFalse((output / "silver" / "quality_report.json").exists())
            metrics = test_silver(self.spark, output / "bronze", output / "silver")
            self.assertEqual(metrics["output_rows"], 1)
            self.assertEqual(json.loads((output / "silver" / "quality_report.json").read_text()), metrics)
            run_gold(self.spark, output / "silver", output / "gold")
            bronze = self.spark.read.parquet(str(root / "output" / "bronze" / "bookings"))
            self.assertIsNone(bronze.first().children)
            monthly_path = root / "output" / "gold" / "revenue_by_hotel_month"
            self.assertEqual(self.spark.read.parquet(str(monthly_path)).count(), 1)
            silver_path = root / "output" / "silver" / "bookings_enriched"
            silver = self.spark.read.parquet(str(silver_path))
            self.assertEqual(silver.first().children, 0)
            self.assertEqual(silver.schema["children"].dataType, T.IntegerType())
            self.assertEqual(silver.schema["arrival_date"].dataType, T.DateType())

    def test_reads_and_writes_all_columns_as_strings(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / "output"
            with (root / "hotel_metadata.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=INPUT_COLUMNS["hotels"] + ["external_code", "effective_date"])
                writer.writeheader()
                writer.writerow({"hotel_name": "City Hotel", "city": "Lisbon", "country_code": "PRT",
                                 "star_rating": "4", "opened_year": "2008",
                                 "external_code": "00123", "effective_date": "2026-10-10"})
            for name, rows in {
                "bookings": [booking()],
                "countries": [{"country_code": "PRT", "country_name": "Portugal", "continent": "Europe"}],
            }.items():
                with (root / INPUT_FILES[name]).open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=INPUT_COLUMNS[name])
                    writer.writeheader()
                    writer.writerows(rows)
            run_bronze(self.spark, root, destination)
            result = self.spark.read.parquet(str(destination / "hotels")).first()
            self.assertEqual(result.star_rating, "4")
            self.assertEqual(result.external_code, "00123")
            self.assertEqual(result.effective_date, "2026-10-10")
            ingested = self.spark.read.parquet(str(destination / "hotels"))
            self.assertTrue(all(isinstance(field.dataType, T.StringType) for field in ingested.schema))
            self.assertFalse(any((destination / "hotels").glob("arrival_date_year=*")))


if __name__ == "__main__":
    unittest.main()
