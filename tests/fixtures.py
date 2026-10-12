"""Shared small Spark fixtures for node and data-validation tests."""

import csv
import unittest
from pathlib import Path

from pyspark.sql import SparkSession, types as T

INPUT_FILES = {
    "bookings": "hotel_bookings.csv",
    "countries": "country_metadata.csv",
    "hotels": "hotel_metadata.csv",
}
INPUT_COLUMNS = {}
for name, file_name in INPUT_FILES.items():
    path = Path(__file__).resolve().parents[1] / "data/input" / file_name
    with path.open(encoding="utf-8", newline="") as handle:
        INPUT_COLUMNS[name] = next(csv.reader(handle))
BOOKING_COLUMNS = INPUT_COLUMNS["bookings"]

def booking(**overrides):
    row = {c: "0" for c in BOOKING_COLUMNS}
    row.update(hotel="City Hotel", arrival_date_year="2016", arrival_date_month="February",
               arrival_date_day_of_month="29", adults="2", children=None, country="PRT",
               stays_in_week_nights="2", adr="100", lead_time="10", market_segment="Direct",
               customer_type="Transient", distribution_channel="Direct",
               reservation_status="Check-Out", reservation_status_date="2016-03-02")
    row.update(overrides)
    return row


class SparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.Builder().master("local[2]").appName("pipeline-tests")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.ansi.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, name, rows):
        columns = INPUT_COLUMNS[name]
        schema = T.StructType([T.StructField(c, T.StringType(), True) for c in columns])
        return self.spark.createDataFrame([[r.get(c) for c in columns] for r in rows], schema)

    def references(self):
        return (self.frame("countries", [{"country_code": "PRT", "country_name": "Portugal",
                                           "continent": "Europe"}]),
                self.frame("hotels", [{"hotel_name": "City Hotel", "city": "Lisbon",
                                        "country_code": "PRT", "star_rating": "4", "opened_year": "2008"}]))

