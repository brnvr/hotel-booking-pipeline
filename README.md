# PySpark hotel-booking pipeline

PySpark pipeline that processes hotel bookings through Bronze, Silver, and Gold using one Spark session. Transformations live in `src/transform/`; data-quality checks live in `src/validation/` and run between stages.

## Run

Requires Docker, GNU Make, and uv on the host. The container provides Python 3.12, PySpark 3.5.3, and Java 17; `make setup` installs the project and locked dependencies with uv. Run from the project root:

```bash
make setup     # download missing bookings and build the image
make run       # execute the pipeline
make unittest  # run all tests in the container
```

`run` and `unittest` include `setup`. Input defaults to `data/input`; output is saved to your machine under `data/output`.

Override paths or Spark's master when needed:

```bash
make run INPUT_DIR=/path/to/input OUTPUT_DIR=/path/to/output MASTER='local[4]'
```

## Input

The complete dataset is processed: 119,390 bookings, with no sampling or year/hotel restriction. The country and hotel metadata CSVs are included. `hotel_bookings.csv` is downloaded from [Kaggle](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand), version 1, and ignored by Git. Existing files are reused; downloads are checksum-verified. The download script lives in `scripts/download.py`.

```bash
make download           # download separately
make download FORCE=1   # replace an existing bookings CSV
```

If Kaggle requests authentication, configure `KAGGLE_USERNAME` and `KAGGLE_KEY` or `~/.kaggle/kaggle.json` on the host.

## Output

All tables are Snappy-compressed Parquet; each run overwrites them.

- `bronze/`: raw bookings, countries, and hotels.
- `silver/bookings_enriched/`: cleaned bookings joined with metadata.
- `silver/quality_report.json`: validation metrics.
- `gold/`: the three aggregate tables described below.

Failed validation stops the pipeline before the next stage. Previously written output remains on disk.

## Layer decisions

**Bronze** reads every CSV column as a string (`inferSchema=False`) and handles literal `NULL` as null. This deliberately differs from the assignment's request for schema inference or an explicit schema at ingestion: in this implementation, numeric and date conversions are deferred to Silver. Keeping source fields as strings avoids inferred types changing their representation, such as losing leading zeros. Bookings are partitioned by arrival year.

**Silver** trims strings, casts numeric fields, parses dates, and builds the arrival date using a month-name mapping. Missing children become `0`; missing guest country becomes `UNK`. It filters bookings with no guests and negative ADR, then LEFT JOINs country and hotel metadata. Derived columns include total nights, total guests, family/long-stay flags, booking status, and revenue (`adr × total_nights` for non-cancelled bookings). Output is partitioned by arrival year and month.

Bronze validation checks normalized reference keys for missing values and duplicates before the joins. Silver validation checks required fields and reconciles input/output rows. With the full dataset, the filters remove 180 bookings without guests and 1 with negative ADR, leaving 119,209 bookings; these counts are recorded in `silver/quality_report.json`.

**Gold** answers the following business questions:

| Table | Business question |
| --- | --- |
| `revenue_by_hotel_month` | How much revenue and how many nights does each hotel sell per month, and what is its cancellation rate? |
| `cancellation_by_segment` | Which market segments, customer types, and channels cancel most, and how far ahead do they book? |
| `top_countries_by_revenue` | Which 20 guest countries generate the most revenue from non-cancelled bookings? |

Revenue is an estimate based on ADR and nights. Nights sold measure booking volume; calculating an occupancy rate would also require room inventory.

## Cloud adaptation

On AWS, the three layers could use separate S3 prefixes with Parquet and year/month partitions. Glue jobs would run the transformations, Step Functions would coordinate transformation and validation steps, and the Glue Data Catalog would expose tables to Athena. Validation failures would stop downstream processing, with quality metrics and alerts in CloudWatch. IAM, KMS, and Lake Formation would control access, encryption, and governance.

On Databricks, the same layers could use Delta tables in cloud storage, with Jobs coordinating execution and Unity Catalog managing access and lineage. Another option would be to convert the transformation functions into Spark Declarative Pipelines (SDP) models, letting the framework manage dependencies and execution. Production ingestion would need batch identifiers, incremental processing, and transactional publication so retries do not duplicate data or expose incomplete batches.
