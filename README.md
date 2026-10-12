# BeFly hotel-booking pipeline

PySpark pipeline that processes hotel bookings through Bronze, Silver, and Gold using one Spark session. Transformations live in `src/transform/`; data-quality checks live in `src/validation/` and run between stages.

## Run

Requires Docker, GNU Make, and uv on the host. Run from the project root:

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

The country and hotel metadata CSVs are included. `hotel_bookings.csv` is downloaded from [Kaggle](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand), version 1, and ignored by Git. Existing files are reused; downloads are checksum-verified.

```bash
make download                   # download separately
make download FORCE=1  # replace an existing bookings CSV
```

If Kaggle requests authentication, configure `KAGGLE_USERNAME` and `KAGGLE_KEY` or `~/.kaggle/kaggle.json` on the host.

## Output

All tables are Snappy-compressed Parquet; each run overwrites them.

- `bronze/`: raw bookings, countries, and hotels.
- `silver/bookings_enriched/`: cleaned bookings joined with metadata.
- `silver/quality_report.json`: validation metrics.
- `gold/`: `revenue_by_hotel_month`, `cancellation_by_segment`, and `top_countries_by_revenue`.

Failed validation stops the pipeline before the next stage. Previously written output remains on disk.
