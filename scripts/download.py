"""Fetch the pinned hotel-booking CSV without adding download dependencies to Spark."""

import argparse
import hashlib
from pathlib import Path
import sys
import tempfile

DATASET = "jessemostipak/hotel-booking-demand/versions/1"
FILE_NAME = "hotel_bookings.csv"
EXPECTED_SHA256 = "7c2ae42a7353905ea136e5c2287f17c92c5435826598bfbb8491c6f0c7b1fc06"


def download(input_dir: Path, *, force: bool = False) -> Path:
    destination = input_dir / FILE_NAME
    if destination.is_file() and destination.stat().st_size and not force:
        print(f"Using existing {destination}")
        return destination

    # The host-side uv command supplies KaggleHub only when a download is needed.
    import kagglehub

    print(f"Downloading {DATASET}/{FILE_NAME}")
    source = Path(kagglehub.dataset_download(
        DATASET, path=FILE_NAME, force_download=force,
    ))
    # Normalize line endings before checking the known dataset checksum.
    content = source.read_bytes().replace(b"\r\n", b"\n")
    if hashlib.sha256(content).hexdigest() != EXPECTED_SHA256:
        raise ValueError("Downloaded bookings CSV does not match the expected checksum")

    input_dir.mkdir(parents=True, exist_ok=True)
    # Publish only a complete, verified CSV; a failed download preserves existing data.
    with tempfile.TemporaryDirectory(prefix=".bookings-", dir=input_dir) as temporary:
        staged = Path(temporary) / FILE_NAME
        staged.write_bytes(content)
        staged.replace(destination)
    print(f"Saved {destination}")
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    try:
        download(args.input_dir, force=args.force)
    except Exception as error:
        # Do not echo API errors that could contain credential-bearing URLs.
        print(
            f"Download failed ({type(error).__name__}). Check network access and "
            "Kaggle credentials, or manually place hotel_bookings.csv in the input "
            "directory. A checksum failure means the file differs from dataset v1.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
