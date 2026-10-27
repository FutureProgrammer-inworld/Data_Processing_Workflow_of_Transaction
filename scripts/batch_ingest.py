
"""Batch ingestion: paginated CSV source simulating a REST API."""
import argparse
import json
import time
from pathlib import Path

import pandas as pd


def get_page(
    df,
    page,
    page_size,
    retries=3,
    fail_first_attempts=0,
):
    """Read one page and optionally simulate temporary failures."""
    start = page * page_size

    for attempt in range(retries):
        try:
            # Testing aid: simulate temporary errors before success.
            if attempt < fail_first_attempts:
                raise RuntimeError("Simulated temporary API failure")

            return df.iloc[start:start + page_size].copy()

        except Exception as exc:
            if attempt == retries - 1:
                raise

            wait_seconds = 2 ** attempt
            print(
                f"page={page} attempt={attempt + 1} "
                f"error={exc}; retry in {wait_seconds}s"
            )
            time.sleep(wait_seconds)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/orders.csv")
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--page-size", type=int, default=25)
    parser.add_argument("--checkpoint", default="data/watermark.json")
    parser.add_argument(
        "--simulate-failures",
        type=int,
        default=0,
        help="Number of temporary failures simulated per page",
    )
    args = parser.parse_args()

    if args.page_size <= 0:
        parser.error("--page-size must be greater than zero")

    if args.simulate_failures < 0:
        parser.error("--simulate-failures cannot be negative")

    if args.simulate_failures >= 3:
        parser.error(
            "--simulate-failures must be less than 3 "
            "because the retry limit is 3 attempts"
        )

    source = pd.read_csv(args.source)

    if "order_id" not in source.columns:
        raise ValueError("Source must contain order_id")

    if "order_ts" in source.columns:
        source["order_ts"] = pd.to_datetime(
            source["order_ts"], errors="coerce", utc=True
        )

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    checkpoint = Path(args.checkpoint)
    target = out_dir / "orders.parquet"

    # Read all source pages; the CSV acts as a paginated API.
    pages = []
    page = 0
    rows_read = 0

    while True:
        chunk = get_page(
            source,
            page,
            args.page_size,
            retries=3,
            fail_first_attempts=args.simulate_failures,
        )

        if chunk.empty:
            break

        rows_read += len(chunk)
        print(f"page={page} rows={len(chunk)}")
        pages.append(chunk)
        page += 1

    incoming = (
        pd.concat(pages, ignore_index=True)
        if pages
        else source.iloc[0:0].copy()
    )

    # Keep the latest occurrence of each order_id in the incoming batch.
    incoming = incoming.drop_duplicates("order_id", keep="last")

    # Load existing raw data, if present.
    if target.exists():
        existing = pd.read_parquet(target)
    else:
        existing = incoming.iloc[0:0].copy()

    existing_ids = set(existing["order_id"].dropna().astype(str))
    incoming_ids = incoming["order_id"].astype("string")

    # Use order_id for sink deduplication so equal or late timestamps
    # do not cause unseen orders to be skipped.
    new_rows = incoming.loc[
        incoming_ids.notna() & ~incoming_ids.isin(existing_ids)
    ].copy()

    combined = pd.concat([existing, new_rows], ignore_index=True)
    combined = combined.drop_duplicates("order_id", keep="last")

    combined.to_parquet(target, index=False)

    # Watermark records the greatest valid timestamp observed in the sink.
    # It is not used alone to decide whether an order is new.
    watermark = None
    if "order_ts" in combined.columns:
        valid_ts = pd.to_datetime(
            combined["order_ts"], errors="coerce", utc=True
        ).dropna()

        if not valid_ts.empty:
            watermark = valid_ts.max().isoformat()

    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(
        json.dumps({"watermark": watermark}, indent=2),
        encoding="utf-8",
    )

    # Full contract validation/rejection is performed in the next stage.
    rejected = 0
    successful = rows_read
    added = len(new_rows)

    print("\n=== INGESTION SUMMARY ===")
    print(f"rows_read={rows_read}")
    print(f"rows_successful={successful}")
    print(f"rows_rejected_at_ingestion={rejected}")
    print(f"rows_added={added}")
    print(f"total_unique_rows={len(combined)}")
    print(f"output={target}")
    print(f"watermark={watermark}")
    print(
        "Note: this is a local CSV pagination simulation, "
        "not a live REST API."
    )


if __name__ == "__main__":
    main()
