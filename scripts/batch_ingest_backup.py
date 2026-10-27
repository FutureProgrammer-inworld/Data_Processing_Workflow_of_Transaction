"""Educational batch ingestion: CSV source simulates a paginated REST API."""
import argparse, json, time
from pathlib import Path
import pandas as pd

def get_page(df, page, page_size, retries=3):
    # In a real API adapter, put requests.get(), timeout, HTTP status checks,
    # Retry-After and exponential backoff inside this function.
    for attempt in range(retries):
        try:
            start = page * page_size
            return df.iloc[start:start + page_size].copy()
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/orders.csv")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--page-size", type=int, default=25)
    ap.add_argument("--checkpoint", default="data/watermark.json")
    args = ap.parse_args()
    source = pd.read_csv(args.source)
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    checkpoint = Path(args.checkpoint)
    last_ts = None
    if checkpoint.exists():
        last_ts = json.loads(checkpoint.read_text()).get("watermark")
    if "order_ts" in source.columns:
        source["order_ts"] = pd.to_datetime(source["order_ts"], errors="coerce", utc=True)
    pages = []
    page = 0
    while True:
        chunk = get_page(source, page, args.page_size)
        if chunk.empty:
            break
        print(f"page={page} rows={len(chunk)}")
        pages.append(chunk)
        page += 1
    all_rows = pd.concat(pages, ignore_index=True) if pages else source.iloc[0:0].copy()
    if last_ts and "order_ts" in all_rows.columns:
        all_rows = all_rows[all_rows["order_ts"] > pd.Timestamp(last_ts)]
    if "order_ts" in all_rows.columns:
        all_rows = all_rows.sort_values("order_ts")
    target = out / "orders.parquet"
    if target.exists():
        old = pd.read_parquet(target)
        all_rows = pd.concat([old, all_rows], ignore_index=True)
    if "order_id" in all_rows.columns:
        all_rows = all_rows.drop_duplicates("order_id", keep="last")
    all_rows.to_parquet(target, index=False)
    if not all_rows.empty and "order_ts" in all_rows.columns:
        watermark = all_rows["order_ts"].max().isoformat()
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        checkpoint.write_text(json.dumps({"watermark": watermark}, indent=2))
    print(f"output={target} total_unique_rows={len(all_rows)}")
    print("Note: this educational adapter paginates a local CSV; replace get_page with a real requests adapter for an actual REST API.")

if __name__ == "__main__":
    main()
