
"""Incremental upsert of transformed orders with a persistent checkpoint."""

import json
from pathlib import Path

import pandas as pd


SOURCE = Path("data/processed/transformed_orders.parquet")
TARGET = Path("data/warehouse/orders.parquet")
CHECKPOINT = Path("data/checkpoints/orders_watermark.json")
LOOKBACK_DAYS = 1


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(f"Input tidak ditemukan: {SOURCE}")

    source_df = pd.read_parquet(SOURCE)
    source_df["order_ts"] = pd.to_datetime(
        source_df["order_ts"], utc=True, errors="raise"
    )

    if source_df["order_id"].isna().any():
        raise ValueError("order_id tidak boleh kosong.")

    if source_df["order_id"].duplicated().any():
        raise ValueError("Source memiliki order_id duplikat.")

    old_watermark = None
    if CHECKPOINT.exists():
        checkpoint_data = json.loads(
            CHECKPOINT.read_text(encoding="utf-8")
        )
        old_watermark = pd.to_datetime(
            checkpoint_data["watermark"], utc=True
        )

    if old_watermark is None:
        # Eksekusi pertama: proses seluruh source.
        candidates = source_df.copy()
    else:
        # Baca ulang lookback window untuk mengakomodasi keterlambatan.
        cutoff = old_watermark - pd.Timedelta(days=LOOKBACK_DAYS)
        candidates = source_df.loc[
            source_df["order_ts"] >= cutoff
        ].copy()

    if TARGET.exists():
        existing_df = pd.read_parquet(TARGET)
        existing_df["order_ts"] = pd.to_datetime(
            existing_df["order_ts"], utc=True, errors="raise"
        )
    else:
        existing_df = pd.DataFrame(columns=source_df.columns)

    # Upsert berdasarkan kunci bisnis order_id.
    combined = pd.concat(
        [existing_df, candidates],
        ignore_index=True,
    )
    combined = combined.drop_duplicates(
        subset=["order_id"], keep="last"
    )
    combined = combined.sort_values(
        "order_ts"
    ).reset_index(drop=True)

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)

    # Simpan tujuan dahulu. Checkpoint baru ditulis setelah sukses.
    combined.to_parquet(TARGET, index=False)

    max_timestamp = source_df["order_ts"].max()
    if old_watermark is not None:
        max_timestamp = max(max_timestamp, old_watermark)

    checkpoint_data = {
        "watermark": max_timestamp.isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "business_key": "order_id",
    }
    CHECKPOINT.write_text(
        json.dumps(checkpoint_data, indent=2),
        encoding="utf-8",
    )

    print("=== UPSERT SUMMARY ===")
    print(f"source_records={len(source_df)}")
    print(f"candidate_records={len(candidates)}")
    print(f"target_records={len(combined)}")
    print(f"unique_order_ids={combined['order_id'].nunique()}")
    print(f"duplicate_order_ids={combined['order_id'].duplicated().sum()}")
    print(f"previous_watermark={old_watermark}")
    print(f"new_watermark={max_timestamp.isoformat()}")
    print(f"target={TARGET}")
    print(f"checkpoint={CHECKPOINT}")


if __name__ == "__main__":
    main()
