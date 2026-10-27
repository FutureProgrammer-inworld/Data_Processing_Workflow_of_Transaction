
"""Transform validated orders and store them as Parquet."""

import json
from pathlib import Path

import pandas as pd


def main():
    source = Path("data/processed/valid_orders.parquet")
    output = Path("data/processed/transformed_orders.parquet")
    summary_path = Path("data/processed/transformation_summary.json")

    if not source.exists():
        raise FileNotFoundError(
            f"Input tidak ditemukan: {source}. "
            "Jalankan validasi terlebih dahulu."
        )

    df = pd.read_parquet(source)

    # Standardisasi timestamp ke UTC.
    df["order_ts"] = pd.to_datetime(
        df["order_ts"],
        utc=True,
        errors="coerce",
    )

    if df["order_ts"].isna().any():
        invalid_count = int(df["order_ts"].isna().sum())
        raise ValueError(
            f"Ditemukan {invalid_count} timestamp yang tidak valid "
            "di dalam valid_orders.parquet."
        )

    # Pastikan kolom numerik menggunakan tipe yang konsisten.
    df["quantity"] = pd.to_numeric(df["quantity"], errors="raise").astype("int64")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="raise").astype("float64")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="raise").astype("float64")

    # Hitung metrik transaksi.
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])

    # Simpan ke Parquet tanpa mengubah raw layer.
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output, index=False)

    summary = {
        "source": str(source),
        "output": str(output),
        "record_count": len(df),
        "unique_order_ids": int(df["order_id"].nunique()),
        "duplicate_order_ids": int(df["order_id"].duplicated().sum()),
        "gross_amount_total": float(df["gross_amount"].sum()),
        "net_amount_total": float(df["net_amount"].sum()),
        "timestamp_timezone": "UTC",
        "numeric_types": {
            "quantity": str(df["quantity"].dtype),
            "unit_price": str(df["unit_price"].dtype),
            "discount_pct": str(df["discount_pct"].dtype),
            "gross_amount": str(df["gross_amount"].dtype),
            "net_amount": str(df["net_amount"].dtype),
        },
    }

    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print("=== TRANSFORMATION SUMMARY ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print("\n=== SAMPLE RESULTS ===")
    print(
        df[
            [
                "order_id",
                "quantity",
                "unit_price",
                "discount_pct",
                "gross_amount",
                "net_amount",
                "order_ts",
            ]
        ].head().to_string(index=False)
    )


if __name__ == "__main__":
    main()
