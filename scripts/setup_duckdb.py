
from pathlib import Path

import duckdb


# Lokasi proyek dan file sumber
ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "analytics.duckdb"
SOURCE = ROOT / "data" / "processed" / "transformed_orders.parquet"

if not SOURCE.exists():
    raise FileNotFoundError(f"File sumber tidak ditemukan: {SOURCE}")

# Hubungkan ke database DuckDB.
# Database akan dibuat otomatis jika belum ada.
con = duckdb.connect(str(DB_PATH))

try:
    # Siapkan schema dan tabel dari data valid hasil transformasi.
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    con.execute(
        """
        CREATE OR REPLACE TABLE raw.raw_orders AS
        SELECT *
        FROM read_parquet(?)
        """,
        [str(SOURCE)],
    )

    # Periksa jumlah baris dan keunikan order_id.
    total_rows = con.execute(
        "SELECT COUNT(*) FROM raw.raw_orders"
    ).fetchone()[0]

    unique_orders = con.execute(
        "SELECT COUNT(DISTINCT order_id) FROM raw.raw_orders"
    ).fetchone()[0]

    print("=== DUCKDB SETUP ===")
    print(f"Database       : {DB_PATH}")
    print(f"Tabel          : raw.raw_orders")
    print(f"Sumber         : {SOURCE}")
    print(f"Jumlah baris   : {total_rows}")
    print(f"Order unik     : {unique_orders}")

    # Tampilkan struktur tabel.
    print("\n=== STRUKTUR TABEL ===")
    schema_info = con.execute(
        "DESCRIBE raw.raw_orders"
    ).fetchdf()
    print(schema_info.to_string(index=False))

    # Tampilkan contoh data.
    print("\n=== 5 BARIS PERTAMA ===")
    sample = con.execute(
        """
        SELECT order_id, customer_id, order_ts,
               product_category, quantity, net_amount, status
        FROM raw.raw_orders
        ORDER BY order_id
        LIMIT 5
        """
    ).fetchdf()
    print(sample.to_string(index=False))

    # Pastikan hasil pemuatan sesuai dengan sumber.
    if total_rows != 30:
        raise ValueError(
            f"Jumlah baris tidak sesuai: diharapkan 30, ditemukan {total_rows}"
        )

    if unique_orders != total_rows:
        raise ValueError("Ditemukan order_id yang tidak unik!")

    print("\nPemeriksaan berhasil: seluruh baris berasal dari data valid.")

finally:
    con.close()
