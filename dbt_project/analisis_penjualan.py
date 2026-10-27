
import duckdb
import json
from pathlib import Path
from collections import Counter

# ============================================================
# KONFIGURASI
# ============================================================

DB_PATH = r"D:\Sem 5\Data Processing Workflow\tugas\praktikum\UTS\analytics.duckdb"
REJECTED_PATH = Path(r"D:\Sem 5\Data Processing Workflow\tugas\praktikum\UTS\data\rejected\rejected_orders.jsonl")

# Koneksi ke database DuckDB
con = duckdb.connect(str(DB_PATH), read_only=True)


def tampilkan_hasil(judul, query):
    """Menjalankan query SQL dan menampilkan hasilnya."""
    print("\n" + "=" * 70)
    print(judul)
    print("=" * 70)

    hasil = con.execute(query).df()
    print(hasil.to_string(index=False))
    return hasil


# ============================================================
# 1. BERAPA NILAI PENJUALAN BERSIH PER HARI?
# ============================================================

sql_penjualan_harian = """
SELECT
    CAST(order_ts AS DATE) AS tanggal,
    ROUND(SUM(net_amount), 2) AS penjualan_bersih
FROM raw.raw_orders
GROUP BY CAST(order_ts AS DATE)
ORDER BY tanggal;
"""

tampilkan_hasil(
    "1. PENJUALAN BERSIH PER HARI",
    sql_penjualan_harian
)


# ============================================================
# 2. KATEGORI PRODUK MANA YANG PENJUALAN BERSIHNYA TERTINGGI?
# ============================================================


sql_penjualan_kategori = """
SELECT
    product_category,
    ROUND(SUM(net_amount), 2) AS penjualan_bersih
FROM raw.raw_orders
GROUP BY product_category
ORDER BY penjualan_bersih DESC;
"""


hasil_kategori = tampilkan_hasil(
    "2. PENJUALAN BERSIH MENURUT KATEGORI PRODUK",
    sql_penjualan_kategori
)

if not hasil_kategori.empty:
    tertinggi = hasil_kategori.iloc[0]
    print(
        f"\nKategori dengan penjualan bersih tertinggi: "
        f"{tertinggi['product_category']} "
        f"(nilai: {tertinggi['penjualan_bersih']})"
    )


# ============================================================
# 3. BAGAIMANA JUMLAH DAN NILAI PESANAN MENURUT STATUS?
# ============================================================

sql_status_pesanan = """
SELECT
    status,
    COUNT(DISTINCT order_id) AS jumlah_pesanan,
    ROUND(
        SUM(quantity * unit_price * (1 - COALESCE(discount_pct, 0))),
        2
    ) AS nilai_penjualan_bersih
FROM raw.raw_orders
GROUP BY status
ORDER BY jumlah_pesanan DESC;
"""

tampilkan_hasil(
    "3. JUMLAH DAN NILAI PESANAN MENURUT STATUS",
    sql_status_pesanan
)


# ============================================================
# 4. METODE PEMBAYARAN MANA YANG PALING SERING DIGUNAKAN?
# ============================================================

sql_metode_pembayaran = """
SELECT
    payment_method,
    COUNT(DISTINCT order_id) AS jumlah_pesanan
FROM raw.raw_orders
GROUP BY payment_method
ORDER BY jumlah_pesanan DESC;
"""

hasil_pembayaran = tampilkan_hasil(
    "4. FREKUENSI METODE PEMBAYARAN",
    sql_metode_pembayaran
)

if not hasil_pembayaran.empty:
    terpopuler = hasil_pembayaran.iloc[0]
    print(
        f"\nMetode pembayaran paling sering digunakan: "
        f"{terpopuler['payment_method']} "
        f"({terpopuler['jumlah_pesanan']} pesanan)"
    )


# ============================================================
# 5. BERAPA RECORD DITOLAK DAN APA PENYEBAB TERBESARNYA?
# ============================================================

print("\n" + "=" * 70)
print("5. RECORD YANG DITOLAK OLEH DATA CONTRACT")
print("=" * 70)

if REJECTED_PATH.exists():
    records = []

    with open(REJECTED_PATH, "r", encoding="utf-8") as file:
        for baris in file:
            baris = baris.strip()
            if baris:
                try:
                    records.append(json.loads(baris))
                except json.JSONDecodeError:
                    print("Peringatan: ditemukan baris JSON yang tidak valid.")

    print(f"Jumlah record ditolak: {len(records)}")

    penyebab = Counter()

    for record in records:
        # Mendukung beberapa kemungkinan nama field alasan.
        alasan = (
            record.get("reason")
            or record.get("rejection_reason")
            or record.get("error")
            or record.get("errors")
            or record.get("reason_code")
            or record.get("validation_errors")
        )

        if alasan is None:
            alasan = "Alasan tidak tercantum"

        if isinstance(alasan, list):
            for item in alasan:
                penyebab[str(item)] += 1
        elif isinstance(alasan, dict):
            for key, value in alasan.items():
                penyebab[f"{key}: {value}"] += 1
        else:
            penyebab[str(alasan)] += 1

    print("\nFrekuensi penyebab penolakan:")

    for alasan, jumlah in penyebab.most_common():
        print(f"- {alasan}: {jumlah}")

    if penyebab:
        alasan_terbesar, jumlah_terbesar = penyebab.most_common(1)[0]
        print(
            f"\nPenyebab penolakan terbanyak: {alasan_terbesar} "
            f"({jumlah_terbesar} kemunculan)"
        )

else:
    print(f"File rejected tidak ditemukan: {REJECTED_PATH}")
    print("Periksa kembali lokasi file rejected_orders.jsonl.")

con.close()
