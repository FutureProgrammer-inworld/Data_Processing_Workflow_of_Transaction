
import csv
import math
import os
import platform
import statistics
import time
from pathlib import Path

import pandas as pd
import polars as pl


# Lokasi proyek dan dataset
ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "raw" / "orders.parquet"
OUTPUT_PATH = ROOT / "data" / "benchmark" / "benchmark_results.csv"

# Jumlah pengulangan pengukuran, setelah warm-up
REPEATS = 10


def benchmark_pandas():
    start = time.perf_counter()

    df = pd.read_parquet(DATA_PATH)

    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = (
        df["gross_amount"] * (1 - df["discount_pct"])
    )

    result = (
        df.groupby("product_category", dropna=False)["net_amount"]
        .sum()
        .reset_index()
    )

    elapsed = time.perf_counter() - start
    return elapsed, result


def benchmark_polars_eager():
    start = time.perf_counter()

    df = pl.read_parquet(DATA_PATH)

    df = df.with_columns(
        (pl.col("quantity") * pl.col("unit_price"))
        .alias("gross_amount"),
        (
            pl.col("quantity")
            * pl.col("unit_price")
            * (1 - pl.col("discount_pct"))
        ).alias("net_amount"),
    )

    result = (
        df.group_by("product_category")
        .agg(pl.col("net_amount").sum())
    )

    elapsed = time.perf_counter() - start
    return elapsed, result


def benchmark_polars_lazy():
    start = time.perf_counter()

    result = (
        pl.scan_parquet(str(DATA_PATH))
        .with_columns(
            (pl.col("quantity") * pl.col("unit_price"))
            .alias("gross_amount"),
            (
                pl.col("quantity")
                * pl.col("unit_price")
                * (1 - pl.col("discount_pct"))
            ).alias("net_amount"),
        )
        .group_by("product_category")
        .agg(pl.col("net_amount").sum())
        .collect()
    )

    elapsed = time.perf_counter() - start
    return elapsed, result


def normalize_result(result, engine):
    """Ubah hasil agregasi menjadi dictionary untuk pemeriksaan."""
    if engine == "pandas":
        records = result.to_dict("records")
    else:
        records = result.to_dicts()

    normalized = {}

    for row in records:
        category = row["product_category"]
        key = "<NULL>" if category is None or pd.isna(category) else str(category)
        value = float(row["net_amount"])
        normalized[key] = value

    return normalized


def check_results(results):
    """Pastikan semua engine menghasilkan agregasi yang setara."""
    reference = normalize_result(results["Pandas"], "pandas")

    for engine in ("Polars eager", "Polars lazy"):
        current = normalize_result(results[engine], "polars")

        if reference.keys() != current.keys():
            raise AssertionError(
                f"Kategori hasil {engine} berbeda dari Pandas."
            )

        for key in reference:
            if not math.isclose(
                reference[key],
                current[key],
                rel_tol=1e-9,
                abs_tol=1e-8,
            ):
                raise AssertionError(
                    f"Nilai kategori {key} berbeda pada {engine}: "
                    f"{reference[key]} vs {current[key]}"
                )

    print("Pemeriksaan hasil: ketiga engine menghasilkan agregasi setara.")


def main():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset tidak ditemukan: {DATA_PATH}")

    # Informasi dataset dan lingkungan
    df_info = pd.read_parquet(DATA_PATH)

    print("=== INFORMASI BENCHMARK ===")
    print(f"Dataset       : {DATA_PATH.relative_to(ROOT)}")
    print(f"Jumlah baris  : {len(df_info)}")
    print(f"Jumlah kolom  : {len(df_info.columns)}")
    print(f"Ukuran file   : {os.path.getsize(DATA_PATH):,} bytes")
    print(f"Python        : {platform.python_version()}")
    print(f"Platform      : {platform.platform()}")
    print(f"Pandas        : {pd.__version__}")
    print(f"Polars        : {pl.__version__}")
    print(f"Pengulangan   : {REPEATS}")
    print("Catatan       : waktu mencakup pembacaan Parquet dan transformasi.")

    engines = {
        "Pandas": benchmark_pandas,
        "Polars eager": benchmark_polars_eager,
        "Polars lazy": benchmark_polars_lazy,
    }

    # Warm-up: menjalankan setiap engine sekali sebelum pengukuran
    print("\n=== WARM-UP ===")
    warmup_results = {}

    for name, func in engines.items():
        _, result = func()
        warmup_results[name] = result
        print(f"{name}: selesai")

    check_results(warmup_results)

    # Pengukuran berulang
    timings = {name: [] for name in engines}
    latest_results = {}

    print("\n=== PENGUKURAN ===")

    for iteration in range(REPEATS):
        print(f"Pengulangan {iteration + 1}/{REPEATS}")

        # Jalankan dalam urutan yang sama pada setiap putaran
        for name, func in engines.items():
            elapsed, result = func()
            timings[name].append(elapsed)
            latest_results[name] = result

    check_results(latest_results)

    # Ringkasan statistik
    rows = []

    print("\n=== HASIL BENCHMARK ===")
    print(f"{'Engine':<16} {'Median (ms)':>14} {'Rata-rata (ms)':>17}")

    for name, values in timings.items():
        median_ms = statistics.median(values) * 1000
        mean_ms = statistics.mean(values) * 1000

        print(f"{name:<16} {median_ms:>14.3f} {mean_ms:>17.3f}")

        rows.append({
            "engine": name,
            "dataset": str(DATA_PATH.relative_to(ROOT)),
            "rows": len(df_info),
            "columns": len(df_info.columns),
            "file_size_bytes": os.path.getsize(DATA_PATH),
            "repeats": REPEATS,
            "median_ms": round(median_ms, 6),
            "mean_ms": round(mean_ms, 6),
        })

    # Simpan hasil agar dapat dipakai dalam laporan
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nHasil tersimpan di: {OUTPUT_PATH.relative_to(ROOT)}")
    print("Catatan: hasil berlaku untuk lingkungan dan ukuran dataset ini.")


if __name__ == "__main__":
    main()
