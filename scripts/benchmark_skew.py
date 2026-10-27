
from pathlib import Path
from time import perf_counter
import platform
import statistics

import pandas as pd
import polars as pl


# ==================================================
# 1. KONFIGURASI
# ==================================================
ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "raw" / "orders.parquet"
OUTPUT_DIR = ROOT / "data" / "benchmark"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPEATS_DATA = 100
SKEW_CUSTOMER = "C_SKEW_DOMINANT"
BENCHMARK_RUNS = 10


# ==================================================
# 2. BACA DATA DAN BUAT DATASET NORMAL + SKEWED
# ==================================================
base_df = pd.read_parquet(SOURCE)

normal_parts = []
skewed_parts = []

for i in range(REPEATS_DATA):
    # Salinan untuk dataset normal
    normal_part = base_df.copy()
    normal_part["order_id"] = (
        normal_part["order_id"].astype(str) + f"_R{i + 1:03d}"
    )
    normal_parts.append(normal_part)

    # Salinan untuk dataset skewed
    skewed_part = base_df.copy()
    skewed_part["order_id"] = (
        skewed_part["order_id"].astype(str) + f"_R{i + 1:03d}"
    )

    # 99 dari 100 salinan menggunakan customer_id yang sama.
    # Salinan terakhir mempertahankan customer_id asli.
    if i < REPEATS_DATA - 1:
        skewed_part["customer_id"] = SKEW_CUSTOMER

    skewed_parts.append(skewed_part)

normal_df = pd.concat(normal_parts, ignore_index=True)
skewed_df = pd.concat(skewed_parts, ignore_index=True)

normal_path = OUTPUT_DIR / "orders_expanded_normal.parquet"
skewed_path = OUTPUT_DIR / "orders_expanded_skewed.parquet"

normal_df.to_parquet(normal_path, index=False)
skewed_df.to_parquet(skewed_path, index=False)

# Kedua dataset harus memiliki jumlah baris yang sama.
assert len(normal_df) == len(skewed_df)

# Hitung nilai net_amount untuk eksperimen agregasi.
for df in (normal_df, skewed_df):
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])


# ==================================================
# 3. ANALISIS DISTRIBUSI CUSTOMER_ID
# ==================================================
distribution_parts = []

for dataset_name, df in [
    ("normal", normal_df),
    ("skewed", skewed_df),
]:
    counts = (
        df.groupby("customer_id", dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values("row_count", ascending=False)
    )

    counts["dataset"] = dataset_name
    counts["percentage"] = counts["row_count"] / len(df) * 100

    distribution_parts.append(counts)

distribution = pd.concat(distribution_parts, ignore_index=True)

distribution_path = OUTPUT_DIR / "skew_customer_distribution.csv"
distribution.to_csv(distribution_path, index=False)


# ==================================================
# 4. FUNGSI AGREGASI
# ==================================================
def pandas_aggregate(df):
    return (
        df.groupby("customer_id", dropna=False)
        .agg(
            order_count=("order_id", "count"),
            total_net_amount=("net_amount", "sum"),
        )
        .reset_index()
    )


def polars_eager_aggregate(df):
    return df.group_by("customer_id").agg(
        pl.len().alias("order_count"),
        pl.col("net_amount").sum().alias("total_net_amount"),
    )


def polars_lazy_aggregate(df):
    return (
        df.lazy()
        .group_by("customer_id")
        .agg(
            pl.len().alias("order_count"),
            pl.col("net_amount").sum().alias("total_net_amount"),
        )
        .collect()
    )


# ==================================================
# 5. UKUR WAKTU AGREGASI
# ==================================================
benchmark_results = []

datasets = {
    "normal": normal_df,
    "skewed": skewed_df,
}

for dataset_name, pandas_df in datasets.items():
    polars_df = pl.from_pandas(pandas_df)

    # Ukuran memori perkiraan dari dataframe di memori.
    memory_sizes = {
        "Pandas": int(pandas_df.memory_usage(deep=True).sum()),
        "Polars eager/lazy": int(polars_df.estimated_size()),
    }

    engines = {
        "Pandas": lambda: pandas_aggregate(pandas_df),
        "Polars eager": lambda: polars_eager_aggregate(polars_df),
        "Polars lazy": lambda: polars_lazy_aggregate(polars_df),
    }

    for engine_name, aggregate_func in engines.items():
        # Warm-up
        aggregate_func()

        timings_ms = []

        for _ in range(BENCHMARK_RUNS):
            start = perf_counter()
            aggregate_func()
            elapsed_ms = (perf_counter() - start) * 1000
            timings_ms.append(elapsed_ms)

        memory_key = (
            "Pandas" if engine_name == "Pandas"
            else "Polars eager/lazy"
        )

        benchmark_results.append({
            "dataset": dataset_name,
            "engine": engine_name,
            "rows": len(pandas_df),
            "customer_groups": pandas_df["customer_id"].nunique(
                dropna=False
            ),
            "median_ms": statistics.median(timings_ms),
            "mean_ms": statistics.mean(timings_ms),
            "estimated_memory_bytes": memory_sizes[memory_key],
            "runs": BENCHMARK_RUNS,
        })


# ==================================================
# 6. SIMPAN DAN TAMPILKAN HASIL
# ==================================================
results_df = pd.DataFrame(benchmark_results)
results_path = OUTPUT_DIR / "skew_benchmark_results.csv"
results_df.to_csv(results_path, index=False)

print("=" * 65)
print("DATA SKEW EXPERIMENT")
print("=" * 65)
print("Python:", platform.python_version())
print("Pandas:", pd.__version__)
print("Polars:", pl.__version__)
print("Dataset sumber:", SOURCE)
print("Baris dataset normal:", len(normal_df))
print("Baris dataset skewed:", len(skewed_df))

print("\nDistribusi customer_id (10 teratas per dataset):")
print(
    distribution.groupby("dataset", group_keys=False)
    .head(10)[["dataset", "customer_id", "row_count", "percentage"]]
    .to_string(index=False)
)

print("\nHasil benchmark:")
print(
    results_df[
        [
            "dataset",
            "engine",
            "rows",
            "customer_groups",
            "median_ms",
            "mean_ms",
            "estimated_memory_bytes",
        ]
    ].to_string(index=False)
)

print("\nFile hasil:")
print("Normal dataset :", normal_path)
print("Skewed dataset :", skewed_path)
print("Distribusi     :", distribution_path)
print("Benchmark      :", results_path)
