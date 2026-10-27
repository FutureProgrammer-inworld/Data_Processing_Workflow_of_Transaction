
from pathlib import Path
import duckdb

PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_DIR / "analytics.duckdb"
OUTPUT_PATH = PROJECT_DIR / "data" / "processed" / "daily_sales.csv"

query = """
SELECT
    CAST(order_ts AS DATE) AS order_date,
    product_category,
    SUM(quantity * unit_price * (1 - discount_pct)) AS net_sales
FROM raw.raw_orders
WHERE status IN ('paid', 'shipped')
GROUP BY 1, 2
ORDER BY 1, 2;
"""

def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        result = con.execute(query)
        df = result.fetchdf()
        df.to_csv(OUTPUT_PATH, index=False)

        print("=== Daily Sales Analysis ===")
        print(df.to_string(index=False))
        print(f"\nJumlah baris hasil: {len(df)}")
        print(f"File tersimpan: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
