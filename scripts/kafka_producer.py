
import argparse
import csv
import json
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

PROJECT_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = PROJECT_DIR / "data" / "orders.csv"
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"


def main():
    parser = argparse.ArgumentParser(
        description="Mengirim event pesanan dari CSV ke Kafka."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=36,
        help="Jumlah baris CSV yang dikirim (default: 36).",
    )
    parser.add_argument(
        "--duplicate-first",
        action="store_true",
        help="Kirim ulang event pertama untuk demonstrasi duplikasi.",
    )
    args = parser.parse_args()

    if args.limit < 1:
        parser.error("--limit harus minimal 1")

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))

    if not rows:
        raise SystemExit("CSV tidak berisi data.")

    selected_rows = rows[:args.limit]

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent = 0

    try:
        for row in selected_rows:
            order_id = row.get("order_id", "")
            if not order_id:
                print("Peringatan: event tanpa order_id akan memakai key kosong.")

            future = producer.send(
                TOPIC,
                key=order_id,
                value=row,
            )
            metadata = future.get(timeout=30)
            sent += 1

            print(
                f"SENT order_id={order_id} "
                f"topic={metadata.topic} "
                f"partition={metadata.partition} "
                f"offset={metadata.offset}"
            )

        if args.duplicate_first:
            row = selected_rows[0]
            order_id = row.get("order_id", "")

            metadata = producer.send(
                TOPIC,
                key=order_id,
                value=row,
            ).get(timeout=30)
            sent += 1

            print(
                f"DUPLICATE_SENT order_id={order_id} "
                f"topic={metadata.topic} "
                f"partition={metadata.partition} "
                f"offset={metadata.offset}"
            )

        producer.flush()
        print(f"Selesai. Total event terkirim: {sent}")

    except KafkaError as exc:
        raise SystemExit(f"Gagal mengirim event Kafka: {exc}") from exc
    finally:
        producer.close()


if __name__ == "__main__":
    main()
