
import json
from datetime import datetime
from pathlib import Path
from typing import Literal

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import BaseModel, ConfigDict, Field, ValidationError


# ============================================================
# KONFIGURASI
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "data" / "streaming"

VALID_PATH = OUTPUT_DIR / "valid_orders.jsonl"
REJECTED_PATH = OUTPUT_DIR / "rejected_orders.jsonl"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = "orders-consumer-v1"


# ============================================================
# KONTRAK DATA - SELARAS DENGAN TAHAP 2
# ============================================================

class OrderContract(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime

    product_category: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)

    payment_method: str
    status: Literal["paid", "shipped", "cancelled", "refunded"]


# ============================================================
# UTILITAS FILE JSONL
# ============================================================

def read_jsonl(path: Path) -> list[dict]:
    """Membaca file JSONL; file belum ada dianggap kosong."""
    if not path.exists():
        return []

    records = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                records.append(json.loads(line))

    return records


def write_jsonl_atomic(path: Path, records: list[dict]) -> None:
    """Menulis file sementara lalu mengganti file tujuan."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")

    with temp_path.open("w", encoding="utf-8", newline="\n") as file:
        for record in records:
            file.write(
                json.dumps(record, ensure_ascii=False, default=str) + "\n"
            )
        file.flush()

    temp_path.replace(path)


# ============================================================
# SINK VALID: UPSERT BERDASARKAN order_id
# ============================================================

def save_valid(order: dict, kafka_metadata: dict) -> str:
    records = read_jsonl(VALID_PATH)

    # Metadata Kafka tidak dihitung sebagai isi bisnis pesanan.
    new_business_data = order
    existing_index = None

    for index, record in enumerate(records):
        if record.get("order_id") == order["order_id"]:
            existing_index = index
            break

    if existing_index is not None:
        existing = records[existing_index]
        old_business_data = {
            key: value for key, value in existing.items()
            if key != "_kafka"
        }

        if old_business_data == new_business_data:
            # Event bisnis sama: tidak menulis duplikat baru.
            return "DUPLICATE_IGNORED"

        # ID sama tetapi data bisnis berubah: lakukan upsert.
        records[existing_index] = {
            **new_business_data,
            "_kafka": kafka_metadata,
        }
        write_jsonl_atomic(VALID_PATH, records)
        return "UPDATED"

    records.append({
        **new_business_data,
        "_kafka": kafka_metadata,
    })
    write_jsonl_atomic(VALID_PATH, records)
    return "INSERTED"


# ============================================================
# SINK REJECTED: SIMPAN ERROR DAN PAYLOAD ASLI
# ============================================================

def save_rejected(
    event_id: str,
    order_id: str | None,
    original_payload,
    raw_payload_text: str,
    errors: list[dict],
    kafka_metadata: dict,
) -> str:
    records = read_jsonl(REJECTED_PATH)

    rejected_record = {
        "_event_id": event_id,
        "order_id": order_id,
        "original_payload": original_payload,
        "raw_payload_text": raw_payload_text,
        "errors": errors,
        "_kafka": kafka_metadata,
    }

    for index, record in enumerate(records):
        if record.get("_event_id") == event_id:
            # Replay pesan Kafka yang sama tidak menambah baris baru.
            records[index] = rejected_record
            write_jsonl_atomic(REJECTED_PATH, records)
            return "REJECTED_REPLAY"

    records.append(rejected_record)
    write_jsonl_atomic(REJECTED_PATH, records)
    return "REJECTED_SAVED"


def validation_errors(exc: ValidationError) -> list[dict]:
    return [
        {
            "field": ".".join(str(part) for part in error["loc"]),
            "message": error["msg"],
            "type": error["type"],
        }
        for error in exc.errors()
    ]


# ============================================================
# CONSUMER
# ============================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    print(f"Consumer aktif untuk topic={TOPIC}")
    print(f"Broker={BOOTSTRAP_SERVERS}, group_id={GROUP_ID}")
    print(f"File valid   : {VALID_PATH}")
    print(f"File rejected: {REJECTED_PATH}")
    print("Menunggu pesan... Tekan Ctrl+C untuk berhenti.")

    try:
        for message in consumer:
            raw_bytes = message.value
            raw_text = (
                raw_bytes.decode("utf-8", errors="replace")
                if raw_bytes is not None else ""
            )

            event_id = (
                f"{message.topic}:{message.partition}:{message.offset}"
            )
            key = (
                message.key.decode("utf-8", errors="replace")
                if message.key is not None else None
            )

            metadata = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "key": key,
            }

            payload = None
            order_id = key

            try:
                payload = json.loads(raw_text)

                if isinstance(payload, dict):
                    candidate_id = payload.get("order_id")
                    if candidate_id is not None:
                        order_id = str(candidate_id)

                validated_order = OrderContract.model_validate(payload)
                order = validated_order.model_dump(mode="json")

                outcome = save_valid(order, metadata)

                print(
                    f"VALID outcome={outcome} "
                    f"order_id={order['order_id']} "
                    f"topic={message.topic} "
                    f"partition={message.partition} "
                    f"offset={message.offset}"
                )

            except json.JSONDecodeError as exc:
                outcome = save_rejected(
                    event_id=event_id,
                    order_id=order_id,
                    original_payload=raw_text,
                    raw_payload_text=raw_text,
                    errors=[{
                        "field": "$",
                        "message": f"Malformed JSON: {exc.msg}",
                        "type": "json_decode_error",
                    }],
                    kafka_metadata=metadata,
                )

                print(
                    f"INVALID outcome={outcome} "
                    f"topic={message.topic} "
                    f"partition={message.partition} "
                    f"offset={message.offset} "
                    f"reason=malformed_json"
                )

            except ValidationError as exc:
                outcome = save_rejected(
                    event_id=event_id,
                    order_id=order_id,
                    original_payload=payload,
                    raw_payload_text=raw_text,
                    errors=validation_errors(exc),
                    kafka_metadata=metadata,
                )

                print(
                    f"INVALID outcome={outcome} "
                    f"order_id={order_id} "
                    f"topic={message.topic} "
                    f"partition={message.partition} "
                    f"offset={message.offset} "
                    f"errors={validation_errors(exc)}"
                )

            # Commit hanya setelah hasil pesan selesai ditulis ke sink.
            # Offset yang di-commit adalah posisi setelah pesan ini.
            tp = TopicPartition(message.topic, message.partition)
            consumer.commit({
                tp: OffsetAndMetadata(message.offset + 1, "")
            })

    except KeyboardInterrupt:
        print("\nConsumer dihentikan oleh pengguna.")

    finally:
        consumer.close()
        print("Consumer ditutup.")


if __name__ == "__main__":
    main()
