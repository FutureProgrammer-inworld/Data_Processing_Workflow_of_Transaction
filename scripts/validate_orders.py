
"""Validate raw orders using a Pydantic data contract."""
import json
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class OrderContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    order_id: str
    customer_id: str
    order_ts: datetime
    quantity: int = Field(gt=0, strict=True)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]

    @field_validator("order_id", "customer_id")
    @classmethod
    def require_nonempty_string(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Field must not be empty")
        return value


def main():
    source = Path("data/raw/orders.parquet")
    valid_path = Path("data/processed/valid_orders.parquet")
    rejected_path = Path("data/rejected/rejected_orders.jsonl")
    summary_path = Path("data/processed/validation_summary.json")

    valid_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(source)

    # Convert records to JSON-compatible values.
    # Invalid timestamps such as NaT become null and fail validation.
    records = json.loads(
        df.to_json(orient="records", date_format="iso")
    )

    valid_orders = []
    rejected_orders = []

    for record in records:
        try:
            order = OrderContract.model_validate(record)
            valid_orders.append(order.model_dump(mode="json"))

        except ValidationError as exc:
            errors = [
                {
                    "field": ".".join(str(part) for part in error["loc"]),
                    "message": error["msg"],
                    "type": error["type"],
                }
                for error in exc.errors()
            ]

            rejected_orders.append(
                {
                    "order_id": record.get("order_id"),
                    "payload": record,
                    "errors": errors,
                }
            )

    # Save valid records in Parquet.
    valid_df = pd.DataFrame(valid_orders)
    if valid_df.empty:
        valid_df = pd.DataFrame(columns=df.columns)

    valid_df.to_parquet(valid_path, index=False)

    # Save rejected records as JSON Lines, including original payload
    # and actionable validation messages.
    with rejected_path.open("w", encoding="utf-8") as file:
        for rejected in rejected_orders:
            file.write(
                json.dumps(rejected, ensure_ascii=False) + "\n"
            )

    summary = {
        "source": str(source),
        "total_records": len(records),
        "valid_records": len(valid_orders),
        "rejected_records": len(rejected_orders),
        "warehouse_eligible_records": len(valid_orders),
        "valid_output": str(valid_path),
        "rejected_output": str(rejected_path),
        "validation_mode": "per-record quarantine",
    }

    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print("\n=== DATA CONTRACT SUMMARY ===")
    print(f"total_records={len(records)}")
    print(f"valid_records={len(valid_orders)}")
    print(f"rejected_records={len(rejected_orders)}")
    print(f"valid_output={valid_path}")
    print(f"rejected_output={rejected_path}")
    print(f"summary_output={summary_path}")

    if rejected_orders:
        print("\n=== REJECTION DETAILS ===")
        for rejected in rejected_orders:
            print(f"order_id={rejected['order_id']}")
            for error in rejected["errors"]:
                print(
                    f"  field={error['field']}; "
                    f"reason={error['message']}"
                )


if __name__ == "__main__":
    main()
